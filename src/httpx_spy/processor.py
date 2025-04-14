import abc
import asyncio
import logging
import threading
from asyncio import CancelledError
from base64 import b64encode
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from functools import wraps
from ipaddress import IPv4Address, IPv6Address, ip_address
from typing import Any, ClassVar, Literal

import httpx
import orjson
from asgiref.sync import sync_to_async

from httpx_spy.monkey import MonkeyClient
from httpx_spy.uniqueue import QueueEmpty, UniQueue

logger = logging.getLogger(__name__)

Json = Mapping[str, "Json"] | Sequence["Json"] | str | int | float | bool | None


@dataclass
class RequestEntry:
    """Serialization of a HTTP request"""

    time: datetime
    uri: httpx.URL
    verb: str
    headers: Sequence[tuple[str, str]]
    body: str
    transfer_encoding: Literal["json", "base64"]


@dataclass
class ResponseEntry:
    """Serialization of a HTTP response"""

    time: datetime
    status: int
    ip_address: IPv4Address | IPv6Address | None
    headers: Sequence[tuple[str, str]]
    body: str
    transfer_encoding: Literal["json", "base64"]


@dataclass
class SpanEntry:
    """Serialization of the current OpenTelemetry span"""

    id: str
    parent_id: str | None = None
    links: Sequence[str] = field(default_factory=list)
    status: int | None = None


@dataclass
class MetaEntry:
    """Additional metadata to be associated with the request"""

    direction: Literal["incoming", "outgoing", "internal"] = "outgoing"
    metadata: Json = None


@dataclass
class CallerEntry:
    """Information about the caller of the request"""

    user_id: str | None = None
    company_id: str | None = None
    subscription_id: str | None = None


@dataclass
class Entry:
    """An entry to send to the handler"""

    request: RequestEntry
    response: ResponseEntry | None
    span: SpanEntry | None
    meta: MetaEntry
    caller: CallerEntry


@dataclass
class EntryToProcess:
    """
    A pre-serialized entry
    """

    request: httpx.Request
    request_time: datetime
    caller: CallerEntry
    metadata: Mapping[str, Any]


@dataclass
class ResponseToProcess:
    """
    A pre-serialized response
    """

    response: httpx.Response
    response_time: datetime


@dataclass
class Handler(abc.ABC):
    """
    Handlers are responsible for sending the entries they receive and send them
    to the appropriate reporting API.
    """

    processor: "Processor | None" = field(init=False, repr=False)

    @abc.abstractmethod
    async def handle(self, requests: Sequence[Entry]) -> None:
        """
        This will receive mostly serialized entries that you can send alone or
        in batches to the reporting API.

        Parameters
        ----------
        requests
            Request entries to send to the reporting API
        """

        raise NotImplementedError


@dataclass
class Processor:
    """
    This is the core of the system, which hooks into httpx and processes all
    the requests into intermediate representations ready to be handled by the
    appropriate classes.
    """

    FLUSH_INTERVAL: ClassVar[timedelta] = timedelta(seconds=1)
    QUEUE_MAX_SIZE: ClassVar[int] = 10_000

    handlers: list[Handler] = field(default_factory=list, init=False)
    metadata_extractors: dict[str, Callable[[httpx.Request], Mapping[str, str]]] = (
        field(default_factory=dict, init=False)
    )
    caller_extractor: Callable[[httpx.Request], CallerEntry] | None = None
    sync_queue: UniQueue = field(
        init=False,
        repr=False,
        default_factory=lambda: UniQueue(max_size=Processor.QUEUE_MAX_SIZE),
    )
    async_queue: UniQueue = field(
        init=False,
        repr=False,
        default_factory=lambda: UniQueue(max_size=Processor.QUEUE_MAX_SIZE),
    )
    pending_requests: dict[int, Entry] = field(init=False, default_factory=dict)
    run_t: asyncio.Task | None = field(init=False, default=None)

    flush_interval: timedelta = field(default_factory=lambda: Processor.FLUSH_INTERVAL)

    def add_handler(self, handler: Handler) -> None:
        """
        Registers a handler that will be used to send the entries to a set of
        reporting APIs which allow us to keep track of the requests that we are
        sending.

        Parameters
        ----------
        handler
            Handler to register
        """

        handler.processor = self
        self.handlers.append(handler)

    def set_caller_extractor(
        self, extractor: Callable[[httpx.Request], CallerEntry]
    ) -> None:
        """
        The caller extractor is here to guess who is doing the request at the
        moment from a business point of view. Typically it might want to dig
        up the Django user ID, for example.
        """

        self.caller_extractor = extractor

    def add_metadata_extractor(
        self, name: str, extractor: Callable[[httpx.Request], Mapping[str, str]]
    ) -> None:
        """
        This gives you an opportunity to add arbitrary metadata to each entry
        that will be sent. It's a simple callable function that will receive
        the request you are sending (when it is not sent yet).

        Parameters
        ----------
        name
            Name of the metadata extractor (will be the key in the metadata
            dictionary)
        extractor
            Extractor function
        """

        self.metadata_extractors[name] = extractor

    async def empty_queue(self, q: UniQueue) -> list[EntryToProcess]:
        """
        Returns all the things currently in the queue
        """

        out = []

        try:
            while True:
                out.append(await q.async_get_nowait())
        except QueueEmpty:
            pass

        return out

    @sync_to_async
    def sync_unstack_events(
        self, batch: list[EntryToProcess | ResponseToProcess]
    ) -> None:
        """
        Unstack request/response events
        """

        for entry in batch:
            match entry:
                case EntryToProcess(
                    request=request,
                    request_time=request_time,
                    caller=caller,
                    metadata=metadata,
                ):
                    self.pending_requests[id(request)] = self.sync_serialize_req(
                        request=request,
                        request_time=request_time,
                        caller=caller,
                        metadata=metadata,
                    )
                case ResponseToProcess(
                    response=response,
                    response_time=response_time,
                ):
                    req_id = id(response.request)

                    if req_id not in self.pending_requests:
                        continue

                    self.pending_requests[req_id].response = self.sync_serialize_resp(
                        response=response,
                        response_time=response_time,
                    )

    async def async_unstack_events(
        self, batch: list[EntryToProcess | ResponseToProcess]
    ) -> None:
        """
        Unstack request/response events
        """

        for entry in batch:
            match entry:
                case EntryToProcess(
                    request=request,
                    request_time=request_time,
                    caller=caller,
                    metadata=metadata,
                ):
                    self.pending_requests[id(request)] = await self.async_serialize_req(
                        request=request,
                        request_time=request_time,
                        caller=caller,
                        metadata=metadata,
                    )
                case ResponseToProcess(
                    response=response,
                    response_time=response_time,
                ):
                    req_id = id(response.request)

                    if req_id not in self.pending_requests:
                        continue

                    self.pending_requests[
                        req_id
                    ].response = await self.async_serialize_resp(
                        response=response,
                        response_time=response_time,
                    )

    async def flush_todo(self) -> None:
        """
        Flushes all eligible pending events into the handlers

        - If they got a response
        - If they did not receive a response for some time
        """

        to_delete = []
        to_handle = []

        for req_id, entry in self.pending_requests.items():
            if entry.response is not None or entry.request.time + timedelta(
                minutes=30
            ) < datetime.now(UTC):
                to_handle.append(entry)
                to_delete.append(req_id)

        for i in to_delete:
            del self.pending_requests[i]

        for handler in self.handlers:
            # noinspection PyBroadException
            try:
                await handler.handle(to_handle)
            except CancelledError:
                raise
            except Exception:
                logger.exception("Could not handle request")

    async def run(self) -> None:
        """
        Regularly polls the requests queue and process them into the handlers
        """

        while True:
            # noinspection PyBroadException
            try:
                await asyncio.sleep(self.flush_interval.total_seconds())

                async_todo = await self.empty_queue(self.async_queue)
                sync_todo = await self.empty_queue(self.sync_queue)

                await self.sync_unstack_events(sync_todo)
                await self.async_unstack_events(async_todo)

                await self.flush_todo()
            except CancelledError:
                raise
            except Exception:
                logger.exception("Something weird happened")

    def start(self) -> None:
        """
        Starts the processor in its own thread
        """

        def _run():
            try:
                loop.run_until_complete(self.run_t)
            except CancelledError:
                pass

        self.monkey_patch()
        loop = asyncio.new_event_loop()
        self.run_t = loop.create_task(self.run())
        thread = threading.Thread(target=_run, daemon=True, name="HttpxSpy")
        thread.start()

    def stop(self) -> None:
        """
        Stops the processing thread
        """

        if self.run_t:
            self.run_t.cancel()
            self.run_t = None

    def monkey_patch(self) -> None:
        """
        Goes on a monkey patches httpx
        """

        httpx.Client.__bases__ = (MonkeyClient,)
        httpx.AsyncClient.__bases__ = (MonkeyClient,)

    @wraps(httpx.AsyncClient)
    def get_client(self, *args, **kwargs) -> httpx.AsyncClient:
        """
        Returns the original, non-monkey-patched HTTPX client class.
        """

        client = httpx.AsyncClient(*args, **kwargs)
        client._no_monkey = True

        return client

    def guess_encoding(
        self, content: bytes
    ) -> tuple[str | Any, Literal["json", "base64"]]:
        """
        For the given content, tries to see if it's valid JSON, otherwise just
        return it as base64.

        Parameters
        ----------
        content
            Content to guess the encoding of
        """

        try:
            orjson.loads(content)
        except orjson.JSONDecodeError:
            return b64encode(content).decode(), "base64"
        else:
            return content.decode(), "json"

    def get_metadata(self, request: httpx.Request) -> Mapping[str, Any]:
        """
        Extracts the metadata from the request and returns it as a dictionary.
        It's important to run this from the thread/call stack of the hook so
        that if the metadata extractor wants to dig into the call stack it can.
        """

        return {k: v(request) for k, v in self.metadata_extractors.items()}

    def get_caller(self, request: httpx.Request) -> CallerEntry:
        """
        Extracts the caller information from the request and returns it as a
        CallerEntry object.
        """

        if self.caller_extractor:
            return self.caller_extractor(request)
        else:
            return CallerEntry()

    def sync_handle_request(self, request: httpx.Request) -> None:
        """
        Synchronous version of the httpx hook.
        """

        logger.debug("Spied on request: %s %s", request.method, request.url)

        metadata = self.get_metadata(request)
        caller = self.get_caller(request)
        request_time = datetime.now(tz=UTC)

        self.sync_queue.sync_put(
            EntryToProcess(
                request=request,
                request_time=request_time,
                caller=caller,
                metadata=metadata,
            )
        )

    def sync_handle_response(self, response: httpx.Response) -> None:
        """
        Synchronous version of the httpx hook.
        """

        logger.debug(
            "Spied on response: %s %s %s",
            response.status_code,
            response.request.method,
            response.request.url,
        )

        response_time = datetime.now(tz=UTC)

        self.sync_queue.sync_put(
            ResponseToProcess(
                response=response,
                response_time=response_time,
            )
        )

    def sync_serialize_req(
        self,
        request: httpx.Request,
        request_time: datetime,
        caller: CallerEntry,
        metadata: Mapping[str, Any],
    ) -> Entry:
        """
        Synchronously serializes the request into an entry
        """

        request.read()
        req_content, req_encoding = self.guess_encoding(request.content)

        req = RequestEntry(
            time=request_time,
            uri=request.url,
            verb=request.method,
            headers=request.headers.multi_items(),
            body=req_content,
            transfer_encoding=req_encoding,
        )

        meta = MetaEntry(
            direction="outgoing",
            metadata=metadata,
        )

        return Entry(
            request=req,
            response=None,
            meta=meta,
            span=None,
            caller=caller,
        )

    def sync_serialize_resp(
        self,
        response: httpx.Response,
        response_time: datetime,
    ) -> ResponseEntry:
        """
        Synchronously serializes the response into an entry
        """

        response.read()
        res_content, res_encoding = self.guess_encoding(response.content)

        try:
            server_ip_str, _ = response.extensions["network_stream"].get_extra_info(
                "server_addr"
            )
            server_ip = ip_address(server_ip_str)
        except (OSError, KeyError):
            server_ip = None

        return ResponseEntry(
            time=response_time,
            status=response.status_code,
            ip_address=server_ip,
            headers=response.headers.multi_items(),
            body=res_content,
            transfer_encoding=res_encoding,
        )

    async def async_handle_request(self, request: httpx.Request) -> None:
        """
        Synchronous version of the httpx hook.
        """

        logger.debug("Spied on request: %s %s", request.method, request.url)

        metadata = self.get_metadata(request)
        caller = self.get_caller(request)
        request_time = datetime.now(tz=UTC)

        await self.async_queue.async_put(
            EntryToProcess(
                request=request,
                request_time=request_time,
                caller=caller,
                metadata=metadata,
            )
        )

    async def async_handle_response(self, response: httpx.Response) -> None:
        """
        Synchronous version of the httpx hook.
        """

        logger.debug(
            "Spied on response: %s %s %s",
            response.status_code,
            response.request.method,
            response.request.url,
        )

        response_time = datetime.now(tz=UTC)

        await self.async_queue.async_put(
            ResponseToProcess(
                response=response,
                response_time=response_time,
            )
        )

    async def async_serialize_req(
        self,
        request: httpx.Request,
        request_time: datetime,
        caller: CallerEntry,
        metadata: Mapping[str, Any],
    ) -> Entry:
        """
        Synchronously serializes the request into an entry
        """

        await request.aread()
        req_content, req_encoding = self.guess_encoding(request.content)

        req = RequestEntry(
            time=request_time,
            uri=request.url,
            verb=request.method,
            headers=request.headers.multi_items(),
            body=req_content,
            transfer_encoding=req_encoding,
        )

        meta = MetaEntry(
            direction="outgoing",
            metadata=metadata,
        )

        return Entry(
            request=req,
            response=None,
            meta=meta,
            span=None,
            caller=caller,
        )

    async def async_serialize_resp(
        self,
        response: httpx.Response,
        response_time: datetime,
    ) -> ResponseEntry:
        """
        Synchronously serializes the response into an entry
        """

        await response.aread()
        res_content, res_encoding = self.guess_encoding(response.content)

        try:
            server_ip_str, _ = response.extensions["network_stream"].get_extra_info(
                "server_addr"
            )
            server_ip = ip_address(server_ip_str)
        except (OSError, KeyError):
            server_ip = None

        return ResponseEntry(
            time=response_time,
            status=response.status_code,
            ip_address=server_ip,
            headers=response.headers.multi_items(),
            body=res_content,
            transfer_encoding=res_encoding,
        )


_processor = None
_processor_lock = threading.Lock()


def get_processor() -> Processor:
    """Access the Processor singleton"""

    global _processor

    if _processor is None:
        with _processor_lock:
            if _processor is None:
                _processor = Processor()

    return _processor
