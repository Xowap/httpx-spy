"""
Elements of the Moesif integration
"""

import logging
from collections import defaultdict
from collections.abc import Iterator, Sequence
from dataclasses import dataclass, field
from typing import ClassVar

import httpx
import orjson

from .processor import Entry, Handler

logger = logging.getLogger(__name__)


@dataclass
class MoesifHandler(Handler):
    """
    Sends the entries to Moesif
    """

    MAX_BATCH_SIZE: ClassVar[int] = 49 * (1024**2)

    app_id: str
    base_url: str | httpx.URL = field(
        default_factory=lambda: httpx.URL("https://api.moesif.net")
    )

    def get_client(self) -> httpx.AsyncClient:
        """
        Returns the HTTPX client. We're using the class given by the processor
        because it is not monkey-patched and thus will not generate an infinite
        loop of requests.
        """

        return self.processor.get_client(
            base_url=self.base_url,
            headers={"X-Moesif-Application-Id": self.app_id},
        )

    async def handle(self, requests: Sequence[Entry]):
        """
        Sends the entries to Moesif

        Parameters
        ----------
        requests
            Request entries to send to Moesif
        """

        as_json = [self.serialize(entry) for entry in requests]

        async with self.get_client() as client:
            for batch in self.batch(as_json):
                logger.debug("Sending to Moesif a batch of %s bytes", len(batch))
                await client.post(
                    "/v1/events/batch",
                    content=batch,
                    headers={"Content-Type": "application/json"},
                )

    def batch(self, as_json: list[dict]) -> Iterator[bytes]:
        """
        We're finding the right cut in the list of entries so that the
        generated JSON is below the maximum body size allowed by Moesif.

        Parameters
        ----------
        as_json
            The things to be sent to Moesif

        Notes
        -----
        The algorithm is kind of a binary search, but not entirely in the sense
        that it's not guaranteed to find the biggest cut. It's just going to
        split the list in half until it finds a cut that is smaller than the
        maximum size.

        If we end up with a single item that is bigger than the maximum size,
        there is a heuristic that will reduce its size b removing the body(ies)
        of the entry.
        """

        while as_json:
            right = len(as_json)

            while True:
                batch = orjson.dumps(as_json[:right])

                if len(batch) < self.MAX_BATCH_SIZE:
                    yield batch
                    as_json = as_json[right:]
                    break

                right = right // 2

                if right == 0:
                    yield self.reduce(as_json[0])
                    as_json = as_json[1:]
                    break

    def reduce(self, entry: dict) -> bytes:
        """
        Reduces the size of the entry by removing the body

        Parameters
        ----------
        entry
            Entry to reduce
        """

        targets = sorted(
            ["response", "request"],
            key=lambda t: len(entry.get(t, {}).get("body", b"")),
            reverse=True,
        )

        for target in targets:
            if target not in entry:
                continue

            entry[target]["body"] = b""
            batch = orjson.dumps(entry)

            if len(batch) < self.MAX_BATCH_SIZE:
                return batch

        msg = "Could not reduce the size of the entry"
        raise ValueError(msg)

    def serialize(self, entry: Entry) -> dict:
        """
        Serializes the entry into a dictionary that can be sent to Moesif

        Parameters
        ----------
        entry
            The entry to serialize
        """

        out = dict(
            request=dict(
                time=entry.request.time.isoformat(),
                uri=str(entry.request.uri),
                verb=entry.request.verb,
                headers=self.serialize_headers(entry.request.headers),
                body=entry.request.body,
                transfer_encoding=entry.request.transfer_encoding,
            ),
            user_id=entry.caller.user_id,
            company_id=entry.caller.company_id,
            subscription_id=entry.caller.subscription_id,
            direction=entry.meta.direction,
            metadata=entry.meta.metadata,
        )

        if entry.response:
            out["response"] = dict(
                time=entry.response.time.isoformat(),
                status=entry.response.status,
                ip_address=str(entry.response.ip_address),
                headers=self.serialize_headers(entry.response.headers),
                body=entry.response.body,
                transfer_encoding=entry.response.transfer_encoding,
            )

        if entry.span:
            out["span"] = dict(
                id=entry.span.id,
                parent_id=entry.span.parent_id,
                links=entry.span.links,
                status=entry.span.status,
            )

        return out

    def serialize_headers(self, headers: Sequence[tuple[str, str]]) -> dict[str, str]:
        """
        HTTP allows multiple headers with the same name, here we need to
        flatten that into one entry per header, with multiple values of the
        same header joined by a comma.

        Parameters
        ----------
        headers
            Headers list to flatten
        """

        out = defaultdict(list)

        for name, value in headers:
            out[name].append(value)

        return {k: ",".join(v) for k, v in out.items()}
