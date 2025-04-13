import asyncio
import re
import time
from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta

import httpx
import pytest
from pytest_httpx import HTTPXMock
from pytest_mock import MockerFixture

from httpx_spy.processor import (
    CallerEntry,
    Entry,
    Handler,
    MetaEntry,
    Processor,
    RequestEntry,
    ResponseEntry,
)


@dataclass
class MockHandler(Handler):
    handled: list[Entry] = field(default_factory=list)

    async def handle(self, requests: Sequence[Entry]) -> None:
        self.handled.extend(requests)


class AnyTime:
    def __eq__(self, other):
        return isinstance(other, datetime)


@dataclass(eq=False)
class ReStr:
    """
    Utility class to be able to have __eq__ match on strings that match a
    specific regular expression instead of a raw match
    """

    exp: str

    def __eq__(self, other):
        if isinstance(other, str):
            return bool(re.match(self.exp, other))

        return super().__eq__(other)


@pytest.fixture
def processor():
    # noinspection PyProtectedMember
    from httpx._client import BaseClient

    p: Processor = Processor(flush_interval=timedelta(milliseconds=10))
    p.monkey_patch()

    yield p

    httpx.Client.__bases__ = (BaseClient,)
    httpx.AsyncClient.__bases__ = (BaseClient,)


@pytest.fixture
def handler(processor: Processor):
    handler = MockHandler()
    processor.add_handler(handler)
    return handler


@pytest.mark.asyncio
async def test_async_serialize(
    processor: Processor,
    mocker: MockerFixture,
    httpx_mock: HTTPXMock,
):
    spy_req = mocker.spy(processor, "async_handle_request")

    httpx_mock.add_response(url="https://foo.bar/hello.txt")

    async with httpx.AsyncClient() as client:
        client._processor = processor
        resp = await client.post("https://foo.bar/hello.txt", json=dict(foo=42))

    req = spy_req.call_args[0][0]
    request_time = datetime(2025, 1, 1, 3, 42, 0, 0, tzinfo=UTC)

    assert await processor.async_serialize_req(
        request=req,
        request_time=request_time,
        caller=CallerEntry(),
        metadata={},
    ) == Entry(
        request=RequestEntry(
            time=request_time,
            uri=httpx.URL("https://foo.bar/hello.txt"),
            verb="POST",
            headers=[
                ("host", "foo.bar"),
                ("accept", "*/*"),
                ("accept-encoding", "gzip, deflate"),
                ("connection", "keep-alive"),
                ("user-agent", ReStr(r"python-httpx/.*")),
                ("content-length", "10"),
                ("content-type", "application/json"),
            ],
            body='{"foo":42}',
            transfer_encoding="json",
        ),
        response=None,
        span=None,
        meta=MetaEntry(direction="outgoing", metadata={}),
        caller=CallerEntry(user_id=None, company_id=None, subscription_id=None),
    )

    assert await processor.async_serialize_resp(resp, request_time) == ResponseEntry(
        time=request_time,
        status=200,
        ip_address=None,
        headers=[],
        body="",
        transfer_encoding="base64",
    )


def test_sync_serialize_req(
    processor: Processor,
    mocker: MockerFixture,
    httpx_mock: HTTPXMock,
):
    spy_req = mocker.spy(processor, "sync_handle_request")

    httpx_mock.add_response(url="https://foo.bar/hello.txt")

    with httpx.Client() as client:
        client._processor = processor
        resp = client.post("https://foo.bar/hello.txt", content=b"hello")

    req = spy_req.call_args[0][0]
    request_time = datetime(2025, 1, 1, 3, 42, 0, 0, tzinfo=UTC)

    assert processor.sync_serialize_req(
        request=req,
        request_time=request_time,
        caller=CallerEntry(),
        metadata={},
    ) == Entry(
        request=RequestEntry(
            time=request_time,
            uri=httpx.URL("https://foo.bar/hello.txt"),
            verb="POST",
            headers=[
                ("host", "foo.bar"),
                ("accept", "*/*"),
                ("accept-encoding", "gzip, deflate"),
                ("connection", "keep-alive"),
                ("user-agent", ReStr(r"python-httpx/.*")),
                ("content-length", "5"),
            ],
            body="aGVsbG8=",
            transfer_encoding="base64",
        ),
        response=None,
        span=None,
        meta=MetaEntry(direction="outgoing", metadata={}),
        caller=CallerEntry(user_id=None, company_id=None, subscription_id=None),
    )

    assert processor.sync_serialize_resp(resp, request_time) == ResponseEntry(
        time=request_time,
        status=200,
        ip_address=None,
        headers=[],
        body="",
        transfer_encoding="base64",
    )


@pytest.mark.asyncio
async def test_run_async(
    processor: Processor,
    httpx_mock: HTTPXMock,
    handler: MockHandler,
):
    processor.start()

    try:
        httpx_mock.add_response(url="https://foo.bar/hello.txt")

        async with httpx.AsyncClient() as client:
            client._processor = processor
            await client.post("https://foo.bar/hello.txt", json=dict(foo=42))

        for _ in range(100):
            await asyncio.sleep(0.01)

            if handler.handled:
                break

        assert len(handler.handled) == 1
        # noinspection PyTypeChecker
        assert handler.handled == [
            Entry(
                request=RequestEntry(
                    time=AnyTime(),
                    uri=httpx.URL("https://foo.bar/hello.txt"),
                    verb="POST",
                    headers=[
                        (
                            "host",
                            "foo.bar",
                        ),
                        (
                            "accept",
                            "*/*",
                        ),
                        (
                            "accept-encoding",
                            "gzip, deflate",
                        ),
                        (
                            "connection",
                            "keep-alive",
                        ),
                        (
                            "user-agent",
                            ReStr(r"python-httpx/.*"),
                        ),
                        (
                            "content-length",
                            "10",
                        ),
                        (
                            "content-type",
                            "application/json",
                        ),
                    ],
                    body='{"foo":42}',
                    transfer_encoding="json",
                ),
                response=ResponseEntry(
                    time=AnyTime(),
                    status=200,
                    ip_address=None,
                    headers=[],
                    body="",
                    transfer_encoding="base64",
                ),
                span=None,
                meta=MetaEntry(
                    direction="outgoing",
                    metadata={},
                ),
                caller=CallerEntry(
                    user_id=None,
                    company_id=None,
                    subscription_id=None,
                ),
            ),
        ]

    finally:
        processor.stop()


def test_run_sync(
    processor: Processor,
    httpx_mock: HTTPXMock,
    handler: MockHandler,
):
    processor.start()

    try:
        httpx_mock.add_response(url="https://foo.bar/hello.txt")

        with httpx.Client() as client:
            client._processor = processor
            client.post("https://foo.bar/hello.txt", content=b"yolo")

        for _ in range(100):
            time.sleep(0.01)

            if handler.handled:
                break

        assert len(handler.handled) == 1
        # noinspection PyTypeChecker
        assert handler.handled == [
            Entry(
                request=RequestEntry(
                    time=AnyTime(),
                    uri=httpx.URL("https://foo.bar/hello.txt"),
                    verb="POST",
                    headers=[
                        (
                            "host",
                            "foo.bar",
                        ),
                        (
                            "accept",
                            "*/*",
                        ),
                        (
                            "accept-encoding",
                            "gzip, deflate",
                        ),
                        (
                            "connection",
                            "keep-alive",
                        ),
                        (
                            "user-agent",
                            ReStr(r"python-httpx/.*"),
                        ),
                        (
                            "content-length",
                            "4",
                        ),
                    ],
                    body="eW9sbw==",
                    transfer_encoding="base64",
                ),
                response=ResponseEntry(
                    time=AnyTime(),
                    status=200,
                    ip_address=None,
                    headers=[],
                    body="",
                    transfer_encoding="base64",
                ),
                span=None,
                meta=MetaEntry(
                    direction="outgoing",
                    metadata={},
                ),
                caller=CallerEntry(
                    user_id=None,
                    company_id=None,
                    subscription_id=None,
                ),
            ),
        ]

    finally:
        processor.stop()
