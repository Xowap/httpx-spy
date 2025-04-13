from datetime import UTC, datetime
from ipaddress import IPv4Address

import httpx
import pytest
from pytest_httpx import HTTPXMock

from httpx_spy.moesif import MoesifHandler
from httpx_spy.processor import (
    CallerEntry,
    Entry,
    MetaEntry,
    Processor,
    RequestEntry,
    ResponseEntry,
)


@pytest.fixture
def processor():
    BaseClient = httpx.Client.__base__

    p = Processor()
    p.monkey_patch()

    yield p

    httpx.Client.__bases__ = (BaseClient,)
    httpx.AsyncClient.__bases__ = (BaseClient,)


@pytest.fixture
def app_id():
    return "yolo"


@pytest.fixture
def handler(processor: Processor, app_id: str):
    handler = MoesifHandler(app_id=app_id)
    handler.processor = processor

    return handler


@pytest.fixture
def entry_1():
    return Entry(
        request=RequestEntry(
            time=datetime(2025, 1, 1, 3, 4, 0, 0, tzinfo=UTC),
            uri=httpx.URL("https://foo.bar/yolo.txt"),
            verb="GET",
            headers=[
                ("Foo", "yes"),
                ("Foo", "also"),
                ("Bar", "yup"),
            ],
            body='{"yes":true}',
            transfer_encoding="json",
        ),
        response=ResponseEntry(
            time=datetime(2025, 1, 1, 3, 4, 1, 0, tzinfo=UTC),
            status=200,
            ip_address=IPv4Address("1.2.3.4"),
            headers=[
                ("Yup", "yolo"),
                ("Yup", "yolt?"),
            ],
            body="aGVsbG8=",
            transfer_encoding="base64",
        ),
        meta=MetaEntry(),
        caller=CallerEntry(),
        span=None,
    )


@pytest.mark.asyncio
async def test_get_client(
    handler: MoesifHandler,
    app_id: str,
    httpx_mock: HTTPXMock,
):
    httpx_mock.add_response(
        url="https://api.moesif.net/foo",
        match_headers={
            "X-Moesif-Application-Id": app_id,
        },
    )

    async with handler.get_client() as client:
        await client.get("foo")


@pytest.mark.asyncio
async def test_handle_small(
    handler: MoesifHandler,
    httpx_mock: HTTPXMock,
    entry_1: Entry,
):
    httpx_mock.add_response(
        url="https://api.moesif.net/v1/events/batch",
        match_json=[
            {
                "request": {
                    "time": "2025-01-01T03:04:00+00:00",
                    "uri": "https://foo.bar/yolo.txt",
                    "verb": "GET",
                    "headers": {"Foo": "yes,also", "Bar": "yup"},
                    "body": '{"yes":true}',
                    "transfer_encoding": "json",
                },
                "user_id": None,
                "company_id": None,
                "subscription_id": None,
                "direction": "outgoing",
                "metadata": None,
                "response": {
                    "time": "2025-01-01T03:04:01+00:00",
                    "status": 200,
                    "ip_address": "1.2.3.4",
                    "headers": {"Yup": "yolo,yolt?"},
                    "body": "aGVsbG8=",
                    "transfer_encoding": "base64",
                },
            }
        ],
    )

    await handler.handle([entry_1])


def test_serialize_headers(handler: MoesifHandler):
    serialized = handler.serialize_headers(
        [
            ("A", "one"),
            ("B", "flop"),
            ("A", "two"),
            ("C", "flip"),
            ("A", "three"),
        ]
    )

    assert list(serialized) == ["A", "B", "C"]
    assert serialized == {
        "A": "one,two,three",
        "B": "flop",
        "C": "flip",
    }
