import httpx
import pytest
from pytest_httpx import HTTPXMock
from pytest_mock import MockerFixture

from httpx_spy.processor import Processor


@pytest.fixture
def processor():
    BaseClient = httpx.Client.__base__

    p: Processor = Processor()
    p.monkey_patch()

    yield p

    httpx.Client.__bases__ = (BaseClient,)
    httpx.AsyncClient.__bases__ = (BaseClient,)


@pytest.mark.asyncio
async def test_hooks_injection_async(
    processor: Processor,
    mocker: MockerFixture,
    httpx_mock: HTTPXMock,
):
    spy_req = mocker.spy(processor, "async_handle_request")
    spy_resp = mocker.spy(processor, "async_handle_response")

    httpx_mock.add_response(url="https://foo.bar/hello.txt")

    async with httpx.AsyncClient() as client:
        client._processor = processor
        resp = await client.get("https://foo.bar/hello.txt")

    assert spy_req.call_count == 1
    assert spy_resp.call_count == 1
    assert spy_resp.call_args[0][0] is resp
    assert spy_req.call_args[0][0] is resp.request


def test_hooks_injection_sync(
    processor: Processor,
    mocker: MockerFixture,
    httpx_mock: HTTPXMock,
):
    spy_req = mocker.spy(processor, "sync_handle_request")
    spy_resp = mocker.spy(processor, "sync_handle_response")

    httpx_mock.add_response(url="https://foo.bar/hello.txt")

    with httpx.Client() as client:
        client._processor = processor
        resp = client.get("https://foo.bar/hello.txt")

    assert spy_req.call_count == 1
    assert spy_resp.call_count == 1
    assert spy_resp.call_args[0][0] is resp
    assert spy_req.call_args[0][0] is resp.request
