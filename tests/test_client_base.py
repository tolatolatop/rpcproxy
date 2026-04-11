"""RpcProxyClientBase tests with ``websockets.connect`` mocked as ``AsyncMock``."""

from __future__ import annotations

import asyncio
from typing import Any
from unittest.mock import AsyncMock, patch

import pytest

from rpcproxy.client.base import RpcProxyClientBase
from rpcproxy.fastapi_ws_rpc import (
    dumps_message,
    extract_request,
    loads_message,
    response_message,
)


class _RecordingClient(RpcProxyClientBase):
    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.envelope_calls: list[dict[str, Any]] = []

    async def receive_envelope(self, **kwargs: Any) -> dict[str, bool]:
        self.envelope_calls.append(kwargs)
        return {"ok": True}


def _mock_transport() -> tuple[AsyncMock, asyncio.Queue[str], list[str]]:
    incoming: asyncio.Queue[str] = asyncio.Queue()
    outgoing: list[str] = []
    ws = AsyncMock()
    ws.send = AsyncMock(side_effect=outgoing.append)
    ws.recv = AsyncMock(side_effect=incoming.get)
    ws.close = AsyncMock()
    return ws, incoming, outgoing


def _response_for_outbound_raw(
    raw: str, result: Any, *, result_type: str | None = None
) -> str:
    msg = loads_message(raw)
    req = extract_request(msg)
    assert req is not None
    rt = result_type if result_type is not None else type(result).__name__
    return dumps_message(response_message(result, call_id=req["call_id"], result_type=rt))


@pytest.fixture
def connect_patch():
    ws, incoming, outgoing = _mock_transport()
    connect = AsyncMock(return_value=ws)
    with patch("rpcproxy.client.base.websockets.connect", connect):
        yield connect, ws, incoming, outgoing


async def test_connect_awaits_websockets_connect_with_uri(connect_patch):
    connect, ws, _incoming, _outgoing = connect_patch
    client = _RecordingClient()
    await client.connect("ws://example.test/rpc")
    connect.assert_awaited_once_with("ws://example.test/rpc")
    assert client._ws is ws
    await client.close()


async def test_set_state_roundtrip(connect_patch):
    _connect, _ws, incoming, outgoing = connect_patch
    client = _RecordingClient(default_call_timeout=5.0)
    await client.connect("ws://x")

    task = asyncio.create_task(client.set_state("k", 42))
    await asyncio.sleep(0)
    assert len(outgoing) == 1
    incoming.put_nowait(_response_for_outbound_raw(outgoing[0], "ack", result_type="str"))
    assert await task == "ack"
    await client.close()


async def test_inbound_receive_envelope_dispatches_and_replies(connect_patch):
    _connect, _ws, incoming, outgoing = connect_patch
    client = _RecordingClient()
    await client.connect("ws://x")

    args = {"request_id": "rid-1", "body": {"n": 1}, "message_type": "t"}
    inbound = {
        "request": {
            "method": "receive_envelope",
            "arguments": args,
            "call_id": "in-1",
        }
    }
    incoming.put_nowait(dumps_message(inbound))
    await asyncio.sleep(0)

    assert len(client.envelope_calls) == 1
    assert client.envelope_calls[0]["request_id"] == "rid-1"
    assert client.envelope_calls[0]["body"] == {"n": 1}
    assert len(outgoing) == 1
    resp = loads_message(outgoing[0])
    assert resp.get("response", {}).get("result") == {"ok": True}
    await client.close()


async def test_wait_relay_predicate_stash_after_inbound(connect_patch):
    _c, _w, incoming, outgoing = connect_patch
    client = _RecordingClient()
    await client.connect("ws://x")

    args = {"request_id": "rid-2", "body": {}}
    inbound = {
        "request": {
            "method": "receive_envelope",
            "arguments": args,
            "call_id": "in-2",
        }
    }
    incoming.put_nowait(dumps_message(inbound))
    await asyncio.sleep(0)

    receipt = await client.wait_relay_predicate("rid-2", 1.0)
    assert receipt == {"ok": True, "arguments": dict(args)}
    await client.close()


async def test_wait_relay_predicate_wakes_before_handler_finishes(connect_patch):
    _c, _w, incoming, outgoing = connect_patch
    gate = asyncio.Event()

    class _SlowClient(_RecordingClient):
        async def receive_envelope(self, **kwargs: Any) -> dict[str, bool]:
            await gate.wait()
            return await super().receive_envelope(**kwargs)

    client = _SlowClient()
    await client.connect("ws://x")

    wait_task = asyncio.create_task(client.wait_relay_predicate("rid-3", 2.0))
    await asyncio.sleep(0)

    args = {"request_id": "rid-3"}
    inbound = {
        "request": {
            "method": "receive_envelope",
            "arguments": args,
            "call_id": "in-3",
        }
    }
    incoming.put_nowait(dumps_message(inbound))
    receipt = await wait_task
    assert receipt["ok"] is True
    assert receipt["arguments"]["request_id"] == "rid-3"

    gate.set()
    await asyncio.sleep(0)
    await client.close()


async def test_wait_relay_predicate_timeout(connect_patch):
    _c, _w, _incoming, _outgoing = connect_patch
    client = _RecordingClient()
    await client.connect("ws://x")

    with pytest.raises(asyncio.TimeoutError):
        await client.wait_relay_predicate("never", 0.05)
    await client.close()


async def test_wait_relay_predicate_second_waiter_runtimeerror(connect_patch):
    _c, _w, _incoming, _outgoing = connect_patch
    client = _RecordingClient()
    await client.connect("ws://x")

    _ = asyncio.create_task(client.wait_relay_predicate("dup", None))
    await asyncio.sleep(0)
    with pytest.raises(RuntimeError, match="already waiting"):
        await client.wait_relay_predicate("dup", 0.1)
    await client.close()


async def test_wait_relay_predicate_empty_request_id_valueerror(connect_patch):
    _c, _w, _incoming, _outgoing = connect_patch
    client = _RecordingClient()
    await client.connect("ws://x")

    with pytest.raises(ValueError, match="request_id"):
        await client.wait_relay_predicate("   ", 1.0)
    await client.close()


async def test_close_cancels_pending_relay_wait(connect_patch):
    _c, _w, _incoming, _outgoing = connect_patch
    client = _RecordingClient()
    await client.connect("ws://x")

    wait_task = asyncio.create_task(client.wait_relay_predicate("orphan", None))
    await asyncio.sleep(0)
    await client.close()
    with pytest.raises(asyncio.CancelledError):
        await wait_task
