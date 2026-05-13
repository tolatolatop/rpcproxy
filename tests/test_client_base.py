"""RpcProxyClientBase tests with ``websockets.connect`` mocked as ``AsyncMock``."""

from __future__ import annotations

import asyncio
from typing import Any
from unittest.mock import AsyncMock, patch

import pytest
from websockets.exceptions import ConnectionClosedError
from websockets.frames import Close

from rpcproxy.client import (
    CHUNK_DATA_KEY,
    CHUNK_MARKER,
    DEFAULT_AUTO_CHUNK_THRESHOLD,
    encode_chunked_bodies,
)
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


def _request_from_outbound_raw(raw: str):
    msg = loads_message(raw)
    req = extract_request(msg)
    assert req is not None
    return req


def _enqueue_receive_envelope(
    incoming: asyncio.Queue[str], args: dict[str, Any], call_id: str
) -> None:
    inbound = {
        "request": {
            "method": "receive_envelope",
            "arguments": args,
            "call_id": call_id,
        }
    }
    incoming.put_nowait(dumps_message(inbound))


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
    connect.assert_awaited_once_with("ws://example.test/rpc", max_size=None)
    assert client._ws is ws
    await client.close()


async def test_reader_loop_logs_connection_closed_reason():
    ws = AsyncMock()
    ws.send = AsyncMock()
    ws.close = AsyncMock()
    ws.recv = AsyncMock(
        side_effect=ConnectionClosedError(
            Close(1009, "frame exceeds limit of 1048576 bytes"),
            None,
        )
    )
    connect = AsyncMock(return_value=ws)

    with (
        patch("rpcproxy.client.base.websockets.connect", connect),
        patch("rpcproxy.client.base.logger.warning") as warning_mock,
    ):
        client = _RecordingClient()
        await client.connect("ws://example.test/rpc")
        await client.wait_until_disconnected()
        warning_mock.assert_called_once_with(
            "websocket closed code=%s reason=%s",
            1009,
            "frame exceeds limit of 1048576 bytes",
        )
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


async def test_post_message_roundtrip(connect_patch):
    _connect, _ws, incoming, outgoing = connect_patch
    client = _RecordingClient(default_call_timeout=5.0)
    await client.connect("ws://x")

    task = asyncio.create_task(
        client.post_message(
            receiver="peer-1",
            body={"text": "hi"},
            request_id="req-99",
        )
    )
    await asyncio.sleep(0)
    assert len(outgoing) == 1
    req = _request_from_outbound_raw(outgoing[0])
    assert req["method"] == "post_message"
    assert req.get("arguments") == {
        "receiver": "peer-1",
        "request_id": "req-99",
        "body": {"text": "hi"},
    }
    incoming.put_nowait(_response_for_outbound_raw(outgoing[0], "msg-ok", result_type="str"))
    assert await task == "msg-ok"
    await client.close()


async def test_post_message_body_none_sends_empty_dict(connect_patch):
    _connect, _ws, incoming, outgoing = connect_patch
    client = _RecordingClient(default_call_timeout=5.0)
    await client.connect("ws://x")

    task = asyncio.create_task(client.post_message(receiver="r"))
    await asyncio.sleep(0)
    assert len(outgoing) == 1
    req = _request_from_outbound_raw(outgoing[0])
    assert req["method"] == "post_message"
    assert req.get("arguments") == {
        "receiver": "r",
        "request_id": "",
        "body": {},
    }
    incoming.put_nowait(_response_for_outbound_raw(outgoing[0], 7, result_type="int"))
    assert await task == "7"
    await client.close()


async def test_post_message_chunked_sends_multiple_transport_requests(connect_patch):
    _connect, _ws, incoming, outgoing = connect_patch
    client = _RecordingClient(default_call_timeout=5.0)
    await client.connect("ws://x")

    large_body = {"payload": "abcdefghij"}
    task = asyncio.create_task(
        client.post_message_chunked(
            receiver="peer-big",
            body=large_body,
            request_id="job-7",
            chunk_size=5,
        )
    )

    seen = 0
    while not task.done():
        await asyncio.sleep(0)
        while seen < len(outgoing):
            raw = outgoing[seen]
            incoming.put_nowait(_response_for_outbound_raw(raw, "ok", result_type="str"))
            seen += 1

    report = await task
    assert report.request_id == "job-7"
    assert report.chunk_count == len(outgoing)
    assert report.responses == ["ok"] * len(outgoing)
    assert report.chunk_count > 1

    first = _request_from_outbound_raw(outgoing[0])
    first_args = first["arguments"]
    assert first["method"] == "post_message"
    assert first_args["receiver"] == "peer-big"
    assert first_args["request_id"].startswith("job-7#chunk:1/")
    assert CHUNK_MARKER in first_args["body"]
    assert CHUNK_DATA_KEY in first_args["body"]
    assert first_args["body"][CHUNK_MARKER]["request_id"] == "job-7"
    await client.close()


async def test_post_message_auto_uses_plain_post_for_small_body(connect_patch):
    _connect, _ws, incoming, outgoing = connect_patch
    client = _RecordingClient(default_call_timeout=5.0)
    await client.connect("ws://x")

    task = asyncio.create_task(
        client.post_message_auto(
            receiver="peer-small",
            body={"msg": "hi"},
            request_id="small-1",
            auto_chunk_threshold=DEFAULT_AUTO_CHUNK_THRESHOLD,
        )
    )
    await asyncio.sleep(0)
    assert len(outgoing) == 1
    req = _request_from_outbound_raw(outgoing[0])
    assert req["arguments"]["body"] == {"msg": "hi"}
    incoming.put_nowait(_response_for_outbound_raw(outgoing[0], "plain-ok", result_type="str"))

    result = await task
    assert result.chunked is False
    assert result.response == "plain-ok"
    assert result.chunk_report is None
    await client.close()


async def test_post_message_auto_switches_to_chunked_for_large_body(connect_patch):
    _connect, _ws, incoming, outgoing = connect_patch
    client = _RecordingClient(default_call_timeout=5.0)
    await client.connect("ws://x")

    big_body = {"payload": "x" * (DEFAULT_AUTO_CHUNK_THRESHOLD + 1024)}
    task = asyncio.create_task(
        client.post_message_auto(
            receiver="peer-large",
            body=big_body,
            request_id="large-1",
            chunk_size=64 * 1024,
        )
    )

    seen = 0
    while not task.done():
        await asyncio.sleep(0)
        while seen < len(outgoing):
            raw = outgoing[seen]
            incoming.put_nowait(_response_for_outbound_raw(raw, "chunk-ok", result_type="str"))
            seen += 1

    result = await task
    assert result.chunked is True
    assert result.response is None
    assert result.chunk_report is not None
    assert result.chunk_report.chunk_count > 1
    assert len(outgoing) == result.chunk_report.chunk_count
    first = _request_from_outbound_raw(outgoing[0])
    assert CHUNK_MARKER in first["arguments"]["body"]
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


async def test_inbound_chunked_receive_envelope_reassembles_before_dispatch(connect_patch):
    _connect, _ws, incoming, outgoing = connect_patch
    client = _RecordingClient()
    await client.connect("ws://x")

    batch = encode_chunked_bodies(
        {"payload": "abcdefghij", "nested": {"n": 1}},
        request_id="logical-42",
        chunk_size=6,
        transfer_id="transfer-42",
    )

    for index, chunk_body in enumerate(batch.bodies, start=1):
        _enqueue_receive_envelope(
            incoming,
            {
                "request_id": f"transport-{index}",
                "sender": "peer-x",
                "body": chunk_body,
            },
            f"in-{index}",
        )
        await asyncio.sleep(0)

    assert len(client.envelope_calls) == 1
    call = client.envelope_calls[0]
    assert call["request_id"] == "logical-42"
    assert call["body"] == {"payload": "abcdefghij", "nested": {"n": 1}}
    assert call["chunk_transfer_id"] == "transfer-42"
    assert call["chunk_count"] == batch.chunk_count
    assert len(outgoing) == batch.chunk_count
    await client.close()


async def test_wait_relay_predicate_stash_after_inbound(connect_patch):
    _c, _w, incoming, outgoing = connect_patch
    client = _RecordingClient()
    await client.connect("ws://x")

    args = {"request_id": "rid-2", "body": {}}
    _enqueue_receive_envelope(incoming, args, "in-2")
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


async def test_relay_stash_lru_evicts_oldest(connect_patch):
    _c, _w, incoming, outgoing = connect_patch
    client = _RecordingClient(relay_stash_max_size=2)
    await client.connect("ws://x")

    _enqueue_receive_envelope(incoming, {"request_id": "r-a"}, "in-a")
    _enqueue_receive_envelope(incoming, {"request_id": "r-b"}, "in-b")
    _enqueue_receive_envelope(incoming, {"request_id": "r-c"}, "in-c")
    await asyncio.sleep(0)
    assert len(outgoing) == 3

    with pytest.raises(asyncio.TimeoutError):
        await client.wait_relay_predicate("r-a", 0.05)

    rb = await client.wait_relay_predicate("r-b", 1.0)
    assert rb["arguments"]["request_id"] == "r-b"
    rc = await client.wait_relay_predicate("r-c", 1.0)
    assert rc["arguments"]["request_id"] == "r-c"
    await client.close()


async def test_relay_stash_max_size_zero_drops_unwaited_receipts(connect_patch):
    _c, _w, incoming, outgoing = connect_patch
    client = _RecordingClient(relay_stash_max_size=0)
    await client.connect("ws://x")

    _enqueue_receive_envelope(incoming, {"request_id": "early"}, "in-1")
    await asyncio.sleep(0)
    assert len(outgoing) == 1

    with pytest.raises(asyncio.TimeoutError):
        await client.wait_relay_predicate("early", 0.05)

    wait_task = asyncio.create_task(client.wait_relay_predicate("late", 1.0))
    await asyncio.sleep(0)
    _enqueue_receive_envelope(incoming, {"request_id": "late"}, "in-2")
    receipt = await wait_task
    assert receipt["arguments"]["request_id"] == "late"
    await client.close()
