"""Tests for HandlerPostMessageClient (mock ``websockets.connect``)."""

from __future__ import annotations

import asyncio
from typing import Any
from unittest.mock import AsyncMock, patch

import pytest

from rpcproxy.client.base import _arg_str
from rpcproxy.client.envelope_types import ReceiveEnvelopeArguments
from rpcproxy.client.handler_client import HandlerPostMessageClient, HandlerResult
from rpcproxy.fastapi_ws_rpc import dumps_message, extract_request, loads_message, response_message


async def _noop_handler(**kw: object) -> HandlerResult:
    return HandlerResult(body={}, skip_post=True)


def test_max_inflight_must_be_positive() -> None:
    with pytest.raises(ValueError, match="max_inflight"):
        HandlerPostMessageClient(_noop_handler, max_inflight=0)


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


async def test_receive_envelope_ack_before_post_message(connect_patch):
    _c, _w, incoming, outgoing = connect_patch

    async def handler(
        *,
        sender: str,
        request_id: str,
        arguments: dict[str, Any],
        **kw: object,
    ) -> HandlerResult:
        await asyncio.sleep(0.12)
        return HandlerResult(body={"answer": 42})

    client = HandlerPostMessageClient(handler, default_call_timeout=5.0)
    await client.connect("ws://x")

    _enqueue_receive_envelope(
        incoming,
        {
            "request_id": "rid-a",
            "sender": "upstream",
            "body": {"n": 1},
        },
        "in-1",
    )
    await asyncio.sleep(0)
    assert len(outgoing) == 1
    r0 = loads_message(outgoing[0])
    assert r0.get("response", {}).get("result") == {"ok": True}

    await asyncio.sleep(0.15)
    assert len(outgoing) == 2
    req = _request_from_outbound_raw(outgoing[1])
    assert req["method"] == "post_message"
    assert req.get("arguments") == {
        "receiver": "upstream",
        "request_id": "rid-a",
        "body": {"answer": 42},
    }
    incoming.put_nowait(_response_for_outbound_raw(outgoing[1], "posted", result_type="str"))
    await asyncio.sleep(0)
    await client.close()


async def test_second_envelope_processed_while_first_handler_slow(connect_patch):
    _c, _w, incoming, outgoing = connect_patch
    calls: list[str] = []

    async def handler(*, request_id: str, **kw: object) -> HandlerResult:
        if request_id == "first":
            await asyncio.sleep(0.2)
        calls.append(request_id)
        return HandlerResult(body={"id": request_id})

    client = HandlerPostMessageClient(handler, default_call_timeout=5.0)
    await client.connect("ws://x")

    _enqueue_receive_envelope(
        incoming,
        {"request_id": "first", "sender": "svc"},
        "in-a",
    )
    _enqueue_receive_envelope(
        incoming,
        {"request_id": "second", "sender": "svc"},
        "in-b",
    )
    await asyncio.sleep(0)
    assert len(outgoing) == 2
    assert loads_message(outgoing[0])["response"]["result"] == {"ok": True}
    assert loads_message(outgoing[1])["response"]["result"] == {"ok": True}

    await asyncio.sleep(0.25)
    assert len(outgoing) == 4
    for i in (2, 3):
        req = _request_from_outbound_raw(outgoing[i])
        assert req["method"] == "post_message"
        assert req["arguments"]["receiver"] == "svc"
    bodies = {_request_from_outbound_raw(outgoing[i])["arguments"]["body"]["id"] for i in (2, 3)}
    assert bodies == {"first", "second"}

    for i in (2, 3):
        incoming.put_nowait(_response_for_outbound_raw(outgoing[i], "ok", result_type="str"))
    await asyncio.sleep(0)
    await client.close()


async def test_skip_post_no_second_rpc(connect_patch):
    _c, _w, incoming, outgoing = connect_patch

    async def handler(**kw: object) -> HandlerResult:
        return HandlerResult(body={}, skip_post=True)

    client = HandlerPostMessageClient(handler, default_call_timeout=5.0)
    await client.connect("ws://x")
    _enqueue_receive_envelope(
        incoming,
        {"request_id": "x", "sender": "s"},
        "in-1",
    )
    await asyncio.sleep(0.05)
    assert len(outgoing) == 1
    await client.close()


async def test_empty_request_id_skips_pipeline(connect_patch):
    _c, _w, incoming, outgoing = connect_patch
    ran = False

    async def handler(**kw: object) -> HandlerResult:
        nonlocal ran
        ran = True
        return HandlerResult(body={})

    client = HandlerPostMessageClient(handler)
    await client.connect("ws://x")
    _enqueue_receive_envelope(incoming, {"sender": "s"}, "in-1")
    await asyncio.sleep(0.05)
    assert len(outgoing) == 1
    assert loads_message(outgoing[0])["response"]["result"] == {"ok": False}
    assert ran is False
    await client.close()


async def test_on_handler_exception_subclass_can_post(connect_patch):
    _c, _w, incoming, outgoing = connect_patch

    async def handler(**kw: object) -> HandlerResult:
        raise ValueError("boom")

    class _Client(HandlerPostMessageClient):
        async def on_handler_exception(
            self, exc: BaseException, arguments: ReceiveEnvelopeArguments
        ) -> None:
            await super().on_handler_exception(exc, arguments)
            await self.post_message(
                receiver=self._post_message_receiver(arguments),
                body={"error": type(exc).__name__},
                request_id=_arg_str(arguments, "request_id"),
            )

    client = _Client(handler, default_call_timeout=5.0)
    await client.connect("ws://x")
    _enqueue_receive_envelope(
        incoming,
        {"request_id": "e1", "sender": "peer"},
        "in-1",
    )
    await asyncio.sleep(0.05)
    assert len(outgoing) == 2
    assert loads_message(outgoing[0])["response"]["result"] == {"ok": True}
    req = _request_from_outbound_raw(outgoing[1])
    assert req["method"] == "post_message"
    assert req["arguments"]["body"] == {"error": "ValueError"}
    incoming.put_nowait(_response_for_outbound_raw(outgoing[1], "ok", result_type="str"))
    await client.close()


async def test_max_inflight_serializes_handlers(connect_patch):
    """Semaphore limits concurrent pipelines; use ``skip_post`` so no RPC wait on post_message."""
    _c, _w, incoming, outgoing = connect_patch
    overlap = asyncio.Event()
    first_in_handler = asyncio.Event()
    order: list[str] = []

    async def handler(*, request_id: str, **kw: object) -> HandlerResult:
        order.append(f"enter:{request_id}")
        if request_id == "a":
            first_in_handler.set()
            await overlap.wait()
        order.append(f"exit:{request_id}")
        return HandlerResult(body={}, skip_post=True)

    client = HandlerPostMessageClient(handler, max_inflight=1)
    await client.connect("ws://x")

    _enqueue_receive_envelope(
        incoming,
        {"request_id": "a", "sender": "p"},
        "in-a",
    )
    _enqueue_receive_envelope(
        incoming,
        {"request_id": "b", "sender": "p"},
        "in-b",
    )
    await asyncio.sleep(0)
    assert len(outgoing) == 2

    await asyncio.wait_for(first_in_handler.wait(), timeout=1.0)
    await asyncio.sleep(0.05)
    assert order == ["enter:a"]
    assert "enter:b" not in order

    overlap.set()
    await asyncio.sleep(0.1)
    assert order == ["enter:a", "exit:a", "enter:b", "exit:b"]
    await client.close()
