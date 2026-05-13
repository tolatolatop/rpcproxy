"""Tests for demo echo handler and DemoRpcProxyClient."""

from __future__ import annotations

import asyncio
from typing import Any, cast
from unittest.mock import AsyncMock, patch

import pytest

from rpcproxy.client.envelope_types import ReceiveEnvelopeArguments
from rpcproxy.client.handler_client import HandlerPostMessageClient, HandlerResult
from rpcproxy.demo_loop import DemoRpcProxyClient, demo_echo_envelope_handler
from rpcproxy.fastapi_ws_rpc import dumps_message, extract_request, loads_message, response_message


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


async def test_demo_echo_envelope_handler_adds_is_echo_to_body_copy() -> None:
    arguments = cast(
        ReceiveEnvelopeArguments,
        {"request_id": "r1", "sender": "peer", "body": {"n": 1, "msg": "hi"}},
    )
    result = await demo_echo_envelope_handler(
        message_type="t",
        kind="k",
        client_id="c1",
        sender="peer",
        receiver="",
        body={"n": 1, "msg": "hi"},
        request_id="r1",
        arguments=arguments,
    )
    assert result.body == {"n": 1, "msg": "hi", "is_echo": True}
    assert "is_echo" not in arguments.get("body", {})


async def test_demo_echo_envelope_handler_empty_body_becomes_is_echo_only() -> None:
    arguments = cast(ReceiveEnvelopeArguments, {"request_id": "r2", "sender": "s"})
    result = await demo_echo_envelope_handler(
        message_type="",
        kind="",
        client_id="",
        sender="s",
        receiver="",
        body=None,
        request_id="r2",
        arguments=arguments,
    )
    assert result.body == {"is_echo": True}


async def test_demo_client_post_message_echo_roundtrip(connect_patch):
    _c, _w, incoming, outgoing = connect_patch
    client = DemoRpcProxyClient()
    await client.connect("ws://x")

    _enqueue_receive_envelope(
        incoming,
        {
            "request_id": "echo-1",
            "sender": "server-a",
            "body": {"payload": 42},
        },
        "in-1",
    )
    await asyncio.sleep(0)
    assert loads_message(outgoing[0])["response"]["result"] == {"ok": True}

    await asyncio.sleep(0)
    assert len(outgoing) == 2
    req = _request_from_outbound_raw(outgoing[1])
    assert req["method"] == "post_message"
    assert req["arguments"] == {
        "receiver": "server-a",
        "request_id": "echo-1",
        "body": {"payload": 42, "is_echo": True},
    }
    incoming.put_nowait(_response_for_outbound_raw(outgoing[1], "ok", result_type="str"))
    await asyncio.sleep(0)
    await client.close()


async def test_handler_client_uses_post_message_auto_for_large_echo(monkeypatch: pytest.MonkeyPatch) -> None:
    async def handler(**_kwargs: Any) -> HandlerResult:
        return HandlerResult(body={"payload": "x" * 32})

    client = HandlerPostMessageClient(handler)
    post_message_auto = AsyncMock(return_value={"chunked": True})
    monkeypatch.setattr(client, "post_message_auto", post_message_auto)

    arguments = cast(
        ReceiveEnvelopeArguments,
        {"request_id": "echo-big", "sender": "server-a", "body": {"payload": "x" * 32}},
    )

    await client._run_handler_pipeline_unlocked(arguments)

    post_message_auto.assert_awaited_once_with(
        receiver="server-a",
        body={"payload": "x" * 32},
        request_id="echo-big",
        auto_chunk_threshold=262144,
        chunk_size=262144,
        compress=False,
    )
