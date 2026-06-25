"""Mock WebSocket RPC peer for testing ``rpcproxy`` CLI tools without a real server.

Usage
-----

    from rpcproxy.testing import MockRelayPeer

    async def test_cli_post_with_mock():
        peer = MockRelayPeer(
            handler_result_factory=lambda body: {"ok": True, "echo": body}
        )
        with peer.patch_connect():
            rid, post_result, receipt = await run_post(
                "ws://mock/rpc", "peer", {"msg": "hi"}, "req-1", 5.0
            )
        assert receipt["arguments"]["body"]["ok"] is True
        assert receipt["arguments"]["body"]["echo"]["msg"] == "hi"

Architecture
------------

``MockRelayPeer`` replaces ``websockets.connect`` (via ``patch_connect()``) so
the client under test talks to a fake transport backed by ``asyncio.Queue``
instead of a real network socket.  For each inbound ``post_message`` RPC call:

1. Respond to the RPC with a success ACK (``"ok"``)
2. Push a ``receive_envelope`` RPC back using a configurable handler result

This simulates the behaviour of a ``fastapi_websocket_rpc``-compatible relay
server paired with a handler client (like ``PlaywrightRpcProxyClient``).
"""

from __future__ import annotations

import asyncio
from typing import Any, Callable
from unittest.mock import AsyncMock, patch

from rpcproxy.fastapi_ws_rpc import (
    builtin_result,
    dumps_message,
    extract_request,
    loads_message,
    response_message,
)

#: Signature for a user-provided handler that maps the inbound ``post_message``
#: body to the ``receive_envelope`` body the mock peer will echo back.
HandlerResultFactory = Callable[[dict[str, Any]], dict[str, Any]]


class MockRelayPeer:
    """Fake WebSocket peer that simulates a ``fastapi_websocket_rpc`` relay
    server paired with a handler client (e.g. Playwright).

    Use ``patch_connect()`` to intercept ``websockets.connect`` --- the client
    under test connects to this peer instead of a real server, and the peer
    faithfully reproduces the relay round-trip::

        post_message RPC  ->  ACK (JSON-RPC response)
                          ->  receive_envelope RPC (handler result pushed back)

    Built-in methods (``_ping_``, ``_get_channel_id_``) are answered
    automatically.

    Parameters
    ----------
    handler_result_factory:
        Called with the ``post_message`` body dict to produce the
        ``receive_envelope`` body dict.  Takes precedence over
        ``default_result``.
    default_result:
        Static result body pushed back for every ``post_message`` when no
        factory is given.  Default: ``{"ok": True}``.
    channel_id:
        Value returned for ``_get_channel_id_`` built-in. Default:
        ``"mock-channel"``.
    """

    def __init__(
        self,
        handler_result_factory: HandlerResultFactory | None = None,
        default_result: dict[str, Any] | None = None,
        *,
        channel_id: str = "mock-channel",
        auto_relay: bool = True,
    ) -> None:
        self._incoming: asyncio.Queue[str] = asyncio.Queue()
        self._outgoing: list[str] = []
        self._channel_id = channel_id
        self._handler_result_factory = handler_result_factory
        self._default_result = (
            default_result if default_result is not None else {"ok": True}
        )
        self._relay_call_counter = 0
        self._auto_relay = auto_relay

        # Fake WebSocket -- the client's reader loop reads from _incoming,
        # and the client writes via ws.send which triggers _on_outgoing.
        self.ws = AsyncMock()
        self.ws.send = AsyncMock(side_effect=self._on_outgoing)
        self.ws.recv = AsyncMock(side_effect=self._incoming.get)
        self.ws.close = AsyncMock()

    # -- Public API --------------------------------------------------

    @property
    def sent_messages(self) -> list[dict[str, Any]]:
        """All outbound frames the client sent, parsed as dicts."""
        return [loads_message(raw) for raw in self._outgoing]

    @property
    def received_bodies(self) -> list[dict[str, Any]]:
        """``post_message`` body dicts the client sent."""
        bodies: list[dict[str, Any]] = []
        for raw in self._outgoing:
            msg = loads_message(raw)
            req = extract_request(msg)
            if req and req.get("method") == "post_message":
                args = req.get("arguments", {})
                if isinstance(args, dict):
                    body = args.get("body", {})
                    bodies.append(body if isinstance(body, dict) else {})
        return bodies

    @property
    def received_request_ids(self) -> list[str]:
        """``request_id`` values from each ``post_message`` the client sent."""
        rids: list[str] = []
        for raw in self._outgoing:
            msg = loads_message(raw)
            req = extract_request(msg)
            if req and req.get("method") == "post_message":
                args = req.get("arguments", {})
                if isinstance(args, dict):
                    rids.append(args.get("request_id", ""))
        return rids

    def inject_receive_envelope(
        self,
        body: dict[str, Any],
        *,
        request_id: str = "",
        sender: str = "mock-peer",
    ) -> None:
        """Manually push a ``receive_envelope`` RPC to the client.

        Use this to simulate delayed / out-of-order relay responses, error
        scenarios, or multiple concurrent relay messages.
        """
        self._relay_call_counter += 1
        inbound = {
            "request": {
                "method": "receive_envelope",
                "arguments": {
                    "request_id": request_id,
                    "body": body,
                    "message_type": "mock",
                    "sender": sender,
                    "receiver": "",
                    "kind": "",
                    "client_id": "",
                },
                "call_id": f"mock-relay-{self._relay_call_counter}",
            }
        }
        self._incoming.put_nowait(dumps_message(inbound))

    def patch_connect(self):
        """Return a context manager that patches ``websockets.connect``.

        Inside the context, any ``RpcProxyClientBase.connect(url)`` call
        connects to this mock peer instead of a real network socket.
        """
        return patch(
            "rpcproxy.client.base.websockets.connect",
            AsyncMock(return_value=self.ws),
        )

    # -- Internal: outbound message handling -------------------------

    def _on_outgoing(self, raw: str) -> None:
        self._outgoing.append(raw)
        self._handle_outbound(raw)

    def _handle_outbound(self, raw: str) -> None:
        msg = loads_message(raw)
        req = extract_request(msg)
        if req is None:
            return  # not a pending RPC call (e.g. a response to our mock)
        method = req["method"]
        call_id = req["call_id"]

        # Built-in methods: answer automatically like a real server.
        b = builtin_result(req, channel_id=self._channel_id)
        if b is not None:
            result_val, result_type = b
            self._queue_rpc_response(result_val, call_id, result_type)
            return

        # post_message -> ACK + schedule handler result relay
        if method == "post_message":
            self._queue_rpc_response("ok", call_id, "str")
            if not self._auto_relay:
                return
            args = req.get("arguments", {})
            if not isinstance(args, dict):
                args = {}
            body: dict[str, Any] = args.get("body", {})
            if not isinstance(body, dict):
                body = {}
            rid: str = args.get("request_id", "")
            handler_body = (
                self._handler_result_factory(body)
                if self._handler_result_factory is not None
                else self._default_result
            )
            self.inject_receive_envelope(handler_body, request_id=rid)
            return

        # Unknown method -- respond with a stub to avoid hanging the client.
        self._queue_rpc_response(None, call_id, "none")

    def _queue_rpc_response(self, result: Any, call_id: str, result_type: str) -> None:
        self._incoming.put_nowait(
            dumps_message(
                response_message(result, call_id=call_id, result_type=result_type)
            )
        )
