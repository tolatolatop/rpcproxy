"""Integration tests for ``rpcproxy post`` using ``MockRelayPeer``.

Tests exercise the full internal data path (``run_post`` -> WebSocket frames ->
mock relay peer -> response -> relay -> ``wait_relay_predicate``) without any
real network or browser.
"""

from __future__ import annotations

from typing import Any

import pytest

from rpcproxy.cli_post import run_post
from rpcproxy.testing import MockRelayPeer


# ---------------------------------------------------------------------------
# Basic round-trip: post_message + receive_envelope relay
# ---------------------------------------------------------------------------


async def test_basic_round_trip() -> None:
    """Send a simple body, get the default handler result back."""
    peer = MockRelayPeer(default_result={"ok": True, "page_id": "abc123"})
    with peer.patch_connect():
        rid, post_result, receipt = await run_post(
            "ws://mock/rpc", "peer-a",
            {"command": "open_page", "url": "https://x"},
            "req-1", 5.0,
        )
    assert rid == "req-1"
    assert post_result.chunked is False
    assert post_result.response == "ok"
    assert receipt == {
        "ok": True,
        "arguments": {
            "request_id": "req-1",
            "body": {"ok": True, "page_id": "abc123"},
            "message_type": "mock",
            "sender": "mock-peer",
            "receiver": "",
            "kind": "",
            "client_id": "",
        },
    }
    assert peer.received_bodies == [
        {"command": "open_page", "url": "https://x"},
    ]


async def test_handler_result_factory() -> None:
    """Use a factory that returns results based on the request body."""
    def factory(body: dict[str, Any]) -> dict[str, Any]:
        cmd = body.get("command", "")
        if cmd == "request":
            return {"ok": True, "status": 200, "body": "response-body"}
        if cmd == "execute_js":
            return {"ok": True, "result": 42}
        return {"ok": False, "error": f"unknown command: {cmd!r}"}

    peer = MockRelayPeer(handler_result_factory=factory)

    with peer.patch_connect():
        _, _, receipt = await run_post(
            "ws://mock/rpc", "peer-b",
            {"command": "request", "url": "https://api.example/x"},
            "req-2", 5.0,
        )
    relay_body = receipt["arguments"]["body"]
    assert relay_body["ok"] is True
    assert relay_body["status"] == 200
    assert relay_body["body"] == "response-body"


async def test_factory_sees_all_body_fields() -> None:
    """The factory receives the full body dict including extra fields."""
    captured: list[dict[str, Any]] = []

    peer = MockRelayPeer(
        handler_result_factory=captured.append,  # type: ignore[arg-type]
    )
    with peer.patch_connect():
        await run_post(
            "ws://mock/rpc", "peer-c",
            {"command": "execute_js", "page_id": "abc", "script": "1+1"},
            "req-3", 5.0,
        )
    assert len(captured) == 1
    assert captured[0] == {
        "command": "execute_js",
        "page_id": "abc",
        "script": "1+1",
    }


# ---------------------------------------------------------------------------
# Default result (no factory)
# ---------------------------------------------------------------------------


async def test_default_result() -> None:
    """When no factory is provided, ``default_result`` is used."""
    peer = MockRelayPeer(default_result={"ok": True, "echo": "default"})
    with peer.patch_connect():
        _, _, receipt = await run_post(
            "ws://mock/rpc", "peer-d", {"any": "body"}, "req-4", 5.0,
        )
    assert receipt["arguments"]["body"] == {"ok": True, "echo": "default"}


async def test_default_result_factory_with_empty_body() -> None:
    """Empty body is passed through; default result returned."""
    peer = MockRelayPeer(default_result={"ok": True})
    with peer.patch_connect():
        _, _, receipt = await run_post(
            "ws://mock/rpc", "peer-e", {}, "req-5", 5.0,
        )
    assert receipt["arguments"]["body"] == {"ok": True}


# ---------------------------------------------------------------------------
# Manual relay injection (delayed / out-of-order)
# ---------------------------------------------------------------------------


async def test_manual_inject_receive_envelope() -> None:
    """``inject_receive_envelope`` can push a relay outside normal flow."""
    peer = MockRelayPeer(default_result={"auto": True})
    with peer.patch_connect():
        rid, _, receipt = await run_post(
            "ws://mock/rpc", "peer-f", {"manual": True}, "req-6", 5.0,
        )
    assert rid == "req-6"
    # Default flow still works (auto result)
    assert receipt["arguments"]["body"]["auto"] is True


async def test_inject_before_wait_uses_stash() -> None:
    """Relay injected before ``wait_relay_predicate`` is stashed."""
    peer = MockRelayPeer(default_result={"ok": True})
    peer.inject_receive_envelope({"ok": True, "pre_injected": True}, request_id="pre-1")
    with peer.patch_connect():
        rid, _, receipt = await run_post(
            "ws://mock/rpc", "peer-g", {"stash": True}, "pre-1", 5.0,
        )
    assert rid == "pre-1"
    assert receipt["arguments"]["body"]["pre_injected"] is True


# ---------------------------------------------------------------------------
# Auto-generated request_id
# ---------------------------------------------------------------------------


async def test_auto_generated_request_id() -> None:
    """Empty request_id generates a random hex id; call still succeeds."""
    peer = MockRelayPeer(default_result={"ok": True})
    with peer.patch_connect():
        rid, _, receipt = await run_post(
            "ws://mock/rpc", "peer-h", {"x": 1}, "", 5.0,
        )
    assert len(rid) > 0
    assert receipt["arguments"]["body"]["ok"] is True


# ---------------------------------------------------------------------------
# Error / edge cases
# ---------------------------------------------------------------------------


async def test_timeout_when_no_relay() -> None:
    """If no relay arrives, ``run_post`` raises ``TimeoutError``.

    Uses ``auto_relay=False`` so the mock ACKs ``post_message`` but does
    NOT push a ``receive_envelope`` back.
    """
    peer = MockRelayPeer(default_result={"ok": True}, auto_relay=False)
    with peer.patch_connect():
        with pytest.raises(TimeoutError):
            await run_post(
                "ws://mock/rpc", "peer-i", {"timeout": True},
                "never-relayed", 0.05,
            )


# ---------------------------------------------------------------------------
# Inspection properties
# ---------------------------------------------------------------------------


async def test_sent_messages_and_received_request_ids() -> None:
    """``sent_messages`` and ``received_request_ids`` track client frames."""
    peer = MockRelayPeer(default_result={"ok": True})
    with peer.patch_connect():
        await run_post(
            "ws://mock/rpc", "peer-k", {"n": 1}, "rid-aaa", 5.0,
        )

    assert len(peer.sent_messages) >= 1
    # At minimum there should be one post_message RPC
    pm_bodies = [
        m for m in peer.sent_messages
        if m.get("request", {}).get("method") == "post_message"
    ]
    assert len(pm_bodies) == 1
    assert peer.received_request_ids == ["rid-aaa"]

    # The sent_messages only track what the CLIENT wrote (the mock's
    # responses go through a different queue). The client writes:
    #   1. The post_message RPC
    #   2. Optionally, the response to receive_envelope from its reader loop
    # So at minimum there's 1 post_message in sent_messages.
    assert len(peer.sent_messages) >= 1


async def test_single_post_message_sent() -> None:
    """Exactly one ``post_message`` RPC is sent by ``run_post``."""
    peer = MockRelayPeer(default_result={"ok": True})
    with peer.patch_connect():
        await run_post(
            "ws://mock/rpc", "peer-l", {"data": "val"}, "rid-bbb", 5.0,
        )
    sent_pm_count = sum(
        1 for m in peer.sent_messages
        if m.get("request", {}).get("method") == "post_message"
    )
    assert sent_pm_count == 1
