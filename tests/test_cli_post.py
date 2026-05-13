"""CLI ``post`` subcommand validation (no live WebSocket)."""

from __future__ import annotations

import io
from unittest.mock import AsyncMock

import pytest
from click.testing import CliRunner

from rpcproxy.cli import main
from rpcproxy.cli_post import _PostCliClient, body_option_callback, run_post


def test_post_body_invalid_json_exits_2() -> None:
    runner = CliRunner()
    r = runner.invoke(main, ["post", "ws://127.0.0.1:1/rpc", "--body", "{not-json"])
    assert r.exit_code == 2


def test_post_body_must_be_json_object() -> None:
    runner = CliRunner()
    r = runner.invoke(main, ["post", "ws://127.0.0.1:1/rpc", "--body", "[]"])
    assert r.exit_code == 2
    combined = (r.stdout or "") + (r.stderr or "")
    assert "object" in combined.lower()


def test_post_timeout_must_be_positive() -> None:
    runner = CliRunner()
    r = runner.invoke(main, ["post", "ws://127.0.0.1:1/rpc", "--timeout", "0"])
    assert r.exit_code == 2


def test_post_body_from_stdin_dash(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("rpcproxy.cli_post.sys.stdin", io.StringIO('{"hello": "name"}'))
    assert body_option_callback(None, None, "-") == {"hello": "name"}


def test_post_body_from_stdin_at_dash(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("rpcproxy.cli_post.sys.stdin", io.StringIO('{"x": 1}\n'))
    assert body_option_callback(None, None, "@-") == {"x": 1}


def test_post_body_from_file(tmp_path) -> None:
    path = tmp_path / "body.json"
    path.write_text('{"a": true}', encoding="utf-8")
    assert body_option_callback(None, None, f"@{path}") == {"a": True}


async def test_run_post_uses_post_message_auto(monkeypatch: pytest.MonkeyPatch) -> None:
    connect = AsyncMock()
    wait_relay_predicate = AsyncMock(return_value={"ok": True, "arguments": {"request_id": "rid-1"}})
    post_message_auto = AsyncMock(return_value={"chunked": True})
    close = AsyncMock()

    monkeypatch.setattr(_PostCliClient, "connect", connect)
    monkeypatch.setattr(_PostCliClient, "wait_relay_predicate", wait_relay_predicate)
    monkeypatch.setattr(_PostCliClient, "post_message_auto", post_message_auto)
    monkeypatch.setattr(_PostCliClient, "close", close)

    rid, post_result, receipt = await run_post(
        "ws://example.test/rpc",
        "peer-a",
        {"payload": "x" * 8},
        "rid-1",
        3.0,
    )

    assert rid == "rid-1"
    post_message_auto.assert_awaited_once_with(
        receiver="peer-a",
        body={"payload": "x" * 8},
        request_id="rid-1",
    )
    assert post_result == {"chunked": True}
    assert receipt == {"ok": True, "arguments": {"request_id": "rid-1"}}
    close.assert_awaited_once()
