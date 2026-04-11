"""CLI ``post`` subcommand validation (no live WebSocket)."""

from __future__ import annotations

import io

import pytest
from click.testing import CliRunner

from rpcproxy.cli import _body_option_callback, main


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
    monkeypatch.setattr("rpcproxy.cli.sys.stdin", io.StringIO('{"hello": "name"}'))
    assert _body_option_callback(None, None, "-") == {"hello": "name"}


def test_post_body_from_stdin_at_dash(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("rpcproxy.cli.sys.stdin", io.StringIO('{"x": 1}\n'))
    assert _body_option_callback(None, None, "@-") == {"x": 1}


def test_post_body_from_file(tmp_path) -> None:
    path = tmp_path / "body.json"
    path.write_text('{"a": true}', encoding="utf-8")
    assert _body_option_callback(None, None, f"@{path}") == {"a": True}
