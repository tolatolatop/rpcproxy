"""ADB handler: validation and mocked adb / shell (no device)."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import AsyncMock

import pytest

from rpcproxy.handlers.adb_handler import (
    AdbHandlerConfig,
    AdbRpcProxyClient,
    AdbSession,
    _escape_input_text_for_android,
    _safe_filename,
    make_adb_handler,
)


def test_safe_filename() -> None:
    assert _safe_filename("a.png") is True
    assert _safe_filename("../x") is False
    assert _safe_filename("a/b") is False


def test_escape_input_text_spaces() -> None:
    assert _escape_input_text_for_android("a b") == "a%sb"


def test_escape_input_text_rejects_bad_chars() -> None:
    with pytest.raises(ValueError):
        _escape_input_text_for_android("你好")


@pytest.mark.asyncio
async def test_ping_uses_oneshot(monkeypatch: pytest.MonkeyPatch) -> None:
    s = AdbSession(AdbHandlerConfig(adb_bin="adb"))

    async def fake_oneshot(
        tail: list[str], *, timeout: float = 30.0
    ) -> tuple[int, bytes]:
        if tail == ["get-state"]:
            return (0, b"device\n")
        if tail == ["get-serialno"]:
            return (0, b"emulator-5554\n")
        return (1, b"err")

    monkeypatch.setattr(s, "_run_adb_oneshot", fake_oneshot)
    out = await s.handle_command({"command": "ping"})
    assert out["ok"] is True
    assert out["serial"] == "emulator-5554"


@pytest.mark.asyncio
async def test_screenshot_writes_png(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    s = AdbSession(AdbHandlerConfig(screenshot_dir=tmp_path, adb_bin="adb"))
    png = b"\x89PNG\r\n\x1a\n" + b"\x00" * 64

    async def fake_oneshot(
        tail: list[str], *, timeout: float = 30.0
    ) -> tuple[int, bytes]:
        return (0, b"device\n")

    async def fake_cap() -> bytes:
        return png

    monkeypatch.setattr(s, "_run_adb_oneshot", fake_oneshot)
    monkeypatch.setattr(s, "_exec_out_screencap", fake_cap)
    out = await s.handle_command({"command": "screenshot"}, request_id="my-rid")
    assert out["ok"] is True
    p = Path(out["path"])
    assert p.is_file()
    assert p.read_bytes() == png
    assert out["size_bytes"] == len(png)


@pytest.mark.asyncio
async def test_tap_swipe_input_via_shell_line(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    s = AdbSession()
    lines: list[str] = []

    async def _noop_ensure() -> None:
        return

    async def _capture_line(line: str) -> str:
        lines.append(line)
        return ""

    monkeypatch.setattr(s, "_ensure_shell_unlocked", _noop_ensure)
    monkeypatch.setattr(s, "_run_shell_line", _capture_line)

    t = await s.handle_command({"command": "tap", "x": 1, "y": 2})
    assert t["ok"] is True
    assert lines[-1] == "input tap 1 2"

    sw = await s.handle_command(
        {"command": "swipe", "x1": 0, "y1": 0, "x2": 10, "y2": 20, "duration_ms": 100}
    )
    assert sw["ok"] is True
    assert lines[-1] == "input swipe 0 0 10 20 100"

    tx = await s.handle_command({"command": "input_text", "text": "a b"})
    assert tx["ok"] is True
    assert "input text " in lines[-1]
    assert "a%sb" in lines[-1]


@pytest.mark.asyncio
async def test_make_adb_handler_missing_body() -> None:
    s = AdbSession()
    h = make_adb_handler(s)
    r = await h(
        message_type="",
        kind="",
        client_id="",
        sender="s",
        receiver="",
        body=None,
        request_id="r",
        arguments={},
    )
    assert r.body["ok"] is False


def test_adb_client_has_session() -> None:
    c = AdbRpcProxyClient(adb_config=AdbHandlerConfig())
    assert isinstance(c._adb_session, AdbSession)
