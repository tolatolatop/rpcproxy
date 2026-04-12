"""ADB-backed :class:`~rpcproxy.client.handler_client.EnvelopeHandler` (persistent shell + exec-out screenshot)."""

from __future__ import annotations

import asyncio
import logging
import re
import secrets
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from platformdirs import user_cache_dir

from rpcproxy.client.envelope_types import ReceiveEnvelopeArguments
from rpcproxy.client.handler_client import (
    EnvelopeHandler,
    HandlerPostMessageClient,
    HandlerResult,
)

logger = logging.getLogger(__name__)

_SHELL_MARKER = "__ADB_RPC_END__"
_MAX_INPUT_TEXT_LEN = 256
_DEFAULT_SHELL_TIMEOUT = 60.0
_DEFAULT_ONESHOT_TIMEOUT = 30.0
_SCREENSHOT_TIMEOUT = 120.0

_ADB_READ_LOOP_SH_C = (
    "while IFS= read -r line; do "
    f'sh -c "$line"; printf "\\n{_SHELL_MARKER}\\n"; '
    "done"
)

_MARKER_BYTES = f"\n{_SHELL_MARKER}\n".encode("ascii")


def _default_screenshot_dir() -> Path:
    return Path(user_cache_dir("rpcproxy", appauthor=False)) / "adb_screenshots"


@dataclass
class AdbHandlerConfig:
    """ADB paths, optional device serial, screenshot directory, shell I/O timeout."""

    serial: str | None = None
    adb_bin: str = "adb"
    screenshot_dir: Path = field(default_factory=_default_screenshot_dir)
    shell_command_timeout: float = _DEFAULT_SHELL_TIMEOUT


def _shell_single_quote(s: str) -> str:
    return "'" + s.replace("'", "'\\''") + "'"


def _escape_input_text_for_android(text: str) -> str:
    """Spaces -> %s for ``input text``; reject unsupported characters."""
    if len(text) > _MAX_INPUT_TEXT_LEN:
        raise ValueError(f"text length must be <= {_MAX_INPUT_TEXT_LEN}")
    allowed = re.compile(r"^[a-zA-Z0-9!@#$%^&*()_+\-=\[\]{};:,./?|\\`~ ]+$")
    if not allowed.match(text):
        raise ValueError(
            "text contains unsupported characters for adb input text (use letters, digits, common punctuation, space)"
        )
    return text.replace(" ", "%s")


def _safe_filename(name: str) -> bool:
    if not name or name != Path(name).name:
        return False
    if ".." in name or "/" in name or "\\" in name:
        return False
    return True


class AdbSession:
    """One device: persistent ``adb shell`` for ``input`` commands; ``exec-out`` for screenshots."""

    def __init__(self, config: AdbHandlerConfig | None = None) -> None:
        self._config = config or AdbHandlerConfig()
        self._lock = asyncio.Lock()
        self._proc: asyncio.subprocess.Process | None = None
        self._stderr_drain: asyncio.Task[None] | None = None
        self._shell_rx_buf = bytearray()

    def _base_args(self) -> list[str]:
        args = [self._config.adb_bin]
        if self._config.serial:
            args.extend(["-s", self._config.serial])
        return args

    async def close(self) -> None:
        async with self._lock:
            await self._stop_shell_unlocked()

    async def _stop_shell_unlocked(self) -> None:
        if self._stderr_drain is not None:
            self._stderr_drain.cancel()
            try:
                await self._stderr_drain
            except asyncio.CancelledError:
                pass
            self._stderr_drain = None
        if self._proc is not None:
            if self._proc.returncode is None:
                self._proc.terminate()
                try:
                    await asyncio.wait_for(self._proc.wait(), timeout=5.0)
                except TimeoutError:
                    self._proc.kill()
                    await self._proc.wait()
            self._proc = None
        self._shell_rx_buf.clear()

    async def _run_adb_oneshot(
        self, tail: list[str], *, timeout: float = _DEFAULT_ONESHOT_TIMEOUT
    ) -> tuple[int, bytes]:
        proc = await asyncio.create_subprocess_exec(
            *self._base_args(),
            *tail,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
        )
        out, _ = await asyncio.wait_for(proc.communicate(), timeout=timeout)
        code = proc.returncode if proc.returncode is not None else -1
        return code, out

    async def _ensure_device(self) -> None:
        code, out = await self._run_adb_oneshot(["get-state"])
        state = out.decode(errors="replace").strip()
        if code != 0 or state != "device":
            raise RuntimeError(
                f"adb device not ready (exit={code}, state={state!r}); check `adb devices`"
            )

    async def _serial_for_display(self) -> str:
        code, out = await self._run_adb_oneshot(["get-serialno"])
        if code == 0 and out.strip():
            return out.decode(errors="replace").strip()
        return self._config.serial or ""

    async def _start_shell_unlocked(self) -> None:
        await self._ensure_device()
        self._shell_rx_buf.clear()
        self._proc = await asyncio.create_subprocess_exec(
            *self._base_args(),
            "shell",
            "sh",
            "-c",
            _ADB_READ_LOOP_SH_C,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        if self._proc.stdin is None or self._proc.stdout is None:
            raise RuntimeError("adb shell missing stdio pipes")

        async def _drain_stderr() -> None:
            assert self._proc is not None and self._proc.stderr is not None
            try:
                while True:
                    line = await self._proc.stderr.readline()
                    if not line:
                        break
                    logger.debug("adb shell stderr: %s", line.decode(errors="replace").rstrip())
            except asyncio.CancelledError:
                raise

        self._stderr_drain = asyncio.create_task(_drain_stderr())

    async def _ensure_shell_unlocked(self) -> None:
        if self._proc is not None and self._proc.returncode is None:
            return
        await self._stop_shell_unlocked()
        await self._start_shell_unlocked()

    async def _run_shell_line(self, line: str) -> str:
        if "\n" in line or "\r" in line:
            raise ValueError("command must be a single line")
        proc = self._proc
        if proc is None or proc.stdin is None or proc.stdout is None:
            raise RuntimeError("adb shell not started")
        if proc.returncode is not None:
            raise RuntimeError("adb shell not running")

        proc.stdin.write((line + "\n").encode("utf-8"))
        await proc.stdin.drain()

        deadline = asyncio.get_event_loop().time() + self._config.shell_command_timeout
        while _MARKER_BYTES not in self._shell_rx_buf:
            timeout = max(0.1, deadline - asyncio.get_event_loop().time())
            try:
                chunk = await asyncio.wait_for(proc.stdout.read(4096), timeout=timeout)
            except TimeoutError:
                await self._stop_shell_unlocked()
                raise RuntimeError("adb shell command timed out") from None
            if not chunk:
                await self._stop_shell_unlocked()
                raise RuntimeError("adb shell closed unexpectedly")
            self._shell_rx_buf.extend(chunk)
            if len(self._shell_rx_buf) > 2_000_000:
                await self._stop_shell_unlocked()
                raise RuntimeError("adb shell output too large")

        idx = self._shell_rx_buf.index(_MARKER_BYTES)
        before = bytes(self._shell_rx_buf[:idx]).decode("utf-8", errors="replace")
        del self._shell_rx_buf[: idx + len(_MARKER_BYTES)]
        return before

    async def _exec_out_screencap(self) -> bytes:
        proc = await asyncio.create_subprocess_exec(
            *self._base_args(),
            "exec-out",
            "screencap",
            "-p",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        out, err = await asyncio.wait_for(proc.communicate(), timeout=_SCREENSHOT_TIMEOUT)
        code = proc.returncode if proc.returncode is not None else -1
        if code != 0:
            msg = err.decode(errors="replace").strip() or out.decode(errors="replace")[:200]
            raise RuntimeError(f"screencap failed (exit={code}): {msg}")
        if len(out) < 8 or not (
            out.startswith(b"\x89PNG\r\n") or out.startswith(b"\x89PNG\n")
        ):
            raise RuntimeError("screencap did not return PNG data")
        return out

    async def handle_command(
        self, body: dict[str, Any], *, request_id: str = ""
    ) -> dict[str, Any]:
        cmd = body.get("command")
        if not isinstance(cmd, str) or not cmd.strip():
            return {
                "ok": False,
                "error": "missing or invalid 'command'",
                "command": cmd if isinstance(cmd, str) else None,
            }
        cmd = cmd.strip()

        handlers: dict[str, Any] = {
            "ping": self._cmd_ping,
            "screenshot": self._cmd_screenshot,
            "tap": self._cmd_tap,
            "swipe": self._cmd_swipe,
            "input_text": self._cmd_input_text,
        }
        fn = handlers.get(cmd)
        if fn is None:
            return {"ok": False, "error": f"unknown command: {cmd!r}", "command": cmd}
        try:
            out = await fn(body, request_id=request_id)
        except Exception as e:
            logger.exception("adb session: command %s failed", cmd)
            return {
                "ok": False,
                "error": str(e),
                "error_type": type(e).__name__,
                "command": cmd,
            }
        out.setdefault("command", cmd)
        return out

    async def _cmd_ping(
        self, _body: dict[str, Any], *, request_id: str = ""
    ) -> dict[str, Any]:
        await self._ensure_device()
        serial = await self._serial_for_display()
        return {"ok": True, "serial": serial}

    async def _cmd_screenshot(
        self, body: dict[str, Any], *, request_id: str = ""
    ) -> dict[str, Any]:
        await self._ensure_device()
        raw = await self._exec_out_screencap()
        self._config.screenshot_dir.mkdir(parents=True, exist_ok=True)

        fn_in = body.get("filename")
        if fn_in is not None:
            if not isinstance(fn_in, str) or not _safe_filename(fn_in):
                return {
                    "ok": False,
                    "error": "invalid 'filename' (use a single path segment, no ..)",
                    "command": "screenshot",
                }
            name = fn_in if fn_in.lower().endswith(".png") else f"{fn_in}.png"
        else:
            rid = re.sub(r"[^a-zA-Z0-9_-]+", "_", request_id.strip())[:64]
            suffix = f"_{rid}" if rid else ""
            name = f"screenshot_{int(time.time() * 1000)}{suffix}_{secrets.token_hex(4)}.png"

        path = (self._config.screenshot_dir / name).resolve()
        try:
            path.relative_to(self._config.screenshot_dir.resolve())
        except ValueError:
            return {"ok": False, "error": "invalid screenshot path", "command": "screenshot"}

        path.write_bytes(raw)
        return {"ok": True, "path": str(path), "size_bytes": len(raw)}

    def _parse_int(self, body: dict[str, Any], key: str) -> int | None:
        v = body.get(key)
        if v is None or isinstance(v, bool):
            return None
        if isinstance(v, int):
            return v
        if isinstance(v, float) and v.is_integer():
            return int(v)
        if isinstance(v, str):
            try:
                return int(v.strip(), 10)
            except ValueError:
                return None
        return None

    async def _cmd_tap(self, body: dict[str, Any], *, request_id: str = "") -> dict[str, Any]:
        x = self._parse_int(body, "x")
        y = self._parse_int(body, "y")
        if x is None or y is None:
            return {"ok": False, "error": "'x' and 'y' must be integers", "command": "tap"}
        line = f"input tap {x} {y}"
        async with self._lock:
            await self._ensure_shell_unlocked()
            out = await self._run_shell_line(line)
        return {"ok": True, "x": x, "y": y, "shell_output": out.strip()}

    async def _cmd_swipe(self, body: dict[str, Any], *, request_id: str = "") -> dict[str, Any]:
        x1 = self._parse_int(body, "x1")
        y1 = self._parse_int(body, "y1")
        x2 = self._parse_int(body, "x2")
        y2 = self._parse_int(body, "y2")
        if None in (x1, y1, x2, y2):
            return {
                "ok": False,
                "error": "x1,y1,x2,y2 must be integers",
                "command": "swipe",
            }
        dur = self._parse_int(body, "duration_ms")
        if dur is None:
            dur = 300
        if dur < 0 or dur > 60000:
            return {
                "ok": False,
                "error": "duration_ms must be between 0 and 60000",
                "command": "swipe",
            }
        line = f"input swipe {x1} {y1} {x2} {y2} {dur}"
        async with self._lock:
            await self._ensure_shell_unlocked()
            out = await self._run_shell_line(line)
        return {
            "ok": True,
            "x1": x1,
            "y1": y1,
            "x2": x2,
            "y2": y2,
            "duration_ms": dur,
            "shell_output": out.strip(),
        }

    async def _cmd_input_text(
        self, body: dict[str, Any], *, request_id: str = ""
    ) -> dict[str, Any]:
        text = body.get("text")
        if not isinstance(text, str):
            return {"ok": False, "error": "'text' must be a string", "command": "input_text"}
        try:
            escaped = _escape_input_text_for_android(text)
        except ValueError as e:
            return {"ok": False, "error": str(e), "command": "input_text"}
        inner = _shell_single_quote(escaped)
        line = f"input text {inner}"
        async with self._lock:
            await self._ensure_shell_unlocked()
            out = await self._run_shell_line(line)
        return {"ok": True, "shell_output": out.strip()}


def make_adb_handler(session: AdbSession) -> EnvelopeHandler:
    """Build an :class:`~rpcproxy.client.handler_client.EnvelopeHandler` bound to ``session``."""

    async def adb_envelope_handler(
        *,
        message_type: str,
        kind: str,
        client_id: str,
        sender: str,
        receiver: str,
        body: dict[str, Any] | None,
        request_id: str,
        arguments: ReceiveEnvelopeArguments,
        **extra: object,
    ) -> HandlerResult:
        if not body:
            return HandlerResult(
                body={
                    "ok": False,
                    "error": "missing body",
                    "command": None,
                }
            )
        out = await session.handle_command(body, request_id=request_id)
        return HandlerResult(body=out)

    return adb_envelope_handler


class AdbRpcProxyClient(HandlerPostMessageClient):
    """Handler client with ADB session; call ``await close()`` to release the shell process."""

    def __init__(
        self,
        *,
        adb_config: AdbHandlerConfig | None = None,
        **kwargs: Any,
    ) -> None:
        self._adb_session = AdbSession(adb_config)
        super().__init__(make_adb_handler(self._adb_session), **kwargs)

    async def close(self) -> None:
        await super().close()
        await self._adb_session.close()
