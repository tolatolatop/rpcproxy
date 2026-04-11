"""Playwright-backed :class:`~rpcproxy.client.handler_client.EnvelopeHandler` for receive_envelope bodies."""

from __future__ import annotations

import asyncio
import base64
import json
import logging
import secrets
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from requests import Request, Session

if TYPE_CHECKING:
    from playwright.async_api import Browser, BrowserContext, Page, Playwright

from rpcproxy.client.envelope_types import ReceiveEnvelopeArguments
from rpcproxy.client.handler_client import (
    EnvelopeHandler,
    HandlerPostMessageClient,
    HandlerResult,
)

logger = logging.getLogger(__name__)

_MAX_RESPONSE_BYTES = 256 * 1024
_PAGE_ID_NBYTES = 3  # secrets.token_hex -> 6-char page_id


@dataclass
class PlaywrightHandlerConfig:
    """Defaults: Edge (Chromium ``msedge`` channel), headless."""

    channel: str = "msedge"
    headless: bool = True


def _json_safe_result(value: Any) -> Any:
    try:
        json.dumps(value)
    except (TypeError, ValueError):
        return str(value)
    return value


# Keyword names accepted by :class:`requests.Request` (RPC ``command`` etc. are omitted).
_REQUEST_KW_KEYS = frozenset(
    {
        "method",
        "url",
        "headers",
        "files",
        "data",
        "params",
        "auth",
        "cookies",
        "hooks",
        "json",
    }
)


def _prepare_fetch_from_request_body(
    body: dict[str, Any],
) -> tuple[str, dict[str, Any]] | str:
    req_kw: dict[str, Any] = {k: body[k] for k in _REQUEST_KW_KEYS if k in body}
    if "method" not in req_kw:
        req_kw["method"] = "GET"
    u = req_kw.get("url")
    if isinstance(u, str):
        req_kw["url"] = u.strip()
    try:
        prepared = Session().prepare_request(Request(**req_kw))
    except Exception as e:
        return f"{type(e).__name__}: {e}"
    fetch_kw: dict[str, Any] = {
        "method": prepared.method,
        "headers": dict(prepared.headers),
    }
    if prepared.body is not None:
        fetch_kw["data"] = prepared.body
    return prepared.url, fetch_kw


class PlaywrightSession:
    """Lazy async Playwright session: one browser context and ``page_id`` → page map."""

    def __init__(self, config: PlaywrightHandlerConfig | None = None) -> None:
        self._config = config or PlaywrightHandlerConfig()
        self._playwright: Playwright | None = None
        self._browser: Browser | None = None
        self._context: BrowserContext | None = None
        self._pages: dict[str, Page] = {}
        self._lock = asyncio.Lock()

    async def ensure_started(self) -> None:
        async with self._lock:
            if self._browser is not None:
                return
            try:
                from playwright.async_api import async_playwright
            except ImportError as e:
                raise RuntimeError(
                    "playwright is not installed; use: pip install 'rpcproxy[playwright]' "
                    "then: playwright install msedge"
                ) from e
            self._playwright = await async_playwright().start()
            self._browser = await self._playwright.chromium.launch(
                channel=self._config.channel,
                headless=self._config.headless,
            )
            self._context = await self._browser.new_context()

    async def close(self) -> None:
        async with self._lock:
            for page in list(self._pages.values()):
                try:
                    await page.close()
                except Exception:
                    logger.debug("playwright session: page.close failed", exc_info=True)
            self._pages.clear()
            if self._context is not None:
                try:
                    await self._context.close()
                except Exception:
                    logger.debug("playwright session: context.close failed", exc_info=True)
                self._context = None
            if self._browser is not None:
                try:
                    await self._browser.close()
                except Exception:
                    logger.debug("playwright session: browser.close failed", exc_info=True)
                self._browser = None
            if self._playwright is not None:
                try:
                    await self._playwright.stop()
                except Exception:
                    logger.debug("playwright session: playwright.stop failed", exc_info=True)
                self._playwright = None

    async def handle_command(self, body: dict[str, Any]) -> dict[str, Any]:
        cmd = body.get("command")
        if not isinstance(cmd, str) or not cmd.strip():
            return {
                "ok": False,
                "error": "missing or invalid 'command'",
                "command": cmd if isinstance(cmd, str) else None,
            }
        cmd = cmd.strip()
        try:
            await self.ensure_started()
        except RuntimeError as e:
            return {"ok": False, "error": str(e), "error_type": "RuntimeError", "command": cmd}
        except Exception as e:
            logger.exception("playwright session: ensure_started failed")
            return {
                "ok": False,
                "error": str(e),
                "error_type": type(e).__name__,
                "command": cmd,
            }

        handlers: dict[str, Any] = {
            "open_page": self._cmd_open_page,
            "execute_js": self._cmd_execute_js,
            "request": self._cmd_request,
            "close_page": self._cmd_close_page,
        }
        fn = handlers.get(cmd)
        if fn is None:
            return {
                "ok": False,
                "error": f"unknown command: {cmd!r}",
                "command": cmd,
            }
        try:
            out = await fn(body)
        except Exception as e:
            logger.exception("playwright session: command %s failed", cmd)
            return {
                "ok": False,
                "error": str(e),
                "error_type": type(e).__name__,
                "command": cmd,
            }
        out.setdefault("command", cmd)
        return out

    async def _cmd_open_page(self, body: dict[str, Any]) -> dict[str, Any]:
        url = body.get("url")
        if not isinstance(url, str) or not url.strip():
            return {"ok": False, "error": "'url' must be a non-empty string", "command": "open_page"}
        url = url.strip()
        assert self._context is not None
        async with self._lock:
            page = await self._context.new_page()
            while True:
                pid = secrets.token_hex(_PAGE_ID_NBYTES)
                if pid not in self._pages:
                    break
            self._pages[pid] = page
        try:
            await page.goto(url)
        except Exception:
            async with self._lock:
                self._pages.pop(pid, None)
            try:
                await page.close()
            except Exception:
                pass
            raise
        return {"ok": True, "page_id": pid, "url": url}

    async def _cmd_execute_js(self, body: dict[str, Any]) -> dict[str, Any]:
        page_id = body.get("page_id")
        script = body.get("script")
        if not isinstance(page_id, str) or not page_id.strip():
            return {
                "ok": False,
                "error": "'page_id' must be a non-empty string",
                "command": "execute_js",
            }
        if not isinstance(script, str):
            return {"ok": False, "error": "'script' must be a string", "command": "execute_js"}
        page_id = page_id.strip()
        async with self._lock:
            page = self._pages.get(page_id)
        if page is None:
            return {
                "ok": False,
                "error": f"unknown page_id: {page_id!r}",
                "command": "execute_js",
            }
        result = await page.evaluate(script)
        return {"ok": True, "result": _json_safe_result(result), "page_id": page_id}

    async def _cmd_request(self, body: dict[str, Any]) -> dict[str, Any]:
        prepared = _prepare_fetch_from_request_body(body)
        if isinstance(prepared, str):
            return {
                "ok": False,
                "error": prepared,
                "error_type": "RequestPrepareError",
                "command": "request",
            }
        fetch_url, fetch_kwargs = prepared

        assert self._context is not None
        req = self._context.request
        resp = await req.fetch(fetch_url, **fetch_kwargs)
        raw = await resp.body()
        truncated = len(raw) > _MAX_RESPONSE_BYTES
        chunk = raw[:_MAX_RESPONSE_BYTES] if truncated else raw
        try:
            body_text = chunk.decode("utf-8")
            body_encoding = "utf-8"
        except UnicodeDecodeError:
            body_text = base64.b64encode(chunk).decode("ascii")
            body_encoding = "base64"

        hdr_map: dict[str, str] = {}
        for k, v in resp.headers.items():
            hdr_map[str(k)] = str(v)

        return {
            "ok": True,
            "status": resp.status,
            "headers": hdr_map,
            "body": body_text,
            "body_encoding": body_encoding,
            "truncated": truncated,
        }

    async def _cmd_close_page(self, body: dict[str, Any]) -> dict[str, Any]:
        page_id = body.get("page_id")
        if not isinstance(page_id, str) or not page_id.strip():
            return {
                "ok": False,
                "error": "'page_id' must be a non-empty string",
                "command": "close_page",
            }
        page_id = page_id.strip()
        async with self._lock:
            page = self._pages.pop(page_id, None)
        if page is None:
            return {
                "ok": False,
                "error": f"unknown page_id: {page_id!r}",
                "command": "close_page",
            }
        await page.close()
        return {"ok": True, "page_id": page_id}


def make_playwright_handler(session: PlaywrightSession) -> EnvelopeHandler:
    """Build an :class:`~rpcproxy.client.handler_client.EnvelopeHandler` bound to ``session``."""

    async def playwright_envelope_handler(
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
        out = await session.handle_command(body)
        return HandlerResult(body=out)

    return playwright_envelope_handler


class PlaywrightRpcProxyClient(HandlerPostMessageClient):
    """Handler client with Playwright session; call ``await close()`` to release browsers."""

    def __init__(
        self,
        *,
        playwright_config: PlaywrightHandlerConfig | None = None,
        **kwargs: Any,
    ) -> None:
        self._pw_session = PlaywrightSession(playwright_config)
        super().__init__(make_playwright_handler(self._pw_session), **kwargs)

    async def close(self) -> None:
        await super().close()
        await self._pw_session.close()
