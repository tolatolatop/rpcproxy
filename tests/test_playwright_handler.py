"""Playwright handler: validation and mocked session (no real browser)."""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from rpcproxy.handlers.playwright_handler import (
    PlaywrightHandlerConfig,
    PlaywrightRpcProxyClient,
    PlaywrightSession,
    make_playwright_handler,
)


@pytest.mark.asyncio
async def test_handle_command_missing_command() -> None:
    s = PlaywrightSession()
    out = await s.handle_command({})
    assert out["ok"] is False
    assert "command" in out["error"].lower() or out.get("command") is None


@pytest.mark.asyncio
async def test_handle_command_import_error_message(monkeypatch: pytest.MonkeyPatch) -> None:
    import builtins

    real_import = builtins.__import__

    def fake_import(
        name: str,
        globals: dict[str, Any] | None = None,
        locals: dict[str, Any] | None = None,
        fromlist: tuple[str, ...] = (),
        level: int = 0,
    ) -> Any:
        if name == "playwright" or name.startswith("playwright."):
            raise ImportError("blocked for test")
        return real_import(name, globals, locals, fromlist, level)

    monkeypatch.setattr(builtins, "__import__", fake_import)
    s = PlaywrightSession()
    out = await s.handle_command({"command": "open_page", "url": "https://x.test"})
    assert out["ok"] is False
    assert "playwright" in out["error"].lower()


@pytest.mark.asyncio
async def test_open_page_mocked_context() -> None:
    session = PlaywrightSession()

    class FakePage:
        def __init__(self) -> None:
            self.goto_url: str | None = None

        async def goto(self, url: str) -> None:
            self.goto_url = url

        async def close(self) -> None:
            pass

    fake_page = FakePage()

    class FakeContext:
        async def new_page(self) -> FakePage:
            return fake_page

    fc = FakeContext()

    async def fake_ensure() -> None:
        session._browser = object()
        session._playwright = object()
        session._context = fc

    session.ensure_started = fake_ensure  # type: ignore[method-assign]

    out = await session.handle_command({"command": "open_page", "url": "https://example.com/"})
    assert out["ok"] is True
    assert out["url"] == "https://example.com/"
    assert "page_id" in out
    assert fake_page.goto_url == "https://example.com/"
    assert out["page_id"] in session._pages


@pytest.mark.asyncio
async def test_execute_js_and_close_page() -> None:
    session = PlaywrightSession()

    class FakePage:
        async def evaluate(self, script: str) -> Any:
            return {"s": script}

        async def close(self) -> None:
            pass

    fp = FakePage()
    pid = "abc123"

    class FakeContext:
        async def new_page(self) -> FakePage:
            return fp

    async def fake_ensure() -> None:
        session._browser = object()
        session._playwright = object()
        session._context = FakeContext()

    session.ensure_started = fake_ensure  # type: ignore[method-assign]
    session._pages[pid] = fp

    out_js = await session.handle_command(
        {"command": "execute_js", "page_id": pid, "script": "1+1"}
    )
    assert out_js["ok"] is True
    assert out_js["result"] == {"s": "1+1"}

    out_close = await session.handle_command({"command": "close_page", "page_id": pid})
    assert out_close["ok"] is True
    assert pid not in session._pages


@pytest.mark.asyncio
async def test_request_mock_fetch() -> None:
    session = PlaywrightSession()

    class FakeResp:
        status = 201
        headers = {"X-Test": "1"}

        async def body(self) -> bytes:
            return b'{"a":1}'

    fake_req = MagicMock()
    fake_req.fetch = AsyncMock(return_value=FakeResp())

    class FakeContext:
        request = fake_req

    async def fake_ensure() -> None:
        session._browser = object()
        session._playwright = object()
        session._context = FakeContext()

    session.ensure_started = fake_ensure  # type: ignore[method-assign]

    out = await session.handle_command(
        {
            "command": "request",
            "url": "https://api.example/x",
            "method": "post",
            "json": {"k": "v"},
        }
    )
    assert out["ok"] is True
    assert out["status"] == 201
    assert out["body"] == '{"a":1}'
    assert out["body_encoding"] == "utf-8"
    fake_req.fetch.assert_awaited_once()
    call_kw = fake_req.fetch.await_args
    assert call_kw[0][0] == "https://api.example/x"
    assert call_kw[1]["method"] == "POST"
    assert "application/json" in call_kw[1]["headers"]["Content-Type"]


@pytest.mark.asyncio
async def test_make_playwright_handler_empty_body() -> None:
    session = PlaywrightSession()
    h = make_playwright_handler(session)
    r = await h(
        message_type="",
        kind="",
        client_id="",
        sender="s",
        receiver="",
        body=None,
        request_id="rid",
        arguments={},
    )
    assert r.body["ok"] is False


def test_playwright_client_has_session() -> None:
    c = PlaywrightRpcProxyClient(playwright_config=PlaywrightHandlerConfig(headless=True))
    assert isinstance(c._pw_session, PlaywrightSession)
