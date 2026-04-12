"""CLI ``playwright`` subcommand (no live WebSocket in most tests)."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest
from click.testing import CliRunner

from rpcproxy.cli import main


def test_playwright_help_shows_options() -> None:
    runner = CliRunner()
    r = runner.invoke(main, ["playwright", "--help"])
    assert r.exit_code == 0
    out = r.output
    assert "--channel" in out
    assert "headless" in out.lower()
    assert "max-inflight" in out


def test_playwright_max_inflight_must_be_positive() -> None:
    runner = CliRunner()
    r = runner.invoke(main, ["playwright", "ws://127.0.0.1:1/rpc", "--max-inflight", "0"])
    assert r.exit_code == 2


@pytest.mark.asyncio
async def test_run_playwright_calls_close(monkeypatch: pytest.MonkeyPatch) -> None:
    import rpcproxy.playwright_loop as pl

    mock_client = MagicMock()
    mock_client.connect = AsyncMock()
    mock_client.wait_until_disconnected = AsyncMock()
    mock_client.close = AsyncMock()

    monkeypatch.setattr(
        pl,
        "PlaywrightRpcProxyClient",
        MagicMock(return_value=mock_client),
    )
    await pl.run_playwright("ws://example/rpc", max_inflight=3)
    mock_client.connect.assert_awaited_once_with("ws://example/rpc")
    mock_client.wait_until_disconnected.assert_awaited_once()
    mock_client.close.assert_awaited_once()
    pl.PlaywrightRpcProxyClient.assert_called_once()
    call_kw = pl.PlaywrightRpcProxyClient.call_args[1]
    assert call_kw["max_inflight"] == 3
