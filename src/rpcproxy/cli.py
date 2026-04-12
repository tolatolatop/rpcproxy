"""Command-line entry for rpcproxy."""

from __future__ import annotations

import asyncio
import sys
from pathlib import Path

import click

from rpcproxy.adb_loop import run_adb
from rpcproxy.cli_post import register_post_command
from rpcproxy.demo_loop import run_demo
from rpcproxy.handlers.adb_handler import AdbHandlerConfig
from rpcproxy.handlers.playwright_handler import PlaywrightHandlerConfig
from rpcproxy.logging_config import setup_logging
from rpcproxy.playwright_loop import run_playwright


def _max_inflight_option_callback(
    _ctx: click.Context, _param: click.Parameter, value: int
) -> int:
    if value < 1:
        raise click.BadParameter("must be >= 1")
    return value


def _shell_timeout_option_callback(
    _ctx: click.Context, _param: click.Parameter, value: float
) -> float:
    if value <= 0:
        raise click.BadParameter("must be positive")
    return value


@click.group()
@click.version_option(package_name="rpcproxy")
def main() -> None:
    """rpcproxy — JSON-RPC over WebSocket (demo client)."""
    setup_logging()


register_post_command(main)


@main.command("demo")
@click.argument("url")
def demo_cmd(url: str) -> None:
    """Connect to WS_URL using DemoRpcProxyClient (HandlerPostMessageClient echo demo).

    After connect, sends one ``set_state`` with a random ``token``. Inbound
    ``receive_envelope`` is logged and echoed via ``post_message`` (body gains
    ``is_echo``). ``_ping_`` / ``_get_channel_id_`` are answered by the base class.
    Unmatched JSON objects are logged at WARNING. Press Ctrl+C to exit.
    """
    try:
        asyncio.run(run_demo(url))
    except KeyboardInterrupt:
        pass


@main.command("playwright")
@click.argument("url")
@click.option(
    "--channel",
    default="msedge",
    show_default=True,
    help="Chromium channel passed to Playwright launch",
)
@click.option(
    "--headless/--no-headless",
    default=True,
    show_default=True,
    help="Run browser headless",
)
@click.option(
    "--max-inflight",
    type=int,
    default=8,
    show_default=True,
    help="Max concurrent receive_envelope handler runs",
    callback=_max_inflight_option_callback,
)
def playwright_cmd(url: str, channel: str, headless: bool, max_inflight: int) -> None:
    """Connect with PlaywrightRpcProxyClient; inbound bodies use command open_page / request / …."""
    try:
        import playwright  # noqa: F401
    except ImportError:
        click.echo(
            "error: playwright is not installed; use: pip install 'rpcproxy[playwright]' "
            "then: playwright install msedge",
            err=True,
        )
        sys.exit(1)
    try:
        cfg = PlaywrightHandlerConfig(channel=channel, headless=headless)
        asyncio.run(
            run_playwright(url, playwright_config=cfg, max_inflight=max_inflight)
        )
    except KeyboardInterrupt:
        raise SystemExit(130) from None
    except Exception as e:
        click.echo(f"error: {e}", err=True)
        sys.exit(1)


@main.command("adb")
@click.argument("url")
@click.option("--serial", default=None, help="adb -s SERIAL (default device if omitted)")
@click.option("--adb-bin", default="adb", show_default=True, help="adb executable path or name")
@click.option(
    "--screenshot-dir",
    type=click.Path(path_type=Path, file_okay=False, writable=True),
    default=None,
    help="Directory for screenshot PNG files (default: user cache dir)",
)
@click.option(
    "--shell-timeout",
    type=float,
    default=60.0,
    show_default=True,
    help="Seconds to wait for each persistent-shell command",
    callback=_shell_timeout_option_callback,
)
@click.option(
    "--max-inflight",
    type=int,
    default=8,
    show_default=True,
    help="Max concurrent receive_envelope handler runs",
    callback=_max_inflight_option_callback,
)
def adb_cmd(
    url: str,
    serial: str | None,
    adb_bin: str,
    screenshot_dir: Path | None,
    shell_timeout: float,
    max_inflight: int,
) -> None:
    """Connect with AdbRpcProxyClient; body.command ping / screenshot / tap / swipe / input_text."""
    cfg = AdbHandlerConfig(
        serial=serial,
        adb_bin=adb_bin,
        shell_command_timeout=shell_timeout,
    )
    if screenshot_dir is not None:
        cfg.screenshot_dir = screenshot_dir
    try:
        asyncio.run(run_adb(url, adb_config=cfg, max_inflight=max_inflight))
    except KeyboardInterrupt:
        raise SystemExit(130) from None
    except Exception as e:
        click.echo(f"error: {e}", err=True)
        sys.exit(1)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(130)
