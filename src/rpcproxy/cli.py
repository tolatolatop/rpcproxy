"""Command-line entry for rpcproxy."""

from __future__ import annotations

import asyncio
import sys

import click

from rpcproxy.demo_loop import run_demo


@click.group()
@click.version_option(package_name="rpcproxy")
def main() -> None:
    """rpcproxy — JSON-RPC over WebSocket (demo client)."""


@main.command("demo")
@click.argument("url")
def demo_cmd(url: str) -> None:
    """Connect to WS_URL using RpcProxyClientBase (fastapi_websocket_rpc RpcMessage).

    After connect, sends one ``set_state`` with a random ``token``. Inbound
    ``receive_envelope`` arguments are printed; ``_ping_`` / ``_get_channel_id_``
    are answered by the base class. Unmatched JSON objects go to stderr. Press Ctrl+C to exit.
    """
    try:
        asyncio.run(run_demo(url))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(130)
