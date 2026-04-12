"""CLI: HandlerPostMessageClient with Playwright-backed receive_envelope handler."""

from __future__ import annotations

import logging
from typing import Any

from rpcproxy.handlers.playwright_handler import (
    PlaywrightHandlerConfig,
    PlaywrightRpcProxyClient,
)

logger = logging.getLogger(__name__)


async def run_playwright(
    uri: str,
    *,
    playwright_config: PlaywrightHandlerConfig | None = None,
    **client_kwargs: Any,
) -> None:
    client = PlaywrightRpcProxyClient(playwright_config=playwright_config, **client_kwargs)
    try:
        await client.connect(uri)
        logger.info(
            "playwright client connected; receive_envelope bodies are handled as Playwright commands"
        )
        await client.wait_until_disconnected()
    finally:
        await client.close()
