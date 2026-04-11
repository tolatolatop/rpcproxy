"""Optional handler implementations (e.g. Playwright)."""

from rpcproxy.handlers.playwright_handler import (
    PlaywrightHandlerConfig,
    PlaywrightRpcProxyClient,
    PlaywrightSession,
    make_playwright_handler,
)

__all__ = [
    "PlaywrightHandlerConfig",
    "PlaywrightRpcProxyClient",
    "PlaywrightSession",
    "make_playwright_handler",
]
