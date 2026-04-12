"""Optional handler implementations (e.g. Playwright, ADB)."""

from rpcproxy.handlers.adb_handler import (
    AdbHandlerConfig,
    AdbRpcProxyClient,
    AdbSession,
    make_adb_handler,
)
from rpcproxy.handlers.playwright_handler import (
    PlaywrightHandlerConfig,
    PlaywrightRpcProxyClient,
    PlaywrightSession,
    make_playwright_handler,
)

__all__ = [
    "AdbHandlerConfig",
    "AdbRpcProxyClient",
    "AdbSession",
    "make_adb_handler",
    "PlaywrightHandlerConfig",
    "PlaywrightRpcProxyClient",
    "PlaywrightSession",
    "make_playwright_handler",
]
