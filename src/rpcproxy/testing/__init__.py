"""Test helpers and mock peers for ``rpcproxy`` — no real server needed.

Modules in this package are **not** required at runtime; they exist to support
integration testing of RPC-based tools (``rpcproxy post``, ``rpcproxy demo``,
etc.) without starting a real WebSocket server or browser.
"""

from rpcproxy.testing.mock_server import HandlerResultFactory, MockRelayPeer

__all__ = [
    "MockRelayPeer",
    "HandlerResultFactory",
]
