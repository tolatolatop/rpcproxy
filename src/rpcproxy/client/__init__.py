"""WebSocket RpcProxy client (fastapi_websocket_rpc wire format, no FastAPI dependency)."""

from rpcproxy.client.base import RpcProxyClientBase
from rpcproxy.client.envelope_types import ReceiveEnvelopeArguments
from rpcproxy.client.handler_client import (
    EnvelopeHandler,
    HandlerPostMessageClient,
    HandlerResult,
)

__all__ = [
    "RpcProxyClientBase",
    "ReceiveEnvelopeArguments",
    "EnvelopeHandler",
    "HandlerPostMessageClient",
    "HandlerResult",
]
