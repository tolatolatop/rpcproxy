"""
``fastapi_websocket_rpc``-compatible WebSocket JSON payloads (client or server side).

Use :mod:`rpcproxy.fastapi_ws_rpc.wire` types and helpers anywhere you need to
parse or emit ``RpcMessage`` / ``RpcRequest`` / ``RpcResponse`` without pulling
the full FastAPI stack into call sites.
"""

from .wire import (
    EXPOSED_BUILT_IN_METHODS,
    PING_RESPONSE,
    call_request_message,
    RpcMessagePayload,
    RpcRequestPayload,
    RpcResponsePayload,
    UnknownMethodHandler,
    builtin_result,
    default_unknown_result,
    dumps_message,
    extract_request,
    is_pending_rpc_call,
    loads_message,
    looks_like_rpc_response_only,
    reply_message_for_envelope,
    reply_message_for_request,
    response_body,
    response_message,
)

__all__ = [
    "EXPOSED_BUILT_IN_METHODS",
    "PING_RESPONSE",
    "call_request_message",
    "RpcMessagePayload",
    "RpcRequestPayload",
    "RpcResponsePayload",
    "UnknownMethodHandler",
    "builtin_result",
    "default_unknown_result",
    "dumps_message",
    "extract_request",
    "is_pending_rpc_call",
    "loads_message",
    "looks_like_rpc_response_only",
    "reply_message_for_envelope",
    "reply_message_for_request",
    "response_body",
    "response_message",
]
