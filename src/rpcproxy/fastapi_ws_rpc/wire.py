"""
Wire format aligned with ``fastapi_websocket_rpc`` (``RpcMessage`` / ``RpcRequest`` / ``RpcResponse``).

Reference: https://github.com/permitio/fastapi_websocket_rpc
"""

from __future__ import annotations

import json
from collections.abc import Callable, Mapping
from typing import Any, NotRequired, Required, TypeAlias, TypedDict

# Mirrors fastapi_websocket_rpc.rpc_methods.PING_RESPONSE
PING_RESPONSE: str = "pong"

# Mirrors fastapi_websocket_rpc.rpc_methods.EXPOSED_BUILT_IN_METHODS
EXPOSED_BUILT_IN_METHODS: frozenset[str] = frozenset({"_ping_", "_get_channel_id_"})


class RpcRequestPayload(TypedDict, total=False):
    """JSON shape of ``RpcRequest`` (text frame body nested under ``request``)."""

    method: Required[str]
    call_id: Required[str]
    arguments: NotRequired[dict[str, Any]]


class RpcResponsePayload(TypedDict, total=False):
    """JSON shape of ``RpcResponse`` (nested under ``response``)."""

    result: Required[Any]
    result_type: Required[str]
    call_id: NotRequired[str | None]


class RpcMessagePayload(TypedDict, total=False):
    """One WebSocket text frame: optional ``request`` and/or ``response``."""

    request: RpcRequestPayload | None
    response: RpcResponsePayload | None


UnknownMethodHandler: TypeAlias = Callable[[RpcRequestPayload], tuple[Any, str]]
"""Returns ``(result, result_type_name)`` for non-built-in methods."""


def is_pending_rpc_call(obj: Mapping[str, Any]) -> bool:
    """True if this is an incoming call: ``request`` has ``method``/``call_id`` and ``response`` is absent/null."""
    req = obj.get("request")
    if not isinstance(req, dict):
        return False
    if "call_id" not in req or "method" not in req:
        return False
    return obj.get("response") is None


def extract_request(obj: Mapping[str, Any]) -> RpcRequestPayload | None:
    """Return the nested ``request`` as a mapping, or ``None`` if not a pending RPC call."""
    if not is_pending_rpc_call(obj):
        return None
    req = obj["request"]
    if not isinstance(req, dict):
        return None
    method = req.get("method")
    call_id = req.get("call_id")
    if not isinstance(method, str) or call_id is None:
        return None
    out: RpcRequestPayload = {"method": method, "call_id": str(call_id)}
    args = req.get("arguments")
    if isinstance(args, dict):
        out["arguments"] = args
    return out


def response_body(result: Any, *, call_id: str, result_type: str) -> RpcResponsePayload:
    """Build the inner ``RpcResponse`` object (value of ``message[\"response\"]``)."""
    return {
        "call_id": call_id,
        "result": result,
        "result_type": result_type,
    }


def response_message(result: Any, *, call_id: str, result_type: str) -> dict[str, Any]:
    """
    Outgoing ``RpcMessage`` with only ``response`` set, as sent by
    ``JsonSerializingWebSocket`` (no ``request`` key).
    """
    return {"response": response_body(result, call_id=call_id, result_type=result_type)}


def call_request_message(
    method: str, arguments: dict[str, Any], call_id: str
) -> dict[str, Any]:
    """Outgoing ``RpcMessage`` with only ``request`` set (client-initiated RPC)."""
    return {
        "request": {
            "method": method,
            "arguments": arguments,
            "call_id": call_id,
        }
    }


def builtin_result(
    request: RpcRequestPayload, *, channel_id: str
) -> tuple[Any, str] | None:
    """
    If ``request.method`` is a library built-in, return ``(result, result_type)``.
    Otherwise return ``None``.
    """
    method = request["method"]
    if method == "_ping_":
        return PING_RESPONSE, "str"
    if method == "_get_channel_id_":
        return channel_id, "str"
    return None


def default_unknown_result(_request: RpcRequestPayload) -> tuple[Any, str]:
    """Demo/default handler: string ``success``."""
    return "success", "str"


def reply_message_for_request(
    request: RpcRequestPayload,
    *,
    channel_id: str,
    on_unknown: UnknownMethodHandler | None = None,
) -> dict[str, Any]:
    """Build a full outgoing ``RpcMessage`` dict for one pending ``RpcRequest``."""
    handler = on_unknown or default_unknown_result
    b = builtin_result(request, channel_id=channel_id)
    if b is not None:
        result, rt = b
    else:
        result, rt = handler(request)
    return response_message(result, call_id=request["call_id"], result_type=rt)


def reply_message_for_envelope(
    message: Mapping[str, Any],
    *,
    channel_id: str,
    on_unknown: UnknownMethodHandler | None = None,
) -> dict[str, Any] | None:
    """If ``message`` is a pending RPC call, return the reply ``RpcMessage``; else ``None``."""
    req = extract_request(message)
    if req is None:
        return None
    return reply_message_for_request(
        req, channel_id=channel_id, on_unknown=on_unknown
    )


def dumps_message(message: Mapping[str, Any], *, ensure_ascii: bool = False) -> str:
    """Serialize ``RpcMessage`` dict to JSON text (one WebSocket text frame)."""
    return json.dumps(message, ensure_ascii=ensure_ascii)


def loads_message(data: str) -> Any:
    """Parse one WebSocket text frame to JSON (caller validates shape)."""
    return json.loads(data)


def looks_like_rpc_response_only(message: Mapping[str, Any]) -> bool:
    """True if ``response`` is present and non-null and there is no pending ``request``."""
    resp = message.get("response")
    if resp is None or not isinstance(resp, dict):
        return False
    req = message.get("request")
    return req is None
