"""WebSocket RpcProxy client (fastapi_websocket_rpc wire format, no FastAPI dependency)."""

from rpcproxy.client.base import RpcProxyClientBase
from rpcproxy.client.chunking import (
    CHUNK_DATA_KEY,
    CHUNK_MARKER,
    CHUNK_VERSION,
    AutoPostMessageResult,
    ChunkSendReport,
    DecodedChunkMessage,
    EncodedChunkBatch,
    DEFAULT_AUTO_CHUNK_THRESHOLD,
    encode_chunked_bodies,
    estimate_body_wire_bytes,
    is_chunk_envelope,
)
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
    "CHUNK_MARKER",
    "CHUNK_DATA_KEY",
    "CHUNK_VERSION",
    "DEFAULT_AUTO_CHUNK_THRESHOLD",
    "EncodedChunkBatch",
    "DecodedChunkMessage",
    "ChunkSendReport",
    "AutoPostMessageResult",
    "encode_chunked_bodies",
    "estimate_body_wire_bytes",
    "is_chunk_envelope",
]
