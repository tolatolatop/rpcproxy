"""Application-level chunked envelope format for large ``post_message`` payloads."""

from __future__ import annotations

import base64
import gzip
import hashlib
import json
import uuid
from collections import OrderedDict
from dataclasses import dataclass, field
from typing import Any

CHUNK_MARKER = "__rpcproxy_chunk__"
CHUNK_DATA_KEY = "data_b64"
CHUNK_VERSION = 1
CHUNK_CONTENT_TYPE = "application/json"
DEFAULT_AUTO_CHUNK_THRESHOLD = 256 * 1024


@dataclass(frozen=True)
class EncodedChunkBatch:
    """Serialized chunk payloads ready to be sent as ``post_message`` bodies."""

    transfer_id: str
    request_id: str
    chunk_count: int
    bodies: list[dict[str, Any]]


@dataclass(frozen=True)
class ChunkSendReport:
    """Result of ``post_message_chunked``."""

    transfer_id: str
    request_id: str
    chunk_count: int
    transport_request_ids: list[str]
    responses: list[str]


@dataclass(frozen=True)
class AutoPostMessageResult:
    """Result of ``post_message_auto`` with explicit mode metadata."""

    chunked: bool
    response: str | None = None
    chunk_report: ChunkSendReport | None = None


@dataclass(frozen=True)
class DecodedChunkMessage:
    """Completed reassembled message."""

    transfer_id: str
    request_id: str
    body: dict[str, Any]
    chunk_count: int
    total_bytes: int
    sha256: str
    content_encoding: str


@dataclass
class _PendingTransfer:
    transfer_id: str
    request_id: str
    chunk_count: int
    total_bytes: int
    sha256: str
    content_encoding: str
    chunks: dict[int, bytes] = field(default_factory=dict)


class ChunkReassembler:
    """Collects chunk envelopes until the original JSON body is complete."""

    def __init__(self, *, max_pending: int = 128) -> None:
        if max_pending < 1:
            raise ValueError("max_pending must be >= 1")
        self._max_pending = max_pending
        self._pending: OrderedDict[str, _PendingTransfer] = OrderedDict()

    def clear(self) -> None:
        self._pending.clear()

    def push(self, body: dict[str, Any]) -> DecodedChunkMessage | None:
        meta = _chunk_meta(body)
        transfer_id = _meta_str(meta, "transfer_id")
        request_id = _meta_str(meta, "request_id")
        chunk_count = _meta_int(meta, "count")
        chunk_index = _meta_int(meta, "index")
        total_bytes = _meta_int(meta, "total_bytes")
        sha256 = _meta_str(meta, "sha256")
        content_encoding = _meta_str(meta, "content_encoding", default="identity")
        if _meta_str(meta, "content_type", default=CHUNK_CONTENT_TYPE) != CHUNK_CONTENT_TYPE:
            raise ValueError("unsupported chunk content_type")
        if chunk_count < 1:
            raise ValueError("chunk count must be >= 1")
        if chunk_index < 0 or chunk_index >= chunk_count:
            raise ValueError("chunk index out of range")

        encoded = body.get(CHUNK_DATA_KEY, "")
        if not isinstance(encoded, str):
            raise ValueError(f"{CHUNK_DATA_KEY} must be a base64 string")
        try:
            chunk_bytes = base64.b64decode(encoded.encode("ascii"), validate=True)
        except Exception as exc:
            raise ValueError("invalid base64 chunk data") from exc

        state = self._pending.get(transfer_id)
        if state is None:
            state = _PendingTransfer(
                transfer_id=transfer_id,
                request_id=request_id,
                chunk_count=chunk_count,
                total_bytes=total_bytes,
                sha256=sha256,
                content_encoding=content_encoding,
            )
            self._pending[transfer_id] = state
            self._evict_if_needed()
        else:
            self._pending.move_to_end(transfer_id)
            if (
                state.request_id != request_id
                or state.chunk_count != chunk_count
                or state.total_bytes != total_bytes
                or state.sha256 != sha256
                or state.content_encoding != content_encoding
            ):
                raise ValueError("inconsistent chunk metadata for transfer")

        prior = state.chunks.get(chunk_index)
        if prior is not None:
            if prior != chunk_bytes:
                raise ValueError("conflicting duplicate chunk")
        else:
            state.chunks[chunk_index] = chunk_bytes

        if len(state.chunks) != state.chunk_count:
            return None

        payload_bytes = b"".join(state.chunks[index] for index in range(state.chunk_count))
        self._pending.pop(transfer_id, None)
        if len(payload_bytes) != state.total_bytes:
            raise ValueError("reassembled payload size mismatch")
        digest = hashlib.sha256(payload_bytes).hexdigest()
        if digest != state.sha256:
            raise ValueError("reassembled payload checksum mismatch")
        payload_bytes = _decode_payload_bytes(payload_bytes, state.content_encoding)
        if payload_bytes:
            payload = json.loads(payload_bytes.decode("utf-8"))
        else:
            payload = {}
        if not isinstance(payload, dict):
            raise TypeError("reassembled payload must decode to a JSON object")
        return DecodedChunkMessage(
            transfer_id=transfer_id,
            request_id=state.request_id,
            body=payload,
            chunk_count=state.chunk_count,
            total_bytes=state.total_bytes,
            sha256=state.sha256,
            content_encoding=state.content_encoding,
        )

    def _evict_if_needed(self) -> None:
        while len(self._pending) > self._max_pending:
            self._pending.popitem(last=False)


def is_chunk_envelope(body: dict[str, Any]) -> bool:
    """Return ``True`` when ``body`` uses rpcproxy chunk format v1."""
    try:
        meta = _chunk_meta(body)
    except ValueError:
        return False
    return _meta_int(meta, "version") == CHUNK_VERSION


def estimate_body_wire_bytes(body: dict[str, Any] | None) -> int:
    """Estimate UTF-8 JSON byte length of the unchunked ``body`` payload."""
    payload = {} if body is None else body
    if not isinstance(payload, dict):
        raise TypeError("body must be a dict or None")
    return len(json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8"))


def encode_chunked_bodies(
    body: dict[str, Any] | None,
    *,
    request_id: str = "",
    chunk_size: int = 256 * 1024,
    compress: bool = False,
    transfer_id: str | None = None,
) -> EncodedChunkBatch:
    """Serialize a JSON object into chunk envelopes for ``post_message`` bodies."""
    if chunk_size < 1:
        raise ValueError("chunk_size must be >= 1")
    payload = {} if body is None else body
    if not isinstance(payload, dict):
        raise TypeError("chunked body must be a dict or None")

    raw_bytes = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    content_encoding = "identity"
    encoded_bytes = raw_bytes
    if compress and raw_bytes:
        compressed = gzip.compress(raw_bytes)
        if len(compressed) < len(raw_bytes):
            encoded_bytes = compressed
            content_encoding = "gzip"

    payload_hash = hashlib.sha256(encoded_bytes).hexdigest()
    transfer = transfer_id or uuid.uuid4().hex
    chunks = [encoded_bytes[i : i + chunk_size] for i in range(0, len(encoded_bytes), chunk_size)]
    if not chunks:
        chunks = [b""]

    bodies: list[dict[str, Any]] = []
    chunk_count = len(chunks)
    for index, chunk in enumerate(chunks):
        bodies.append(
            {
                CHUNK_MARKER: {
                    "version": CHUNK_VERSION,
                    "transfer_id": transfer,
                    "request_id": request_id,
                    "index": index,
                    "count": chunk_count,
                    "total_bytes": len(encoded_bytes),
                    "sha256": payload_hash,
                    "content_type": CHUNK_CONTENT_TYPE,
                    "content_encoding": content_encoding,
                },
                CHUNK_DATA_KEY: base64.b64encode(chunk).decode("ascii"),
            }
        )
    return EncodedChunkBatch(
        transfer_id=transfer,
        request_id=request_id,
        chunk_count=chunk_count,
        bodies=bodies,
    )


def make_chunk_transport_request_ids(
    *, transfer_id: str, request_id: str, chunk_count: int
) -> list[str]:
    """Stable per-chunk transport request ids for application-level chunk sends."""
    if chunk_count < 1:
        raise ValueError("chunk_count must be >= 1")
    prefix = request_id.strip() or transfer_id
    return [f"{prefix}#chunk:{index + 1}/{chunk_count}" for index in range(chunk_count)]


def _chunk_meta(body: dict[str, Any]) -> dict[str, Any]:
    meta = body.get(CHUNK_MARKER)
    if not isinstance(meta, dict):
        raise ValueError("missing chunk metadata")
    return meta


def _meta_str(meta: dict[str, Any], key: str, default: str = "") -> str:
    value = meta.get(key, default)
    if value is None:
        return default
    if isinstance(value, str):
        return value
    return str(value)


def _meta_int(meta: dict[str, Any], key: str) -> int:
    value = meta.get(key)
    if isinstance(value, bool):
        raise ValueError(f"chunk metadata {key} must be an integer")
    if isinstance(value, int):
        return value
    if isinstance(value, str) and value.strip():
        try:
            return int(value)
        except ValueError as exc:
            raise ValueError(f"chunk metadata {key} must be an integer") from exc
    raise ValueError(f"chunk metadata {key} is required")


def _decode_payload_bytes(payload_bytes: bytes, content_encoding: str) -> bytes:
    if content_encoding == "identity":
        return payload_bytes
    if content_encoding == "gzip":
        return gzip.decompress(payload_bytes)
    raise ValueError(f"unsupported content_encoding: {content_encoding}")
