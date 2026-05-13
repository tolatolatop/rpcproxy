"""Async WebSocket client base: fastapi_websocket_rpc-compatible RpcMessage without that dependency."""

from __future__ import annotations

import asyncio
import json
import logging
import uuid
from abc import ABC, abstractmethod
from collections import OrderedDict
from typing import Any

import websockets
from websockets.exceptions import ConnectionClosed

from rpcproxy.fastapi_ws_rpc import (
    builtin_result,
    call_request_message,
    dumps_message,
    extract_request,
    is_pending_rpc_call,
    loads_message,
    response_message,
)
from rpcproxy.client.chunking import (
    AutoPostMessageResult,
    ChunkReassembler,
    ChunkSendReport,
    DEFAULT_AUTO_CHUNK_THRESHOLD,
    encode_chunked_bodies,
    estimate_body_wire_bytes,
    is_chunk_envelope,
    make_chunk_transport_request_ids,
)

logger = logging.getLogger(__name__)

_RECEIVE_ENVELOPE_KEYS = frozenset(
    {
        "message_type",
        "kind",
        "client_id",
        "sender",
        "receiver",
        "body",
        "request_id",
    }
)


def _arg_str(args: dict[str, Any], key: str, default: str = "") -> str:
    v = args.get(key, default)
    if v is None:
        return default
    if isinstance(v, str):
        return v
    return str(v)


def _log_assigned_id_if_present(client_label: str, payload: dict[str, Any]) -> None:
    """If ``payload`` is an ``assigned_id`` notice, log the server ``client_id``."""
    if payload.get("kind") != "assigned_id":
        return
    cid = payload.get("client_id")
    if cid is None:
        logger.warning("%s assigned_id message missing client_id", client_label)
    else:
        logger.info("%s assigned_id client_id=%s", client_label, cid)


class RpcProxyClientBase(ABC):
    """
    Single-reader WebSocket client: handles inbound ``_ping_``, ``_get_channel_id_``,
    and abstract ``receive_envelope``; outbound ``set_state`` / ``post_message`` RPC.
    """

    def __init__(
        self,
        *,
        default_call_timeout: float | None = 30.0,
        relay_stash_max_size: int = 256,
        chunk_reassembly_max_pending: int = 128,
    ) -> None:
        self._channel_id: str = uuid.uuid4().hex
        self._default_call_timeout = default_call_timeout
        self._relay_stash_max_size = relay_stash_max_size
        self._ws: Any | None = None
        self._pending: dict[str, asyncio.Future[dict[str, Any]]] = {}
        self._reader_task: asyncio.Task[None] | None = None
        self._relay_stash: OrderedDict[str, dict[str, Any]] = OrderedDict()
        self._relay_waiters: dict[str, asyncio.Future[dict[str, Any]]] = {}
        self._chunk_reassembler = ChunkReassembler(
            max_pending=chunk_reassembly_max_pending
        )

    @property
    def channel_id(self) -> str:
        return self._channel_id

    @abstractmethod
    async def receive_envelope(
        self,
        message_type: str = "",
        kind: str = "",
        client_id: str = "",
        sender: str = "",
        receiver: str = "",
        body: dict[str, Any] | None = None,
        request_id: str = "",
        **extra: object,
    ) -> dict[str, bool]:
        """Handle server-initiated ``receive_envelope`` RPC."""

    async def connect(self, uri: str) -> None:
        if self._ws is not None:
            raise RuntimeError("already connected")
        self._ws = await websockets.connect(uri)
        self._reader_task = asyncio.create_task(self._reader_loop())

    def on_unmatched_message(self, msg: dict[str, Any]) -> None:
        """Hook for frames that are not an outbound reply nor an inbound Rpc call."""
        return

    async def wait_until_disconnected(self) -> None:
        """Block until the reader stops (peer closed the socket or :meth:`close` ran)."""
        if self._reader_task is None:
            raise RuntimeError("not connected")
        try:
            await self._reader_task
        except asyncio.CancelledError:
            pass

    async def close(self) -> None:
        if self._reader_task is not None:
            self._reader_task.cancel()
            try:
                await self._reader_task
            except asyncio.CancelledError:
                pass
            self._reader_task = None
        for fut in self._pending.values():
            if not fut.done():
                fut.cancel()
        self._pending.clear()
        self._relay_cleanup()
        self._chunk_reassembler.clear()
        if self._ws is not None:
            try:
                await self._ws.close()
            except Exception:
                logger.debug("close websocket", exc_info=True)
            self._ws = None

    async def _call_remote(self, method: str, arguments: dict[str, Any]) -> Any:
        if self._ws is None:
            raise RuntimeError("not connected")
        loop = asyncio.get_running_loop()
        call_id = uuid.uuid4().hex
        fut: asyncio.Future[dict[str, Any]] = loop.create_future()
        self._pending[call_id] = fut
        payload = call_request_message(method, arguments, call_id)
        try:
            await self._ws.send(dumps_message(payload))
            if self._default_call_timeout is None:
                resp = await fut
            else:
                resp = await asyncio.wait_for(
                    fut, timeout=self._default_call_timeout
                )
            return resp.get("result")
        except TimeoutError:
            raise
        finally:
            self._pending.pop(call_id, None)
            if not fut.done():
                fut.cancel()

    async def set_state(self, key: str, value: Any) -> Any:
        return await self._call_remote("set_state", {"key": key, "value": value})

    async def post_message(
        self,
        receiver: str = "",
        body: dict[str, Any] | None = None,
        request_id: str = "",
    ) -> str:
        """Call remote ``post_message``; ``body`` defaults to ``{}`` when omitted."""
        args: dict[str, Any] = {
            "receiver": receiver,
            "request_id": request_id,
            "body": body if body is not None else {},
        }
        return str(await self._call_remote("post_message", args))

    async def post_message_chunked(
        self,
        receiver: str = "",
        body: dict[str, Any] | None = None,
        request_id: str = "",
        *,
        chunk_size: int = 256 * 1024,
        compress: bool = False,
    ) -> ChunkSendReport:
        """Split one large JSON object into multiple ``post_message`` RPC calls."""
        batch = encode_chunked_bodies(
            body,
            request_id=request_id,
            chunk_size=chunk_size,
            compress=compress,
        )
        transport_request_ids = make_chunk_transport_request_ids(
            transfer_id=batch.transfer_id,
            request_id=request_id,
            chunk_count=batch.chunk_count,
        )
        responses: list[str] = []
        for transport_request_id, chunk_body in zip(
            transport_request_ids, batch.bodies, strict=True
        ):
            responses.append(
                await self.post_message(
                    receiver=receiver,
                    body=chunk_body,
                    request_id=transport_request_id,
                )
            )
        return ChunkSendReport(
            transfer_id=batch.transfer_id,
            request_id=request_id,
            chunk_count=batch.chunk_count,
            transport_request_ids=transport_request_ids,
            responses=responses,
        )

    async def post_message_auto(
        self,
        receiver: str = "",
        body: dict[str, Any] | None = None,
        request_id: str = "",
        *,
        auto_chunk_threshold: int = DEFAULT_AUTO_CHUNK_THRESHOLD,
        chunk_size: int = 256 * 1024,
        compress: bool = False,
    ) -> AutoPostMessageResult:
        """Send directly below threshold, otherwise fall back to chunked transport."""
        if auto_chunk_threshold < 1:
            raise ValueError("auto_chunk_threshold must be >= 1")
        body_wire_bytes = estimate_body_wire_bytes(body)
        if body_wire_bytes < auto_chunk_threshold:
            response = await self.post_message(
                receiver=receiver,
                body=body,
                request_id=request_id,
            )
            return AutoPostMessageResult(chunked=False, response=response)
        chunk_report = await self.post_message_chunked(
            receiver=receiver,
            body=body,
            request_id=request_id,
            chunk_size=chunk_size,
            compress=compress,
        )
        return AutoPostMessageResult(chunked=True, chunk_report=chunk_report)

    @staticmethod
    def _relay_receipt(args: dict[str, Any]) -> dict[str, Any]:
        return {"ok": True, "arguments": dict(args)}

    def _relay_publish(self, request_id: str, args: dict[str, Any]) -> None:
        if not request_id.strip():
            logger.debug("relay: skip publish, empty request_id")
            return
        receipt = self._relay_receipt(args)
        waiter = self._relay_waiters.pop(request_id, None)
        if waiter is not None and not waiter.done():
            waiter.set_result(receipt)
            return
        if self._relay_stash_max_size <= 0:
            logger.debug(
                "relay: stash disabled, dropping receipt for request_id=%s", request_id
            )
            return
        self._relay_stash.pop(request_id, None)
        self._relay_stash[request_id] = receipt
        while len(self._relay_stash) > self._relay_stash_max_size:
            evicted_rid, _ = self._relay_stash.popitem(last=False)
            logger.debug("relay stash evicted request_id=%s", evicted_rid)

    def _relay_cleanup(self) -> None:
        for fut in self._relay_waiters.values():
            if not fut.done():
                fut.cancel()
        self._relay_waiters.clear()
        self._relay_stash.clear()

    async def wait_relay_predicate(
        self, request_id: str, timeout: float | None
    ) -> dict[str, Any]:
        """
        Wait until an inbound ``receive_envelope`` RPC carries this ``request_id``,
        or return a stashed receipt if one arrived earlier (stash is bounded LRU;
        see ``relay_stash_max_size``).

        Returns ``{"ok": True, "arguments": {...}}`` (shallow copy of wire arguments).
        """
        if not request_id.strip():
            raise ValueError("request_id must be non-empty")
        rid = request_id

        stashed = self._relay_stash.pop(rid, None)
        if stashed is not None:
            return stashed

        if rid in self._relay_waiters:
            raise RuntimeError("already waiting for this request_id")

        loop = asyncio.get_running_loop()
        fut: asyncio.Future[dict[str, Any]] = loop.create_future()
        self._relay_waiters[rid] = fut
        try:
            if timeout is None:
                return await fut
            return await asyncio.wait_for(fut, timeout=timeout)
        except asyncio.TimeoutError:
            raise
        finally:
            if self._relay_waiters.get(rid) is fut:
                self._relay_waiters.pop(rid, None)
                if not fut.done():
                    fut.cancel()

    async def _reader_loop(self) -> None:
        assert self._ws is not None
        try:
            while True:
                try:
                    raw = await self._ws.recv()
                except ConnectionClosed:
                    break
                if isinstance(raw, bytes):
                    logger.debug("ignore binary frame")
                    continue
                try:
                    msg = loads_message(raw)
                except json.JSONDecodeError:
                    logger.warning("invalid JSON on wire")
                    continue
                if not isinstance(msg, dict):
                    continue
                if self._try_complete_pending(msg):
                    continue
                if is_pending_rpc_call(msg):
                    await self._dispatch_inbound(msg)
                    continue
                _log_assigned_id_if_present(type(self).__name__, msg)
                self.on_unmatched_message(msg)
        except asyncio.CancelledError:
            pass
        finally:
            for fut in self._pending.values():
                if not fut.done():
                    fut.cancel()
            self._pending.clear()
            self._relay_cleanup()

    def _try_complete_pending(self, msg: dict[str, Any]) -> bool:
        resp = msg.get("response")
        if not isinstance(resp, dict):
            return False
        cid = resp.get("call_id")
        if cid is None:
            return False
        cid_s = str(cid)
        fut = self._pending.pop(cid_s, None)
        if fut is None:
            return False
        if not fut.done():
            try:
                fut.set_result(resp)
            except asyncio.InvalidStateError:
                logger.debug("dropped late RPC response for %s", cid_s)
        return True

    async def _dispatch_inbound(self, envelope: dict[str, Any]) -> None:
        assert self._ws is not None
        req = extract_request(envelope)
        if req is None:
            return
        method = req["method"]
        call_id = req["call_id"]

        if method in ("_ping_", "_get_channel_id_"):
            b = builtin_result(req, channel_id=self._channel_id)
            if b is None:
                return
            result, rt = b
        elif method == "receive_envelope":
            args = req.get("arguments") or {}
            if not isinstance(args, dict):
                args = {}
            result = await self._invoke_receive_envelope(args)
            rt = type(result).__name__
        else:
            return

        out = response_message(result, call_id=call_id, result_type=rt)
        await self._ws.send(dumps_message(out))

    async def _invoke_receive_envelope(self, args: dict[str, Any]) -> dict[str, bool]:
        _log_assigned_id_if_present(type(self).__name__, args)
        normalized_args = dict(args)
        body = normalized_args.get("body")
        if isinstance(body, dict) and is_chunk_envelope(body):
            try:
                assembled = self._chunk_reassembler.push(body)
            except Exception:
                logger.warning("invalid chunked receive_envelope body", exc_info=True)
                return {"ok": False}
            if assembled is None:
                return {"ok": True}
            normalized_args["body"] = assembled.body
            if assembled.request_id.strip():
                normalized_args["request_id"] = assembled.request_id
            normalized_args["chunk_transfer_id"] = assembled.transfer_id
            normalized_args["chunk_count"] = assembled.chunk_count
            normalized_args["chunk_total_bytes"] = assembled.total_bytes
            normalized_args["chunk_sha256"] = assembled.sha256
            normalized_args["chunk_content_encoding"] = assembled.content_encoding

        rid = _arg_str(normalized_args, "request_id")
        self._relay_publish(rid, normalized_args)
        body = normalized_args.get("body")
        if body is not None and not isinstance(body, dict):
            body = None
        extra = {
            k: v for k, v in normalized_args.items() if k not in _RECEIVE_ENVELOPE_KEYS
        }
        return await self.receive_envelope(
            message_type=_arg_str(normalized_args, "message_type"),
            kind=_arg_str(normalized_args, "kind"),
            client_id=_arg_str(normalized_args, "client_id"),
            sender=_arg_str(normalized_args, "sender"),
            receiver=_arg_str(normalized_args, "receiver"),
            body=body,
            request_id=_arg_str(normalized_args, "request_id"),
            **extra,
        )
