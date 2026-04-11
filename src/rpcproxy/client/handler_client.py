"""Concrete client: async handler processes inbound envelopes, then ``post_message`` reply."""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from typing import Any, Protocol, cast

from rpcproxy.client.base import RpcProxyClientBase, _RECEIVE_ENVELOPE_KEYS, _arg_str
from rpcproxy.client.envelope_types import ReceiveEnvelopeArguments

logger = logging.getLogger(__name__)


@dataclass
class HandlerResult:
    """Return value of :class:`HandlerPostMessageClient` handler: maps to ``post_message``."""

    body: dict[str, Any]
    request_id: str = ""
    skip_post: bool = False


class EnvelopeHandler(Protocol):
    async def __call__(
        self,
        *,
        message_type: str,
        kind: str,
        client_id: str,
        sender: str,
        receiver: str,
        body: dict[str, Any] | None,
        request_id: str,
        arguments: ReceiveEnvelopeArguments,
        **extra: object,
    ) -> HandlerResult:
        ...


def _arguments_to_receive_kwargs(arguments: ReceiveEnvelopeArguments) -> dict[str, Any]:
    body = arguments.get("body")
    if body is not None and not isinstance(body, dict):
        body = None
    extra = {k: v for k, v in arguments.items() if k not in _RECEIVE_ENVELOPE_KEYS}
    return {
        "message_type": _arg_str(arguments, "message_type"),
        "kind": _arg_str(arguments, "kind"),
        "client_id": _arg_str(arguments, "client_id"),
        "sender": _arg_str(arguments, "sender"),
        "receiver": _arg_str(arguments, "receiver"),
        "body": body,
        "request_id": _arg_str(arguments, "request_id"),
        **extra,
    }


class HandlerPostMessageClient(RpcProxyClientBase):
    """
    Runs an async **handler** after each inbound ``receive_envelope`` RPC, then sends
    the handler output via ``post_message``. The RPC handler returns immediately
    (``{"ok": True}``) so the reader loop is not blocked by ``post_message`` or slow work.

    Concurrency is always capped by ``max_inflight`` (default ``8``).
    Inbound envelopes must include a non-empty ``request_id`` (whitespace-only counts as empty).
    ``post_message`` is always addressed to the inbound envelope's ``sender`` (the party that
    pushed the RPC); empty ``sender`` yields an empty ``post_message`` receiver.

    Subclasses may override :meth:`on_handler_exception` to add reporting (e.g. ``post_message``);
    the default implementation only logs.
    """

    def __init__(
        self,
        handler: EnvelopeHandler,
        *,
        max_inflight: int = 8,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        if max_inflight < 1:
            raise ValueError("max_inflight must be >= 1")
        self._handler = handler
        self._handler_sem = asyncio.Semaphore(max_inflight)

    async def on_handler_exception(
        self, exc: BaseException, arguments: ReceiveEnvelopeArguments
    ) -> None:
        """Called when the user ``handler`` raises. Default: log with traceback only."""
        logger.error("handler client: handler failed", exc_info=exc)

    @staticmethod
    def _post_message_receiver(arguments: ReceiveEnvelopeArguments) -> str:
        return _arg_str(arguments, "sender")

    def _resolve_post_request_id(
        self, result: HandlerResult, arguments: ReceiveEnvelopeArguments
    ) -> str:
        if result.request_id.strip():
            return result.request_id
        return _arg_str(arguments, "request_id")

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
        arguments: dict[str, Any] = {
            "message_type": message_type,
            "kind": kind,
            "client_id": client_id,
            "sender": sender,
            "receiver": receiver,
            "request_id": request_id,
            "body": body if body is not None else {},
        }
        arguments.update({k: v for k, v in extra.items()})

        if not _arg_str(arguments, "request_id").strip():
            logger.debug("handler client: skip pipeline, empty request_id")
            return {"ok": False}

        typed = cast(ReceiveEnvelopeArguments, arguments)
        asyncio.create_task(self._run_handler_pipeline(typed))
        return {"ok": True}

    async def _run_handler_pipeline(self, arguments: ReceiveEnvelopeArguments) -> None:
        async with self._handler_sem:
            await self._run_handler_pipeline_unlocked(arguments)

    async def _run_handler_pipeline_unlocked(
        self, arguments: ReceiveEnvelopeArguments
    ) -> None:
        kw = _arguments_to_receive_kwargs(arguments)
        try:
            result = await self._handler(
                **kw,
                arguments=cast(ReceiveEnvelopeArguments, dict(arguments)),
            )
        except Exception as exc:
            await self.on_handler_exception(exc, cast(ReceiveEnvelopeArguments, dict(arguments)))
            return

        if result.skip_post:
            return
        recv = self._post_message_receiver(arguments)
        rid = self._resolve_post_request_id(result, arguments)
        try:
            await self.post_message(receiver=recv, body=result.body, request_id=rid)
        except Exception:
            logger.exception("handler client: post_message failed")
