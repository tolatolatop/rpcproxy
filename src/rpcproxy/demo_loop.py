"""CLI demo: HandlerPostMessageClient with echo handler (fastapi_websocket_rpc wire)."""

from __future__ import annotations

import json
import logging
import secrets
from typing import Any

from rpcproxy.client.envelope_types import ReceiveEnvelopeArguments
from rpcproxy.client.handler_client import (
    HandlerPostMessageClient,
    HandlerResult,
)

logger = logging.getLogger(__name__)


async def demo_echo_envelope_handler(
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
    """Log envelope like the legacy demo, then echo ``body`` with ``is_echo`` via ``post_message``."""
    payload: dict[str, Any] = {
        "message_type": message_type,
        "kind": kind,
        "client_id": client_id,
        "sender": sender,
        "receiver": receiver,
        "body": body,
        "request_id": request_id,
    }
    if extra:
        payload["extra"] = extra
    logger.info("receive_envelope %s", json.dumps(payload, ensure_ascii=False))

    out_body = dict(body) if body else {}
    out_body["is_echo"] = True
    return HandlerResult(body=out_body)


class DemoRpcProxyClient(HandlerPostMessageClient):
    """Echo inbound ``receive_envelope`` through ``post_message``; log unmatched dict frames."""

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(demo_echo_envelope_handler, default_call_timeout=None, **kwargs)

    def on_unmatched_message(self, msg: dict[str, Any]) -> None:
        logger.warning("unmatched_message %s", json.dumps(msg, ensure_ascii=False))


async def _push_demo_token(client: DemoRpcProxyClient) -> None:
    token = secrets.token_urlsafe(32)
    await client.set_state("token", token)
    logger.info(
        "set_state %s",
        json.dumps(
            {"demo": "set_state", "key": "token", "token": token},
            ensure_ascii=False,
        ),
    )


async def run_demo(uri: str) -> None:
    client = DemoRpcProxyClient()
    try:
        await client.connect(uri)
        await _push_demo_token(client)
        await client.wait_until_disconnected()
    finally:
        await client.close()
