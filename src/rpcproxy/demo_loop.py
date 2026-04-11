"""CLI demo: RpcProxyClientBase listening on a WebSocket (fastapi_websocket_rpc wire)."""

from __future__ import annotations

import json
import logging
import secrets
from typing import Any

from rpcproxy.client.base import RpcProxyClientBase

logger = logging.getLogger(__name__)


class DemoRpcProxyClient(RpcProxyClientBase):
    """Log ``receive_envelope`` arguments; log unmatched dict frames as warnings."""

    def on_unmatched_message(self, msg: dict[str, Any]) -> None:
        logger.warning("unmatched_message %s", json.dumps(msg, ensure_ascii=False))

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
        return {"ok": True}


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
    client = DemoRpcProxyClient(default_call_timeout=None)
    try:
        await client.connect(uri)
        await _push_demo_token(client)
        await client.wait_until_disconnected()
    finally:
        await client.close()
