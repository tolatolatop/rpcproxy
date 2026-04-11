"""CLI demo: RpcProxyClientBase listening on a WebSocket (fastapi_websocket_rpc wire)."""

from __future__ import annotations

import json
import sys
from typing import Any

from rpcproxy.client.base import RpcProxyClientBase


class DemoRpcProxyClient(RpcProxyClientBase):
    """Print ``receive_envelope`` arguments to stdout; unmatched dict frames to stderr."""

    def on_unmatched_message(self, msg: dict[str, Any]) -> None:
        print(json.dumps(msg, ensure_ascii=False), file=sys.stderr, flush=True)

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
        print(json.dumps(payload, ensure_ascii=False), flush=True)
        return {"ok": True}


async def run_demo(uri: str) -> None:
    client = DemoRpcProxyClient(default_call_timeout=None)
    try:
        await client.connect(uri)
        await client.wait_until_disconnected()
    finally:
        await client.close()
