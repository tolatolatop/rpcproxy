"""CLI: HandlerPostMessageClient with ADB-backed receive_envelope handler."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

from rpcproxy.handlers.adb_handler import AdbHandlerConfig, AdbRpcProxyClient

logger = logging.getLogger(__name__)


async def run_adb(
    uri: str,
    *,
    adb_config: AdbHandlerConfig | None = None,
    **client_kwargs: Any,
) -> None:
    client = AdbRpcProxyClient(adb_config=adb_config, **client_kwargs)
    try:
        await client.connect(uri)
        logger.info(
            "adb client connected; receive_envelope bodies use command ping / screenshot / tap / swipe / input_text"
        )
        await client.wait_until_disconnected()
    finally:
        await client.close()
