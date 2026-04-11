"""TypedDict for ``receive_envelope`` RPC ``arguments`` (fastapi-websocket-rpc wire)."""

from __future__ import annotations

from typing import Any, TypedDict


class ReceiveEnvelopeArguments(TypedDict, total=False):
    """
    Standard keys for the ``arguments`` object of an inbound ``receive_envelope`` call.

    The wire payload may include additional keys; they are kept in the same mapping at
    runtime (see :class:`HandlerPostMessageClient`).
    """

    message_type: str
    kind: str
    client_id: str
    sender: str
    receiver: str
    body: dict[str, Any]
    request_id: str
