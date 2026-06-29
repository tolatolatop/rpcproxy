"""Shared authentication role names."""

from __future__ import annotations

from enum import StrEnum


class RoleName(StrEnum):
    """Named roles in the rpcproxy authentication model."""

    JRPC_CALLER = "JrpcCaller"
    JRPC_CALLEE = "JrpcCallee"
    RELAY_SERVER = "RelayServer"
    AUTHORIZATION_SERVER = "AuthorizationServer"
