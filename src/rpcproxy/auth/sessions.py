"""Temporary login session primitives."""

from __future__ import annotations

import time
from collections.abc import Callable
from dataclasses import dataclass
from enum import StrEnum

from rpcproxy.auth.errors import LoginSessionError


class LoginSessionStatus(StrEnum):
    """Lifecycle state for a temporary login session."""

    PENDING = "pending"
    COMPLETED = "completed"
    EXPIRED = "expired"


@dataclass(frozen=True)
class LoginSession:
    """Login-only state keyed by the temporary caller_id."""

    caller_id: str
    expires_at: float
    status: LoginSessionStatus = LoginSessionStatus.PENDING
    push_channel_id: str | None = None

    def is_expired(self, now: float | None = None) -> bool:
        current = time.time() if now is None else now
        return current >= self.expires_at


class InMemoryLoginSessionStore:
    """In-memory store for temporary caller login sessions."""

    def __init__(self, *, clock: Callable[[], float] | None = None) -> None:
        self._sessions: dict[str, LoginSession] = {}
        self._clock = clock or time.time

    def create_session(
        self,
        caller_id: str,
        *,
        ttl_seconds: float = 300.0,
        push_channel_id: str | None = None,
    ) -> LoginSession:
        if not caller_id:
            raise LoginSessionError("caller_id must not be empty")
        if ttl_seconds <= 0:
            raise LoginSessionError("ttl_seconds must be positive")
        session = LoginSession(
            caller_id=caller_id,
            expires_at=float(self._clock()) + ttl_seconds,
            push_channel_id=push_channel_id,
        )
        self._sessions[caller_id] = session
        return session

    def get_pending_session(self, caller_id: str) -> LoginSession | None:
        session = self._sessions.get(caller_id)
        if session is None:
            return None
        if session.status is not LoginSessionStatus.PENDING:
            return None
        if session.is_expired(float(self._clock())):
            expired = LoginSession(
                caller_id=session.caller_id,
                expires_at=session.expires_at,
                status=LoginSessionStatus.EXPIRED,
                push_channel_id=session.push_channel_id,
            )
            self._sessions[caller_id] = expired
            return None
        return session

    def complete_session(self, caller_id: str) -> LoginSession:
        session = self.get_pending_session(caller_id)
        if session is None:
            raise LoginSessionError("login session is not pending")
        completed = LoginSession(
            caller_id=session.caller_id,
            expires_at=session.expires_at,
            status=LoginSessionStatus.COMPLETED,
            push_channel_id=session.push_channel_id,
        )
        self._sessions[caller_id] = completed
        return completed

    def remove_session(self, caller_id: str) -> None:
        self._sessions.pop(caller_id, None)
