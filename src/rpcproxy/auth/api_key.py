"""Callee-issued API key primitives.

API keys intentionally bind only to the issuing callee. Login-time caller IDs are
not stored in key records and are not used during verification.
"""

from __future__ import annotations

import hashlib
import hmac
import re
import secrets
import time
from dataclasses import dataclass
from collections.abc import Callable
from typing import Protocol

from rpcproxy.auth.errors import InvalidApiKeyError

_API_KEY_PREFIX = "rpcp"
_API_KEY_KIND = "ck"
_SEGMENT_RE = re.compile(r"^[A-Za-z0-9.-]+$")


def _validate_segment(name: str, value: str) -> None:
    if not value:
        raise InvalidApiKeyError(f"{name} must not be empty")
    if not _SEGMENT_RE.fullmatch(value):
        raise InvalidApiKeyError(
            f"{name} may contain only letters, digits, dot, or dash"
        )


@dataclass(frozen=True)
class ApiKeyParts:
    """Parsed API key fields."""

    callee_id: str
    key_id: str
    secret: str


@dataclass(frozen=True)
class ApiKeyRecord:
    """Server-side API key record without the plaintext secret."""

    callee_id: str
    key_id: str
    secret_hash: str
    created_at: float
    revoked: bool = False


@dataclass(frozen=True)
class ApiKeyIssue:
    """A newly issued plaintext API key and its server-side record."""

    api_key: str
    record: ApiKeyRecord


class ApiKeyRecordStore(Protocol):
    """Storage used by callees for API key records."""

    def save_record(self, record: ApiKeyRecord) -> None: ...

    def get_record(self, callee_id: str, key_id: str) -> ApiKeyRecord | None: ...

    def revoke_record(self, callee_id: str, key_id: str) -> None: ...


class ApiKeyStore(Protocol):
    """Storage used by callers for plaintext API keys by callee ID."""

    def save_api_key(self, expected_callee_id: str, api_key: str) -> None: ...

    def get_api_key(self, callee_id: str) -> str | None: ...

    def remove_api_key(self, callee_id: str) -> None: ...


def hash_api_key_secret(secret: str) -> str:
    """Hash a plaintext API key secret for server-side storage."""

    return hashlib.sha256(secret.encode("utf-8")).hexdigest()


def generate_api_key(
    callee_id: str,
    *,
    key_id: str | None = None,
    secret: str | None = None,
) -> str:
    """Create an API key containing a parseable callee ID."""

    _validate_segment("callee_id", callee_id)
    key_id = key_id or secrets.token_hex(8)
    secret = secret or secrets.token_hex(32)
    _validate_segment("key_id", key_id)
    _validate_segment("secret", secret)
    return f"{_API_KEY_PREFIX}_{_API_KEY_KIND}_{callee_id}_{key_id}_{secret}"


def parse_api_key(api_key: str) -> ApiKeyParts:
    """Parse and validate a callee-issued API key."""

    parts = api_key.split("_", 4)
    if len(parts) != 5:
        raise InvalidApiKeyError("API key has invalid format")
    prefix, kind, callee_id, key_id, secret = parts
    if prefix != _API_KEY_PREFIX or kind != _API_KEY_KIND:
        raise InvalidApiKeyError("API key has invalid prefix")
    _validate_segment("callee_id", callee_id)
    _validate_segment("key_id", key_id)
    _validate_segment("secret", secret)
    return ApiKeyParts(callee_id=callee_id, key_id=key_id, secret=secret)


def mask_api_key(api_key: str) -> str:
    """Return a log-safe representation of an API key."""

    try:
        parts = parse_api_key(api_key)
    except InvalidApiKeyError:
        return "<invalid-api-key>"
    return f"{_API_KEY_PREFIX}_{_API_KEY_KIND}_{parts.callee_id}_{parts.key_id}_..."


class InMemoryApiKeyRecordStore:
    """In-memory callee-side API key record store."""

    def __init__(self) -> None:
        self._records: dict[tuple[str, str], ApiKeyRecord] = {}

    def save_record(self, record: ApiKeyRecord) -> None:
        self._records[(record.callee_id, record.key_id)] = record

    def get_record(self, callee_id: str, key_id: str) -> ApiKeyRecord | None:
        return self._records.get((callee_id, key_id))

    def revoke_record(self, callee_id: str, key_id: str) -> None:
        record = self._records.get((callee_id, key_id))
        if record is not None:
            self._records[(callee_id, key_id)] = ApiKeyRecord(
                callee_id=record.callee_id,
                key_id=record.key_id,
                secret_hash=record.secret_hash,
                created_at=record.created_at,
                revoked=True,
            )


class InMemoryApiKeyStore:
    """In-memory caller-side API key store indexed by callee ID."""

    def __init__(self) -> None:
        self._api_keys: dict[str, str] = {}

    def save_api_key(self, expected_callee_id: str, api_key: str) -> None:
        parsed = parse_api_key(api_key)
        if parsed.callee_id != expected_callee_id:
            raise InvalidApiKeyError("API key callee_id does not match target callee")
        self._api_keys[expected_callee_id] = api_key

    def get_api_key(self, callee_id: str) -> str | None:
        return self._api_keys.get(callee_id)

    def remove_api_key(self, callee_id: str) -> None:
        self._api_keys.pop(callee_id, None)


class ApiKeyIssuer:
    """Issue API keys for one stable callee ID."""

    def __init__(
        self,
        callee_id: str,
        record_store: ApiKeyRecordStore,
        *,
        clock: Callable[[], float] | None = None,
    ) -> None:
        _validate_segment("callee_id", callee_id)
        self.callee_id = callee_id
        self._record_store = record_store
        self._clock = clock or time.time

    def issue_api_key(self) -> ApiKeyIssue:
        api_key = generate_api_key(self.callee_id)
        parsed = parse_api_key(api_key)
        record = ApiKeyRecord(
            callee_id=parsed.callee_id,
            key_id=parsed.key_id,
            secret_hash=hash_api_key_secret(parsed.secret),
            created_at=float(self._clock()),
        )
        self._record_store.save_record(record)
        return ApiKeyIssue(api_key=api_key, record=record)


class ApiKeyVerifier:
    """Verify API keys for one stable callee ID."""

    def __init__(self, callee_id: str, record_store: ApiKeyRecordStore) -> None:
        _validate_segment("callee_id", callee_id)
        self.callee_id = callee_id
        self._record_store = record_store

    def verify_api_key(self, api_key: str) -> bool:
        try:
            parsed = parse_api_key(api_key)
        except InvalidApiKeyError:
            return False
        if parsed.callee_id != self.callee_id:
            return False
        record = self._record_store.get_record(parsed.callee_id, parsed.key_id)
        if record is None or record.revoked:
            return False
        expected = record.secret_hash
        actual = hash_api_key_secret(parsed.secret)
        return hmac.compare_digest(expected, actual)
