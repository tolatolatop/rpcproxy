"""Independent authentication primitives for rpcproxy."""

from rpcproxy.auth.api_key import (
    ApiKeyIssue,
    ApiKeyIssuer,
    ApiKeyParts,
    ApiKeyRecord,
    ApiKeyRecordStore,
    ApiKeyStore,
    ApiKeyVerifier,
    InMemoryApiKeyRecordStore,
    InMemoryApiKeyStore,
    generate_api_key,
    hash_api_key_secret,
    mask_api_key,
    parse_api_key,
)
from rpcproxy.auth.errors import (
    AuthError,
    InvalidApiKeyError,
    LocalAuthorizationError,
    LocalAuthorizationServerError,
    LoginSessionError,
)
from rpcproxy.auth.local_authorization import (
    LocalAuthorizationServer,
    LocalAuthorizationServerConfig,
    PushApiKeyCallback,
    build_authorization_url,
)
from rpcproxy.auth.sessions import (
    InMemoryLoginSessionStore,
    LoginSession,
    LoginSessionStatus,
)
from rpcproxy.auth.types import RoleName

__all__ = [
    "ApiKeyIssue",
    "ApiKeyIssuer",
    "ApiKeyParts",
    "ApiKeyRecord",
    "ApiKeyRecordStore",
    "ApiKeyStore",
    "ApiKeyVerifier",
    "AuthError",
    "InMemoryApiKeyRecordStore",
    "InMemoryApiKeyStore",
    "InMemoryLoginSessionStore",
    "InvalidApiKeyError",
    "LocalAuthorizationError",
    "LocalAuthorizationServer",
    "LocalAuthorizationServerConfig",
    "LocalAuthorizationServerError",
    "LoginSession",
    "LoginSessionError",
    "LoginSessionStatus",
    "RoleName",
    "PushApiKeyCallback",
    "build_authorization_url",
    "generate_api_key",
    "hash_api_key_secret",
    "mask_api_key",
    "parse_api_key",
]
