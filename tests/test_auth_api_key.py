from __future__ import annotations

import pytest

from rpcproxy.auth import (
    ApiKeyIssuer,
    ApiKeyVerifier,
    InMemoryApiKeyRecordStore,
    InMemoryApiKeyStore,
    InMemoryLoginSessionStore,
    InvalidApiKeyError,
    LoginSessionError,
    LoginSessionStatus,
    RoleName,
    generate_api_key,
    mask_api_key,
    parse_api_key,
)


# 验证四个认证角色的公开命名保持稳定，避免协议侧出现兼容性变化。
def test_role_names_are_protocol_stable() -> None:
    assert RoleName.JRPC_CALLER == "JrpcCaller"
    assert RoleName.JRPC_CALLEE == "JrpcCallee"
    assert RoleName.RELAY_SERVER == "RelayServer"
    assert RoleName.AUTHORIZATION_SERVER == "AuthorizationServer"


# 验证 API Key 可解析出 callee_id，并且脱敏输出不会泄露 secret。
def test_api_key_parses_callee_id_and_masks_secret() -> None:
    api_key = generate_api_key("callee-1", key_id="kid1", secret="secret1")

    parsed = parse_api_key(api_key)

    assert parsed.callee_id == "callee-1"
    assert parsed.key_id == "kid1"
    assert parsed.secret == "secret1"
    assert mask_api_key(api_key) == "rpcp_ck_callee-1_kid1_..."
    assert "secret1" not in mask_api_key(api_key)


# 验证下划线等会破坏分段解析的 callee_id 会被拒绝。
def test_api_key_rejects_ambiguous_callee_id() -> None:
    with pytest.raises(InvalidApiKeyError):
        generate_api_key("callee_1")


# 验证 Callee 只能校验自己签发的 API Key，不能接受其他 Callee 的 key。
def test_callee_verifies_own_key_and_rejects_wrong_callee() -> None:
    records = InMemoryApiKeyRecordStore()
    issue = ApiKeyIssuer("callee-1", records, clock=lambda: 10.0).issue_api_key()

    assert ApiKeyVerifier("callee-1", records).verify_api_key(issue.api_key)
    assert not ApiKeyVerifier("callee-2", records).verify_api_key(issue.api_key)


# 验证错误 secret 和已撤销 API Key 都不能通过 Callee 校验。
def test_callee_rejects_wrong_secret_and_revoked_key() -> None:
    records = InMemoryApiKeyRecordStore()
    issue = ApiKeyIssuer("callee-1", records).issue_api_key()
    parsed = parse_api_key(issue.api_key)
    tampered = generate_api_key(
        parsed.callee_id,
        key_id=parsed.key_id,
        secret="badsecret",
    )

    verifier = ApiKeyVerifier("callee-1", records)

    assert not verifier.verify_api_key(tampered)
    records.revoke_record(parsed.callee_id, parsed.key_id)
    assert not verifier.verify_api_key(issue.api_key)


# 验证 Caller 保存 API Key 前必须确认 key 中的 callee_id 符合目标 Callee。
def test_caller_store_requires_expected_callee_id() -> None:
    store = InMemoryApiKeyStore()
    api_key = generate_api_key("callee-1", key_id="kid1", secret="secret1")

    store.save_api_key("callee-1", api_key)
    assert store.get_api_key("callee-1") == api_key

    with pytest.raises(InvalidApiKeyError):
        store.save_api_key("callee-2", api_key)


# 验证 API Key 记录不包含 caller_id，避免把登录临时身份变成长期绑定。
def test_api_key_records_do_not_contain_caller_id() -> None:
    records = InMemoryApiKeyRecordStore()
    issue = ApiKeyIssuer("callee-1", records).issue_api_key()

    assert not hasattr(issue.record, "caller_id")


# 验证 caller_id 只在登录会话中用于关联流程，完成后不再是待处理会话。
def test_login_session_uses_temporary_caller_id_only_during_login() -> None:
    now = 100.0
    sessions = InMemoryLoginSessionStore(clock=lambda: now)

    session = sessions.create_session(
        "caller-login-1",
        ttl_seconds=30,
        push_channel_id="rpc-channel-1",
    )

    assert session.caller_id == "caller-login-1"
    assert sessions.get_pending_session("caller-login-1") == session
    completed = sessions.complete_session("caller-login-1")
    assert completed.status is LoginSessionStatus.COMPLETED
    assert sessions.get_pending_session("caller-login-1") is None


# 验证过期的登录会话不能继续完成授权。
def test_expired_login_session_cannot_complete() -> None:
    current = 100.0
    sessions = InMemoryLoginSessionStore(clock=lambda: current)
    sessions.create_session("caller-login-1", ttl_seconds=1)
    current = 102.0

    assert sessions.get_pending_session("caller-login-1") is None
    with pytest.raises(LoginSessionError):
        sessions.complete_session("caller-login-1")
