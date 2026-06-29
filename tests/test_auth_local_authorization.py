from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass
from urllib.parse import parse_qs, urlparse

import pytest
import requests

from rpcproxy.auth import (
    ApiKeyIssuer,
    ApiKeyVerifier,
    InMemoryApiKeyRecordStore,
    InMemoryLoginSessionStore,
    LocalAuthorizationServer,
    LocalAuthorizationServerConfig,
    LoginSession,
    build_authorization_url,
)


@dataclass
class _AuthFixture:
    records: InMemoryApiKeyRecordStore
    sessions: InMemoryLoginSessionStore
    pushed: list[tuple[str, str, LoginSession]]
    server: LocalAuthorizationServer


@pytest.fixture
def auth_server() -> Iterator[_AuthFixture]:
    records = InMemoryApiKeyRecordStore()
    sessions = InMemoryLoginSessionStore(clock=lambda: 100.0)
    pushed: list[tuple[str, str, LoginSession]] = []
    server = LocalAuthorizationServer(
        config=LocalAuthorizationServerConfig(port=0),
        sessions=sessions,
        issuer=ApiKeyIssuer("callee-1", records, clock=lambda: 101.0),
        push_api_key=lambda caller_id, api_key, session: pushed.append(
            (caller_id, api_key, session)
        ),
    )
    server.start()
    try:
        yield _AuthFixture(records, sessions, pushed, server)
    finally:
        server.stop()


# 验证授权 URL 会把 caller_id 放入 state 参数，供本次登录流程关联使用。
def test_build_authorization_url_adds_state() -> None:
    url = build_authorization_url("http://127.0.0.1:17653/authorize", "caller-1")

    parsed = urlparse(url)
    assert parsed.path == "/authorize"
    assert parse_qs(parsed.query) == {"state": ["caller-1"]}


# 验证本地授权服务提供 healthz 端点，便于确认监听已启动。
def test_healthz_returns_ok(auth_server: _AuthFixture) -> None:
    response = requests.get(f"{auth_server.server.server_url}/healthz", timeout=5)

    assert response.status_code == 200
    assert response.text == "ok"


# 验证授权页只展示确认表单和登录 state，不在页面泄露 API Key。
def test_authorize_page_for_pending_session(auth_server: _AuthFixture) -> None:
    auth_server.sessions.create_session(
        "caller-1",
        ttl_seconds=30,
        push_channel_id="channel-1",
    )

    response = requests.get(auth_server.server.authorization_url("caller-1"), timeout=5)

    assert response.status_code == 200
    assert 'name="state" value="caller-1"' in response.text
    assert "/authorize/confirm" in response.text
    assert "rpcp_ck_" not in response.text


# 验证缺少 state 的授权请求会被拒绝。
def test_authorize_page_requires_state(auth_server: _AuthFixture) -> None:
    response = requests.get(f"{auth_server.server.server_url}/authorize", timeout=5)

    assert response.status_code == 400
    assert "missing state" in response.text


# 验证未知 state 不会创建授权页，避免绕过登录会话初始化。
def test_authorize_page_rejects_unknown_state(auth_server: _AuthFixture) -> None:
    response = requests.get(auth_server.server.authorization_url("missing"), timeout=5)

    assert response.status_code == 400
    assert "missing or expired" in response.text


# 验证确认授权会签发 API Key、推送给 Caller，并完成登录会话。
def test_confirm_authorization_pushes_api_key_and_completes_session(
    auth_server: _AuthFixture,
) -> None:
    auth_server.sessions.create_session(
        "caller-1",
        ttl_seconds=30,
        push_channel_id="channel-1",
    )

    response = requests.post(
        f"{auth_server.server.server_url}/authorize/confirm",
        data={"state": "caller-1"},
        timeout=5,
    )

    assert response.status_code == 200
    assert "授权完成" in response.text
    assert len(auth_server.pushed) == 1
    caller_id, api_key, session = auth_server.pushed[0]
    assert caller_id == "caller-1"
    assert session.push_channel_id == "channel-1"
    assert ApiKeyVerifier("callee-1", auth_server.records).verify_api_key(api_key)
    assert auth_server.sessions.get_pending_session("caller-1") is None


# 验证过期登录会话不能确认授权，也不会签发或推送 API Key。
def test_confirm_authorization_rejects_expired_session() -> None:
    current = 100.0
    records = InMemoryApiKeyRecordStore()
    sessions = InMemoryLoginSessionStore(clock=lambda: current)
    pushed: list[tuple[str, str, LoginSession]] = []
    server = LocalAuthorizationServer(
        config=LocalAuthorizationServerConfig(port=0),
        sessions=sessions,
        issuer=ApiKeyIssuer("callee-1", records),
        push_api_key=lambda caller_id, api_key, session: pushed.append(
            (caller_id, api_key, session)
        ),
    )
    sessions.create_session("caller-1", ttl_seconds=1)
    current = 102.0
    server.start()
    try:
        response = requests.post(
            f"{server.server_url}/authorize/confirm",
            data={"state": "caller-1"},
            timeout=5,
        )
    finally:
        server.stop()

    assert response.status_code == 400
    assert pushed == []


# 验证推送 API Key 失败时返回 500，且登录会话仍保持 pending。
def test_confirm_authorization_push_failure_does_not_complete_session() -> None:
    records = InMemoryApiKeyRecordStore()
    sessions = InMemoryLoginSessionStore(clock=lambda: 100.0)
    sessions.create_session("caller-1", ttl_seconds=30)

    def fail_push(caller_id: str, api_key: str, session: LoginSession) -> None:
        raise RuntimeError("rpc push failed")

    server = LocalAuthorizationServer(
        config=LocalAuthorizationServerConfig(port=0),
        sessions=sessions,
        issuer=ApiKeyIssuer("callee-1", records),
        push_api_key=fail_push,
    )
    server.start()
    try:
        response = requests.post(
            f"{server.server_url}/authorize/confirm",
            data={"state": "caller-1"},
            timeout=5,
        )
    finally:
        server.stop()

    assert response.status_code == 500
    assert sessions.get_pending_session("caller-1") is not None
