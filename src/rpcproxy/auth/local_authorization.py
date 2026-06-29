"""Local browser authorization server for callee-issued API keys."""

from __future__ import annotations

import html
import threading
from collections.abc import Callable
from dataclasses import dataclass
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlencode, urlparse

from rpcproxy.auth.api_key import ApiKeyIssuer
from rpcproxy.auth.errors import (
    LocalAuthorizationError,
    LocalAuthorizationServerError,
    LoginSessionError,
)
from rpcproxy.auth.sessions import InMemoryLoginSessionStore, LoginSession

PushApiKeyCallback = Callable[[str, str, LoginSession], None]


@dataclass(frozen=True)
class LocalAuthorizationServerConfig:
    """Configuration for the local callee authorization HTTP server."""

    host: str = "127.0.0.1"
    port: int = 17653
    authorize_path: str = "/authorize"
    confirm_path: str = "/authorize/confirm"
    health_path: str = "/healthz"


def build_authorization_url(base_url: str, caller_id: str) -> str:
    """Build the browser URL carrying the login-session caller_id as state."""

    separator = "&" if "?" in base_url else "?"
    return f"{base_url}{separator}{urlencode({'state': caller_id})}"


class LocalAuthorizationServer:
    """Callee-owned local HTTP server for confirming API key authorization."""

    def __init__(
        self,
        *,
        config: LocalAuthorizationServerConfig | None = None,
        sessions: InMemoryLoginSessionStore,
        issuer: ApiKeyIssuer,
        push_api_key: PushApiKeyCallback,
    ) -> None:
        self.config = config or LocalAuthorizationServerConfig()
        self._sessions = sessions
        self._issuer = issuer
        self._push_api_key = push_api_key
        self._httpd: ThreadingHTTPServer | None = None
        self._thread: threading.Thread | None = None

    @property
    def server_url(self) -> str:
        if self._httpd is not None:
            host, port = self._httpd.server_address[:2]
            return f"http://{host}:{port}"
        return f"http://{self.config.host}:{self.config.port}"

    def authorization_url(self, caller_id: str) -> str:
        return build_authorization_url(
            f"{self.server_url}{self.config.authorize_path}",
            caller_id,
        )

    def start(self) -> None:
        if self._httpd is not None:
            raise LocalAuthorizationServerError("local authorization server is running")

        owner = self

        class Handler(_LocalAuthorizationRequestHandler):
            server_owner = owner

        try:
            self._httpd = ThreadingHTTPServer(
                (self.config.host, self.config.port),
                Handler,
            )
        except OSError as exc:
            raise LocalAuthorizationServerError(
                f"failed to bind local authorization server: {exc}"
            ) from exc

        self._thread = threading.Thread(
            target=self._httpd.serve_forever,
            name="rpcproxy-local-authorization",
            daemon=True,
        )
        self._thread.start()

    def stop(self) -> None:
        httpd = self._httpd
        thread = self._thread
        self._httpd = None
        self._thread = None
        if httpd is not None:
            httpd.shutdown()
            httpd.server_close()
        if thread is not None:
            thread.join(timeout=5.0)

    def resolve_pending_session(self, caller_id: str) -> LoginSession:
        session = self._sessions.get_pending_session(caller_id)
        if session is None:
            raise LocalAuthorizationError("login session is missing or expired")
        return session

    def confirm_authorization(self, caller_id: str) -> None:
        session = self.resolve_pending_session(caller_id)
        issue = self._issuer.issue_api_key()
        self._push_api_key(caller_id, issue.api_key, session)
        try:
            self._sessions.complete_session(caller_id)
        except LoginSessionError as exc:
            raise LocalAuthorizationError("login session could not be completed") from exc


class _LocalAuthorizationRequestHandler(BaseHTTPRequestHandler):
    server_owner: LocalAuthorizationServer

    def do_GET(self) -> None:
        parsed = urlparse(self.path)
        if parsed.path == self.server_owner.config.health_path:
            self._send_text(HTTPStatus.OK, "ok")
            return

        if parsed.path == self.server_owner.config.authorize_path:
            self._handle_authorize_get(parsed.query)
            return

        self._send_text(HTTPStatus.NOT_FOUND, "not found")

    def do_POST(self) -> None:
        parsed = urlparse(self.path)
        if parsed.path != self.server_owner.config.confirm_path:
            self._send_text(HTTPStatus.NOT_FOUND, "not found")
            return

        length = int(self.headers.get("Content-Length", "0") or "0")
        body = self.rfile.read(length).decode("utf-8")
        state = _first_value(parse_qs(body), "state")
        if not state:
            self._send_text(HTTPStatus.BAD_REQUEST, "missing state")
            return

        try:
            self.server_owner.confirm_authorization(state)
        except LocalAuthorizationError as exc:
            self._send_html(HTTPStatus.BAD_REQUEST, _error_page(str(exc)))
            return
        except Exception:
            self._send_html(
                HTTPStatus.INTERNAL_SERVER_ERROR,
                _error_page("failed to push api key"),
            )
            return

        self._send_html(HTTPStatus.OK, _success_page())

    def log_message(self, format: str, *args: object) -> None:
        return

    def _handle_authorize_get(self, query: str) -> None:
        state = _first_value(parse_qs(query), "state")
        if not state:
            self._send_text(HTTPStatus.BAD_REQUEST, "missing state")
            return

        try:
            session = self.server_owner.resolve_pending_session(state)
        except LocalAuthorizationError as exc:
            self._send_html(HTTPStatus.BAD_REQUEST, _error_page(str(exc)))
            return

        self._send_html(HTTPStatus.OK, _authorization_page(session, self.server_owner))

    def _send_text(self, status: HTTPStatus, text: str) -> None:
        self._send_bytes(status, text.encode("utf-8"), "text/plain; charset=utf-8")

    def _send_html(self, status: HTTPStatus, body: str) -> None:
        self._send_bytes(status, body.encode("utf-8"), "text/html; charset=utf-8")

    def _send_bytes(self, status: HTTPStatus, body: bytes, content_type: str) -> None:
        self.send_response(int(status))
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)


def _first_value(values: dict[str, list[str]], key: str) -> str:
    items = values.get(key, [])
    return items[0] if items else ""


def _authorization_page(
    session: LoginSession,
    server: LocalAuthorizationServer,
) -> str:
    state = html.escape(session.caller_id, quote=True)
    action = html.escape(server.config.confirm_path, quote=True)
    channel = html.escape(session.push_channel_id or "anonymous", quote=True)
    return f"""<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8">
  <title>rpcproxy authorization</title>
</head>
<body>
  <h1>授权 rpcproxy Caller</h1>
  <p>登录会话: <code>{state}</code></p>
  <p>推送通道: <code>{channel}</code></p>
  <form method="post" action="{action}">
    <input type="hidden" name="state" value="{state}">
    <button type="submit">确认授权</button>
  </form>
</body>
</html>
"""


def _success_page() -> str:
    return """<!doctype html>
<html lang="zh-CN">
<head><meta charset="utf-8"><title>rpcproxy authorized</title></head>
<body><h1>授权完成</h1><p>API Key 已发送给 Caller。</p></body>
</html>
"""


def _error_page(message: str) -> str:
    safe = html.escape(message)
    return f"""<!doctype html>
<html lang="zh-CN">
<head><meta charset="utf-8"><title>rpcproxy authorization failed</title></head>
<body><h1>授权失败</h1><p>{safe}</p></body>
</html>
"""
