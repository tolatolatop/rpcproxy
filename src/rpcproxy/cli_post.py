"""Click subcommand: ``post`` — one-shot ``post_message`` + ``wait_relay_predicate``."""

from __future__ import annotations

import asyncio
from dataclasses import asdict, is_dataclass
import json
import sys
import uuid
from typing import Any

import click

from rpcproxy.client.base import RpcProxyClientBase


class _PostCliClient(RpcProxyClientBase):
    """Minimal client for one-shot ``post_message``; ACK inbound ``receive_envelope``."""

    async def receive_envelope(self, **kwargs: Any) -> dict[str, bool]:
        return {"ok": True}


def _read_body_raw(value: str) -> str:
    """Resolve ``--body`` string: literal JSON, ``-`` / ``@-`` = stdin, ``@path`` = file."""
    if value == "-" or value == "@-":
        return sys.stdin.read()
    if value.startswith("@"):
        path = value[1:]
        if not path:
            raise click.BadParameter("use @path (e.g. @payload.json) or @- for stdin")
        try:
            with open(path, encoding="utf-8") as f:
                return f.read()
        except OSError as e:
            raise click.BadParameter(f"cannot read body file: {e}") from e
    return value


def body_option_callback(
    _ctx: click.Context, _param: click.Parameter, value: str
) -> dict[str, Any]:
    raw = _read_body_raw(value)
    try:
        val = json.loads(raw)
    except json.JSONDecodeError as e:
        raise click.BadParameter(f"invalid JSON: {e}") from e
    if not isinstance(val, dict):
        raise click.BadParameter("must be a JSON object (e.g. {})")
    return val


def timeout_option_callback(
    _ctx: click.Context, _param: click.Parameter, value: float
) -> float:
    if value <= 0:
        raise click.BadParameter("must be positive")
    return value


async def run_post(
    url: str,
    receiver: str,
    body: dict[str, Any],
    request_id: str,
    timeout: float,
) -> tuple[str, Any, dict[str, Any]]:
    """Send ``post_message_auto`` and wait for matching ``receive_envelope`` relay receipt."""
    rid = request_id.strip() or uuid.uuid4().hex
    client = _PostCliClient(default_call_timeout=timeout)
    await client.connect(url)
    wait_task = asyncio.create_task(client.wait_relay_predicate(rid, timeout))
    await asyncio.sleep(0)
    try:
        post_result = await client.post_message_auto(
            receiver=receiver, body=body, request_id=rid
        )
        receipt = await wait_task
    except BaseException:
        if not wait_task.done():
            wait_task.cancel()
            try:
                await wait_task
            except (asyncio.CancelledError, TimeoutError):
                pass
        raise
    finally:
        await client.close()
    return rid, post_result, receipt


def register_post_command(group: click.Group) -> None:
    """Attach the ``post`` subcommand to ``group``."""

    @group.command("post")
    @click.argument("url")
    @click.option("--receiver", "-r", default="", show_default=True, help="post_message receiver")
    @click.option(
        "--body",
        default="{}",
        show_default=True,
        help="JSON object; or - / @- (stdin), or @FILE (UTF-8 path)",
        callback=body_option_callback,
    )
    @click.option(
        "--request-id",
        default="",
        show_default=True,
        help="Correlation id; if empty, a random id is generated",
    )
    @click.option(
        "--timeout",
        type=float,
        default=30.0,
        show_default=True,
        help="Seconds for post_message RPC and wait_relay_predicate each",
        callback=timeout_option_callback,
    )
    def post_cmd(
        url: str,
        receiver: str,
        body: dict[str, Any],
        request_id: str,
        timeout: float,
    ) -> None:
        """Connect, post_message_auto then wait_relay_predicate; print JSON to stdout."""
        try:
            rid, post_result, receipt = asyncio.run(
                run_post(url, receiver, body, request_id, timeout)
            )
            if is_dataclass(post_result):
                post_result = asdict(post_result)
            print(
                json.dumps(
                    {
                        "request_id": rid,
                        "post_message": post_result,
                        "relay": receipt,
                    },
                    ensure_ascii=False,
                    default=str,
                )
            )
        except TimeoutError:
            click.echo("error: timed out (post_message or wait_relay_predicate)", err=True)
            sys.exit(1)
        except KeyboardInterrupt:
            raise SystemExit(130) from None
        except Exception as e:
            click.echo(f"error: {e}", err=True)
            sys.exit(1)
