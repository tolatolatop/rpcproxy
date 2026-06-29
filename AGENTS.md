# Repository Guidelines

## Project Structure & Module Organization

This Python package uses a `src/` layout. Core code lives in `src/rpcproxy/`.
Client abstractions are in `src/rpcproxy/client/`, optional command handlers in
`src/rpcproxy/handlers/`, and wire helpers in `src/rpcproxy/fastapi_ws_rpc/`.
CLI entry points are implemented through `src/rpcproxy/cli.py`.

Tests live in `tests/` and cover the client base, handler client, CLI commands,
demo loop, ADB, and Playwright. JSON command examples are in `tests/example/`.

## Build, Test, and Development Commands

- `uv sync`: create/update the virtual environment and install the package.
- `uv sync --group dev`: install development dependencies, including pytest.
- `uv sync --extra playwright`: install optional Playwright support.
- `uv run --group dev pytest`: run the full test suite.
- `uv run --group dev pytest tests/test_client_base.py -v`: run one test file.
- `uv run rpcproxy demo ws://127.0.0.1:8080/rpc`: run the demo WebSocket client.
- `uv run rpcproxy post WS_URL -r RECEIVER --body '{}'`: send a one-shot message.

Build metadata is in `pyproject.toml`; the wheel is built with Hatchling.

## Coding Style & Naming Conventions

Use Python 3.11+ idioms, type hints, and `asyncio` for asynchronous I/O. Follow
PEP 8 with 4-space indentation. Keep protocol field names centralized in existing
helpers, especially under `rpcproxy.fastapi_ws_rpc`.

Use snake_case for modules, functions, methods, variables, and test names.
Classes should use PascalCase, such as `RpcProxyClientBase` or
`HandlerPostMessageClient`.

## Testing Guidelines

The suite uses `pytest` with `pytest-asyncio`; configuration is in `pyproject.toml`
with `asyncio_mode = "auto"` and `testpaths = ["tests"]`.
Name new tests `test_*.py` and keep async tests focused on observable behavior.
Prefer mocks or fake queues for WebSocket behavior so tests avoid real network
services or browsers unless explicitly targeting integration setup.

Run `uv run --group dev pytest` before submitting changes.

## Commit & Pull Request Guidelines

Recent history uses short imperative summaries, often Conventional Commit style,
for example `feat(cli): add Playwright command` or `Fix large-message post and
demo disconnects`. Keep commits focused and mention the affected area when useful.

Pull requests should include a concise description, test results, linked issues
when applicable, and notes for optional dependencies such as Playwright or ADB.
For CLI behavior changes, include example commands or output.

## Security & Configuration Tips

Do not commit local logs, virtual environments, credentials, or device-specific
configuration. Runtime logging can be configured with `RPCPROXY_LOG_DIR` and
`RPCPROXY_LOG_LEVEL`; keep examples generic and avoid exposing private endpoints.
