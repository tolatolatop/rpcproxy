"""Configure rotating file logs under the user directory and stderr."""

from __future__ import annotations

import logging
import os
import sys
from logging.handlers import RotatingFileHandler
from pathlib import Path

from platformdirs import user_log_dir

_LOG_ROOT_ENV = "RPCPROXY_LOG_DIR"
_LOG_LEVEL_ENV = "RPCPROXY_LOG_LEVEL"
_APP_NAME = "rpcproxy"

_MAX_BYTES = 10 * 1024 * 1024
_BACKUP_COUNT = 4

_log_dir_resolved: Path | None = None
_setup_done = False


def _default_log_dir() -> Path:
    return Path(user_log_dir(_APP_NAME, appauthor=False))


def _resolve_log_dir() -> Path:
    override = os.environ.get(_LOG_ROOT_ENV, "").strip()
    if override:
        return Path(override).expanduser().resolve()
    return _default_log_dir().resolve()


def _resolve_level() -> int:
    name = os.environ.get(_LOG_LEVEL_ENV, "INFO").strip().upper()
    return getattr(logging, name, logging.INFO)


def setup_logging() -> Path:
    """
    Attach rotating file + stderr handlers to the ``rpcproxy`` logger.
    Idempotent. Logs the resolved log directory at INFO.
    """
    global _setup_done, _log_dir_resolved
    if _setup_done and _log_dir_resolved is not None:
        return _log_dir_resolved

    log_dir = _resolve_log_dir()
    log_dir.mkdir(parents=True, exist_ok=True)
    _log_dir_resolved = log_dir

    level = _resolve_level()
    fmt = logging.Formatter(
        "%(asctime)s %(levelname)s [%(name)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    root_pkg = logging.getLogger("rpcproxy")
    root_pkg.setLevel(level)
    root_pkg.propagate = False

    log_path = log_dir / "rpcproxy.log"
    file_handler = RotatingFileHandler(
        log_path,
        maxBytes=_MAX_BYTES,
        backupCount=_BACKUP_COUNT,
        encoding="utf-8",
    )
    file_handler.setLevel(level)
    file_handler.setFormatter(fmt)

    console_handler = logging.StreamHandler(sys.stderr)
    console_handler.setLevel(level)
    console_handler.setFormatter(fmt)

    root_pkg.addHandler(file_handler)
    root_pkg.addHandler(console_handler)

    _setup_done = True
    root_pkg.info("日志目录: %s", log_dir)
    return log_dir
