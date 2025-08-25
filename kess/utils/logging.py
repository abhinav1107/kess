import json
import logging
import os
import sys
from datetime import datetime, timezone
from typing import Any, Dict


_CONFIGURED = False
_STD_FIELDS = {
    "name", "msg", "args", "levelname", "levelno", "pathname", "filename", "module",
    "exc_info", "exc_text", "stack_info", "lineno", "funcName", "created", "msecs",
    "relativeCreated", "thread", "threadName", "processName", "process",
}

def _iso_utc(ts: float | None = None) -> str:
    dt = datetime.fromtimestamp(ts if ts is not None else datetime.now(tz=timezone.utc).timestamp(), tz=timezone.utc)
    return dt.replace(tzinfo=timezone.utc).isoformat().replace("+00:00", "Z")

class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload: Dict[str, Any] = {
            "ts": _iso_utc(record.created),
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
            "module": record.module,
            "line": record.lineno,
        }

        for k, v in record.__dict__.items():
            if k not in _STD_FIELDS and not k.startswith("_"):
                payload[k] = v
        if record.exc_info:
            payload["exc_info"] = self.formatException(record.exc_info)

        return json.dumps(payload, separators=(",", ":"), ensure_ascii=False)

def init_logging(level: str | None = None, json_format: bool | None = None) -> None:
    """
    Initialize root logger once.
    - level: explicit level or env KESS_LOG_LEVEL (default INFO)
    - json_format: True/False or env KESS_LOG_FORMAT=json|text (default json)
    """
    global _CONFIGURED
    if _CONFIGURED:
        return

    level_str = (level or os.getenv("KESS_LOG_LEVEL", "INFO")).upper()
    fmt_env = os.getenv("KESS_LOG_FORMAT", "json").lower()
    json_fmt = json_format if json_format is not None else (fmt_env == "json")

    lvl = {
        "DEBUG": logging.DEBUG,
        "INFO": logging.INFO,
        "WARN": logging.WARN,
        "WARNING": logging.WARNING,
        "ERROR": logging.ERROR,
        "CRITICAL": logging.CRITICAL,
    }.get(level_str, logging.INFO)

    root = logging.getLogger()
    root.setLevel(lvl)

    for h in list(root.handlers):
        root.removeHandler(h)

    handler = logging.StreamHandler(sys.stdout)
    if json_fmt:
        handler.setFormatter(JsonFormatter())
    else:
        handler.setFormatter(logging.Formatter(
            fmt="%(asctime)s %(levelname)s %(name)s: %(message)s",
            datefmt="%Y-%m-%dT%H:%M:%SZ",
        ))
    root.addHandler(handler)

    logging.getLogger("boto3").setLevel(logging.WARNING)
    logging.getLogger("botocore").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("kubernetes").setLevel(logging.INFO)

    _CONFIGURED = True

def get_logger(name: str | None = None) -> logging.Logger:
    """Get a module-scoped logger"""
    return logging.getLogger(name or "kess")

class _CtxAdapter(logging.LoggerAdapter):
    """LoggerAdapter that merges contextual fields into every log call."""
    def process(self, msg, kwargs):
        ctx = dict(self.extra)
        extra = kwargs.get("extra") or {}
        ctx.update(extra)
        kwargs["extra"] = ctx
        return msg, kwargs

def with_context(logger: logging.Logger, **ctx: Any) -> logging.LoggerAdapter:
    """Bind static context fields: log = with_context(get_logger(__name__), registry=r, ns=ns)"""
    return _CtxAdapter(logger, ctx)

def set_level(level: str) -> None:
    """Dynamically adjust level"""
    logging.getLogger().setLevel(level.upper())
