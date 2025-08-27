import json
import logging
import os
import sys
import time
from datetime import datetime, timezone
from typing import Any, Dict


_CONFIGURED = False
_TIMEFMT = "%Y-%m-%dT%H:%M:%SZ"
_TEXT_FMT = "%(asctime)s %(levelname)-7s %(prog)-4s %(module)-15.15s:%(lineno)4d: %(message)s"
_STD_FIELDS = {
    "name", "msg", "args", "levelname", "levelno", "pathname", "filename", "module",
    "exc_info", "exc_text", "stack_info", "lineno", "funcName", "created", "msecs",
    "relativeCreated", "thread", "threadName", "processName", "process",
}

class _UTCFORMATTER(logging.Formatter):
    converter = staticmethod(time.gmtime)


class _ProgramFilter(logging.Filter):
    """Injects program identifier and computed 'source' into each record."""
    def __init__(self, prog: str = "kess"):
        super().__init__()
        self._prog = prog

    def filter(self, record: logging.LogRecord) -> bool:
        # Ensure these fields always exist
        if not hasattr(record, "prog"):
            record.prog = self._prog
        if not hasattr(record, "source"):
            record.source = f"{record.module}:{record.lineno}"
        return True


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        ts = datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat().replace("+00:00", "Z")

        payload: Dict[str, Any] = {
            "timestamp": ts,
            "level": record.levelname,
            "prog": getattr(record, "prog", "kess"),
            "source": f"{record.module}:{record.lineno}",
            "logger": record.name,
            "msg": record.getMessage(),
        }

        # include any extra fields
        for k, v in record.__dict__.items():
            if k in _STD_FIELDS or k.startswith("_") or k in ("prog", "source"):
                continue
            payload[k] = v
        if record.exc_info:
            payload["exc"] = self.formatException(record.exc_info)

        return json.dumps(payload, separators=(",", ":"), ensure_ascii=False)


def _default_format() -> str:
    # Default to JSON if we appear to be inside Kubernetes; else text
    if os.getenv("KUBERNETES_SERVICE_HOST"):
        return "json"

    return "text"

def init_logging(*, level: str | None = None, fmt: str | None = None, prog: str = "kess") -> None:
    """
    Initialize root logger once.
    - level: explicit level or env KESS_LOG_LEVEL (default INFO)
    - fmt: "json" or "text" or env KESS_LOG_FORMAT (default auto-detect)
    """
    global _CONFIGURED
    if _CONFIGURED:
        return

    level_str = (level or os.getenv("KESS_LOG_LEVEL", "INFO")).upper()
    fmt_str = (fmt or os.getenv("KESS_LOG_FORMAT", _default_format())).lower()

    lvl = {
        "DEBUG": logging.DEBUG,
        "INFO": logging.INFO,
        "WARN": logging.WARNING,
        "WARNING": logging.WARNING,
        "ERROR": logging.ERROR,
        "CRITICAL": logging.CRITICAL,
    }.get(level_str, logging.INFO)

    root = logging.getLogger()
    root.setLevel(lvl)

    for h in list(root.handlers):
        root.removeHandler(h)

    handler = logging.StreamHandler(sys.stdout)
    handler.addFilter(_ProgramFilter(prog=prog))

    if fmt_str == "json":
        handler.setFormatter(JsonFormatter())
    else:
        handler.setFormatter(_UTCFORMATTER(fmt=_TEXT_FMT, datefmt=_TIMEFMT))

    root.addHandler(handler)

    # Adjust levels for noisy libraries
    logging.getLogger("boto3").setLevel(logging.WARNING)
    logging.getLogger("botocore").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("kubernetes").setLevel(logging.INFO)

    _CONFIGURED = True


def get_logger(name: str = "kess") -> logging.Logger:
    return logging.getLogger(name)


class _CtxAdapter(logging.LoggerAdapter):
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
