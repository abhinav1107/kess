import argparse
from dataclasses import dataclass, replace
from typing import Optional, Any, Dict
from kess.utils.log_setup import get_logger, with_context
import os
import yaml

@dataclass(frozen=True)
class Config:

    # config file path
    config_file: str = "/etc/kess/config.yaml"

    # time intervals
    loop_interval: int = 5                  # in minutes
    token_refresh_threshold: int = 11       # in hours

    # health and metrics
    health_host: str = "0.0.0.0"
    health_port: int = 8080
    metrics_port: int = 9090


# global
_CONFIG: Optional[Config] = None
_log = get_logger(__name__)
log_ctx = with_context(_log, source="config")

def _load_file(path: str) -> Dict[str, Any]:
    try:
        with open(path, "r", encoding="utf-8") as f:
            try:
                data = yaml.safe_load(f)
                if not isinstance(data, dict):
                    return {}

                return {k.replace("-", "_").lower(): v for k, v in data.items()}
            except yaml.YAMLError as e:
                log_ctx.warning("Failed to parse YAML file %s: %s", path, e)
                log_ctx.debug("Defaulting to empty config")
                return {}
    except FileNotFoundError:
        log_ctx.debug("Config file %s not found, using defaults", path)
        return {}

def _coerce(value: str, to_type: Any):
    if to_type is bool:
        return value.lower() in ("1", "true", "yes", "on")
    if to_type is int:
        return int(value)
    if to_type is float:
        return float(value)
    return value

def _load_env_overrides(model: type[Config]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    prefix = "KESS_"
    ann = model.__annotations__
    for k, v in os.environ.items():
        if not k.startswith(prefix):
            continue
        key = k[len(prefix):].lower()
        if key in ann:
            try:
                out[key] = _coerce(v, ann[key])
            except Exception:
                pass

    return out

def _cli_overrides(args: argparse.Namespace) -> Dict[str, Any]:
    return {k: v for k, v in vars(args).items() if v is not None and k in Config.__annotations__}

def init_config(args: argparse.Namespace | None = None) -> Config:
    """
    Merge: defaults <- file <- env <- CLI (CLI wins). Call once at startup.
    """
    cfg = Config()

    # load from file
    file_path = (getattr(args, "config_file", None) if args else None or os.getenv("KESS_CONFIG")) or cfg.config_file
    file_vals = _load_file(file_path)
    cfg = replace(cfg, **{k: v for k, v in file_vals.items() if k in Config.__annotations__})

    # load from env
    env_vals = _load_env_overrides(Config)
    cfg = replace(cfg, **env_vals)

    # load from CLI
    if args:
        cli_vals = _cli_overrides(args)
        cfg = replace(cfg, **cli_vals)

    global _CONFIG
    _CONFIG = cfg
    return cfg

def get_config() -> Config:
    """Get the global config, must call init_config() first"""
    if _CONFIG is None:
        raise RuntimeError("Config not initialized, call init_config() first")
    return _CONFIG
