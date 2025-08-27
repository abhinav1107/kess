import argparse
from importlib.metadata import PackageNotFoundError, version as pkg_version
from kess.utils.log_setup import get_logger, with_context
from pathlib import Path

_log = get_logger(__name__)

def resolve_version() -> str:
    """Resolve application version"""
    try:
        return pkg_version("kess")
    except PackageNotFoundError:
        from kess import __version__ as ver
        return ver

def create_parser() -> argparse.ArgumentParser:
    """Create command line argument parser"""
    parser = argparse.ArgumentParser(
        prog="kess",
        description="KESS — Kubernetes ECR Secret Sync",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run with default configuration
  kess

  # Run with custom config file
  kess --config /path/to/config.yaml

  # Run with custom loop interval
  kess --loop-interval 600

  # Run with custom token refresh threshold
  kess --token-refresh-threshold 43200

  # Show version and exit
  kess --version
        """
    )

    parser.add_argument(
        '--config', '-c',
        dest="config_file",
        default="/etc/kess/config.yaml",
        help="Path to application configuration file (default: /etc/kess/config.yaml)"
    )

    parser.add_argument(
        '--loop-interval', '-i',
        type=int,
        dest="loop_interval",
        help="Main loop interval in minutes (default: 5, overrides config file)"
    )

    parser.add_argument(
        '--token-refresh-threshold', '-t',
        type=int,
        dest="token_refresh_threshold",
        help="Token refresh threshold in hours (default: 11, overrides config file)"
    )

    parser.add_argument(
        '--health-port', '-P',
        type=int,
        dest="health_port",
        help="Health server port (default: 8080, overrides config file)"
    )

    parser.add_argument(
        '--health-host', '-H',
        dest="health_host",
        help="Health server host (default: 0.0.0.0, overrides config file)"
    )

    parser.add_argument(
        '--metrics-port',
        type=int,
        dest="metrics_port",
        help="Health server host (default: 0.0.0.0, overrides config file)"
    )

    parser.add_argument(
        '--log-level',
        dest="log_level",
        help="Enable verbose logging (default: INFO)"
    )

    parser.add_argument(
        '--log-format',
        dest="log_format",
        help="Set log format (default: text. text or json)"
    )

    parser.add_argument(
        '--version', '-v',
        action='version',
        version=f'%(prog)s {resolve_version()}'
    )

    return parser


def validate_arguments(args: argparse.Namespace) -> bool:
    """Validate command line arguments"""
    log_ctx = with_context(_log, source="validate_arguments")

    if args.config_file and args.config_file != '/etc/kess/config.yaml':
        config_path = Path(args.config_file)
        if not config_path.exists():
            log_ctx.error(f"Configuration file not found: {args.config}")
            return False

    if args.loop_interval and args.loop_interval != 5 and args.loop_interval <= 0:
        log_ctx.error("Loop interval must be positive")
        return False

    if args.token_refresh_threshold and args.token_refresh_threshold != 11 and args.token_refresh_threshold <= 0:
        log_ctx.error("Token refresh threshold must be positive")
        return False

    if args.health_port and args.health_port != 8080 and (args.health_port < 1 or args.health_port > 65535):
        log_ctx.error("Health port must be between 1 and 65535")
        return False

    return True
