from kess.core.config import init_config
from kess.utils.log_setup import init_logging, get_logger, with_context
from kess.utils.startup import create_parser, validate_arguments, resolve_version
from kess.health import HealthServer, MetricsServer
import time

def main() -> int:
    parser = create_parser()
    args = parser.parse_args()

    init_logging(level=args.log_level, fmt=args.log_format)
    log = get_logger("main")
    log_ctx = with_context(log, component="main")
    if not validate_arguments(args):
        return 2
    cfg = init_config(args)
    health = HealthServer(port=cfg.health_port, host=cfg.health_host)
    metrics = MetricsServer(port=cfg.metrics_port, host=cfg.health_host)

    try:
        log_ctx.info("Starting kess, version %s", resolve_version())
        log_ctx.debug("Configuration: %s", cfg)

        # start health server
        log_ctx.info("Starting http server for health checks")
        health.start()
        health.set_ready(True)
        health.register_readiness_check("k8s_client", lambda: (True, None))
        health.register_readiness_check("config_loaded", lambda: (cfg is not None, None))
        log_ctx.info("kess http server started")

        # start metrics server
        log_ctx.info("Starting metrics server for prometheus scrapping")
        metrics.start()
        log_ctx.info("kess metrics server started")
        metrics.liveness.set(1)
        metrics.readiness.set(1)

        time.sleep(60)
        log_ctx.info("No runner wired yet; exiting cleanly.")

        health.set_ready(False)
        metrics.readiness.set(0)
        health.stop()
        log_ctx.info("kess http server stopped")
        metrics.stop()
        log_ctx.info("kess metrics server stopped")

        return 0

    except KeyboardInterrupt:
        log_ctx.info("Received interrupt signal, shutting down")
        health.set_ready(False)
        health.stop()
        log_ctx.info("kess http server stopped")
        metrics.stop()
        log_ctx.info("kess metrics server stopped")
        return 0
    except Exception as e:
        log_ctx.exception("Fatal error: %s", e)
        health.set_ready(False)
        health.stop()
        log_ctx.info("kess http server stopped")
        metrics.stop()
        log_ctx.info("kess metrics server stopped")
        return 1


if __name__ == "__main__":
    import sys
    sys.exit(main())
