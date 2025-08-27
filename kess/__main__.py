from kess.core.config import init_config
from kess.utils.log_setup import init_logging, get_logger, with_context
from kess.utils.startup import create_parser, validate_arguments, resolve_version
from kess.utils.shutdown import init_shutdown_manager, is_shutdown_requested
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

    # Initialize shutdown manager
    shutdown_manager = init_shutdown_manager()

    health = HealthServer(port=cfg.health_port, host=cfg.health_host)
    metrics = MetricsServer(port=cfg.metrics_port, host=cfg.health_host)

    # Register shutdown hooks
    def shutdown_health():
        health.set_shutting_down(True)
        health.set_ready(False)
        metrics.shutdown_status.set(1)  # Set shutdown status metric
        health.stop()
        log_ctx.info("Health server stopped")

    def shutdown_metrics():
        metrics.stop()
        log_ctx.info("Metrics server stopped")

    shutdown_manager.register_shutdown_hook(shutdown_health)
    shutdown_manager.register_shutdown_hook(shutdown_metrics)

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

        # TODO: change this with actual application logic later.
        # Main loop with shutdown checking (keeping the 60-second sleep as requested)
        log_ctx.info("Entering main loop (sleeping for 60 seconds)")
        start_time = time.time()

        while time.time() - start_time < 60:
            if is_shutdown_requested():
                log_ctx.info("Shutdown requested, breaking out of main loop")
                break
            time.sleep(1)  # Check shutdown every second instead of blocking for 60s

        if not is_shutdown_requested():
            log_ctx.info("No runner wired yet; exiting cleanly.")
        else:
            log_ctx.info("Shutdown requested, executing shutdown sequence")
            shutdown_manager.execute_shutdown()

        return 0

    except KeyboardInterrupt:
        log_ctx.info("Received interrupt signal, shutting down")
        shutdown_manager.request_shutdown()
        shutdown_manager.execute_shutdown()
        return 0

    except Exception as e:
        log_ctx.exception("Fatal error: %s", e)
        shutdown_manager.request_shutdown()
        shutdown_manager.execute_shutdown()
        return 1

    finally:
        # Cleanup shutdown manager
        shutdown_manager.cleanup()


if __name__ == "__main__":
    import sys
    sys.exit(main())
