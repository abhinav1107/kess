"""
- Wrapper around prometheus_client to start a metrics HTTP server
- default port is 9090
- exposes metrics as attributes
"""
import threading
from prometheus_client import Counter, Gauge, Histogram, start_http_server
from contextlib import nullcontext
from kess.utils.log_setup import get_logger


class MetricsServer:
    """
    Prometheus metrics

    Usage:
        ms = MetricsService(port=9090)
        ms.start()
        ms.readiness.set(1)
        with ms.sync_timer():
            ... do one sync ...
        ms.secrets_synced.inc()
        ms.token_expiry.labels(registry=host).set(expires_ts)
    """
    def __init__(
        self,
        port: int = 9090,
        host: str = "0.0.0.0",
        *,
        prog: str = "kess",
    ) -> None:
        self.port = port
        self.host = host
        self._log = get_logger(__name__)
        self._started = False
        self._lock = threading.Lock()

        self.secrets_synced = None
        self.sync_failures = None
        self.token_expiry = None
        self.last_sync = None
        self.next_sync_eta = None
        self.readiness = None
        self.liveness = None
        self.sync_duration = None


    def start(self) -> None:
        with self._lock:
            if self._started:
                return

            # Start the HTTP server (daemon thread managed by the library)
            start_http_server(self.port, addr=self.host)

            # Define metrics
            self.secrets_synced = Counter(
                "kess_secrets_synced_total",
                "Number of ImagePullSecrets created or patched"
            )

            self.sync_failures = Counter(
                "kess_sync_failures_total",
                "Number of sync failures"
            )

            self.token_expiry = Gauge(
                "kess_token_expiry_timestamp",
                "ECR token expiry as a Unix timestamp",
                ["registry"]
            )

            self.last_sync = Gauge(
                "kess_last_sync_timestamp",
                "Last successful sync time (unix timestamp)"
            )

            self.next_sync_eta = Gauge(
                "kess_next_sync_eta_seconds",
                "Seconds until next planned sync (>=0; 0/omit if unknown)"
            )

            self.readiness = Gauge("kess_readiness", "Readiness (1=ready, 0=not ready)")
            self.liveness = Gauge("kess_liveness", "Liveness (1=live, 0=not live)")

            self.sync_duration = Histogram(
                "kess_sync_duration_seconds",
                "Duration of a full sync round",
                buckets=(0.1, 0.25, 0.5, 1, 2, 5, 10, 20, 30, 60),
            )

            self._started = True
            self._log.info("Metrics server started on %s:%s", self.host, self.port)

    def stop(self) -> None:
        self._log.info("Metrics server stopping (will terminate with process)")

    def sync_timer(self):
        """Context manager to time a sync cycle even if metrics aren't started."""
        if self._started and self.sync_duration is not None:
            return self.sync_duration.time()
        return nullcontext()
