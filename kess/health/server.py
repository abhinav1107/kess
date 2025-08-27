"""
Lightweight health server for kess.

Endpoints:
  GET /healthz  -> 200 if liveness OK, else 503
  GET /readyz   -> 200 if ready AND all readiness checks pass (within timeout), else 503
  GET /status   -> 200 JSON with ready/live flags, timestamps, next_sync_in, and check details

Notes:
  - Uses stdlib http.server on a daemon thread (no extra deps).
  - Readiness is an explicit flag you toggle; liveness can be forced to 503 via set_liveness_ok(False).
  - Readiness checks are fast, non-blocking callables; each is executed with a short timeout.
  - Intended to be started very early with ready=False, then set to True once the app is ready.
"""
import json
import threading
import time
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeout
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Any, Callable, Dict, List, Optional, Tuple

from kess.utils.log_setup import get_logger

ReadinessCheck = Callable[[], Tuple[bool, Optional[str]]]
_JSON_CT = "application/json; charset=utf-8"
_TEXT_CT = "text/plain; charset=utf-8"


class HealthServer:
    """
    Start/stop a tiny HTTP server exposing /healthz, /readyz, /status.

    Usage:
        health = HealthServer(port=8080)
        health.start()
        # ... after first successful init/sync:
        health.set_ready(True)
        # on shutdown:
        health.set_ready(False)
        health.stop()
    """

    def __init__(
        self,
        host: str = "0.0.0.0",
        port: int = 8080,
        *,
        prog: str = "kess",
        checks_timeout_seconds: float = 0.25,
        max_check_workers: int = 4
    ) -> None:
        self.host = host
        self.port = port
        self.prog = prog
        self.checks_timeout_seconds = checks_timeout_seconds
        self.max_check_workers = max(1, max_check_workers)

        self._log = get_logger(__name__)

        # Flags & state
        self._ready = threading.Event()
        self._live_ok = True
        self._shutting_down = False
        self._started_at = int(time.time())
        self._last_sync_ts: Optional[int] = None
        self._next_sync_in: Optional[int] = None
        self._state_lock = threading.Lock()

        # Readiness checks registry
        self._checks_lock = threading.Lock()
        self._checks: Dict[str, ReadinessCheck] = {}

        # HTTP server infra
        self._httpd: Optional[HTTPServer] = None
        self._thread: Optional[threading.Thread] = None

    # -------------------- Public API --------------------

    def start(self) -> None:
        """Start the health server in a daemon thread (idempotent)."""
        if self._thread and self._thread.is_alive():
            return

        # Build handler bound to this instance
        server = self

        class _Handler(BaseHTTPRequestHandler):
            # Silence default access logs; rely on structured logging.
            def log_message(self, _format: str, *_args: Any) -> None:  # noqa: N802
                return

            def _write(
                self, code: int, body: str, content_type: str = _TEXT_CT, headers: Optional[Dict[str, str]] = None
            ) -> None:
                self.send_response(code)
                self.send_header("Content-Type", content_type)
                self.send_header("Cache-Control", "no-store, must-revalidate")
                if headers:
                    for k, v in headers.items():
                        self.send_header(k, v)
                self.end_headers()
                self.wfile.write(body.encode("utf-8"))

            def do_GET(self) -> None:  # noqa: N802
                path = self.path
                if path == "/healthz" or path == "/livez":
                    code, body = server._handle_healthz()
                    self._write(code, body)
                    return
                if path == "/readyz" or path == "/readiness":
                    code, body = server._handle_readyz()
                    self._write(code, body)
                    return
                if path == "/status":
                    payload = server._status_payload()
                    self._write(200, json.dumps(payload, separators=(",", ":")), content_type=_JSON_CT)
                    return
                self._write(404, "not found\n")

        httpd = HTTPServer((self.host, self.port), _Handler)
        self._httpd = httpd

        self._thread = threading.Thread(target=httpd.serve_forever, name="kess-health", daemon=True)
        self._thread.start()
        self._log.info("Health server started on %s:%s", self.host, self.port)

    def stop(self, *, timeout: float = 2.0) -> None:
        """Stop the HTTP server gracefully."""
        httpd, thread = self._httpd, self._thread
        if httpd is None or thread is None:
            return
        try:
            httpd.shutdown()
            httpd.server_close()
        except Exception:
            # best-effort; process is shutting down
            pass
        finally:
            thread.join(timeout=timeout)
            self._httpd = None
            self._thread = None
            self._log.info("Health server stopped")

    def set_ready(self, ready: bool) -> None:
        """Flip readiness flag (drain immediately by setting False)."""
        if ready:
            self._ready.set()
            self._log.info("Readiness set to true")
        else:
            self._ready.clear()
            self._log.info("Readiness set to false")

    def set_shutting_down(self, shutting_down: bool) -> None:
        """Set shutdown flag to indicate application is shutting down."""
        with self._state_lock:
            self._shutting_down = shutting_down
        if shutting_down:
            self._log.info("Health server marked as shutting down")
        else:
            self._log.info("Health server shutdown flag cleared")

    def set_liveness_ok(self, ok: bool, *, reason: Optional[str] = None) -> None:
        """
        If set to False, /healthz will return 503. Keep it True during graceful shutdown
        so the process can exit cleanly without being restarted.
        """
        with self._state_lock:
            self._live_ok = ok
        if ok:
            self._log.warning("Liveness restored to OK")
        else:
            self._log.error("Liveness set to NOT OK%s", f" (reason: {reason})" if reason else "")

    def set_last_sync(self, when_unix: Optional[int] = None) -> None:
        """Record the time of the last successful full sync (defaults to now)."""
        with self._state_lock:
            self._last_sync_ts = int(when_unix or time.time())

    def set_next_sync_in(self, seconds: Optional[int]) -> None:
        """Record ETA (in seconds) until the next planned sync; set None if unknown."""
        with self._state_lock:
            self._next_sync_in = None if seconds is None else max(0, int(seconds))

    def register_readiness_check(self, name: str, fn: ReadinessCheck) -> None:
        """
        Register a fast, non-blocking readiness check.
        fn must return (ok: bool, reason: Optional[str]).
        """
        if not callable(fn):
            raise TypeError("readiness check must be callable")
        with self._checks_lock:
            self._checks[name] = fn
        self._log.info("Registered readiness check: %s", name)

    # -------------------- Internal helpers --------------------

    def _eval_checks(self) -> Tuple[bool, Dict[str, Dict[str, Any]]]:
        """
        Execute registered readiness checks with a small timeout.
        Returns (all_ok, details_by_check).
        """
        with self._checks_lock:
            items = list(self._checks.items())

        if not items:
            return True, {}

        all_ok = True
        details: Dict[str, Dict[str, Any]] = {}

        # Small thread pool to avoid blocking the request
        workers = min(len(items), self.max_check_workers)
        start_batch = time.perf_counter()
        with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="kess-ready") as pool:
            futures = {
                name: pool.submit(self._safe_check_wrapper, name, fn)
                for name, fn in items
            }
            for name, fut in futures.items():
                t0 = time.perf_counter()
                try:
                    ok, reason = fut.result(timeout=self.checks_timeout_seconds)
                    dur_ms = int((time.perf_counter() - t0) * 1000)
                except FuturesTimeout:
                    ok, reason, dur_ms = False, "timeout", int((time.perf_counter() - t0) * 1000)
                except Exception as e:
                    ok, reason, dur_ms = False, f"error: {e}", int((time.perf_counter() - t0) * 1000)

                details[name] = {"ok": ok, "reason": reason, "duration_ms": dur_ms}
                all_ok = all_ok and ok

        _ = start_batch  # reserved for future aggregate metrics
        return all_ok, details

    @staticmethod
    def _safe_check_wrapper(_name: str, fn: ReadinessCheck) -> Tuple[bool, Optional[str]]:
        res = fn()
        if isinstance(res, tuple) and len(res) == 2:
            ok, reason = res
            return bool(ok), (None if reason in ("", None) else str(reason))
        # Allow boolean-only returns
        return bool(res), None

    def _handle_healthz(self) -> Tuple[int, str]:
        with self._state_lock:
            live_ok = self._live_ok
            shutting_down = self._shutting_down
        
        if shutting_down:
            return 200, "shutting down\n"  # Still alive during shutdown
        elif live_ok:
            return 200, "ok\n"
        else:
            return 503, "not ok\n"

    def _handle_readyz(self) -> Tuple[int, str]:
        with self._state_lock:
            shutting_down = self._shutting_down
        
        if shutting_down:
            return 503, "shutting down\n"  # Not ready for new work during shutdown
        
        if not self._ready.is_set():
            return 503, "not ready\n"

        all_ok, details = self._eval_checks()
        if all_ok:
            return 200, "ready\n"

        # Aggregate reasons (short)
        problems: List[str] = []
        for name, d in details.items():
            if not d.get("ok", False):
                reason = d.get("reason") or "failed"
                problems.append(f"{name}: {reason}")
        body = "not ready: " + "; ".join(problems) + "\n"
        return 503, body

    def _status_payload(self) -> Dict[str, Any]:
        with self._state_lock:
            last_sync = self._last_sync_ts
            next_sync = self._next_sync_in
            live_ok = self._live_ok
            shutting_down = self._shutting_down
            started = self._started_at

        ready_flag = self._ready.is_set()
        _, details = self._eval_checks()
        payload: Dict[str, Any] = {
            "prog": self.prog,
            "ready": bool(ready_flag) and not shutting_down,
            "live": bool(live_ok),
            "shutting_down": shutting_down,
            "since": started,            # unix ts the server started
            "last_sync": last_sync,      # unix ts or null
            "next_sync_in": next_sync,   # seconds or null
            "checks": details,           # { name: { ok, reason, duration_ms } }
        }
        return payload
