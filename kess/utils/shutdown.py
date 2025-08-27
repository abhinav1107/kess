"""
Shutdown manager for coordinating graceful application shutdown.

Handles signal processing, shutdown coordination, and cleanup orchestration.
"""
import signal
import threading
import time
from typing import Callable, List, Optional
from kess.utils.log_setup import get_logger, with_context


class ShutdownManager:
    """
    Manages graceful application shutdown.

    Handles SIGTERM/SIGINT signals and coordinates shutdown across components.
    """

    def __init__(self, grace_period_seconds: int = 10):
        """
        Initialize shutdown manager.

        Args:
            grace_period_seconds: Maximum time to wait for graceful shutdown
        """
        self._log = get_logger(__name__)
        self._log_ctx = with_context(self._log, component="shutdown_manager")

        self.grace_period_seconds = grace_period_seconds
        self._shutdown_requested = threading.Event()
        self._shutdown_complete = threading.Event()
        self._shutdown_start_time: Optional[float] = None

        # Components to shut down
        self._shutdown_hooks: List[Callable[[], None]] = []
        self._shutdown_lock = threading.Lock()

        # Signal handling
        self._original_handlers = {}
        self._setup_signal_handlers()

        self._log_ctx.info("Shutdown manager initialized with %d second grace period", grace_period_seconds)

    def _setup_signal_handlers(self) -> None:
        """Setup signal handlers for SIGTERM and SIGINT."""
        try:
            self._original_handlers[signal.SIGTERM] = signal.signal(signal.SIGTERM, self._signal_handler)
            self._original_handlers[signal.SIGINT] = signal.signal(signal.SIGINT, self._signal_handler)
            self._log_ctx.debug("Signal handlers installed for SIGTERM and SIGINT")
        except Exception as e:
            self._log_ctx.warning("Failed to install signal handlers: %s", e)

    def _signal_handler(self, signum: int, frame) -> None:
        """Handle shutdown signals."""
        signal_name = signal.Signals(signum).name if hasattr(signal, 'Signals') else f"SIG{signum}"
        self._log_ctx.info("Received %s signal, initiating shutdown", signal_name)
        self.request_shutdown()

    def register_shutdown_hook(self, hook: Callable[[], None]) -> None:
        """
        Register a function to be called during shutdown.

        Args:
            hook: Function to call during shutdown (should be fast and non-blocking)
        """
        with self._shutdown_lock:
            self._shutdown_hooks.append(hook)
        self._log_ctx.debug("Registered shutdown hook: %s", hook.__name__ if hasattr(hook, '__name__') else str(hook))

    def request_shutdown(self) -> None:
        """Request application shutdown."""
        if self._shutdown_requested.is_set():
            self._log_ctx.debug("Shutdown already requested, ignoring duplicate request")
            return

        self._shutdown_requested.set()
        self._shutdown_start_time = time.time()
        self._log_ctx.info("Shutdown requested")

    def is_shutdown_requested(self) -> bool:
        """Check if shutdown has been requested."""
        return self._shutdown_requested.is_set()

    def wait_for_shutdown(self, timeout: Optional[float] = None) -> bool:
        """
        Wait for shutdown to be requested.

        Args:
            timeout: Maximum time to wait (None = wait indefinitely)

        Returns:
            True if shutdown was requested, False if timeout occurred
        """
        return self._shutdown_requested.wait(timeout)

    def shutdown_complete(self) -> None:
        """Mark shutdown as complete."""
        self._shutdown_complete.set()
        self._log_ctx.info("Shutdown completed")

    def is_shutdown_complete(self) -> bool:
        """Check if shutdown is complete."""
        return self._shutdown_complete.is_set()

    def get_remaining_grace_time(self) -> float:
        """Get remaining grace period time in seconds."""
        if not self._shutdown_start_time:
            return self.grace_period_seconds

        elapsed = time.time() - self._shutdown_start_time
        remaining = max(0, self.grace_period_seconds - elapsed)
        return remaining

    def execute_shutdown(self) -> None:
        """
        Execute the shutdown sequence.

        Calls all registered shutdown hooks and waits for completion.
        """
        if not self._shutdown_requested.is_set():
            self._log_ctx.warning("Shutdown not requested, skipping execution")
            return

        self._log_ctx.info("Executing shutdown sequence")

        # Execute shutdown hooks
        with self._shutdown_lock:
            hooks = list(self._shutdown_hooks)

        if hooks:
            self._log_ctx.info("Executing %d shutdown hooks", len(hooks))
            for hook in hooks:
                try:
                    hook_name = hook.__name__ if hasattr(hook, '__name__') else str(hook)
                    self._log_ctx.debug("Executing shutdown hook: %s", hook_name)
                    hook()
                except Exception as e:
                    self._log_ctx.error("Error in shutdown hook %s: %s",
                                      hook.__name__ if hasattr(hook, '__name__') else str(hook), e)
        else:
            self._log_ctx.debug("No shutdown hooks registered")

        self.shutdown_complete()
        self._log_ctx.info("Shutdown sequence completed")

    def wait_for_shutdown_completion(self, timeout: Optional[float] = None) -> bool:
        """
        Wait for shutdown to complete.

        Args:
            timeout: Maximum time to wait (None = wait indefinitely)

        Returns:
            True if shutdown completed, False if timeout occurred
        """
        return self._shutdown_complete.wait(timeout)

    def cleanup(self) -> None:
        """Cleanup and restore original signal handlers."""
        try:
            # Restore original signal handlers
            for signum, handler in self._original_handlers.items():
                signal.signal(signum, handler)
            self._log_ctx.debug("Original signal handlers restored")
        except Exception as e:
            self._log_ctx.warning("Failed to restore signal handlers: %s", e)

        self._log_ctx.info("Shutdown manager cleanup completed")


# Global shutdown manager instance
_shutdown_manager: Optional[ShutdownManager] = None


def get_shutdown_manager() -> ShutdownManager:
    """Get the global shutdown manager instance."""
    global _shutdown_manager
    if _shutdown_manager is None:
        raise RuntimeError("Shutdown manager not initialized. Call init_shutdown_manager() first.")
    return _shutdown_manager


def init_shutdown_manager(grace_period_seconds: int = 10) -> ShutdownManager:
    """Initialize the global shutdown manager."""
    global _shutdown_manager
    if _shutdown_manager is not None:
        raise RuntimeError("Shutdown manager already initialized")

    _shutdown_manager = ShutdownManager(grace_period_seconds)
    return _shutdown_manager


def request_shutdown() -> None:
    """Request application shutdown."""
    get_shutdown_manager().request_shutdown()


def is_shutdown_requested() -> bool:
    """Check if shutdown has been requested."""
    return get_shutdown_manager().is_shutdown_requested()


def register_shutdown_hook(hook: Callable[[], None]) -> None:
    """Register a shutdown hook."""
    get_shutdown_manager().register_shutdown_hook(hook)
