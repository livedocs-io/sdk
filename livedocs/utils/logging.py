"""
Unified logging for Livedocs SDK.

This module provides a context-aware logging system that:
- In IPYTHON mode: Routes logs to middleman_debug() for Jupyter display interception
- In RELAY mode: Uses standard Python logging with [livedocs-sdk] prefix

Usage:
    from livedocs.utils.logging import get_logger

    logger = get_logger(__name__)
    logger.info("Processing data...")
    logger.error("Something went wrong", exc_info=True)
"""

from __future__ import annotations

import json
import logging
import sys
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from livedocs.types import SDKContext

# Module-level context storage
_sdk_context: SDKContext | None = None
_configured: bool = False

# Logger name prefix
LOGGER_PREFIX = "livedocs-sdk"


class LivedocsSDKHandler(logging.Handler):
    """
    Custom logging handler that routes logs based on SDK context.

    - IPYTHON mode: Uses middleman_debug() for Jupyter display interception
    - RELAY mode: Outputs to stderr with [livedocs-sdk] prefix
    """

    def emit(self, record: logging.LogRecord) -> None:
        try:
            msg = self.format(record)
            level = "error" if record.levelno >= logging.WARNING else "info"

            if _sdk_context is not None:
                from livedocs.types import SDKContext

                if _sdk_context == SDKContext.IPYTHON:
                    self._emit_ipython(record, msg, level)
                    return

            # RELAY mode or context not set - use stderr
            self._emit_relay(record, msg)

        except Exception:
            self.handleError(record)

    def _emit_ipython(self, record: logging.LogRecord, msg: str, level: str) -> None:
        """Emit log via middleman_debug() for IPYTHON context."""
        try:
            from IPython.display import display

            # Format data for middleman
            try:
                content_str = json.dumps(msg, indent=2, default=str, ensure_ascii=False)
                mime_type = "application/json"
            except Exception:
                content_str = str(msg)
                mime_type = "text/plain"

            # Get a clean label from the logger name
            label = record.name
            if label.startswith(f"{LOGGER_PREFIX}."):
                label = label[len(LOGGER_PREFIX) + 1 :]
            elif label == LOGGER_PREFIX:
                label = "sdk"

            print(f"[Middleman Debug - {level.upper()}] {label}: {msg}")
            display(
                {mime_type: content_str},
                metadata={
                    "middleman_debug": True,
                    "middleman_debug_label": f"[{label}]",
                    "middleman_debug_level": level.lower(),
                },
                raw=True,
            )
        except ImportError:
            # IPython not available, fall back to stderr
            self._emit_relay(record, msg)

    def _emit_relay(self, record: logging.LogRecord, msg: str) -> None:
        """Emit log to stderr for RELAY context."""
        # Get level name
        level_name = record.levelname

        # Get a clean module name
        module = record.name
        if module.startswith(f"{LOGGER_PREFIX}."):
            module = module[len(LOGGER_PREFIX) + 1 :]
        elif module == LOGGER_PREFIX:
            module = "sdk"

        # Format: [livedocs-sdk] [LEVEL] [module] message
        formatted = f"[livedocs-sdk] [{level_name}] [{module}] {msg}"
        print(formatted, file=sys.stderr)


class LivedocsSDKFormatter(logging.Formatter):
    """Simple formatter for SDK logs."""

    def format(self, record: logging.LogRecord) -> str:
        # Include exception info if present
        msg = record.getMessage()
        if record.exc_info:
            if not record.exc_text:
                record.exc_text = self.formatException(record.exc_info)
            if record.exc_text:
                if msg[-1:] != "\n":
                    msg = msg + "\n"
                msg = msg + record.exc_text
        return msg


def configure_logging(context: SDKContext, level: int = logging.INFO) -> None:
    """
    Configure SDK logging for the given context.

    This should be called once during SDK initialization (in Livedocs.initialize()).

    Args:
        context: The SDK context (IPYTHON or RELAY)
        level: The logging level (default: INFO)
    """
    global _sdk_context, _configured

    _sdk_context = context
    _configured = True

    # Get the root SDK logger
    root_logger = logging.getLogger(LOGGER_PREFIX)
    root_logger.setLevel(level)

    # Remove any existing handlers
    root_logger.handlers.clear()

    # Add our custom handler
    handler = LivedocsSDKHandler()
    handler.setFormatter(LivedocsSDKFormatter())
    root_logger.addHandler(handler)

    # Prevent propagation to root logger (avoids duplicate logs)
    root_logger.propagate = False


def get_logger(name: str | None = None) -> logging.Logger:
    """
    Get a logger instance for the SDK.

    Args:
        name: Optional module name. If provided, creates a child logger
              under the 'livedocs-sdk' namespace.
              Example: get_logger("bigquery") -> "livedocs-sdk.bigquery"

    Returns:
        A configured logger instance

    Usage:
        # In a module file
        from livedocs.utils.logging import get_logger
        logger = get_logger(__name__)  # or get_logger("bigquery")

        logger.info("Starting operation")
        logger.error("Failed", exc_info=True)
    """
    if name is None:
        return logging.getLogger(LOGGER_PREFIX)

    # Clean up the name - extract just the module part
    # e.g., "livedocs.datasources.bigquery" -> "bigquery"
    if "." in name:
        name = name.rsplit(".", 1)[-1]

    return logging.getLogger(f"{LOGGER_PREFIX}.{name}")


def set_context(context: SDKContext) -> None:
    """
    Set the SDK context for logging.

    This is useful if you need to change context after initialization
    or configure logging manually.

    Args:
        context: The SDK context (IPYTHON or RELAY)
    """
    global _sdk_context
    _sdk_context = context

    # If not yet configured, do initial configuration
    if not _configured:
        configure_logging(context)


def get_context() -> SDKContext | None:
    """Get the current SDK context."""
    return _sdk_context


# Convenience functions for quick logging without getting a logger instance
def log_info(message: str, **kwargs: Any) -> None:
    """Log an info message."""
    get_logger().info(message, **kwargs)


def log_warning(message: str, **kwargs: Any) -> None:
    """Log a warning message."""
    get_logger().warning(message, **kwargs)


def log_error(message: str, exc_info: bool = False, **kwargs: Any) -> None:
    """Log an error message."""
    get_logger().error(message, exc_info=exc_info, **kwargs)


def log_debug(message: str, **kwargs: Any) -> None:
    """Log a debug message."""
    get_logger().debug(message, **kwargs)


__all__ = [
    "configure_logging",
    "get_logger",
    "set_context",
    "get_context",
    "log_info",
    "log_warning",
    "log_error",
    "log_debug",
    "LOGGER_PREFIX",
]
