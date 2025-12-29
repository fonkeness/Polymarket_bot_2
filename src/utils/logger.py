"""Structured logging configuration for blockchain operations."""

from __future__ import annotations

import logging
import sys
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Callable

# Create logs directory
LOGS_DIR = Path(__file__).parent.parent.parent / "logs"
LOGS_DIR.mkdir(exist_ok=True)

# Log file path
LOG_FILE = LOGS_DIR / "blockchain_parser.log"


class PerformanceLogger:
    """Logger with performance metrics tracking."""

    def __init__(self, name: str) -> None:
        """
        Initialize performance logger.

        Args:
            name: Logger name (usually module name)
        """
        self.logger = logging.getLogger(name)
        self.logger.setLevel(logging.DEBUG)

        # Prevent duplicate handlers
        if not self.logger.handlers:
            # Console handler
            console_handler = logging.StreamHandler(sys.stdout)
            console_handler.setLevel(logging.INFO)
            console_formatter = logging.Formatter(
                "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
                datefmt="%Y-%m-%d %H:%M:%S",
            )
            console_handler.setFormatter(console_formatter)

            # File handler
            file_handler = logging.FileHandler(LOG_FILE, encoding="utf-8")
            file_handler.setLevel(logging.DEBUG)
            file_formatter = logging.Formatter(
                "%(asctime)s - %(name)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s",
                datefmt="%Y-%m-%d %H:%M:%S",
            )
            file_handler.setFormatter(file_formatter)

            self.logger.addHandler(console_handler)
            self.logger.addHandler(file_handler)

        # Performance metrics
        self.metrics: dict[str, float] = {}

    def debug(self, message: str, *args: object, **kwargs: object) -> None:
        """Log debug message."""
        self.logger.debug(message, *args, **kwargs)

    def info(self, message: str, *args: object, **kwargs: object) -> None:
        """Log info message."""
        self.logger.info(message, *args, **kwargs)

    def warning(self, message: str, *args: object, **kwargs: object) -> None:
        """Log warning message."""
        self.logger.warning(message, *args, **kwargs)

    def error(self, message: str, *args: object, **kwargs: object) -> None:
        """Log error message."""
        self.logger.error(message, *args, **kwargs)

    def exception(self, message: str, *args: object, **kwargs: object) -> None:
        """Log exception with traceback."""
        self.logger.exception(message, *args, **kwargs)

    def record_metric(self, name: str, value: float) -> None:
        """
        Record a performance metric.

        Args:
            name: Metric name (e.g., "blocks_per_sec", "events_per_sec")
            value: Metric value
        """
        self.metrics[name] = value
        self.debug(f"Metric {name}: {value:.2f}")

    def log_progress(
        self,
        current: int,
        total: int,
        item_name: str = "items",
        update_interval: int = 100,
    ) -> None:
        """
        Log progress with percentage.

        Args:
            current: Current progress
            total: Total items
            item_name: Name of items being processed
            update_interval: Log every N items
        """
        if current % update_interval == 0 or current == total:
            percentage = (current / total * 100) if total > 0 else 0
            self.info(
                f"Progress: {current}/{total} {item_name} ({percentage:.1f}%)",
            )

    def log_summary(self) -> None:
        """Log summary of all recorded metrics."""
        if not self.metrics:
            return

        self.info("=== Performance Summary ===")
        for name, value in self.metrics.items():
            self.info(f"{name}: {value:.2f}")
        self.info("===========================")


def get_logger(name: str) -> PerformanceLogger:
    """
    Get or create a logger instance.

    Args:
        name: Logger name (usually __name__)

    Returns:
        PerformanceLogger instance
    """
    return PerformanceLogger(name)

