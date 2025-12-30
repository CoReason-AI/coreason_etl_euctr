import os
import sys

from loguru import logger

# Remove default handler
logger.remove()

# Sink 1 (Stdout): Human readable, level=INFO
logger.add(
    sys.stderr,
    level=os.getenv("LOG_LEVEL", "INFO"),
    format=(
        "<green>{time:YYYY-MM-DD HH:mm:ss}</green> | "
        "<level>{level: <8}</level> | "
        "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - "
        "<level>{message}</level>"
    ),
)

# Sink 2 (File): JSON format for machines
logger.add(
    "logs/app.log",
    level="DEBUG",
    rotation="500 MB",
    retention="10 days",
    serialize=True,
    enqueue=True,
)

__all__ = ["logger"]
