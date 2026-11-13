"""Structured (JSON-lines) logging plus a readable console stream."""
from __future__ import annotations

import json
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path

LOGGER_NAME = "dataflow"
_STANDARD_ATTRS = set(vars(logging.LogRecord("", 0, "", 0, "", (), None)).keys()) | {"message", "asctime"}


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "ts": datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat(timespec="milliseconds"),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        for key, value in vars(record).items():
            if key not in _STANDARD_ATTRS and not key.startswith("_"):
                payload[key] = value
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload, default=str)


class ConsoleFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        stage = getattr(record, "stage", "-")
        run_id = getattr(record, "run_id", "-")
        return f"{datetime.now().strftime('%H:%M:%S')} {record.levelname:<7} [{run_id}] [{stage}] {record.getMessage()}"


def configure_logging(log_file: Path, console: bool = True, level: int = logging.INFO) -> logging.Logger:
    """Attach a JSON file handler for ``log_file`` (idempotent per path)."""
    logger = logging.getLogger(LOGGER_NAME)
    logger.setLevel(level)
    logger.propagate = False
    log_file = Path(log_file)
    log_file.parent.mkdir(parents=True, exist_ok=True)
    target = str(log_file.resolve())

    for handler in list(logger.handlers):
        if isinstance(handler, logging.FileHandler) and handler.baseFilename != target:
            logger.removeHandler(handler)
            handler.close()
    if not any(isinstance(h, logging.FileHandler) and h.baseFilename == target for h in logger.handlers):
        file_handler = logging.FileHandler(target, encoding="utf-8")
        file_handler.setFormatter(JsonFormatter())
        logger.addHandler(file_handler)
    has_console = any(getattr(h, "_dataflow_console", False) for h in logger.handlers)
    if console and not has_console:
        stream = logging.StreamHandler(sys.stderr)
        stream.setFormatter(ConsoleFormatter())
        stream._dataflow_console = True  # type: ignore[attr-defined]
        logger.addHandler(stream)
    return logger


class StageLogger(logging.LoggerAdapter):
    """Adapter that merges per-call ``extra`` with the run/stage context (works on 3.11+)."""

    def process(self, msg, kwargs):
        kwargs["extra"] = {**self.extra, **kwargs.get("extra", {})}
        return msg, kwargs


def get_logger(run_id: str | None = None, stage: str | None = None) -> StageLogger:
    return StageLogger(logging.getLogger(LOGGER_NAME), {"run_id": run_id or "-", "stage": stage or "-"})
