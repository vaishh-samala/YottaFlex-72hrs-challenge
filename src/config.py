"""Central configuration for DataFlow.

All paths are resolved relative to the project root unless absolute. Every value can be
overridden by an environment variable (``DATAFLOW_*``) or by passing an explicit
``Settings`` instance, which is how the test-suite isolates itself in temp directories.
"""
from __future__ import annotations

import os
from dataclasses import dataclass, field, replace
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
SQL_DIR = PROJECT_ROOT / "sql"


def _env_path(name: str, default: Path) -> Path:
    value = os.environ.get(name)
    if not value:
        return default
    path = Path(value)
    return path if path.is_absolute() else PROJECT_ROOT / path


@dataclass(frozen=True)
class Settings:
    """Runtime settings for one pipeline execution."""

    db_path: Path = field(default_factory=lambda: _env_path("DATAFLOW_DB_PATH", PROJECT_ROOT / "data" / "warehouse.duckdb"))
    raw_dir: Path = field(default_factory=lambda: _env_path("DATAFLOW_RAW_DIR", PROJECT_ROOT / "data" / "raw"))
    processed_dir: Path = field(default_factory=lambda: _env_path("DATAFLOW_PROCESSED_DIR", PROJECT_ROOT / "data" / "processed"))
    quarantine_dir: Path = field(default_factory=lambda: _env_path("DATAFLOW_QUARANTINE_DIR", PROJECT_ROOT / "data" / "quarantine"))
    logs_dir: Path = field(default_factory=lambda: _env_path("DATAFLOW_LOGS_DIR", PROJECT_ROOT / "logs"))
    orders_path: Path = field(default_factory=lambda: _env_path("DATAFLOW_ORDERS_PATH", PROJECT_ROOT / "data" / "sample" / "orders.csv"))
    products_path: Path = field(default_factory=lambda: _env_path("DATAFLOW_PRODUCTS_PATH", PROJECT_ROOT / "data" / "sample" / "products.csv"))
    # Batch-level guard: if more than this share of order rows is rejected the whole
    # batch is treated as a bad delivery and nothing is loaded.
    max_reject_ratio: float = float(os.environ.get("DATAFLOW_MAX_REJECT_RATIO", "0.5"))
    # Seconds to wait for the DuckDB write lock (e.g. while a dashboard query is open).
    db_connect_timeout: float = float(os.environ.get("DATAFLOW_DB_CONNECT_TIMEOUT", "30"))
    # Write processed / quarantine Parquet copies (disabled in some benchmarks/tests).
    write_parquet: bool = os.environ.get("DATAFLOW_WRITE_PARQUET", "1") != "0"

    @property
    def lock_path(self) -> Path:
        return self.logs_dir / "pipeline.lock"

    @property
    def log_file(self) -> Path:
        return self.logs_dir / "pipeline.log"

    def with_overrides(self, **kwargs) -> "Settings":
        clean = {k: (Path(v) if k.endswith(("_path", "_dir")) and v is not None else v) for k, v in kwargs.items() if v is not None}
        return replace(self, **clean)

    def ensure_dirs(self) -> None:
        for directory in (self.raw_dir, self.processed_dir, self.quarantine_dir, self.logs_dir, self.db_path.parent):
            directory.mkdir(parents=True, exist_ok=True)

    @classmethod
    def for_root(cls, root: Path, **kwargs) -> "Settings":
        """Settings with every writable location under ``root`` (used by tests/benchmarks)."""
        root = Path(root)
        base = dict(
            db_path=root / "warehouse.duckdb",
            raw_dir=root / "raw",
            processed_dir=root / "processed",
            quarantine_dir=root / "quarantine",
            logs_dir=root / "logs",
        )
        base.update(kwargs)
        return cls(**base)
