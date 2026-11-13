"""DuckDB connection helpers and schema bootstrap."""
from __future__ import annotations

import time
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

import duckdb

from src.config import SQL_DIR


class DatabaseBusyError(RuntimeError):
    """The database file is locked by another process (e.g. a running pipeline)."""


def _is_lock_error(exc: Exception) -> bool:
    text = str(exc).lower()
    return "lock" in text or "conflicting" in text or "being used" in text


def connect(db_path: Path | str, read_only: bool = False, timeout: float = 30.0) -> duckdb.DuckDBPyConnection:
    """Open DuckDB, retrying while another process holds the file lock."""
    db_path = Path(db_path)
    if not read_only:
        db_path.parent.mkdir(parents=True, exist_ok=True)
    deadline = time.monotonic() + timeout
    delay = 0.1
    while True:
        try:
            return duckdb.connect(str(db_path), read_only=read_only)
        except (duckdb.IOException, duckdb.ConnectionException) as exc:
            if not _is_lock_error(exc) or time.monotonic() >= deadline:
                if _is_lock_error(exc):
                    raise DatabaseBusyError(f"Database is locked: {exc}") from exc
                raise
            time.sleep(delay)
            delay = min(delay * 2, 1.0)


@contextmanager
def read_connection(db_path: Path | str, timeout: float = 5.0) -> Iterator[duckdb.DuckDBPyConnection]:
    con = connect(db_path, read_only=True, timeout=timeout)
    try:
        yield con
    finally:
        con.close()


def init_schema(con: duckdb.DuckDBPyConnection) -> None:
    con.execute((SQL_DIR / "ddl" / "warehouse.sql").read_text())
