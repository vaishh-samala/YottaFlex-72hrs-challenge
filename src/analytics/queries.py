"""Reusable access to the SQL files in sql/analytics, sql/reconciliation and sql/monitoring.

Every metric shown anywhere (CLI reports, dashboard) is computed by these queries against the
real DuckDB tables - nothing is hard-coded.
"""
from __future__ import annotations

from pathlib import Path

import duckdb
import pandas as pd

from src.config import SQL_DIR
from src.warehouse.database import init_schema, read_connection

GROUPS = ("analytics", "reconciliation", "monitoring")


def list_queries(group: str) -> list[str]:
    return sorted(p.stem for p in (SQL_DIR / group).glob("*.sql"))


def query_sql(group: str, name: str) -> str:
    path = SQL_DIR / group / f"{name}.sql"
    if not path.exists():
        raise KeyError(f"Unknown query {group}/{name}")
    return path.read_text()


def run_query(con: duckdb.DuckDBPyConnection, group: str, name: str) -> pd.DataFrame:
    return con.execute(query_sql(group, name)).df()


def run_group(con: duckdb.DuckDBPyConnection, group: str) -> dict[str, pd.DataFrame]:
    return {name: run_query(con, group, name) for name in list_queries(group)}


def fetch(db_path: Path | str, group: str, name: str) -> pd.DataFrame:
    """Open a short-lived read-only connection, run one query, close (dashboard-friendly)."""
    with read_connection(db_path) as con:
        return run_query(con, group, name)


def fetch_many(db_path: Path | str, items: list[tuple[str, str]]) -> dict[str, pd.DataFrame]:
    with read_connection(db_path) as con:
        return {name: run_query(con, group, name) for group, name in items}


def reconciliation_passes(con: duckdb.DuckDBPyConnection) -> dict[str, bool]:
    """True/False per reconciliation check (used by tests and the analytics report)."""
    results = run_group(con, "reconciliation")
    checks = {}
    runs = results["01_run_reconciliation"]
    checks["runs_input_balanced"] = bool(runs["input_balanced"].all()) if len(runs) else True
    checks["runs_quarantine_balanced"] = bool(runs["quarantine_balanced"].all()) if len(runs) else True
    checks["runs_warehouse_balanced"] = bool(runs["warehouse_balanced"].all()) if len(runs) else True
    files = results["02_file_reconciliation"]
    checks["files_input_balanced"] = bool(files["input_balanced"].all()) if len(files) else True
    checks["files_quarantine_balanced"] = bool(files["quarantine_balanced"].all()) if len(files) else True
    breakdown = results["04_reject_breakdown"]
    for col in ("input_balanced", "rejects_fully_categorised", "valid_rows_in_warehouse"):
        checks[f"breakdown_{col}"] = bool(breakdown[col].all()) if len(breakdown) else True
    wh = results["03_warehouse_reconciliation"]
    checks["warehouse_all_checks_pass"] = bool(wh["all_checks_pass"].iloc[0]) if len(wh) else True
    return checks


def ensure_schema(db_path: Path | str) -> None:
    """Create an empty warehouse if none exists (so queries return real zeros, not errors)."""
    con = duckdb.connect(str(db_path))
    try:
        init_schema(con)
    finally:
        con.close()
