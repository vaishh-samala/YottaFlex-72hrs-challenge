"""Persistent pipeline run history (pipeline_runs table)."""
from __future__ import annotations

import json
from datetime import datetime
from typing import Any

import duckdb

RUN_METRIC_COLUMNS = (
    "input_rows", "valid_rows", "rejected_rows", "output_rows",
    "orders_input_rows", "orders_valid_rows", "orders_rejected_rows",
    "products_input_rows", "products_valid_rows", "products_rejected_rows",
    "facts_inserted", "facts_updated", "facts_unchanged", "facts_stale_skipped", "product_versions_inserted", "customers_inserted",
    "ingestion_seconds", "validation_seconds", "transformation_seconds", "load_seconds",
)


def start_run(con: duckdb.DuckDBPyConnection, run_id: str, start_time: datetime, trigger: str, forced: bool,
              source_files: list[dict]) -> None:
    con.execute(
        "INSERT INTO pipeline_runs (run_id, start_time, execution_status, trigger_source, forced, source_files) "
        "VALUES (?, ?, 'RUNNING', ?, ?, ?)",
        [run_id, start_time, trigger, forced, json.dumps(source_files)],
    )


def finish_run(con: duckdb.DuckDBPyConnection, run_id: str, *, status: str, end_time: datetime,
               duration_seconds: float, metrics: dict[str, Any], warnings: list[str],
               error_message: str | None, source_files: list[dict]) -> None:
    cols = [c for c in RUN_METRIC_COLUMNS if c in metrics]
    assignments = ", ".join(f"{c} = ?" for c in cols)
    params = [metrics[c] for c in cols]
    con.execute(
        f"UPDATE pipeline_runs SET execution_status = ?, end_time = ?, duration_seconds = ?, warnings = ?, "
        f"error_message = ?, source_files = ?{', ' + assignments if assignments else ''} WHERE run_id = ?",
        [status, end_time, duration_seconds, json.dumps(warnings), error_message, json.dumps(source_files), *params, run_id],
    )


def mark_stale_runs(con: duckdb.DuckDBPyConnection) -> int:
    """Runs left RUNNING by a killed process (no lock holder) are marked FAILED."""
    n = con.execute(
        "UPDATE pipeline_runs SET execution_status = 'FAILED', "
        "error_message = COALESCE(error_message, 'Process terminated before completion (recovered on next start)') "
        "WHERE execution_status = 'RUNNING'"
    ).fetchall()[0][0]
    return int(n)
