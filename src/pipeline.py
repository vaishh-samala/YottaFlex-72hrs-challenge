"""DataFlow pipeline orchestrator.

Source files -> ingestion -> raw staging -> validation -> quarantine -> transformation
-> dimensional warehouse (single transaction) -> processed Parquet exports.

Usage:
    python -m src.pipeline                                   # configured sample input
    python -m src.pipeline --orders data/sample/orders_batch_2.csv --products data/sample/products_update.csv
    python -m src.pipeline --force                           # reprocess files already loaded (backfill)
"""
from __future__ import annotations

import argparse
import json
import sys
import time
import traceback
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

import pandas as pd

from src.config import Settings
from src.ingestion.reader import BatchError, IngestedSource, file_sha256, preserve_raw, read_source
from src.monitoring.lock import PipelineLockedError, pipeline_lock
from src.monitoring.logging_setup import configure_logging, get_logger
from src.monitoring.run_history import finish_run, mark_stale_runs, start_run
from src.transformation.transform import transform
from src.validation.rules import QUARANTINE_COLUMNS, ValidationResult, validate_orders, validate_products
from src.warehouse.database import connect, init_schema
from src.warehouse.loader import (LoadStats, existing_order_customers, existing_product_versions, file_sequence,
                                  insert_quarantine, known_product_ids, ledger_status, load_batch, record_file)


class RejectThresholdExceeded(BatchError):
    code = "REJECT_THRESHOLD_EXCEEDED"


@dataclass
class RunResult:
    run_id: str
    status: str
    duration_seconds: float = 0.0
    metrics: dict[str, Any] = field(default_factory=dict)
    warnings: list[str] = field(default_factory=list)
    error_message: str | None = None
    load_stats: LoadStats | None = None

    def summary(self) -> dict[str, Any]:
        return {"run_id": self.run_id, "status": self.status, "duration_seconds": round(self.duration_seconds, 3),
                "metrics": self.metrics, "warnings": self.warnings, "error_message": self.error_message}


def _utcnow() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def new_run_id() -> str:
    return f"run_{datetime.now(timezone.utc):%Y%m%dT%H%M%S}_{uuid.uuid4().hex[:6]}"


def _empty_result(source_type: str) -> ValidationResult:
    return ValidationResult(valid=pd.DataFrame(), rejected=pd.DataFrame(columns=QUARANTINE_COLUMNS))


def _write_parquet_outputs(settings: Settings, con, run_id: str, results: dict[str, ValidationResult]) -> None:
    processed = settings.processed_dir / run_id
    quarantine = settings.quarantine_dir / run_id
    for source_type, result in results.items():
        if len(result.valid):
            processed.mkdir(parents=True, exist_ok=True)
            result.valid.to_parquet(processed / f"{source_type}_valid.parquet", index=False)
        if len(result.rejected):
            quarantine.mkdir(parents=True, exist_ok=True)
            result.rejected.to_parquet(quarantine / f"{source_type}_rejected.parquet", index=False)
    # Current warehouse snapshot for downstream analytical consumers.
    snapshot = settings.processed_dir / "warehouse"
    snapshot.mkdir(parents=True, exist_ok=True)
    for table in ("fact_sales", "dim_product", "dim_customer", "dim_date"):
        target = (snapshot / f"{table}.parquet").as_posix().replace("'", "''")
        con.execute(f"COPY {table} TO '{target}' (FORMAT PARQUET)")


def run_pipeline(settings: Settings | None = None, orders_path: Path | str | None = None,
                 products_path: Path | str | None = None, force: bool = False, trigger: str = "cli",
                 before_commit: Callable | None = None, console_log: bool = True) -> RunResult:
    """Execute one pipeline run. Raises PipelineLockedError if another run is active."""
    settings = settings or Settings()
    settings.ensure_dirs()
    configure_logging(settings.log_file, console=console_log)
    orders_path = Path(orders_path or settings.orders_path)
    products_path = Path(products_path or settings.products_path)
    run_id = new_run_id()
    log = get_logger(run_id, "pipeline")

    with pipeline_lock(settings.lock_path, run_id):
        return _run_locked(settings, run_id, orders_path, products_path, force, trigger, before_commit, log)


def _run_locked(settings: Settings, run_id: str, orders_path: Path, products_path: Path, force: bool,
                trigger: str, before_commit, log) -> RunResult:
    t0 = time.perf_counter()
    start_time = _utcnow()
    metrics: dict[str, Any] = {}
    warnings: list[str] = []
    files: list[dict] = [
        {"source_type": "products", "file_name": products_path.name, "path": str(products_path), "file_hash": None, "action": "PENDING"},
        {"source_type": "orders", "file_name": orders_path.name, "path": str(orders_path), "file_hash": None, "action": "PENDING"},
    ]
    sources: dict[str, IngestedSource] = {}
    results: dict[str, ValidationResult] = {}
    con = connect(settings.db_path, timeout=settings.db_connect_timeout)
    in_txn = False
    try:
        init_schema(con)
        recovered = mark_stale_runs(con)
        if recovered:
            log.warning("Marked %d interrupted run(s) as FAILED", recovered)
        start_run(con, run_id, start_time, trigger, force, files)
        log.info("Run started", extra={"orders_path": str(orders_path), "products_path": str(products_path), "force": force})

        # ---------------------------------------------------------------- ingestion
        stage = time.perf_counter()
        ilog = get_logger(run_id, "ingestion")
        for entry in files:
            try:
                source = read_source(entry["path"], entry["source_type"])
            except BatchError:
                if Path(entry["path"]).is_file():  # keep the failed file traceable in the ledger
                    entry["file_hash"] = file_sha256(Path(entry["path"]))
                    entry["action"] = "FAILED"
                raise
            sources[entry["source_type"]] = source
            entry["file_hash"] = source.file_hash
            preserve_raw(source, settings.raw_dir, run_id)
            warnings.extend(f"{source.source_type}: {w}" for w in source.warnings)
            metrics[f"{source.source_type}_input_rows"] = source.row_count
            metrics["input_rows"] = sum(s.row_count for s in sources.values())
            ilog.info("Ingested %s: %d rows (%d malformed)", source.file_name, source.row_count, len(source.malformed),
                      extra={"source_type": source.source_type, "file_hash": source.file_hash, "rows": source.row_count})
            prior = ledger_status(con, source.file_hash, source.source_type)
            if prior and prior[0] == "SUCCESS" and not force:
                entry["action"] = "SKIPPED_ALREADY_PROCESSED"
                warnings.append(f"{source.source_type}: file already processed successfully in {prior[1]}; skipped "
                                f"(use --force to reprocess)")
                ilog.info("%s already processed in %s - skipping", source.file_name, prior[1])
            else:
                entry["action"] = "PROCESSED"
        metrics["input_rows"] = sum(s.row_count for s in sources.values())
        metrics["ingestion_seconds"] = time.perf_counter() - stage

        process = {e["source_type"]: e["action"] == "PROCESSED" for e in files}
        if not any(process.values()):
            for e in files:
                src = sources[e["source_type"]]
                record_file(con, run_id=run_id, source_type=e["source_type"], file_name=src.file_name,
                            file_hash=src.file_hash, action=e["action"], input_rows=src.row_count, valid_rows=0,
                            rejected_rows=0, at=_utcnow(), ledger_status_value=None)
            return _finish(con, run_id, "SKIPPED", t0, metrics, warnings, None, files, log)

        # --------------------------------------------------------------- validation
        stage = time.perf_counter()
        vlog = get_logger(run_id, "validation")
        run_time = datetime.now(timezone.utc)
        results["products"] = (validate_products(sources["products"], existing_product_versions(con), run_time)
                               if process["products"] else _empty_result("products"))
        if process["orders"]:
            known = known_product_ids(con) | set(results["products"].valid.get("product_id", pd.Series(dtype=object)))
            existing = existing_order_customers(con, sources["orders"].data["order_id"])
            results["orders"] = validate_orders(sources["orders"], known, existing, run_time)
        else:
            results["orders"] = _empty_result("orders")
        for source_type, result in results.items():
            metrics[f"{source_type}_valid_rows"] = result.valid_count
            metrics[f"{source_type}_rejected_rows"] = result.rejected_count
            if process[source_type]:
                vlog.info("%s: %d valid, %d rejected", source_type, result.valid_count, result.rejected_count,
                          extra={"source_type": source_type, "valid": result.valid_count, "rejected": result.rejected_count})
        metrics["valid_rows"] = sum(r.valid_count for r in results.values())
        metrics["rejected_rows"] = sum(r.rejected_count for r in results.values())
        metrics["validation_seconds"] = time.perf_counter() - stage

        for source_type, result in results.items():  # batch-level guard
            total = result.valid_count + result.rejected_count
            if total and result.rejected_count / total > settings.max_reject_ratio:
                raise RejectThresholdExceeded(
                    f"{source_type}: {result.rejected_count}/{total} rows rejected "
                    f"({result.rejected_count / total:.1%}) exceeds max_reject_ratio={settings.max_reject_ratio:.0%}; "
                    f"batch not loaded")

        # ----------------------------------------------------------- transformation
        stage = time.perf_counter()
        orders_seq = file_sequence(con, sources["orders"].file_hash, "orders")
        staged = transform(results["products"].valid, results["orders"].valid, run_id, orders_seq)
        metrics["transformation_seconds"] = time.perf_counter() - stage
        get_logger(run_id, "transformation").info(
            "Staged %d product versions, %d facts, %d customers, %d dates", len(staged.products),
            len(staged.orders), len(staged.customers), len(staged.dates))

        # ---------------------------------------------------------------- loading
        stage = time.perf_counter()
        llog = get_logger(run_id, "load")
        now = _utcnow()
        con.execute("BEGIN TRANSACTION")
        in_txn = True
        stats = load_batch(con, staged, now, before_commit=before_commit)
        rejected = pd.concat([r.rejected for r in results.values() if len(r.rejected)], ignore_index=True) \
            if any(len(r.rejected) for r in results.values()) else pd.DataFrame(columns=QUARANTINE_COLUMNS)
        stats.quarantined = insert_quarantine(con, rejected, run_id, {k: s.file_hash for k, s in sources.items()}, now)
        for e in files:
            src, res = sources[e["source_type"]], results[e["source_type"]]
            processed = e["action"] == "PROCESSED"
            record_file(con, run_id=run_id, source_type=e["source_type"], file_name=src.file_name,
                        file_hash=src.file_hash, action=e["action"], input_rows=src.row_count,
                        valid_rows=res.valid_count, rejected_rows=res.rejected_count, at=now,
                        ledger_status_value="SUCCESS" if processed else None)
        con.execute("COMMIT")
        in_txn = False
        metrics["load_seconds"] = time.perf_counter() - stage
        metrics.update({
            "facts_inserted": stats.facts_inserted, "facts_updated": stats.facts_updated,
            "facts_unchanged": stats.facts_unchanged, "facts_stale_skipped": stats.facts_stale_skipped,
            "product_versions_inserted": stats.product_versions_inserted,
            "customers_inserted": stats.customers_inserted, "output_rows": stats.output_rows,
        })
        llog.info("Committed: %d facts inserted, %d updated, %d unchanged, %d stale-skipped, %d product versions, "
                  "%d quarantined", stats.facts_inserted, stats.facts_updated, stats.facts_unchanged,
                  stats.facts_stale_skipped,
                  stats.product_versions_inserted, stats.quarantined, extra={"integrity": stats.integrity})

        if settings.write_parquet:
            _write_parquet_outputs(settings, con, run_id, results)
        result = _finish(con, run_id, "SUCCESS", t0, metrics, warnings, None, files, log)
        result.load_stats = stats
        return result

    except Exception as exc:  # noqa: BLE001 - every failure must be recorded, then reported
        if in_txn:
            con.execute("ROLLBACK")
        level = "batch" if isinstance(exc, BatchError) else "system"
        code = getattr(exc, "code", type(exc).__name__)
        message = f"[{level}:{code}] {exc}"
        log.error("Run failed: %s", message, extra={"traceback": traceback.format_exc()})
        _record_failure(con, run_id, files, sources, results, exc, log)
        return _finish(con, run_id, "FAILED", t0, metrics, warnings, message, files, log)
    finally:
        con.close()


def _record_failure(con, run_id: str, files: list[dict], sources: dict, results: dict, exc: Exception, log) -> None:
    """Persist diagnostics for a failed run (outside the rolled-back load transaction)."""
    try:
        now = _utcnow()
        con.execute("BEGIN TRANSACTION")
        if isinstance(exc, RejectThresholdExceeded):
            rejected = pd.concat([r.rejected for r in results.values() if len(r.rejected)], ignore_index=True)
            insert_quarantine(con, rejected, run_id, {k: s.file_hash for k, s in sources.items()}, now)
        for e in files:
            src = sources.get(e["source_type"])
            if src is None and not e.get("file_hash"):
                continue  # file missing entirely: nothing to fingerprint
            file_hash = src.file_hash if src else e["file_hash"]
            res = results.get(e["source_type"])
            prior = ledger_status(con, file_hash, e["source_type"])
            keep_success = prior is not None and prior[0] == "SUCCESS"
            if e["action"] == "PROCESSED":
                e["action"] = "FAILED"
            record_file(con, run_id=run_id, source_type=e["source_type"], file_name=e["file_name"],
                        file_hash=file_hash, action=e["action"], input_rows=src.row_count if src else 0,
                        valid_rows=res.valid_count if res else 0, rejected_rows=res.rejected_count if res else 0,
                        at=now, ledger_status_value=None if keep_success or e["action"] != "FAILED" else "FAILED",
                        error=str(exc)[:1000])
        con.execute("COMMIT")
    except Exception:  # pragma: no cover - diagnostics must never mask the original error
        con.execute("ROLLBACK")
        log.exception("Could not persist failure diagnostics")


def _finish(con, run_id: str, status: str, t0: float, metrics: dict, warnings: list[str], error: str | None,
            files: list[dict], log) -> RunResult:
    duration = time.perf_counter() - t0
    finish_run(con, run_id, status=status, end_time=_utcnow(), duration_seconds=duration, metrics=metrics,
               warnings=warnings, error_message=error, source_files=files)
    log.info("Run finished: %s in %.2fs", status, duration, extra={"status": status, "metrics": metrics})
    return RunResult(run_id=run_id, status=status, duration_seconds=duration, metrics=metrics, warnings=warnings,
                     error_message=error)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run the DataFlow pipeline")
    parser.add_argument("--orders", type=Path, help="Orders CSV (default: configured sample)")
    parser.add_argument("--products", type=Path, help="Products CSV (default: configured sample)")
    parser.add_argument("--db", type=Path, help="DuckDB file (default: data/warehouse.duckdb)")
    parser.add_argument("--force", action="store_true", help="Reprocess files already loaded successfully")
    parser.add_argument("--max-reject-ratio", type=float, help="Batch-level rejection threshold (0-1)")
    parser.add_argument("--trigger", default="cli", help=argparse.SUPPRESS)
    args = parser.parse_args(argv)

    settings = Settings().with_overrides(db_path=args.db, max_reject_ratio=args.max_reject_ratio)
    try:
        result = run_pipeline(settings, args.orders, args.products, force=args.force, trigger=args.trigger)
    except PipelineLockedError as exc:
        print(json.dumps({"status": "LOCKED", "error_message": str(exc)}), file=sys.stdout)
        return 2
    print(json.dumps(result.summary(), indent=2, default=str))
    return 0 if result.status in ("SUCCESS", "SKIPPED") else 1


if __name__ == "__main__":
    raise SystemExit(main())
