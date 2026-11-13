"""Idempotent, transactional warehouse loading.

All writes for a batch happen inside ONE DuckDB transaction:
dimensions -> facts (upsert by order_line_id) -> quarantine -> file ledger.
Referential integrity is verified before COMMIT; any failure rolls everything back, so a
failed batch never leaves partial data and can simply be retried.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime

import duckdb
import pandas as pd

from src.transformation.transform import StagedBatch, surrogate_key


class IntegrityError(RuntimeError):
    """Raised when referential-integrity checks fail; the batch is rolled back."""


@dataclass
class LoadStats:
    dates_inserted: int = 0
    customers_inserted: int = 0
    product_versions_inserted: int = 0
    facts_inserted: int = 0
    facts_updated: int = 0
    facts_unchanged: int = 0
    facts_stale_skipped: int = 0
    facts_rekeyed: int = 0
    quarantined: int = 0
    integrity: dict[str, int] = field(default_factory=dict)

    @property
    def output_rows(self) -> int:
        return self.facts_inserted + self.facts_updated


# ------------------------------------------------------------------------- lookups (pre-load)
def known_product_ids(con: duckdb.DuckDBPyConnection) -> set[str]:
    return {r[0] for r in con.execute("SELECT DISTINCT product_id FROM dim_product").fetchall()}


def existing_product_versions(con: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    df = con.execute("SELECT product_id, valid_from, row_hash FROM dim_product").df()
    df["valid_from"] = pd.to_datetime(df["valid_from"]).astype("datetime64[us]")
    return df


def existing_order_customers(con: duckdb.DuckDBPyConnection, order_ids: pd.Series) -> pd.DataFrame:
    ids = pd.DataFrame({"order_id": order_ids.dropna().unique()})
    if ids.empty:
        return pd.DataFrame(columns=["order_id", "customer_id"])
    con.register("tmp_order_ids", ids)
    try:
        return con.execute(
            "SELECT DISTINCT f.order_id, f.customer_id FROM fact_sales f JOIN tmp_order_ids t USING (order_id)"
        ).df()
    finally:
        con.unregister("tmp_order_ids")


def file_sequence(con: duckdb.DuckDBPyConnection, file_hash: str, source_type: str) -> int:
    """Per-source arrival sequence of a file: existing number if seen before, otherwise the next one."""
    rows = con.execute("SELECT file_seq FROM processed_files WHERE file_hash = ? AND source_type = ?",
                       [file_hash, source_type]).fetchall()
    if rows:
        return int(rows[0][0])
    return int(con.execute("SELECT COALESCE(max(file_seq), 0) + 1 FROM processed_files WHERE source_type = ?",
                           [source_type]).fetchall()[0][0])


def ledger_status(con: duckdb.DuckDBPyConnection, file_hash: str, source_type: str) -> tuple[str, str] | None:
    row = con.execute("SELECT status, last_run_id FROM processed_files WHERE file_hash = ? AND source_type = ?",
                      [file_hash, source_type]).fetchall()
    return (row[0][0], row[0][1]) if row else None


# ------------------------------------------------------------------------------- load steps
def _load_dates(con, staged: StagedBatch) -> int:
    if staged.dates.empty:
        return 0
    con.register("stg_dates", staged.dates)
    n = con.execute("""
        INSERT INTO dim_date
        SELECT s.date_key, s.full_date, s.day, s.month, s.month_name, s.quarter, s.year, s.weekday, s.is_weekend
        FROM stg_dates s WHERE NOT EXISTS (SELECT 1 FROM dim_date d WHERE d.date_key = s.date_key)
    """).fetchall()[0][0]
    con.unregister("stg_dates")
    return int(n)


def _load_customers(con, staged: StagedBatch) -> int:
    if staged.customers.empty:
        return 0
    con.register("stg_customers", staged.customers)
    n = con.execute("""
        INSERT INTO dim_customer (customer_key, customer_id, first_order_date, created_run_id)
        SELECT s.customer_key, s.customer_id, s.first_order_date, s.run_id FROM stg_customers s
        WHERE NOT EXISTS (SELECT 1 FROM dim_customer c WHERE c.customer_id = s.customer_id)
    """).fetchall()[0][0]
    # Type 1: a late-arriving earlier order moves first_order_date back.
    con.execute("""
        UPDATE dim_customer c SET first_order_date = s.first_order_date
        FROM stg_customers s
        WHERE c.customer_id = s.customer_id AND s.first_order_date < c.first_order_date
    """)
    con.unregister("stg_customers")
    return int(n)


def _load_products(con, staged: StagedBatch) -> tuple[int, list[str]]:
    if staged.products.empty:
        return 0, []
    con.register("stg_products", staged.products)
    n = con.execute("""
        INSERT INTO dim_product (product_key, product_id, product_name, category, brand, list_price,
                                 product_status, valid_from, valid_to, is_current, row_hash, source_file, created_run_id)
        SELECT s.product_key, s.product_id, s.product_name, s.category, s.brand, CAST(s.list_price AS DECIMAL(12,2)),
               s.product_status, s.valid_from, NULL, TRUE, s.row_hash, s.source_file, s.run_id
        FROM stg_products s
        WHERE NOT EXISTS (SELECT 1 FROM dim_product d WHERE d.product_id = s.product_id AND d.valid_from = s.valid_from)
    """).fetchall()[0][0]
    affected = [r[0] for r in con.execute("""
        SELECT DISTINCT s.product_id FROM stg_products s
        JOIN dim_product d ON d.product_id = s.product_id AND d.created_run_id = s.run_id AND d.valid_from = s.valid_from
    """).fetchall()]
    con.unregister("stg_products")
    if affected:
        # Recompute Type 2 validity windows for changed products (handles out-of-order updates).
        con.register("affected_products", pd.DataFrame({"product_id": affected}))
        con.execute("""
            UPDATE dim_product d
            SET valid_to = w.next_from, is_current = (w.next_from IS NULL)
            FROM (
                SELECT product_key,
                       LEAD(valid_from) OVER (PARTITION BY product_id ORDER BY valid_from) AS next_from
                FROM dim_product WHERE product_id IN (SELECT product_id FROM affected_products)
            ) w
            WHERE d.product_key = w.product_key
              AND (d.valid_to IS DISTINCT FROM w.next_from OR d.is_current IS DISTINCT FROM (w.next_from IS NULL))
        """)
        con.unregister("affected_products")
    return int(n), affected


_RESOLVE_PRODUCT_SQL = """
    -- Product version valid at the order timestamp; orders older than the first known
    -- version fall back to the earliest version (product existed but catalog row is newer).
    SELECT s.*, COALESCE(a.product_key, e.first_key) AS product_key
    FROM {source} s
    ASOF LEFT JOIN dim_product a ON s.product_id = a.product_id AND s.order_timestamp >= a.valid_from
    LEFT JOIN (SELECT product_id, arg_min(product_key, valid_from) AS first_key FROM dim_product GROUP BY product_id) e
           ON e.product_id = s.product_id
"""


def _load_facts(con, staged: StagedBatch, loaded_at: datetime) -> tuple[int, int, int, int]:
    """Upsert facts by order_line_id. Returns (inserted, updated, unchanged, stale_skipped).

    A changed row only overwrites the warehouse when it comes from a file that arrived at
    the same time or later (``source_seq``); replaying an older file therefore cannot undo
    a newer correction (e.g. pending -> shipped).
    """
    if staged.orders.empty:
        return 0, 0, 0, 0
    con.register("stg_orders", staged.orders)
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE stg_fact AS
        SELECT r.sales_key, r.order_line_id, r.order_id, r.product_key, r.customer_key, r.customer_id,
               CAST(r.date_key AS INTEGER) AS date_key, r.order_timestamp,
               CAST(r.quantity AS INTEGER) AS quantity,
               CAST(r.unit_price AS DECIMAL(12,2)) AS unit_price,
               CAST(CAST(r.quantity AS DECIMAL(12,0)) * CAST(r.unit_price AS DECIMAL(12,2)) AS DECIMAL(18,2)) AS total_amount,
               r.order_status, r.row_hash, r.source_file, CAST(r.source_seq AS BIGINT) AS source_seq, r.run_id
        FROM ({_RESOLVE_PRODUCT_SQL.format(source='stg_orders')}) r
    """)
    con.unregister("stg_orders")
    missing = con.execute("SELECT count(*) FROM stg_fact WHERE product_key IS NULL").fetchall()[0][0]
    if missing:
        raise IntegrityError(f"{missing} staged fact rows could not be resolved to a product version")

    counts = con.execute("""
        SELECT count(*) FILTER (WHERE f.sales_key IS NULL) AS inserts,
               count(*) FILTER (WHERE f.sales_key IS NOT NULL AND f.row_hash <> s.row_hash
                                AND s.source_seq >= f.source_seq) AS updates,
               count(*) FILTER (WHERE f.sales_key IS NOT NULL AND f.row_hash = s.row_hash) AS unchanged,
               count(*) FILTER (WHERE f.sales_key IS NOT NULL AND f.row_hash <> s.row_hash
                                AND s.source_seq < f.source_seq) AS stale
        FROM stg_fact s LEFT JOIN fact_sales f ON f.order_line_id = s.order_line_id
    """).fetchall()[0]
    con.execute("""
        UPDATE fact_sales f SET
            order_id = s.order_id, product_key = s.product_key, customer_key = s.customer_key,
            customer_id = s.customer_id, date_key = s.date_key, order_timestamp = s.order_timestamp,
            quantity = s.quantity, unit_price = s.unit_price, total_amount = s.total_amount,
            order_status = s.order_status, row_hash = s.row_hash, source_file = s.source_file,
            source_seq = s.source_seq, run_id = s.run_id, loaded_at = ?
        FROM stg_fact s
        WHERE f.order_line_id = s.order_line_id AND f.row_hash <> s.row_hash AND s.source_seq >= f.source_seq
    """, [loaded_at])
    con.execute("""
        INSERT INTO fact_sales (sales_key, order_line_id, order_id, product_key, customer_key, customer_id, date_key,
                                order_timestamp, quantity, unit_price, total_amount, order_status, row_hash,
                                source_file, source_seq, run_id, first_run_id, loaded_at)
        SELECT s.sales_key, s.order_line_id, s.order_id, s.product_key, s.customer_key, s.customer_id, s.date_key,
               s.order_timestamp, s.quantity, s.unit_price, s.total_amount, s.order_status, s.row_hash,
               s.source_file, s.source_seq, s.run_id, s.run_id, ?
        FROM stg_fact s
        WHERE NOT EXISTS (SELECT 1 FROM fact_sales f WHERE f.order_line_id = s.order_line_id)
    """, [loaded_at])
    con.execute("DROP TABLE stg_fact")
    return int(counts[0]), int(counts[1]), int(counts[2]), int(counts[3])


def _rekey_facts(con, affected_products: list[str]) -> int:
    """After product history changed, re-point existing facts to the version valid at order time."""
    if not affected_products:
        return 0
    con.register("affected_products", pd.DataFrame({"product_id": affected_products}))
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE rekey AS
        SELECT r.sales_key, r.product_key FROM ({_RESOLVE_PRODUCT_SQL.format(source='''(
            SELECT f.sales_key, p.product_id, f.order_timestamp, f.product_key AS old_key
            FROM fact_sales f JOIN dim_product p ON p.product_key = f.product_key
            WHERE p.product_id IN (SELECT product_id FROM affected_products))''')}) r
        WHERE r.product_key <> r.old_key
    """)
    con.unregister("affected_products")
    n = con.execute("SELECT count(*) FROM rekey").fetchall()[0][0]
    if n:
        con.execute("UPDATE fact_sales f SET product_key = r.product_key FROM rekey r WHERE f.sales_key = r.sales_key")
    con.execute("DROP TABLE rekey")
    return int(n)


def insert_quarantine(con, rejected: pd.DataFrame, run_id: str, file_hashes: dict[str, str],
                      rejected_at: datetime) -> int:
    if rejected.empty:
        return 0
    frame = rejected.copy()
    frame["run_id"] = run_id
    frame["file_hash"] = frame["source_type"].map(file_hashes)
    frame["rejected_at"] = rejected_at
    frame["quarantine_id"] = [surrogate_key("quarantine", run_id, t, r)
                              for t, r in zip(frame["source_type"], frame["source_row_number"])]
    frame["record_id"] = frame["record_id"].astype(object).where(frame["record_id"].notna(), None)
    con.register("stg_quarantine", frame)
    con.execute("""
        INSERT INTO quarantine (quarantine_id, run_id, source_type, source_file, file_hash, source_row_number,
                                record_id, primary_reason, rejection_reasons, raw_record, rejected_at, source_line)
        SELECT quarantine_id, run_id, source_type, source_file, file_hash, CAST(source_row_number AS BIGINT),
               CAST(record_id AS VARCHAR), primary_reason, rejection_reasons, raw_record, rejected_at,
               CAST(source_line AS VARCHAR)
        FROM stg_quarantine
    """)
    con.unregister("stg_quarantine")
    return len(frame)


def record_file(con, *, run_id: str, source_type: str, file_name: str, file_hash: str, action: str,
                input_rows: int, valid_rows: int, rejected_rows: int, at: datetime,
                ledger_status_value: str | None, error: str | None = None) -> None:
    con.execute("""
        INSERT INTO run_files VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT (run_id, source_type) DO UPDATE SET action = excluded.action, input_rows = excluded.input_rows,
            valid_rows = excluded.valid_rows, rejected_rows = excluded.rejected_rows
    """, [run_id, source_type, file_name, file_hash, action, input_rows, valid_rows, rejected_rows])
    if ledger_status_value is None:
        return
    con.execute("""
        INSERT INTO processed_files (file_hash, source_type, file_name, row_count, status, last_run_id,
                                     first_seen_at, last_processed_at, attempts, last_error, file_seq)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, 1, ?, ?)
        ON CONFLICT (file_hash, source_type) DO UPDATE SET
            file_name = excluded.file_name, row_count = excluded.row_count, status = excluded.status,
            last_run_id = excluded.last_run_id, last_processed_at = excluded.last_processed_at,
            attempts = processed_files.attempts + 1, last_error = excluded.last_error
    """, [file_hash, source_type, file_name, input_rows, ledger_status_value, run_id, at, at, error,
          file_sequence(con, file_hash, source_type)])


# ------------------------------------------------------------------------- integrity checks
INTEGRITY_SQL = {
    "fact_missing_product": "SELECT count(*) FROM fact_sales f LEFT JOIN dim_product p USING (product_key) WHERE p.product_key IS NULL",
    "fact_missing_customer": "SELECT count(*) FROM fact_sales f LEFT JOIN dim_customer c USING (customer_key) WHERE c.customer_key IS NULL",
    "fact_missing_date": "SELECT count(*) FROM fact_sales f LEFT JOIN dim_date d USING (date_key) WHERE d.date_key IS NULL",
    "fact_customer_mismatch": "SELECT count(*) FROM fact_sales f JOIN dim_customer c USING (customer_key) WHERE c.customer_id <> f.customer_id",
    "duplicate_order_lines": "SELECT count(*) - count(DISTINCT order_line_id) FROM fact_sales",
    "bad_total_amount": "SELECT count(*) FROM fact_sales WHERE total_amount <> quantity * unit_price",
    "product_multiple_current": "SELECT count(*) FROM (SELECT product_id FROM dim_product WHERE is_current GROUP BY 1 HAVING count(*) > 1)",
    "product_overlapping_versions": """SELECT count(*) FROM dim_product a JOIN dim_product b
        ON a.product_id = b.product_id AND a.product_key <> b.product_key
        AND a.valid_from < COALESCE(b.valid_to, TIMESTAMP '9999-12-31') AND b.valid_from < COALESCE(a.valid_to, TIMESTAMP '9999-12-31')""",
}


def check_integrity(con: duckdb.DuckDBPyConnection) -> dict[str, int]:
    return {name: int(con.execute(sql).fetchall()[0][0]) for name, sql in INTEGRITY_SQL.items()}


def load_batch(con: duckdb.DuckDBPyConnection, staged: StagedBatch, loaded_at: datetime,
               before_commit=None) -> LoadStats:
    """Load one staged batch. Caller owns the transaction (BEGIN/COMMIT/ROLLBACK).

    ``before_commit`` is an optional hook (used by tests to simulate a crash mid-load).
    """
    stats = LoadStats()
    stats.dates_inserted = _load_dates(con, staged)
    stats.customers_inserted = _load_customers(con, staged)
    stats.product_versions_inserted, affected = _load_products(con, staged)
    stats.facts_rekeyed = _rekey_facts(con, affected)
    (stats.facts_inserted, stats.facts_updated, stats.facts_unchanged,
     stats.facts_stale_skipped) = _load_facts(con, staged, loaded_at)
    if before_commit is not None:
        before_commit(con)
    stats.integrity = check_integrity(con)
    broken = {k: v for k, v in stats.integrity.items() if v}
    if broken:
        raise IntegrityError(f"Referential integrity check failed: {broken}")
    return stats
