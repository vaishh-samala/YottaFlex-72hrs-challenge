"""Ingestion: normal files, empty files, malformed rows, schema changes (requirements 1, 2, 4, 8)."""
from __future__ import annotations

from src.ingestion.reader import SchemaValidationError, read_source, to_snake_case
from tests.conftest import BASE_ORDERS, BASE_PRODUCTS, ORDER_HEADER, write_csv

import pytest


# 1. Normal ingestion -----------------------------------------------------------------------
def test_normal_ingestion_loads_everything(env, base_files):
    orders, products = base_files
    result = env.run(orders, products)
    assert result.status == "SUCCESS", result.error_message
    assert result.metrics["orders_input_rows"] == 4 and result.metrics["products_input_rows"] == 3
    assert result.metrics["rejected_rows"] == 0
    assert env.scalar("SELECT count(*) FROM fact_sales") == 4
    assert env.scalar("SELECT count(*) FROM dim_product") == 3
    assert env.scalar("SELECT count(*) FROM dim_customer") == 2
    # Raw input preserved byte-for-byte under the run id.
    raw = env.settings.raw_dir / result.run_id / "orders__orders.csv"
    assert raw.read_bytes() == orders.read_bytes()
    # Lineage: source file names and hashes recorded.
    files = env.query("SELECT source_type, file_name, action FROM run_files WHERE run_id = ? ORDER BY 1", [result.run_id])
    assert files == [("orders", "orders.csv", "PROCESSED"), ("products", "products.csv", "PROCESSED")]
    assert env.scalar("SELECT count(DISTINCT source_file) FROM fact_sales") == 1


def test_run_ids_are_unique_and_history_persisted(env, base_files):
    orders, products = base_files
    first = env.run(orders, products)
    second = env.run(orders, products)
    assert first.run_id != second.run_id
    rows = env.query("SELECT run_id, execution_status, start_time IS NOT NULL, end_time IS NOT NULL, "
                     "duration_seconds >= 0 FROM pipeline_runs ORDER BY start_time")
    assert [r[1] for r in rows] == ["SUCCESS", "SKIPPED"]
    assert all(r[2] and r[3] and r[4] for r in rows)


# 2. Empty input --------------------------------------------------------------------------
def test_header_only_orders_file(env, base_files):
    _, products = base_files
    empty = env.orders([], name="empty_orders.csv")
    result = env.run(empty, products)
    assert result.status == "SUCCESS"
    assert result.metrics["orders_input_rows"] == 0
    assert any("EMPTY_FILE" in w for w in result.warnings)
    assert env.scalar("SELECT count(*) FROM fact_sales") == 0
    assert env.scalar("SELECT count(*) FROM dim_product") == 3


def test_zero_byte_file_is_empty_not_schema_error(env, base_files):
    _, products = base_files
    zero = env.inputs / "zero.csv"
    zero.write_bytes(b"")
    result = env.run(zero, products)
    assert result.status == "SUCCESS"
    assert any("EMPTY_FILE" in w for w in result.warnings)


def test_missing_file_is_batch_failure(env, base_files):
    _, products = base_files
    result = env.run(env.inputs / "does_not_exist.csv", products)
    assert result.status == "FAILED"
    assert "SOURCE_UNAVAILABLE" in result.error_message
    assert env.scalar("SELECT count(*) FROM fact_sales") == 0
    assert env.scalar("SELECT execution_status FROM pipeline_runs") == "FAILED"


# 4. Malformed records --------------------------------------------------------------------
def test_malformed_rows_are_quarantined_not_fatal(env, base_files):
    _, products = base_files
    rows = BASE_ORDERS + [
        ["OL9-1", "O9", "C9", "P1", "2026-03-01T10:00:00Z", "1"],                                   # too few fields
        ["OL9-2", "O9", "C9", "P1", "2026-03-01T10:00:00Z", "1", "5.00", "paid", "x.csv", "extra"],  # too many
    ]
    orders = env.orders(rows)
    source = read_source(orders, "orders")
    assert len(source.data) == 4 and len(source.malformed) == 2
    result = env.run(orders, products)
    assert result.status == "SUCCESS"
    assert env.scalar("SELECT count(*) FROM fact_sales") == 4
    q = env.query("SELECT record_id, primary_reason, source_row_number, raw_record FROM quarantine ORDER BY 3")
    assert [r[1] for r in q] == ["MALFORMED_RECORD", "MALFORMED_RECORD"]
    assert [r[2] for r in q] == [6, 7]                      # physical line numbers in the file
    assert "OL9-2" in q[1][3]                               # original line preserved


# 8. Unexpected schema changes ------------------------------------------------------------
def test_missing_required_column_fails_batch_and_can_be_retried(env, base_files):
    _, products = base_files
    header = [c for c in ORDER_HEADER if c != "unit_price"]
    rows = [r[:6] + r[7:] for r in BASE_ORDERS]
    broken = write_csv(env.inputs / "orders_v2.csv", header, rows)
    result = env.run(broken, products)
    assert result.status == "FAILED"
    assert "batch:SCHEMA_MISMATCH" in result.error_message and "unit_price" in result.error_message
    assert env.scalar("SELECT count(*) FROM fact_sales") == 0
    assert env.scalar("SELECT count(*) FROM dim_product") == 0          # whole batch rejected
    assert env.scalar("SELECT status FROM processed_files WHERE source_type = 'orders'") == "FAILED"
    fixed = env.orders(BASE_ORDERS, name="orders_v2_fixed.csv")
    retry = env.run(fixed, products)
    assert retry.status == "SUCCESS"
    assert env.scalar("SELECT count(*) FROM fact_sales") == 4


def test_extra_column_is_schema_drift_warning(env, base_files):
    _, products = base_files
    header = ORDER_HEADER + ["coupon_code"]
    rows = [r + ["orders_extra.csv", "SAVE10"] for r in BASE_ORDERS]
    extra = write_csv(env.inputs / "orders_extra.csv", header, rows, add_source=False)
    result = env.run(extra, products)
    assert result.status == "SUCCESS"
    assert any("SCHEMA_DRIFT" in w and "coupon_code" in w for w in result.warnings)
    assert env.scalar("SELECT count(*) FROM fact_sales") == 4


def test_headers_normalised_to_snake_case(env, base_files):
    _, products = base_files
    header = ["Order Line ID", "orderId", "Customer-ID", "ProductID", "Order Timestamp", "QUANTITY", "UnitPrice",
              "order_status", "Source File"]
    orders = write_csv(env.inputs / "orders_camel.csv", header, BASE_ORDERS)
    result = env.run(orders, products)
    assert result.status == "SUCCESS"
    assert env.scalar("SELECT count(*) FROM fact_sales") == 4
    assert any("COLUMNS_NORMALISED" in w for w in result.warnings)


@pytest.mark.parametrize("raw,expected", [
    ("Order Line ID", "order_line_id"), ("orderLineId", "order_line_id"), ("ORDER-LINE-ID", "order_line_id"),
    ("ProductID", "product_id"), ("  unit price ", "unit_price"), ("﻿product_id", "product_id"),
])
def test_to_snake_case(raw, expected):
    assert to_snake_case(raw) == expected


def test_duplicate_columns_after_normalisation_rejected(env):
    path = write_csv(env.inputs / "dup_cols.csv", ["product_id", "Product ID"] + list(BASE_PRODUCTS[0][1:]) , [])
    with pytest.raises(SchemaValidationError):
        read_source(path, "products")
