"""Record-level validation (requirements 3, 5, 6, 7, 9, 10 and further rules)."""
from __future__ import annotations

import json

import pytest

from tests.conftest import BASE_ORDERS, BASE_PRODUCTS

GOOD = ["OLX-1", "OX", "CX", "P1", "2026-03-10T09:00:00Z", "1", "10.00", "paid"]


def with_row(**changes) -> list[str]:
    cols = ["order_line_id", "order_id", "customer_id", "product_id", "order_timestamp", "quantity", "unit_price",
            "order_status"]
    row = list(GOOD)
    for key, value in changes.items():
        row[cols.index(key)] = value
    return row


def run_single(env, row, products_rows=None):
    orders = env.orders(BASE_ORDERS + [row])
    products = env.products(products_rows or BASE_PRODUCTS)
    result = env.run(orders, products)
    assert result.status == "SUCCESS", result.error_message
    return result


# 3. Missing required fields ---------------------------------------------------------------
@pytest.mark.parametrize("column", ["order_id", "customer_id", "product_id", "order_timestamp", "quantity",
                                    "unit_price", "order_status"])
def test_missing_required_field(env, column):
    result = run_single(env, with_row(**{column: ""}))
    assert result.metrics["orders_rejected_rows"] == 1
    assert f"MISSING_REQUIRED_FIELD: {column}" in env.reasons("OLX-1")
    assert env.scalar("SELECT count(*) FROM fact_sales WHERE order_line_id = 'OLX-1'") == 0


def test_missing_primary_key_is_traceable_by_line_number(env):
    run_single(env, with_row(order_line_id=""))
    row = env.query("SELECT record_id, source_row_number, rejection_reasons FROM quarantine")[0]
    assert row[0] is None and row[1] == 6 and "order_line_id" in row[2]


# 6. Null values ---------------------------------------------------------------------------
@pytest.mark.parametrize("token", ["NULL", "null", "None", "NaN", "N/A", "   "])
def test_null_tokens_treated_as_missing(env, token):
    run_single(env, with_row(customer_id=token))
    assert "MISSING_REQUIRED_FIELD: customer_id" in env.reasons("OLX-1")


def test_optional_brand_may_be_null(env):
    products = BASE_PRODUCTS + [["P4", "Plain Item", "Grocery", "", "3.00", "active", "2026-01-01T00:00:00Z"]]
    result = run_single(env, with_row(product_id="P4"), products)
    assert result.metrics["rejected_rows"] == 0
    assert env.scalar("SELECT brand IS NULL FROM dim_product WHERE product_id = 'P4'")


# 5. Duplicate order-line IDs --------------------------------------------------------------
def test_exact_duplicate_keeps_first_quarantines_rest(env, base_files):
    _, products = base_files
    orders = env.orders(BASE_ORDERS + [GOOD, GOOD, GOOD])
    result = env.run(orders, products)
    assert result.metrics["orders_rejected_rows"] == 2
    assert env.scalar("SELECT count(*) FROM fact_sales WHERE order_line_id = 'OLX-1'") == 1
    lines = [r[0] for r in env.query("SELECT source_row_number FROM quarantine ORDER BY 1")]
    assert lines == [7, 8]                      # first copy (line 6) kept
    assert "DUPLICATE_RECORD" in env.reasons("OLX-1")


def test_conflicting_duplicate_rejects_all_copies(env, base_files):
    _, products = base_files
    orders = env.orders(BASE_ORDERS + [GOOD, with_row(quantity="5")])
    result = env.run(orders, products)
    assert result.metrics["orders_rejected_rows"] == 2
    assert env.scalar("SELECT count(*) FROM fact_sales WHERE order_line_id = 'OLX-1'") == 0
    assert env.scalar("SELECT count(*) FROM quarantine WHERE primary_reason = 'CONFLICTING_DUPLICATE_KEY'") == 2


def test_duplicate_product_id_latest_version_wins(env):
    products = BASE_PRODUCTS + [["P1", "Old Kettle", "Home & Kitchen", "Acme", "30.00", "active", "2025-06-01T00:00:00Z"]]
    run_single(env, GOOD, products)
    assert env.scalar("SELECT product_name FROM dim_product WHERE product_id = 'P1'") == "Red Kettle"
    assert "SUPERSEDED_DUPLICATE" in env.reasons("P1")


# 7. Invalid product references ------------------------------------------------------------
def test_invalid_product_reference(env):
    run_single(env, with_row(product_id="P404"))
    assert "INVALID_PRODUCT_REFERENCE" in env.reasons("OLX-1")


def test_reference_to_product_rejected_in_same_batch_is_invalid(env):
    products = BASE_PRODUCTS + [["P5", "Bad Price Thing", "Toys", "Fun", "-1.00", "active", "2026-01-01T00:00:00Z"]]
    run_single(env, with_row(product_id="P5"), products)
    assert "INVALID_PRICE" in env.reasons("P5")
    assert "INVALID_PRODUCT_REFERENCE" in env.reasons("OLX-1")


def test_reference_resolved_from_earlier_batch(env, base_files):
    orders, products = base_files
    env.run(orders, products)
    later = env.orders([with_row(order_line_id="OL7-1", order_id="O7", product_id="P2")], name="orders_2.csv")
    empty_products = env.products([], name="products_empty.csv")
    result = env.run(later, empty_products)
    assert result.status == "SUCCESS" and result.metrics["orders_rejected_rows"] == 0
    assert env.scalar("SELECT count(*) FROM fact_sales") == 5


# 9 / 10. Negative and zero quantities ----------------------------------------------------
def test_negative_quantity(env):
    run_single(env, with_row(quantity="-2"))
    assert "NEGATIVE_QUANTITY" in env.reasons("OLX-1")


def test_zero_quantity(env):
    run_single(env, with_row(quantity="0"))
    assert "ZERO_QUANTITY" in env.reasons("OLX-1")


# Other rules ----------------------------------------------------------------------------
@pytest.mark.parametrize("value,code", [
    ("two", "INVALID_DATA_TYPE"), ("3.5", "INVALID_DATA_TYPE"), ("1001", "QUANTITY_OUT_OF_RANGE")])
def test_quantity_rules(env, value, code):
    run_single(env, with_row(quantity=value))
    assert code in env.reasons("OLX-1")


@pytest.mark.parametrize("value,code", [
    ("abc", "INVALID_DATA_TYPE"), ("12,50", "INVALID_DATA_TYPE"), ("0", "INVALID_PRICE"), ("-3.00", "INVALID_PRICE"),
    ("10.999", "INVALID_PRICE_PRECISION"), ("1000000.01", "INVALID_PRICE")])
def test_price_rules(env, value, code):
    run_single(env, with_row(unit_price=value))
    assert code in env.reasons("OLX-1")


@pytest.mark.parametrize("value,code", [
    ("2026-02-30T10:00:00Z", "INVALID_TIMESTAMP"), ("not-a-date", "INVALID_TIMESTAMP"),
    ("31/12/2026 10:00", "INVALID_TIMESTAMP"), ("2099-01-01T00:00:00Z", "TIMESTAMP_OUT_OF_RANGE"),
    ("1999-12-31T23:59:59Z", "TIMESTAMP_OUT_OF_RANGE")])
def test_timestamp_rules(env, value, code):
    run_single(env, with_row(order_timestamp=value))
    assert code in env.reasons("OLX-1")


def test_invalid_status_and_normalisation(env, base_files):
    _, products = base_files
    orders = env.orders(BASE_ORDERS + [with_row(order_status="teleported"),
                                       with_row(order_line_id="OLX-2", order_status="  SHIPPED ")])
    result = env.run(orders, products)
    assert result.metrics["orders_rejected_rows"] == 1
    assert "INVALID_STATUS" in env.reasons("OLX-1")
    assert env.scalar("SELECT order_status FROM fact_sales WHERE order_line_id = 'OLX-2'") == "shipped"


def test_multiple_reasons_recorded_for_one_record(env):
    run_single(env, with_row(quantity="-1", unit_price="abc", order_status="??"))
    reasons = env.reasons("OLX-1")
    for code in ("NEGATIVE_QUANTITY", "INVALID_DATA_TYPE", "INVALID_STATUS"):
        assert code in reasons


def test_quarantine_record_has_full_lineage(env):
    result = run_single(env, with_row(quantity="0"))
    row = env.query("SELECT run_id, source_file, source_row_number, raw_record, rejected_at, file_hash "
                    "FROM quarantine")[0]
    assert row[0] == result.run_id and row[1] == "orders.csv" and row[2] == 6
    assert json.loads(row[3])["quantity"] == "0"
    assert row[4] is not None and len(row[5]) == 64
    parquet = env.settings.quarantine_dir / result.run_id / "orders_rejected.parquet"
    assert parquet.exists()


def test_boundary_values_accepted(env, base_files):
    _, products = base_files
    orders = env.orders(BASE_ORDERS + [with_row(quantity="1000"), with_row(order_line_id="OLX-2", unit_price="0.01")])
    result = env.run(orders, products)
    assert result.metrics["orders_rejected_rows"] == 0
    assert env.scalar("SELECT total_amount FROM fact_sales WHERE order_line_id = 'OLX-1'") == 10000
