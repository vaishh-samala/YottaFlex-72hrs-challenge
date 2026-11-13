"""Warehouse behaviour (requirements 11-16) and domain-specific edge cases."""
from __future__ import annotations

import threading
from decimal import Decimal

import pytest

from src.monitoring.lock import PipelineLockedError, pipeline_lock
from src.transformation.transform import surrogate_key
from src.warehouse.loader import check_integrity
from tests.conftest import BASE_ORDERS, BASE_PRODUCTS

FACT_SNAPSHOT = ("SELECT sales_key, order_line_id, product_key, customer_key, date_key, quantity, unit_price, "
                 "total_amount, order_status, run_id FROM fact_sales ORDER BY order_line_id")


# 11. Late-arriving events --------------------------------------------------------------------
def test_late_arriving_event_lands_in_correct_period(env, base_files):
    orders, products = base_files
    env.run(orders, products)
    late = env.orders([["OL0-1", "O0", "C3", "P1", "2025-11-15T20:00:00Z", "1", "39.00", "delivered"]],
                      name="orders_late.csv")
    result = env.run(late, env.products([], name="no_products.csv"))
    assert result.status == "SUCCESS" and result.metrics["facts_inserted"] == 1
    assert env.scalar("SELECT date_key FROM fact_sales WHERE order_line_id = 'OL0-1'") == 20251115
    assert env.scalar("SELECT month_name FROM dim_date WHERE date_key = 20251115") == "November"
    # Order predates the product's first catalog version -> earliest version is used (no orphan).
    assert env.scalar("SELECT p.product_id FROM fact_sales f JOIN dim_product p USING (product_key) "
                      "WHERE order_line_id = 'OL0-1'") == "P1"
    assert sum(check_integrity_values(env)) == 0


def test_late_order_rewinds_customer_first_order_date(env, base_files):
    orders, products = base_files
    env.run(orders, products)
    late = env.orders([["OL0-9", "O09", "C1", "P2", "2026-01-02T00:00:00Z", "1", "12.50", "paid"]], name="late2.csv")
    env.run(late, env.products([], name="none.csv"))
    assert str(env.scalar("SELECT first_order_date FROM dim_customer WHERE customer_id = 'C1'")) == "2026-01-02"


# 12. Product catalog updates (Type 2) --------------------------------------------------------
def test_catalog_update_creates_new_version_and_keeps_history(env, base_files):
    orders, products = base_files
    env.run(orders, products)
    update = env.products([["P1", "Red Kettle Pro", "Home & Kitchen", "Acme", "55.00", "discontinued",
                            "2026-04-01T00:00:00Z"]], name="products_update.csv")
    new_orders = env.orders([["OL5-1", "O5", "C2", "P1", "2026-04-10T12:00:00Z", "1", "52.00", "paid"]],
                            name="orders_april.csv")
    result = env.run(new_orders, update)
    assert result.metrics["product_versions_inserted"] == 1
    versions = env.query("SELECT list_price, product_status, valid_from, valid_to, is_current FROM dim_product "
                         "WHERE product_id = 'P1' ORDER BY valid_from")
    assert len(versions) == 2
    assert versions[0][0] == Decimal("40.00") and versions[0][4] is False and versions[0][3] == versions[1][2]
    assert versions[1][0] == Decimal("55.00") and versions[1][4] is True and versions[1][3] is None
    # Historical order keeps its transaction price and points at the old version.
    old = env.query("SELECT f.unit_price, f.total_amount, p.list_price FROM fact_sales f JOIN dim_product p "
                    "USING (product_key) WHERE order_line_id = 'OL1-1'")[0]
    assert old == (Decimal("35.00"), Decimal("70.00"), Decimal("40.00"))
    new = env.query("SELECT f.unit_price, p.list_price, p.product_name FROM fact_sales f JOIN dim_product p "
                    "USING (product_key) WHERE order_line_id = 'OL5-1'")[0]
    assert new == (Decimal("52.00"), Decimal("55.00"), "Red Kettle Pro")


def test_out_of_order_catalog_update_rekeys_facts(env, base_files):
    orders, products = base_files
    env.run(orders, products)
    # A version effective 2026-02-15 arrives late; OL1-1 (2026-03-01) must move to it.
    backdated = env.products([["P1", "Red Kettle v2", "Home & Kitchen", "Acme", "45.00", "active",
                               "2026-02-15T00:00:00Z"]], name="backdated.csv")
    env.run(env.orders([], name="none.csv"), backdated)
    name = env.scalar("SELECT p.product_name FROM fact_sales f JOIN dim_product p USING (product_key) "
                      "WHERE order_line_id = 'OL1-1'")
    assert name == "Red Kettle v2"
    assert env.scalar("SELECT unit_price FROM fact_sales WHERE order_line_id = 'OL1-1'") == Decimal("35.00")


def test_conflicting_redelivery_of_same_product_version_rejected(env, base_files):
    orders, products = base_files
    env.run(orders, products)
    conflict = env.products([["P1", "Totally Different", "Toys", "X", "1.00", "active", "2026-01-01T00:00:00Z"]],
                            name="conflict.csv")
    env.run(env.orders([], name="none.csv"), conflict)
    assert "CONFLICTING_PRODUCT_VERSION" in env.reasons("P1")
    assert env.scalar("SELECT count(*) FROM dim_product WHERE product_id = 'P1'") == 1


# 13. Partial failure and retry ---------------------------------------------------------------
def test_partial_failure_rolls_back_and_retry_succeeds(env, base_files):
    orders, products = base_files
    first = env.run(orders, products)
    before = env.query(FACT_SNAPSHOT)
    batch2 = env.orders([["OL8-1", "O8", "C8", "P3", "2026-05-01T00:00:00Z", "2", "20.00", "paid"]], name="b2.csv")
    upd = env.products([["P3", "Green Mat XL", "Sports", "FitCo", "30.00", "active", "2026-04-20T00:00:00Z"]],
                       name="p2.csv")

    def crash(con):
        # Dimensions and facts are already written inside the transaction at this point.
        raise RuntimeError("simulated crash during load")

    failed = env.run(batch2, upd, before_commit=crash)
    assert failed.status == "FAILED" and "simulated crash" in failed.error_message
    assert env.query(FACT_SNAPSHOT) == before                                   # nothing partial committed
    assert env.scalar("SELECT count(*) FROM dim_product WHERE product_id = 'P3'") == 1
    assert env.scalar("SELECT count(*) FROM dim_customer WHERE customer_id = 'C8'") == 0
    assert env.scalar("SELECT status FROM processed_files WHERE file_name = 'b2.csv'") == "FAILED"

    retry = env.run(batch2, upd)                                                  # same files, no --force needed
    assert retry.status == "SUCCESS" and retry.metrics["facts_inserted"] == 1
    assert env.scalar("SELECT count(*) FROM fact_sales") == 5
    assert env.scalar("SELECT status, attempts FROM processed_files WHERE file_name = 'b2.csv'") == "SUCCESS"
    assert env.query("SELECT attempts FROM processed_files WHERE file_name = 'b2.csv'")[0][0] == 2
    statuses = [r[0] for r in env.query("SELECT execution_status FROM pipeline_runs ORDER BY start_time")]
    assert statuses == ["SUCCESS", "FAILED", "SUCCESS"] and first.status == "SUCCESS"


def test_interrupted_run_is_marked_failed_on_next_start(env, base_files):
    orders, products = base_files
    env.run(orders, products)
    import duckdb
    con = duckdb.connect(str(env.settings.db_path))
    con.execute("INSERT INTO pipeline_runs (run_id, start_time, execution_status) VALUES ('ghost', now(), 'RUNNING')")
    con.close()
    env.run(orders, products)
    assert env.scalar("SELECT execution_status FROM pipeline_runs WHERE run_id = 'ghost'") == "FAILED"


# 14. Idempotent reruns -----------------------------------------------------------------------
def test_rerun_of_identical_files_is_skipped(env, base_files):
    orders, products = base_files
    env.run(orders, products)
    snapshot = env.query(FACT_SNAPSHOT)
    again = env.run(orders, products)
    assert again.status == "SKIPPED"
    assert env.query(FACT_SNAPSHOT) == snapshot


def test_forced_rerun_creates_no_duplicates_and_no_changes(env, base_files):
    orders, products = base_files
    env.run(orders, products)
    snapshot = env.query(FACT_SNAPSHOT)
    dims = env.query("SELECT * FROM dim_product ORDER BY product_key")
    for _ in range(2):
        forced = env.run(orders, products, force=True)
        assert forced.status == "SUCCESS"
        assert forced.metrics["facts_inserted"] == 0 and forced.metrics["facts_updated"] == 0
        assert forced.metrics["facts_unchanged"] == 4
    assert env.query(FACT_SNAPSHOT) == snapshot          # same keys, same run_id (row untouched)
    assert env.query("SELECT * FROM dim_product ORDER BY product_key") == dims
    assert env.scalar("SELECT count(*) - count(DISTINCT order_line_id) FROM fact_sales") == 0


def test_same_content_under_new_filename_is_recognised(env, base_files):
    orders, products = base_files
    env.run(orders, products)
    copy = env.inputs / "orders_copy.csv"
    copy.write_bytes(orders.read_bytes())
    assert env.run(copy, products).status == "SKIPPED"


def test_surrogate_keys_identical_across_independent_warehouses(tmp_path, base_files, env):
    from tests.conftest import Env
    orders, products = base_files
    other = Env(tmp_path / "second")
    env.run(orders, products)
    other.run(orders, products)
    keys = "SELECT order_line_id, sales_key, product_key, customer_key FROM fact_sales ORDER BY 1"
    assert env.query(keys) == other.query(keys)
    assert env.scalar("SELECT sales_key FROM fact_sales WHERE order_line_id = 'OL1-1'") == surrogate_key("sale", "OL1-1")


# 15. Referential integrity --------------------------------------------------------------------
def check_integrity_values(env):
    import duckdb
    con = duckdb.connect(str(env.settings.db_path), read_only=True)
    try:
        return list(check_integrity(con).values())
    finally:
        con.close()


def test_fact_to_dimension_referential_integrity(env, base_files):
    orders, products = base_files
    env.run(orders, products)
    assert all(v == 0 for v in check_integrity_values(env))
    for dim, key in (("dim_product", "product_key"), ("dim_customer", "customer_key"), ("dim_date", "date_key")):
        assert env.scalar(f"SELECT count(*) FROM fact_sales f LEFT JOIN {dim} d USING ({key}) "
                          f"WHERE d.{key} IS NULL") == 0


def test_integrity_violation_blocks_commit(env, base_files):
    orders, products = base_files
    env.run(orders, products)
    batch = env.orders([["OL6-1", "O6", "C6", "P2", "2026-05-01T00:00:00Z", "1", "12.50", "paid"]], name="b.csv")

    def corrupt(con):
        con.execute("UPDATE fact_sales SET product_key = -1 WHERE order_line_id = 'OL6-1'")

    result = env.run(batch, env.products([], name="none.csv"), before_commit=corrupt)
    assert result.status == "FAILED" and "Referential integrity" in result.error_message
    assert env.scalar("SELECT count(*) FROM fact_sales WHERE order_line_id = 'OL6-1'") == 0


# 16. Revenue calculation correctness -----------------------------------------------------------
def test_revenue_calculation(env, base_files):
    orders, products = base_files
    env.run(orders, products)
    amounts = dict(env.query("SELECT order_line_id, total_amount FROM fact_sales"))
    assert amounts == {"OL1-1": Decimal("70.00"), "OL1-2": Decimal("12.50"), "OL2-1": Decimal("74.97"),
                       "OL3-1": Decimal("40.00")}
    assert env.scalar("SELECT sum(total_amount) FROM v_sales WHERE is_revenue") == Decimal("197.47")


def test_decimal_precision_no_float_drift(env):
    products = env.products(BASE_PRODUCTS)
    rows = [[f"OLP-{i}", f"OP{i}", "C1", "P2", "2026-03-01T00:00:00Z", "3", "0.10", "paid"] for i in range(1000)]
    env.run(env.orders(rows), products)
    assert env.scalar("SELECT sum(total_amount) FROM fact_sales") == Decimal("300.00")


# Extra domain-specific edge cases ---------------------------------------------------------------
def test_cancelled_and_returned_lines_excluded_from_revenue(env):
    products = env.products(BASE_PRODUCTS)
    rows = BASE_ORDERS + [["OL4-1", "O4", "C2", "P1", "2026-03-06T00:00:00Z", "1", "40.00", "cancelled"],
                          ["OL4-2", "O4", "C2", "P2", "2026-03-06T00:00:00Z", "1", "12.50", "returned"],
                          ["OL5-1", "O5", "C2", "P3", "2026-03-06T00:00:00Z", "1", "25.00", "pending"]]
    env.run(env.orders(rows), products)
    assert env.scalar("SELECT count(*) FROM fact_sales") == 7          # preserved in the warehouse
    assert env.scalar("SELECT sum(total_amount) FROM v_sales WHERE is_revenue") == Decimal("197.47")
    assert env.scalar("SELECT count(DISTINCT order_id) FROM v_sales WHERE is_revenue") == 3


def test_order_with_two_customers_rejected_as_a_whole(env):
    rows = BASE_ORDERS + [["OL9-1", "O9", "C1", "P1", "2026-03-07T00:00:00Z", "1", "40.00", "paid"],
                          ["OL9-2", "O9", "C2", "P2", "2026-03-07T00:00:00Z", "1", "12.50", "paid"]]
    result = env.run(env.orders(rows), env.products(BASE_PRODUCTS))
    assert result.metrics["orders_rejected_rows"] == 2
    assert env.scalar("SELECT count(*) FROM quarantine WHERE primary_reason = 'INCONSISTENT_ORDER_CUSTOMER'") == 2


def test_order_customer_conflict_with_warehouse_detected(env, base_files):
    orders, products = base_files
    env.run(orders, products)
    clash = env.orders([["OL1-3", "O1", "C9", "P3", "2026-03-01T10:00:00Z", "1", "25.00", "paid"]], name="clash.csv")
    result = env.run(clash, env.products([], name="none.csv"))
    assert result.metrics["orders_rejected_rows"] == 1
    assert "INCONSISTENT_ORDER_CUSTOMER" in env.reasons("OL1-3")


def test_timezone_offsets_normalised_to_utc_date(env):
    rows = [["OLT-1", "OT", "C1", "P1", "2026-03-02T02:00:00+05:30", "1", "40.00", "paid"],   # = 03-01 20:30 UTC
            ["OLT-2", "OT", "C1", "P2", "2026-03-01 23:59:59", "1", "12.50", "paid"]]       # naive = UTC
    env.run(env.orders(rows), env.products(BASE_PRODUCTS))
    rows = dict(env.query("SELECT order_line_id, date_key FROM fact_sales"))
    assert rows == {"OLT-1": 20260301, "OLT-2": 20260301}
    assert str(env.scalar("SELECT order_timestamp FROM fact_sales WHERE order_line_id = 'OLT-1'")) == "2026-03-01 20:30:00"


def test_status_correction_updates_fact_in_place(env, base_files):
    orders, products = base_files
    env.run(orders, products)
    correction = env.orders([["OL3-1", "O3", "C1", "P2", "2026-03-05T08:00:00Z", "4", "10.00", "delivered"]],
                            name="correction.csv")
    result = env.run(correction, env.products([], name="none.csv"))
    assert result.metrics["facts_updated"] == 1 and result.metrics["facts_inserted"] == 0
    assert env.scalar("SELECT order_status FROM fact_sales WHERE order_line_id = 'OL3-1'") == "delivered"
    assert env.scalar("SELECT count(*) FROM fact_sales") == 4


def test_replaying_older_file_does_not_undo_newer_correction(env, base_files):
    orders, products = base_files
    env.run(orders, products)
    correction = env.orders([["OL3-1", "O3", "C1", "P2", "2026-03-05T08:00:00Z", "4", "10.00", "delivered"]],
                            name="correction.csv")
    env.run(correction, env.products([], name="none.csv"))
    replay = env.run(orders, products, force=True)                       # backfill of the original file
    assert replay.metrics["facts_stale_skipped"] == 1 and replay.metrics["facts_updated"] == 0
    assert env.scalar("SELECT order_status FROM fact_sales WHERE order_line_id = 'OL3-1'") == "delivered"


def test_reject_threshold_fails_batch(env, base_files):
    _, products = base_files
    bad = env.orders([["OLB-1", "OB", "C1", "P404", "2026-03-01T00:00:00Z", "1", "1.00", "paid"],
                      ["OLB-2", "OB", "C1", "P404", "2026-03-01T00:00:00Z", "1", "1.00", "paid"],
                      ["OLB-3", "OB", "C1", "P1", "2026-03-01T00:00:00Z", "1", "1.00", "paid"]], name="bad.csv")
    result = env.run(bad, products)
    assert result.status == "FAILED" and "REJECT_THRESHOLD_EXCEEDED" in result.error_message
    assert env.scalar("SELECT count(*) FROM fact_sales") == 0
    assert env.scalar("SELECT count(*) FROM dim_product") == 0           # products of this batch not loaded either
    assert env.scalar("SELECT count(*) FROM quarantine") == 2            # diagnostics persisted


def test_simultaneous_execution_prevented(env, base_files):
    orders, products = base_files
    with pipeline_lock(env.settings.lock_path, "holder"):
        with pytest.raises(PipelineLockedError):
            env.run(orders, products)
    assert env.run(orders, products).status == "SUCCESS"                  # lock released


def test_concurrent_threads_only_one_runs(env, base_files):
    orders, products = base_files
    outcomes: list[str] = []
    barrier = threading.Barrier(3)

    def worker():
        barrier.wait()
        try:
            outcomes.append(env.run(orders, products).status)
        except PipelineLockedError:
            outcomes.append("LOCKED")

    threads = [threading.Thread(target=worker) for _ in range(3)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert outcomes.count("SUCCESS") >= 1
    assert env.scalar("SELECT count(*) FROM fact_sales") == 4
    assert env.scalar("SELECT count(*) FROM pipeline_runs WHERE execution_status = 'SUCCESS'") == outcomes.count("SUCCESS")
