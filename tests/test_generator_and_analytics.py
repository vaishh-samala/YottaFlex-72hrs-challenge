"""Synthetic data generator, end-to-end sample run, analytical SQL and reconciliation."""
from __future__ import annotations

import hashlib
import json
from pathlib import Path

import duckdb
import pandas as pd

from src.analytics.queries import list_queries, reconciliation_passes, run_group, run_query
from src.config import PROJECT_ROOT
from src.datagen import generate_dataset

SAMPLE_DIR = PROJECT_ROOT / "data" / "sample"


def _digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def test_generator_is_deterministic(tmp_path):
    a = generate_dataset(tmp_path / "a", tmp_path / "la", n_orders=500, n_products=40, seed=7, with_increment=True)
    b = generate_dataset(tmp_path / "b", tmp_path / "lb", n_orders=500, n_products=40, seed=7, with_increment=True)
    for key in a.files:
        assert _digest(a.files[key].path) == _digest(b.files[key].path)
    c = generate_dataset(tmp_path / "c", tmp_path / "lc", n_orders=500, n_products=40, seed=8)
    assert _digest(a.files["orders"].path) != _digest(c.files["orders"].path)


def test_labels_are_kept_out_of_production_inputs(tmp_path):
    result = generate_dataset(tmp_path / "in", tmp_path / "labels", n_orders=300, n_products=30)
    assert result.labels_path.parent == tmp_path / "labels"
    for info in result.files.values():
        header = info.path.read_text().splitlines()[0]
        assert "defect" not in header and "label" not in header and "expected" not in header


def test_generator_scales_to_100k(tmp_path):
    result = generate_dataset(tmp_path / "big", tmp_path / "l", n_orders=100_000, n_products=1000)
    assert result.files["orders"].rows >= 100_000


def test_sample_end_to_end_matches_injected_labels(env, tmp_path):
    """Run both sample batches in an isolated warehouse; every injected defect must be quarantined."""
    before = {p.name: _digest(p) for p in SAMPLE_DIR.glob("*.csv")}
    out, labels_dir = tmp_path / "sample", tmp_path / "labels"
    gen = generate_dataset(out, labels_dir, n_orders=600, n_products=60, seed=42, with_increment=True)
    r1 = env.run(out / "orders.csv", out / "products.csv")
    r2 = env.run(out / "orders_batch_2.csv", out / "products_update.csv")
    assert r1.status == r2.status == "SUCCESS"

    labels = pd.read_csv(gen.labels_path)
    con = duckdb.connect(str(env.settings.db_path), read_only=True)
    try:
        q = con.execute("SELECT source_file, record_id, primary_reason FROM quarantine").df()
        facts = set(con.execute("SELECT order_line_id FROM fact_sales").df()["order_line_id"])
        late_keys = con.execute("SELECT count(*) FROM fact_sales WHERE date_key < 20260301 "
                                "AND source_file = 'orders_batch_2.csv'").fetchall()[0][0]
        checks = reconciliation_passes(con)
    finally:
        con.close()

    accepted = {"boundary_max_quantity", "boundary_min_price", "normalisable_status", "late_arriving_event",
                "status_correction", "catalog_update", "new_product"}
    defects = labels[~labels["defect"].isin(accepted)]
    for row in defects.itertuples():
        if row.record_id == "(blank)":
            assert ((q["source_file"] == row.file) & q["record_id"].isna()).any()
        else:
            assert ((q["source_file"] == row.file) & (q["record_id"] == row.record_id)).any(), row
    for row in labels[labels["defect"].isin({"boundary_max_quantity", "boundary_min_price", "normalisable_status",
                                             "late_arriving_event", "status_correction"})].itertuples():
        assert row.record_id in facts, row
    assert late_keys > 0
    assert all(checks.values()), checks
    # Tests never touch the committed sample files.
    assert {p.name: _digest(p) for p in SAMPLE_DIR.glob("*.csv")} == before


def test_all_analytical_queries_run_on_real_tables(env, base_files):
    orders, products = base_files
    env.run(orders, products)
    con = duckdb.connect(str(env.settings.db_path), read_only=True)
    try:
        assert len(list_queries("analytics")) == 10
        results = run_group(con, "analytics")
        assert round(float(results["01_total_revenue"]["total_revenue"].iloc[0]), 2) == 197.47
        assert results["02_total_orders"]["revenue_orders"].iloc[0] == 3
        assert results["03_total_quantity"]["total_quantity"].iloc[0] == 10
        assert round(float(results["09_average_order_value"]["average_order_value"].iloc[0]), 2) == 65.82
        assert len(results["04_daily_revenue"]) == 3
        assert round(float(results["06_revenue_by_category"]["revenue"].sum()), 2) == 197.47
        assert results["07_top_10_products"]["product_id"].iloc[0] == "P3"         # 74.97
        assert results["08_top_10_customers"]["customer_id"].iloc[0] == "C1"       # 70 + 12.5 + 40
        assert results["10_rejected_record_pct"].set_index("source_type").loc["ALL SOURCES", "rejected_pct"] == 0
        for group in ("reconciliation", "monitoring"):
            for name in list_queries(group):
                run_query(con, group, name)                                         # every query executes
    finally:
        con.close()


def test_rejected_percentage_and_reconciliation_with_rejects(env, base_files):
    _, products = base_files
    rows = [["OL1-1", "O1", "C1", "P1", "2026-03-01T10:00:00Z", "2", "35.00", "delivered"],
            ["OL1-2", "O1", "C1", "P404", "2026-03-01T10:00:00Z", "1", "12.50", "delivered"],
            ["OL2-1", "O2", "C2", "P3", "2026-03-02T11:30:00Z", "0", "24.99", "shipped"],
            ["OL3-1", "O3", "C1", "P2", "2026-03-05T08:00:00Z", "4", "10.00", "paid"]]
    env.run(env.orders(rows), products)
    env.run(env.orders(rows), products, force=True)            # rerun must not double count
    con = duckdb.connect(str(env.settings.db_path), read_only=True)
    try:
        pct = run_query(con, "analytics", "10_rejected_record_pct").set_index("source_type")
        assert pct.loc["orders", "rejected_rows"] == 2 and pct.loc["orders", "input_rows"] == 4
        assert pct.loc["orders", "rejected_pct"] == 50.0
        assert all(reconciliation_passes(con).values())
    finally:
        con.close()


def test_generated_dataset_full_reconciliation_equation(env, tmp_path):
    """Requested lines -> physical rows -> valid + rejected -> facts, with every reject explained."""
    n_orders, n_products = 1000, 60
    gen = generate_dataset(tmp_path / "in", tmp_path / "labels", n_orders=n_orders, n_products=n_products, seed=42)
    result = env.run(gen.files["orders"].path, gen.files["products"].path)
    assert result.status == "SUCCESS"
    labels = pd.read_csv(gen.labels_path, dtype=str)
    order_labels = labels[labels["file"] == "orders.csv"]

    # 1. Physical rows = requested lines + injected extra copies (exact + conflicting duplicates).
    n_extra = int(order_labels["defect"].isin({"exact_duplicate", "conflicting_duplicate"}).sum())
    assert gen.files["orders"].rows == n_orders + n_extra

    con = duckdb.connect(str(env.settings.db_path), read_only=True)
    try:
        b = run_query(con, "reconciliation", "04_reject_breakdown").set_index("source_type")
        q = con.execute("SELECT record_id, primary_reason, raw_record FROM quarantine "
                        "WHERE source_type = 'orders'").df()
        facts = con.execute("SELECT count(*) FROM fact_sales").fetchone()[0]
        checks = reconciliation_passes(con)
    finally:
        con.close()
    o, p = b.loc["orders"], b.loc["products"]

    # 2. input = valid + rejected, for each file and in total.
    assert o["input_rows"] == gen.files["orders"].rows == o["valid_rows"] + o["rejected_rows"]
    assert p["input_rows"] == gen.files["products"].rows == p["valid_rows"] + p["rejected_rows"]
    # 3. Rejected = sum of categories; valid order rows = fact rows (fresh warehouse).
    cats = ["malformed", "duplicates", "missing_values", "invalid_types", "business_rule", "invalid_references"]
    assert o[cats].sum() == o["rejected_rows"] and p[cats].sum() == p["rejected_rows"]
    assert o["valid_rows"] == facts == o["facts_accounted"]
    # Exact duplicates reject only the extra copy; conflicting duplicates reject every copy.
    n_exact = int((order_labels["defect"] == "exact_duplicate").sum())
    n_conflict = int((order_labels["defect"] == "conflicting_duplicate").sum())
    assert o["duplicates"] == n_exact + 2 * n_conflict
    assert o["malformed"] == int((order_labels["defect"] == "malformed_record").sum())
    assert all(checks.values()), checks

    # 4. Every quarantined order row is explained: labelled defect, blank key, or a sibling line of an
    #    order the generator attached to a second customer (the whole order is rejected by design).
    order_ids = pd.Series([json.loads(r).get("order_id") for r in q["raw_record"]])
    hijacked = set(order_ids[q["record_id"].isin(
        order_labels.loc[order_labels["defect"] == "inconsistent_order_customer", "record_id"])])
    explained = (q["record_id"].isin(order_labels["record_id"])
                 | q["record_id"].isna()
                 | ((q["primary_reason"] == "INCONSISTENT_ORDER_CUSTOMER") & order_ids.isin(hijacked)))
    assert explained.all(), q[~explained]
    assert q["record_id"].isna().sum() == int((order_labels["record_id"] == "(blank)").sum())

    # 5. Every rejected row (orders and products) carries its exact original line.
    con = duckdb.connect(str(env.settings.db_path), read_only=True)
    try:
        traced = con.execute("SELECT source_file, source_row_number, source_line FROM quarantine").fetchall()
    finally:
        con.close()
    lines = {name: f.path.read_text(encoding="utf-8").splitlines() for name, f in
             (("orders.csv", gen.files["orders"]), ("products.csv", gen.files["products"]))}
    assert traced and all(lines[f][n - 1] == text for f, n, text in traced)


def test_run_summary_does_not_count_skipped_runs_as_ingested(env, base_files):
    """A rerun that is SKIPPED by the file ledger must not inflate 'Records ingested' on the dashboard."""
    orders, products = base_files
    assert env.run(orders, products).status == "SUCCESS"
    assert env.run(orders, products).status == "SKIPPED"
    con = duckdb.connect(str(env.settings.db_path), read_only=True)
    try:
        s = run_query(con, "monitoring", "run_summary").iloc[0]
    finally:
        con.close()
    assert s["total_runs"] == 2 and s["skipped_runs"] == 1
    assert s["total_records_ingested"] == s["total_valid_records"] + s["total_rejected_records"] == 7
