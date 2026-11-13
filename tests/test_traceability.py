"""Raw-record traceability and row accounting (no input record may disappear silently)."""
from __future__ import annotations

import duckdb
import numpy as np
import pandas as pd
import pytest

import src.validation.rules as rules
from src.ingestion.reader import read_source
from src.validation.rules import ValidationInvariantError, check_accounting
from tests.conftest import BASE_ORDERS, BASE_PRODUCTS, ORDER_HEADER

HEADER = ",".join(ORDER_HEADER)

# Every rejected line below must come back byte-for-byte from quarantine.source_line.
TRICKY_LINES = {
    "null_token":  'OL5-1,O5,NULL,P1,2026-03-01T10:00:00Z,1,5.00,paid,orders.csv',
    "quoted":      '"OL6-1","O6","C6","P1","2026-03-01T10:00:00Z","  2 ","abc","paid","orders.csv"',
    "comma_quote": 'OL7-1,O7,C7,"P1, ""special""",2026-03-01T10:00:00Z,1,5.00,paid,orders.csv',
    "multiline":   'OL8-1,O8,C8,P1,2026-03-01T10:00:00Z,1,5.00,"pa\nid",orders.csv',
    "malformed":   'OL9-1,"O9, still O9",C9,P1,2026-03-01T10:00:00Z',
    "malf_nokey":  ',O10',
}


def _write_raw(env, lines: list[str], newline: str = "\n", name: str = "orders.csv"):
    path = env.inputs / name
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes((newline.join(lines) + newline).encode("utf-8"))
    return path


# Extra valid lines keep every test batch below the 50 % reject threshold.
FILLER = [[f"OLF-{i}", f"OF{i}", "CF", "P3", "2026-03-03T12:00:00Z", "1", "25.00", "paid"] for i in range(8)]


def _good_lines() -> list[str]:
    return [",".join(r + ["orders.csv"]) for r in BASE_ORDERS + FILLER]


@pytest.mark.parametrize("newline", ["\n", "\r\n"])
def test_rejected_records_keep_exact_original_text(env, newline):
    lines = [HEADER] + _good_lines() + list(TRICKY_LINES.values())
    orders = _write_raw(env, lines, newline)
    products = env.products(BASE_PRODUCTS)
    result = env.run(orders, products)
    assert result.status == "SUCCESS", result.error_message

    rows = env.query("SELECT source_row_number, record_id, primary_reason, source_line, source_file, run_id, "
                     "rejection_reasons FROM quarantine ORDER BY source_row_number")
    by_text = {r[3]: r for r in rows}
    assert set(by_text) == set(TRICKY_LINES.values())          # every reject, text identical to the file
    physical = orders.read_bytes().decode("utf-8").splitlines(keepends=True)
    for line_no, record_id, reason, text, source_file, run_id, reasons in rows:
        # source_row_number is the first physical line of the record; the exact text starts there and is
        # followed by the line terminator.
        assert "".join(physical[line_no - 1:]).startswith(text + newline)
        assert source_file == "orders.csv" and run_id == result.run_id and reason and reasons
    assert by_text[TRICKY_LINES["malformed"]][1] == "OL9-1"     # key taken from the parsed field
    assert by_text[TRICKY_LINES["malf_nokey"]][1] is None       # blank key stays null, row still traced
    assert by_text[TRICKY_LINES["multiline"]][0] == 17          # multi-line record: its first line
    assert by_text[TRICKY_LINES["null_token"]][2] == "MISSING_REQUIRED_FIELD"

    # Re-assembling header + valid lines + rejected lines reproduces the input file exactly.
    con = duckdb.connect(str(env.settings.db_path), read_only=True)
    try:
        assert con.execute("SELECT count(*) FROM quarantine WHERE source_line IS NULL").fetchone()[0] == 0
    finally:
        con.close()
    source = read_source(orders, "orders")
    rebuilt = dict(zip(source.data["source_row_number"], source.source_lines))
    rebuilt.update({m.row_number: m.raw_line for m in source.malformed})
    assert newline.join([HEADER] + [rebuilt[k] for k in sorted(rebuilt)]) + newline == orders.read_bytes().decode()


def test_quarantine_parquet_carries_source_line(env):
    orders = _write_raw(env, [HEADER] + _good_lines() + [TRICKY_LINES["quoted"], TRICKY_LINES["malformed"]])
    result = env.run(orders, env.products(BASE_PRODUCTS))
    frame = pd.read_parquet(env.settings.quarantine_dir / result.run_id / "orders_rejected.parquet")
    assert set(frame["source_line"]) == {TRICKY_LINES["quoted"], TRICKY_LINES["malformed"]}


def test_blank_lines_are_counted_and_reported(env):
    orders = _write_raw(env, [HEADER, _good_lines()[0], "", _good_lines()[1], ""])
    source = read_source(orders, "orders")
    assert len(source.data) == 2 and source.blank_lines == 2
    assert any(w.startswith("BLANK_LINES: 2") for w in source.warnings)
    assert source.data["source_row_number"].tolist() == [2, 4]   # numbering still physical


def test_existing_warehouse_without_source_line_is_upgraded(env, base_files):
    """A warehouse created before the column existed keeps working; new rejects get their text."""
    orders, products = base_files
    assert env.run(orders, products).status == "SUCCESS"
    con = duckdb.connect(str(env.settings.db_path))
    con.execute("ALTER TABLE quarantine DROP COLUMN source_line")
    con.close()
    bad = _write_raw(env, [HEADER] + _good_lines() + [TRICKY_LINES["quoted"]], name="orders_2.csv")
    assert env.run(bad, products).status == "SUCCESS"
    assert env.query("SELECT source_line FROM quarantine") == [(TRICKY_LINES["quoted"],)]


# Duplicate handling / accounting -----------------------------------------------------------
def test_check_accounting_rejects_lost_rows_and_duplicate_keys():
    frame = pd.DataFrame({"order_line_id": ["A", "B", "C"], "source_row_number": [2, 3, 4]})
    check_accounting(frame, frame.iloc[:2], np.array([False, False, True]), ["order_line_id"])  # balanced
    with pytest.raises(ValidationInvariantError, match="row accounting"):
        check_accounting(frame, frame.iloc[:1], np.array([False, False, True]), ["order_line_id"])
    dup = pd.DataFrame({"order_line_id": ["A", "A"], "source_row_number": [2, 3]})
    with pytest.raises(ValidationInvariantError, match="share a"):
        check_accounting(dup, dup, np.array([False, False]), ["order_line_id"])


def test_duplicate_leaking_past_rules_fails_batch_instead_of_vanishing(env, base_files, monkeypatch):
    """If the duplicate rules ever stop flagging a copy, the run must FAIL loudly; the old code silently
    dropped the second copy and the input no longer reconciled."""
    original_add = rules._Collector.add

    def add_without_duplicate_rules(self, mask, code, detail):
        if code not in ("DUPLICATE_RECORD", "CONFLICTING_DUPLICATE_KEY"):
            original_add(self, mask, code, detail)

    monkeypatch.setattr(rules._Collector, "add", add_without_duplicate_rules)
    _, products = base_files
    orders = env.orders(BASE_ORDERS + [BASE_ORDERS[0]])
    result = env.run(orders, products)
    assert result.status == "FAILED"
    assert "ValidationInvariantError" in result.error_message or "share a" in result.error_message
    assert env.scalar("SELECT count(*) FROM fact_sales") == 0                 # nothing committed


def test_every_input_row_is_accounted_for_with_duplicates(env, base_files):
    _, products = base_files
    conflicting = list(BASE_ORDERS[1]); conflicting[5] = "9"
    rows = BASE_ORDERS + FILLER + [BASE_ORDERS[0], BASE_ORDERS[0], conflicting]  # 2 exact copies + 1 conflict
    result = env.run(env.orders(rows), products)
    assert result.status == "SUCCESS", result.error_message
    m = result.metrics
    assert m["orders_input_rows"] == 15 == m["orders_valid_rows"] + m["orders_rejected_rows"]
    assert m["orders_valid_rows"] == env.scalar("SELECT count(*) FROM fact_sales") == 11
    reasons = dict(env.query("SELECT primary_reason, count(*) FROM quarantine GROUP BY 1"))
    assert reasons == {"DUPLICATE_RECORD": 2, "CONFLICTING_DUPLICATE_KEY": 2}
