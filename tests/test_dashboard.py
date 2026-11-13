"""Dashboard smoke test: every page renders against a real warehouse without exceptions, and the headline
numbers are the ones the SQL returns (nothing hard-coded)."""
from __future__ import annotations

from pathlib import Path

import pytest
from streamlit.testing.v1 import AppTest

from tests.conftest import BASE_ORDERS

PAGE_SCRIPT = str(Path(__file__).with_name("dashboard_page.py"))
PAGES = ["Pipeline Overview", "Data Quality", "Business Analytics", "Pipeline Runs"]


def _render(monkeypatch, env, page: str) -> AppTest:
    for key, value in {"DATAFLOW_DB_PATH": env.settings.db_path, "DATAFLOW_LOGS_DIR": env.settings.logs_dir,
                       "DATAFLOW_RAW_DIR": env.settings.raw_dir, "DATAFLOW_PROCESSED_DIR": env.settings.processed_dir,
                       "DATAFLOW_QUARANTINE_DIR": env.settings.quarantine_dir, "DF_PAGE": page}.items():
        monkeypatch.setenv(key, str(value))
    at = AppTest.from_file(PAGE_SCRIPT, default_timeout=60)
    at.run()
    return at


@pytest.mark.parametrize("page", PAGES)
def test_dashboard_page_renders_from_warehouse(env, base_files, monkeypatch, page):
    orders, products = base_files
    bad = env.orders(BASE_ORDERS + [["OL9-9", "O9", "C9", "P404", "2026-03-01T10:00:00Z", "1", "5.00", "paid"]],
                     name="orders_2.csv")
    assert env.run(orders, products).status == "SUCCESS"
    assert env.run(bad, products).status == "SUCCESS"
    at = _render(monkeypatch, env, page)
    assert not at.exception, [e.value for e in at.exception]
    assert [t.value for t in at.title] == [page]
    metrics = {m.label: m.value for m in at.metric}
    if page == "Pipeline Overview":
        assert metrics["Total runs"] == "2" and metrics["Rejected records"] == "1"
        assert metrics["Records ingested"] == "15"           # 3 + 4 + 3 + 5 physical rows
    elif page == "Data Quality":
        assert metrics["Rejected records"] == "1" and metrics["Invalid references"] == "1"
    elif page == "Business Analytics":
        assert metrics["Total revenue"] == "197.47"
    else:
        run_button = [b for b in at.button if "Run pipeline" in b.label]
        assert len(run_button) == 1 and not run_button[0].disabled   # idle: run button available


def test_dashboard_without_warehouse_shows_empty_state(env, monkeypatch):
    at = _render(monkeypatch, env, "Pipeline Overview")
    assert not at.exception
    assert any("No warehouse yet" in i.value for i in at.info)
    assert not at.metric
