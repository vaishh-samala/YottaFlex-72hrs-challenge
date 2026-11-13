"""Shared fixtures: every test gets its own temp warehouse, data and log directories."""
from __future__ import annotations

import csv
from pathlib import Path

import duckdb
import pytest

from src.config import Settings
from src.pipeline import RunResult, run_pipeline

ORDER_HEADER = ["order_line_id", "order_id", "customer_id", "product_id", "order_timestamp", "quantity",
                "unit_price", "order_status", "source_file"]
PRODUCT_HEADER = ["product_id", "product_name", "category", "brand", "list_price", "product_status", "updated_at",
                  "source_file"]

BASE_PRODUCTS = [
    ["P1", "Red Kettle", "Home & Kitchen", "Acme", "40.00", "active", "2026-01-01T00:00:00Z"],
    ["P2", "Blue Novel", "Books", "Pages Ltd", "12.50", "active", "2026-01-01T00:00:00Z"],
    ["P3", "Green Mat", "Sports", "FitCo", "25.00", "active", "2026-01-01T00:00:00Z"],
]
BASE_ORDERS = [
    ["OL1-1", "O1", "C1", "P1", "2026-03-01T10:00:00Z", "2", "35.00", "delivered"],
    ["OL1-2", "O1", "C1", "P2", "2026-03-01T10:00:00Z", "1", "12.50", "delivered"],
    ["OL2-1", "O2", "C2", "P3", "2026-03-02T11:30:00Z", "3", "24.99", "shipped"],
    ["OL3-1", "O3", "C1", "P2", "2026-03-05T08:00:00Z", "4", "10.00", "paid"],
]


def write_csv(path: Path, header: list[str], rows: list[list[str]], add_source: bool = True) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(header)
        for row in rows:
            writer.writerow(row + [path.name] if add_source and len(row) == len(header) - 1 else row)
    return path


class Env:
    """A self-contained pipeline environment rooted in a pytest tmp_path."""

    def __init__(self, root: Path):
        self.root = root
        self.inputs = root / "inputs"
        self.settings = Settings.for_root(root / "work", db_connect_timeout=5)

    def orders(self, rows: list[list[str]], name: str = "orders.csv", header: list[str] | None = None) -> Path:
        return write_csv(self.inputs / name, header or ORDER_HEADER, rows)

    def products(self, rows: list[list[str]], name: str = "products.csv", header: list[str] | None = None) -> Path:
        return write_csv(self.inputs / name, header or PRODUCT_HEADER, rows)

    def run(self, orders: Path, products: Path, **kwargs) -> RunResult:
        kwargs.setdefault("trigger", "test")
        kwargs.setdefault("console_log", False)
        return run_pipeline(self.settings, orders, products, **kwargs)

    def query(self, sql: str, params: list | None = None) -> list[tuple]:
        con = duckdb.connect(str(self.settings.db_path), read_only=True)
        try:
            return con.execute(sql, params or []).fetchall()
        finally:
            con.close()

    def scalar(self, sql: str, params: list | None = None):
        return self.query(sql, params)[0][0]

    def reasons(self, record_id: str) -> str:
        rows = self.query("SELECT rejection_reasons FROM quarantine WHERE record_id = ? ORDER BY rejected_at DESC",
                          [record_id])
        return rows[0][0] if rows else ""


@pytest.fixture
def env(tmp_path: Path) -> Env:
    return Env(tmp_path)


@pytest.fixture
def base_files(env: Env) -> tuple[Path, Path]:
    return env.orders(BASE_ORDERS), env.products(BASE_PRODUCTS)
