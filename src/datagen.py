"""Reproducible synthetic data generator for the two DataFlow sources.

Produces production-like CSV inputs with deliberately injected defects. The list of
injected defects ("labels") is written to a *separate* file so expected answers never
leak into the pipeline input.
"""
from __future__ import annotations

import csv
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path

import numpy as np
import pandas as pd
from faker import Faker

from src.schemas import ORDERS, PRODUCTS

CATALOG = {
    "Electronics": ["Headphones", "Smartwatch", "Charger", "Speaker", "Keyboard", "Monitor", "Webcam"],
    "Home & Kitchen": ["Blender", "Kettle", "Cookware Set", "Lamp", "Vacuum", "Toaster"],
    "Books": ["Novel", "Cookbook", "Biography", "Textbook", "Comic"],
    "Clothing": ["T-Shirt", "Jacket", "Sneakers", "Jeans", "Scarf"],
    "Sports": ["Yoga Mat", "Dumbbells", "Football", "Cycling Helmet", "Tennis Racket"],
    "Beauty": ["Face Serum", "Shampoo", "Perfume", "Lipstick"],
    "Toys": ["Puzzle", "Building Blocks", "Doll", "RC Car"],
    "Grocery": ["Coffee Beans", "Green Tea", "Olive Oil", "Granola"],
}
PRICE_RANGE = {
    "Electronics": (15, 1500), "Home & Kitchen": (10, 600), "Books": (5, 80), "Clothing": (8, 250),
    "Sports": (10, 400), "Beauty": (5, 150), "Toys": (6, 200), "Grocery": (2, 60),
}
STATUS_WEIGHTS = {"delivered": 0.45, "shipped": 0.2, "paid": 0.15, "pending": 0.1, "cancelled": 0.06, "returned": 0.04}

ORDER_DEFECTS = [
    "missing_required_field", "null_token", "invalid_quantity_type", "negative_quantity", "zero_quantity",
    "invalid_price", "invalid_timestamp", "future_timestamp", "invalid_product_reference", "invalid_status",
    "exact_duplicate", "conflicting_duplicate", "malformed_record", "inconsistent_order_customer",
    "quantity_above_max",
]
PRODUCT_DEFECTS = ["missing_required_field", "invalid_price", "invalid_status", "invalid_timestamp", "duplicate_product_id"]


@dataclass
class GeneratedFile:
    path: Path
    rows: int


@dataclass
class GenerationResult:
    files: dict[str, GeneratedFile] = field(default_factory=dict)
    labels_path: Path | None = None
    labels: list[dict] = field(default_factory=list)


def _iso(ts: pd.Series, rng: np.random.Generator) -> list[str]:
    """Mix of ISO-8601 spellings (Z, space separator, +05:30 offset) - all valid."""
    choice = rng.random(len(ts))
    z = ts.dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    space = ts.dt.strftime("%Y-%m-%d %H:%M:%S")
    ist = (ts + pd.Timedelta(hours=5, minutes=30)).dt.strftime("%Y-%m-%dT%H:%M:%S+05:30")
    return np.where(choice < 0.80, z, np.where(choice < 0.95, space, ist)).tolist()


class DataGenerator:
    def __init__(self, seed: int = 42):
        self.seed = seed
        self.rng = np.random.default_rng(seed)
        self.fake = Faker()
        self.fake.seed_instance(seed)

    # ------------------------------------------------------------------ products
    def products(self, n: int, updated_at: datetime, start_index: int = 1) -> pd.DataFrame:
        cats = list(CATALOG)
        records = []
        for i in range(start_index, start_index + n):
            category = cats[int(self.rng.integers(len(cats)))]
            noun = CATALOG[category][int(self.rng.integers(len(CATALOG[category])))]
            low, high = PRICE_RANGE[category]
            records.append({
                "product_id": f"P{i:05d}",
                "product_name": f"{self.fake.color_name()} {noun}",
                "category": category,
                "brand": self.fake.company().replace(",", ""),
                "list_price": round(float(self.rng.uniform(low, high)), 2),
                "product_status": "active" if self.rng.random() > 0.08 else "discontinued",
                "updated_at": (updated_at + timedelta(minutes=int(self.rng.integers(0, 1440)))).strftime("%Y-%m-%dT%H:%M:%SZ"),
            })
        return pd.DataFrame(records)

    # -------------------------------------------------------------------- orders
    def orders(self, n_lines: int, products: pd.DataFrame, n_customers: int, start: datetime, end: datetime,
               order_offset: int = 0) -> pd.DataFrame:
        rng = self.rng
        sizes = rng.integers(1, 6, size=max(1, n_lines // 2 + 5))
        sizes = sizes[: int(np.searchsorted(np.cumsum(sizes), n_lines)) + 1]
        order_nums = np.repeat(np.arange(1, len(sizes) + 1), sizes)[:n_lines] + order_offset
        line_nums = np.concatenate([np.arange(1, s + 1) for s in sizes])[:n_lines]
        n_orders = len(sizes)

        span = int((end - start).total_seconds())
        order_ts = pd.to_datetime(start) + pd.to_timedelta(rng.integers(0, span, n_orders), unit="s")
        order_customer = rng.integers(1, n_customers + 1, n_orders)
        statuses = list(STATUS_WEIGHTS)
        order_status = rng.choice(statuses, size=n_orders, p=list(STATUS_WEIGHTS.values()))
        idx = order_nums - order_offset - 1

        prod_idx = rng.integers(0, len(products), n_lines)
        list_prices = products["list_price"].to_numpy()[prod_idx]
        discount = np.where(rng.random(n_lines) < 0.4, 1.0, rng.uniform(0.8, 1.0, n_lines))
        unit_price = np.maximum(np.round(list_prices * discount, 2), 0.01)
        qty = rng.choice([1, 1, 1, 2, 2, 3, 4, 5], size=n_lines)

        ts = pd.Series(order_ts[idx])
        frame = pd.DataFrame({
            "order_line_id": [f"OL{o:08d}-{l}" for o, l in zip(order_nums, line_nums)],
            "order_id": [f"O{o:08d}" for o in order_nums],
            "customer_id": [f"C{c:06d}" for c in order_customer[idx]],
            "product_id": products["product_id"].to_numpy()[prod_idx],
            "order_timestamp": _iso(ts, rng),
            "quantity": qty.astype(str),
            "unit_price": [f"{p:.2f}" for p in unit_price],
            "order_status": order_status[idx],
        })
        return frame

    # ------------------------------------------------------------------- defects
    def inject_order_defects(self, frame: pd.DataFrame, rate: float, file_name: str,
                             valid_product_ids: list[str]) -> tuple[list[list[str]], list[dict]]:
        """Return CSV rows (some malformed / duplicated) and the defect labels."""
        rng = self.rng
        rows = frame.astype(str).values.tolist()
        cols = list(frame.columns)
        c = {name: i for i, name in enumerate(cols)}
        n = len(rows)
        n_defects = int(round(n * rate)) if n else 0
        labels: list[dict] = []
        targets = rng.choice(n, size=min(n_defects, n), replace=False) if n_defects else []
        extra_rows: list[tuple[int, list[str]]] = []
        malformed_at: set[int] = set()
        for k, i in enumerate(targets):
            i = int(i)
            defect = ORDER_DEFECTS[k % len(ORDER_DEFECTS)]
            row = rows[i]
            rid = row[c["order_line_id"]]
            if defect == "missing_required_field":
                col = ORDERS.required[int(rng.integers(len(ORDERS.required)))]
                if col == "order_line_id":
                    rid = "(blank)"
                row[c[col]] = ""
            elif defect == "null_token":
                row[c["customer_id"]] = "NULL"
            elif defect == "invalid_quantity_type":
                row[c["quantity"]] = rng.choice(["two", "3.5", "1e3x"])
            elif defect == "negative_quantity":
                row[c["quantity"]] = str(-int(rng.integers(1, 5)))
            elif defect == "zero_quantity":
                row[c["quantity"]] = "0"
            elif defect == "invalid_price":
                row[c["unit_price"]] = rng.choice(["abc", "0", "-12.50", "12,99"])
            elif defect == "invalid_timestamp":
                row[c["order_timestamp"]] = rng.choice(["2026-02-30T10:00:00Z", "not-a-date", "31/12/2026 99:00"])
            elif defect == "future_timestamp":
                row[c["order_timestamp"]] = "2031-01-01T00:00:00Z"
            elif defect == "invalid_product_reference":
                row[c["product_id"]] = "P99999"
            elif defect == "invalid_status":
                row[c["order_status"]] = rng.choice(["teleported", "lost", "??"])
            elif defect == "quantity_above_max":
                row[c["quantity"]] = "1001"
            elif defect == "exact_duplicate":
                extra_rows.append((i, list(row)))
            elif defect == "conflicting_duplicate":
                dup = list(row)
                dup[c["quantity"]] = str(int(dup[c["quantity"]]) + 7) if dup[c["quantity"]].lstrip("-").isdigit() else "9"
                extra_rows.append((i, dup))
            elif defect == "malformed_record":
                malformed_at.add(i)
                rows[i] = row[: len(cols) - 3]
            elif defect == "inconsistent_order_customer":
                # Attach this line to a neighbouring order but keep a different customer.
                neighbour = rows[i - 1] if i > 0 else rows[i + 1]
                row[c["order_id"]] = neighbour[c["order_id"]]
                row[c["customer_id"]] = "C999999"
            labels.append({"file": file_name, "record_id": rid, "defect": defect})

        # Valid boundary values (labelled as boundary, NOT defects).
        target_set = {int(t) for t in targets}
        clean = [i for i in range(n) if i not in target_set]
        for value_col, value, tag in (("quantity", "1000", "boundary_max_quantity"),
                                      ("unit_price", "0.01", "boundary_min_price")):
            if clean:
                i = clean.pop(int(rng.integers(len(clean))))
                rows[i][c[value_col]] = value
                labels.append({"file": file_name, "record_id": rows[i][c["order_line_id"]], "defect": tag})
        if clean:
            i = clean.pop(int(rng.integers(len(clean))))
            rows[i][c["order_status"]] = "  " + rows[i][c["order_status"]].upper() + " "
            labels.append({"file": file_name, "record_id": rows[i][c["order_line_id"]], "defect": "normalisable_status"})

        # Duplicates are appended right after their originals (keep-first rule applies).
        for i, dup in sorted(extra_rows, key=lambda t: t[0], reverse=True):
            rows.insert(i + 1, dup)
        return rows, labels

    def inject_product_defects(self, frame: pd.DataFrame, n_defects: int, file_name: str,
                               next_index: int) -> tuple[list[list[str]], list[dict]]:
        rows = frame.astype(str).values.tolist()
        cols = list(frame.columns)
        c = {name: i for i, name in enumerate(cols)}
        labels: list[dict] = []
        for k in range(n_defects):
            defect = PRODUCT_DEFECTS[k % len(PRODUCT_DEFECTS)]
            if defect == "duplicate_product_id" and rows:
                victim = list(rows[int(self.rng.integers(len(rows)))])
                victim[c["product_name"]] = victim[c["product_name"]] + " (stale copy)"
                victim[c["updated_at"]] = "2025-01-01T00:00:00Z"  # older than the original -> loses
                rows.append(victim)
                labels.append({"file": file_name, "record_id": victim[c["product_id"]], "defect": defect})
                continue
            pid = f"P{next_index + k:05d}"
            row = [pid, f"Broken Item {k}", "Grocery", "Acme", "9.99", "active", "2026-01-01T00:00:00Z"]
            if defect == "missing_required_field":
                row[c["product_name"]] = ""
            elif defect == "invalid_price":
                row[c["list_price"]] = "-4.00"
            elif defect == "invalid_status":
                row[c["product_status"]] = "recalled??"
            elif defect == "invalid_timestamp":
                row[c["updated_at"]] = "yesterday"
            rows.append(row)
            labels.append({"file": file_name, "record_id": pid, "defect": defect})
        return rows, labels


def _write_csv(path: Path, columns: list[str], rows: list[list[str]], source_file: str) -> int:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(columns + ["source_file"])
        for row in rows:
            # Malformed rows stay short (no source_file appended) so they really are malformed.
            writer.writerow(row + [source_file] if len(row) == len(columns) else row)
    return len(rows)


def _write_labels(path: Path, labels: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(labels, columns=["file", "record_id", "defect"]).to_csv(path, index=False)


def generate_dataset(out_dir: Path, labels_dir: Path, n_orders: int, n_products: int, seed: int = 42,
                     defect_rate: float = 0.03, n_customers: int | None = None,
                     with_increment: bool = False) -> GenerationResult:
    """Generate products.csv + orders.csv (and optionally an incremental second batch)."""
    out_dir, labels_dir = Path(out_dir), Path(labels_dir)
    gen = DataGenerator(seed)
    result = GenerationResult()
    n_customers = n_customers or max(10, n_orders // 8)
    product_cols = [c for c in PRODUCTS.columns if c != "source_file"]
    order_cols = [c for c in ORDERS.columns if c != "source_file"]

    base_updated = datetime(2025, 12, 1, tzinfo=timezone.utc).replace(tzinfo=None)
    products = gen.products(n_products, base_updated)
    n_prod_defects = max(len(PRODUCT_DEFECTS), int(round(n_products * 0.04)))
    prod_rows, prod_labels = gen.inject_product_defects(products[product_cols], n_prod_defects, "products.csv",
                                                        next_index=90000)
    result.files["products"] = GeneratedFile(out_dir / "products.csv",
                                             _write_csv(out_dir / "products.csv", product_cols, prod_rows, "products.csv"))

    orders = gen.orders(n_orders, products, n_customers, datetime(2026, 1, 1), datetime(2026, 7, 1))
    order_rows, order_labels = gen.inject_order_defects(orders[order_cols], defect_rate, "orders.csv",
                                                        products["product_id"].tolist())
    result.files["orders"] = GeneratedFile(out_dir / "orders.csv",
                                           _write_csv(out_dir / "orders.csv", order_cols, order_rows, "orders.csv"))
    labels = prod_labels + order_labels

    if with_increment:
        labels += _generate_increment(gen, out_dir, products, orders, n_customers, product_cols, order_cols,
                                      result, n_products)

    result.labels = labels
    result.labels_path = labels_dir / "expected_defects.csv"
    _write_labels(result.labels_path, labels)
    return result


def _generate_increment(gen: DataGenerator, out_dir: Path, products: pd.DataFrame, orders: pd.DataFrame,
                        n_customers: int, product_cols: list[str], order_cols: list[str],
                        result: GenerationResult, n_products: int) -> list[dict]:
    """Second batch: catalog updates (SCD2), new product, late events, status corrections."""
    rng = gen.rng
    labels: list[dict] = []
    # --- products_update.csv: ~10% price/status changes effective 2026-07-01 + one new product
    changed = products.sample(n=max(3, n_products // 10), random_state=gen.seed).copy()
    changed["list_price"] = (changed["list_price"] * 1.10).round(2)
    changed.loc[changed.index[0], "product_status"] = "discontinued"
    changed["updated_at"] = "2026-07-01T00:00:00Z"
    new_product = gen.products(1, datetime(2026, 7, 1), start_index=n_products + 1)
    update = pd.concat([changed, new_product], ignore_index=True)
    rows, plabels = gen.inject_product_defects(update[product_cols], 1, "products_update.csv", next_index=95000)
    for pid in changed["product_id"]:
        labels.append({"file": "products_update.csv", "record_id": pid, "defect": "catalog_update"})
    labels.append({"file": "products_update.csv", "record_id": new_product["product_id"].iloc[0], "defect": "new_product"})
    labels += plabels
    result.files["products_update"] = GeneratedFile(out_dir / "products_update.csv",
                                                    _write_csv(out_dir / "products_update.csv", product_cols, rows, "products_update.csv"))

    # --- orders_batch_2.csv
    n_new = max(20, len(orders) // 5)
    all_products = pd.concat([products, new_product], ignore_index=True)
    order_offset = int(orders["order_id"].str[1:].astype(int).max())
    fresh = gen.orders(n_new, all_products, n_customers, datetime(2026, 7, 1), datetime(2026, 8, 1), order_offset)
    late = gen.orders(max(10, n_new // 4), products, n_customers, datetime(2026, 1, 1), datetime(2026, 3, 1),
                      order_offset + 10 * n_new)
    for rid in late["order_line_id"]:
        labels.append({"file": "orders_batch_2.csv", "record_id": rid, "defect": "late_arriving_event"})
    # Status corrections: re-delivered lines of pending orders now shipped (upsert, not duplicate).
    pending = orders[orders["order_status"] == "pending"].head(5).copy()
    pending["order_status"] = "shipped"
    for rid in pending["order_line_id"]:
        labels.append({"file": "orders_batch_2.csv", "record_id": rid, "defect": "status_correction"})
    batch2 = pd.concat([fresh, late, pending], ignore_index=True)[order_cols]
    rows = batch2.astype(str).values.tolist()
    bad = list(rows[0])
    bad[order_cols.index("order_line_id")] = "OL99999999-1"
    bad[order_cols.index("product_id")] = "P88888"
    rows.append(bad)
    labels.append({"file": "orders_batch_2.csv", "record_id": "OL99999999-1", "defect": "invalid_product_reference"})
    result.files["orders_batch_2"] = GeneratedFile(out_dir / "orders_batch_2.csv",
                                                   _write_csv(out_dir / "orders_batch_2.csv", order_cols, rows, "orders_batch_2.csv"))
    return labels
