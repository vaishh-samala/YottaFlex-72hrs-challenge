"""Deterministic transformations: surrogate keys, derived attributes, staged dimension/fact frames.

Surrogate keys are derived from business keys with MD5 (first 60 bits -> positive BIGINT),
so the same input always yields the same key regardless of run order or DuckDB version.
"""
from __future__ import annotations

import hashlib
from dataclasses import dataclass

import pandas as pd

from src.validation.rules import product_row_hash


def surrogate_key(*parts: object) -> int:
    text = "|".join("" if p is None else str(p) for p in parts)
    return int(hashlib.md5(text.encode("utf-8")).hexdigest()[:15], 16)


def surrogate_keys(prefix: str, *columns: pd.Series) -> pd.Series:
    joined = columns[0].astype(str)
    for col in columns[1:]:
        joined = joined + "|" + col.astype(str)
    return pd.Series([surrogate_key(prefix, v) for v in joined], index=columns[0].index, dtype="int64")


def date_key(ts: pd.Series) -> pd.Series:
    return (ts.dt.year * 10000 + ts.dt.month * 100 + ts.dt.day).astype("int64")


@dataclass
class StagedBatch:
    products: pd.DataFrame   # candidate dim_product versions
    orders: pd.DataFrame     # fact candidates (product_key resolved later in SQL, as-of order time)
    customers: pd.DataFrame
    dates: pd.DataFrame


def _iso_utc(ts: pd.Series) -> pd.Series:
    return ts.dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def stage_products(valid: pd.DataFrame, run_id: str) -> pd.DataFrame:
    if valid.empty:
        return pd.DataFrame(columns=["product_key", "product_id", "product_name", "category", "brand", "list_price",
                                     "product_status", "valid_from", "row_hash", "source_file", "run_id"])
    out = pd.DataFrame({
        "product_key": surrogate_keys("product", valid["product_id"], _iso_utc(valid["updated_at"])),
        "product_id": valid["product_id"],
        "product_name": valid["product_name"],
        "category": valid["category"],
        "brand": valid["brand"],
        "list_price": valid["list_price"],
        "product_status": valid["product_status"],
        "valid_from": valid["updated_at"],
        "row_hash": product_row_hash(valid),
        "source_file": valid["source_file"],
        "run_id": run_id,
    })
    return out.reset_index(drop=True)


def stage_orders(valid: pd.DataFrame, run_id: str, source_seq: int = 0) -> pd.DataFrame:
    cols = ["sales_key", "order_line_id", "order_id", "customer_key", "customer_id", "product_id", "date_key",
            "order_timestamp", "quantity", "unit_price", "order_status", "row_hash", "source_file", "source_seq",
            "run_id"]
    if valid.empty:
        return pd.DataFrame(columns=cols)
    hash_cols = ["order_id", "customer_id", "product_id", "quantity", "unit_price", "order_status"]
    content = valid[hash_cols].astype(str)
    content["order_timestamp"] = _iso_utc(valid["order_timestamp"])
    out = pd.DataFrame({
        "sales_key": surrogate_keys("sale", valid["order_line_id"]),
        "order_line_id": valid["order_line_id"],
        "order_id": valid["order_id"],
        "customer_key": surrogate_keys("customer", valid["customer_id"]),
        "customer_id": valid["customer_id"],
        "product_id": valid["product_id"],
        "date_key": date_key(valid["order_timestamp"]),
        "order_timestamp": valid["order_timestamp"],
        "quantity": valid["quantity"].astype("int64"),
        "unit_price": valid["unit_price"],        # kept as exact decimal text, cast to DECIMAL in SQL
        "order_status": valid["order_status"],
        "row_hash": pd.util.hash_pandas_object(content, index=False).astype("uint64").astype(str),
        "source_file": valid["source_file"],
        "source_seq": source_seq,
        "run_id": run_id,
    })
    return out[cols].reset_index(drop=True)


def stage_customers(orders: pd.DataFrame, run_id: str) -> pd.DataFrame:
    if orders.empty:
        return pd.DataFrame(columns=["customer_key", "customer_id", "first_order_date", "run_id"])
    grouped = orders.groupby("customer_id", as_index=False).agg(
        customer_key=("customer_key", "first"), first_order_ts=("order_timestamp", "min"))
    grouped["first_order_date"] = grouped["first_order_ts"].dt.date
    grouped["run_id"] = run_id
    return grouped[["customer_key", "customer_id", "first_order_date", "run_id"]]


def stage_dates(orders: pd.DataFrame) -> pd.DataFrame:
    cols = ["date_key", "full_date", "day", "month", "month_name", "quarter", "year", "weekday", "is_weekend"]
    if orders.empty:
        return pd.DataFrame(columns=cols)
    days = pd.Series(pd.to_datetime(orders["order_timestamp"]).dt.normalize().unique())
    full = pd.Series(pd.date_range(days.min(), days.max(), freq="D"))  # contiguous calendar
    return pd.DataFrame({
        "date_key": date_key(full),
        "full_date": full.dt.date,
        "day": full.dt.day.astype("int32"),
        "month": full.dt.month.astype("int32"),
        "month_name": full.dt.month_name(),
        "quarter": full.dt.quarter.astype("int32"),
        "year": full.dt.year.astype("int32"),
        "weekday": full.dt.day_name(),
        "is_weekend": full.dt.dayofweek >= 5,
    })[cols]


def transform(valid_products: pd.DataFrame, valid_orders: pd.DataFrame, run_id: str,
              orders_seq: int = 0) -> StagedBatch:
    orders = stage_orders(valid_orders, run_id, orders_seq)
    return StagedBatch(
        products=stage_products(valid_products, run_id),
        orders=orders,
        customers=stage_customers(orders, run_id),
        dates=stage_dates(orders),
    )
