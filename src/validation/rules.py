"""Record-level validation rules for orders and products.

Each rule produces a boolean mask plus a reason code. A record can fail several rules;
all reasons are kept. Rows failing any rule go to quarantine, never silently dropped.

Reason codes (see docs/data_dictionary.md):
    MALFORMED_RECORD, MISSING_REQUIRED_FIELD, INVALID_DATA_TYPE, INVALID_TIMESTAMP,
    TIMESTAMP_OUT_OF_RANGE, NEGATIVE_QUANTITY, ZERO_QUANTITY, QUANTITY_OUT_OF_RANGE,
    INVALID_PRICE, INVALID_PRICE_PRECISION, INVALID_STATUS, DUPLICATE_RECORD,
    CONFLICTING_DUPLICATE_KEY, SUPERSEDED_DUPLICATE, INVALID_PRODUCT_REFERENCE,
    INCONSISTENT_ORDER_CUSTOMER
"""
from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

import numpy as np
import pandas as pd

from src.ingestion.reader import ROW_NUMBER_COL, IngestedSource
from src.schemas import (FUTURE_TOLERANCE_HOURS, MAX_PRICE, MAX_QUANTITY, MIN_TIMESTAMP, ORDER_STATUSES, ORDERS,
                         PRODUCT_STATUSES, PRODUCTS, SourceContract)

INT_PATTERN = r"[+-]?\d+"
NUM_PATTERN = r"[+-]?(?:\d+(?:\.\d*)?|\.\d+)"
PRICE_PATTERN = r"[+-]?\d+(?:\.\d{1,2})?"

# Reason code -> dashboard category
REASON_CATEGORIES = {
    "MALFORMED_RECORD": "Malformed record",
    "MISSING_REQUIRED_FIELD": "Missing values",
    "INVALID_DATA_TYPE": "Invalid data types",
    "INVALID_TIMESTAMP": "Invalid data types",
    "TIMESTAMP_OUT_OF_RANGE": "Business rule",
    "NEGATIVE_QUANTITY": "Business rule",
    "ZERO_QUANTITY": "Business rule",
    "QUANTITY_OUT_OF_RANGE": "Business rule",
    "INVALID_PRICE": "Business rule",
    "INVALID_PRICE_PRECISION": "Business rule",
    "INVALID_STATUS": "Business rule",
    "DUPLICATE_RECORD": "Duplicates",
    "CONFLICTING_DUPLICATE_KEY": "Duplicates",
    "SUPERSEDED_DUPLICATE": "Duplicates",
    "CONFLICTING_PRODUCT_VERSION": "Duplicates",
    "INVALID_PRODUCT_REFERENCE": "Invalid references",
    "INCONSISTENT_ORDER_CUSTOMER": "Invalid references",
}


@dataclass
class ValidationResult:
    valid: pd.DataFrame       # typed, clean rows ready for transformation
    rejected: pd.DataFrame    # one row per rejected source record (see QUARANTINE_COLUMNS)

    @property
    def valid_count(self) -> int:
        return len(self.valid)

    @property
    def rejected_count(self) -> int:
        return len(self.rejected)


QUARANTINE_COLUMNS = ["source_type", "source_file", "source_row_number", "record_id", "primary_reason",
                      "rejection_reasons", "raw_record", "source_line"]


class ValidationInvariantError(RuntimeError):
    """Validation produced an inconsistent split (a row lost, double counted or a duplicate key kept).

    Raised instead of silently repairing the data; the pipeline records the run as FAILED and rolls back.
    """


def check_accounting(frame: pd.DataFrame, good: pd.DataFrame, bad: np.ndarray, key: list[str]) -> None:
    """Every well-formed input row must end up exactly once in ``good`` or among the rejects, and the
    valid rows must be unique on the target grain ``key``."""
    n_bad = int(np.count_nonzero(bad))
    if len(good) + n_bad != len(frame):
        raise ValidationInvariantError(
            f"row accounting broken: {len(frame)} input rows != {len(good)} valid + {n_bad} rejected")
    dup = good.duplicated(subset=key, keep=False)
    if dup.any():
        sample = good.loc[dup, key + [ROW_NUMBER_COL]].head(5).to_dict("records")
        raise ValidationInvariantError(
            f"{int(dup.sum())} valid rows share a {key} key that should have been rejected as duplicates: {sample}")


class _Collector:
    """Accumulates (mask, code, detail) triples into per-row reason lists."""

    def __init__(self, n: int):
        self.n = n
        self.masks: list[np.ndarray] = []
        self.labels: list[str] = []

    def add(self, mask: pd.Series | np.ndarray, code: str, detail: str) -> None:
        arr = np.asarray(mask, dtype=bool)
        if arr.any():
            self.masks.append(arr)
            self.labels.append(f"{code}: {detail}")

    def reasons(self) -> list[list[str]]:
        out: list[list[str]] = [[] for _ in range(self.n)]
        for arr, label in zip(self.masks, self.labels):
            for i in np.flatnonzero(arr):
                out[i].append(label)
        return out

    def any_mask(self) -> np.ndarray:
        if not self.masks:
            return np.zeros(self.n, dtype=bool)
        return np.logical_or.reduce(self.masks)


def _missing(frame: pd.DataFrame, contract: SourceContract, rc: _Collector) -> None:
    for column in contract.required:
        rc.add(frame[column].isna(), "MISSING_REQUIRED_FIELD", column)


def _parse_ts(series: pd.Series) -> pd.Series:
    """Parse ISO-8601 (any offset / 'Z' / space separator) to UTC; invalid -> NaT."""
    return pd.to_datetime(series, format="ISO8601", utc=True, errors="coerce")


def _timestamp_rules(frame: pd.DataFrame, column: str, rc: _Collector, run_time: datetime) -> pd.Series:
    present = frame[column].notna()
    parsed = _parse_ts(frame[column])
    rc.add(present & parsed.isna(), "INVALID_TIMESTAMP", f"{column} is not a valid ISO-8601 timestamp")
    upper = pd.Timestamp(run_time) + pd.Timedelta(hours=FUTURE_TOLERANCE_HOURS)
    out_of_range = parsed.notna() & ((parsed < pd.Timestamp(MIN_TIMESTAMP)) | (parsed > upper))
    rc.add(out_of_range, "TIMESTAMP_OUT_OF_RANGE", f"{column} before {MIN_TIMESTAMP[:10]} or in the future")
    return parsed


def _price_rules(frame: pd.DataFrame, column: str, rc: _Collector) -> pd.Series:
    raw = frame[column]
    present = raw.notna()
    is_number = raw.str.fullmatch(NUM_PATTERN).fillna(False).astype(bool)
    rc.add(present & ~is_number, "INVALID_DATA_TYPE", f"{column} is not numeric")
    value = pd.to_numeric(raw.where(is_number), errors="coerce")
    rc.add(is_number & ((value <= 0) | (value > MAX_PRICE)), "INVALID_PRICE",
           f"{column} must be > 0 and <= {MAX_PRICE:,.0f}")
    precise = raw.str.fullmatch(PRICE_PATTERN).fillna(False).astype(bool)
    rc.add(is_number & (value > 0) & ~precise, "INVALID_PRICE_PRECISION", f"{column} has more than 2 decimals")
    return value


def _raw_records(frame: pd.DataFrame, columns: tuple[str, ...]) -> list[str]:
    records = frame[list(columns)].astype(object).where(frame[list(columns)].notna(), None)
    return [json.dumps(dict(zip(columns, row)), default=str) for row in records.itertuples(index=False, name=None)]


def _build_rejects(frame: pd.DataFrame, reasons: list[list[str]], mask: np.ndarray, contract: SourceContract,
                   source: IngestedSource) -> pd.DataFrame:
    idx = np.flatnonzero(mask)
    subset = frame.iloc[idx]
    rejected = pd.DataFrame({
        "source_type": contract.name,
        "source_file": source.file_name,
        "source_row_number": subset[ROW_NUMBER_COL].astype("int64").to_numpy(),
        "record_id": subset[contract.primary_key].astype(object).to_numpy(),
        "primary_reason": [reasons[i][0].split(":")[0] for i in idx],
        "rejection_reasons": ["; ".join(reasons[i]) for i in idx],
        "raw_record": _raw_records(subset, contract.columns),
        "source_line": [source.source_lines[i] for i in idx],
    })
    malformed = pd.DataFrame({
        "source_type": contract.name,
        "source_file": source.file_name,
        "source_row_number": [m.row_number for m in source.malformed],
        "record_id": [m.record_id for m in source.malformed],
        "primary_reason": "MALFORMED_RECORD",
        "rejection_reasons": [f"MALFORMED_RECORD: {m.reason}" for m in source.malformed],
        "raw_record": [json.dumps({"raw_line": m.raw_line}) for m in source.malformed],
        "source_line": [m.raw_line for m in source.malformed],
    }, columns=QUARANTINE_COLUMNS)
    out = pd.concat([df for df in (rejected, malformed) if len(df)], ignore_index=True) if (len(rejected) or len(malformed)) \
        else pd.DataFrame(columns=QUARANTINE_COLUMNS)
    return out.sort_values("source_row_number", kind="stable").reset_index(drop=True)[QUARANTINE_COLUMNS]


def _business_fingerprint(frame: pd.DataFrame, columns: list[str]) -> pd.Series:
    return pd.util.hash_pandas_object(frame[columns].astype(str), index=False)


# ---------------------------------------------------------------------------------- products
def validate_products(source: IngestedSource, existing_versions: pd.DataFrame | None = None,
                      run_time: datetime | None = None) -> ValidationResult:
    """Validate the product master file.

    ``existing_versions``: (product_id, valid_from, row_hash) already in dim_product, used
    to detect a conflicting re-delivery of the same product version.
    """
    run_time = run_time or datetime.now(timezone.utc)
    frame = source.data
    rc = _Collector(len(frame))
    _missing(frame, PRODUCTS, rc)
    price = _price_rules(frame, "list_price", rc)
    status = frame["product_status"].str.strip().str.lower()
    rc.add(status.notna() & ~status.isin(PRODUCT_STATUSES), "INVALID_STATUS",
           f"product_status not in {sorted(PRODUCT_STATUSES)}")
    updated = _timestamp_rules(frame, "updated_at", rc, run_time)

    # Duplicates within the batch: exact copies -> keep first; different content with a
    # later updated_at wins; same key + same updated_at + different content -> reject all.
    pid = frame["product_id"]
    content_cols = ["product_id", "product_name", "category", "brand", "list_price", "product_status", "updated_at"]
    fp = _business_fingerprint(frame, content_cols)
    keyed = pid.notna()
    exact_dup = keyed & pd.DataFrame({"p": pid, "f": fp}).duplicated(keep="first")
    rc.add(exact_dup, "DUPLICATE_RECORD", "exact duplicate of an earlier row (first kept)")
    candidates = keyed & ~exact_dup
    work = pd.DataFrame({"pid": pid, "ts": updated, "fp": fp})[candidates]
    if work["pid"].duplicated(keep=False).any():
        max_ts = work.groupby("pid")["ts"].transform("max")
        superseded = work["ts"] < max_ts
        rc.add(superseded.reindex(frame.index, fill_value=False), "SUPERSEDED_DUPLICATE",
               "product_id repeated in file with an older updated_at")
        latest = work[~superseded]
        n_variants = latest.groupby("pid")["fp"].transform("nunique")
        rc.add((n_variants > 1).reindex(frame.index, fill_value=False), "CONFLICTING_DUPLICATE_KEY",
               "product_id repeated with same updated_at but different attributes")

    if existing_versions is not None and len(existing_versions) and len(frame):
        probe = pd.DataFrame({"product_id": pid, "valid_from": updated.dt.tz_convert(None), "row_hash": product_row_hash(frame)})
        merged = probe.merge(existing_versions, on=["product_id", "valid_from"], how="left", suffixes=("", "_old"))
        conflict = merged["row_hash_old"].notna() & (merged["row_hash_old"] != merged["row_hash"])
        rc.add(conflict.to_numpy(), "CONFLICTING_PRODUCT_VERSION",
               "same product_id and updated_at already loaded with different attributes")

    reasons = rc.reasons()
    bad = rc.any_mask()
    good = frame[~bad].copy()
    good["list_price"] = good["list_price"].astype(str).str.lstrip("+")
    good["product_status"] = status[~bad]
    good["updated_at"] = updated[~bad].dt.tz_convert(None)
    check_accounting(frame, good, bad, ["product_id", "updated_at"])
    return ValidationResult(valid=good.reset_index(drop=True), rejected=_build_rejects(frame, reasons, bad, PRODUCTS, source))


def product_row_hash(frame: pd.DataFrame) -> pd.Series:
    """Stable hash of a product version's descriptive attributes (Type 2 change detection)."""
    cols = ["product_name", "category", "brand", "list_price", "product_status"]
    norm = frame[cols].astype(object).where(frame[cols].notna(), "").astype(str)
    norm["product_status"] = norm["product_status"].str.strip().str.lower()
    norm["list_price"] = pd.to_numeric(norm["list_price"], errors="coerce").round(2).astype(str)
    return pd.util.hash_pandas_object(norm, index=False).astype("uint64").astype(str)


# ------------------------------------------------------------------------------------ orders
def validate_orders(source: IngestedSource, known_product_ids: set[str],
                    existing_order_customers: pd.DataFrame | None = None,
                    run_time: datetime | None = None) -> ValidationResult:
    """Validate order lines.

    ``known_product_ids``: valid products from this batch plus those already in dim_product.
    ``existing_order_customers``: (order_id, customer_id) pairs already in the warehouse.
    """
    run_time = run_time or datetime.now(timezone.utc)
    frame = source.data
    rc = _Collector(len(frame))
    _missing(frame, ORDERS, rc)

    qty_raw = frame["quantity"]
    qty_is_int = qty_raw.str.fullmatch(INT_PATTERN).fillna(False).astype(bool)
    rc.add(qty_raw.notna() & ~qty_is_int, "INVALID_DATA_TYPE", "quantity is not an integer")
    qty = pd.to_numeric(qty_raw.where(qty_is_int), errors="coerce")
    rc.add(qty < 0, "NEGATIVE_QUANTITY", "quantity < 0")
    rc.add(qty == 0, "ZERO_QUANTITY", "quantity = 0")
    rc.add(qty > MAX_QUANTITY, "QUANTITY_OUT_OF_RANGE", f"quantity > {MAX_QUANTITY}")

    _price_rules(frame, "unit_price", rc)
    ts = _timestamp_rules(frame, "order_timestamp", rc, run_time)

    status = frame["order_status"].str.strip().str.lower()
    rc.add(status.notna() & ~status.isin(ORDER_STATUSES), "INVALID_STATUS",
           f"order_status not in {sorted(ORDER_STATUSES)}")

    pid = frame["product_id"]
    rc.add(pid.notna() & ~pid.isin(known_product_ids), "INVALID_PRODUCT_REFERENCE",
           "product_id not found in product master")

    # Duplicate order_line_id within the batch.
    line_id = frame["order_line_id"]
    content_cols = [c for c in ORDERS.columns if c != "source_file"]
    fp = _business_fingerprint(frame, content_cols)
    keyed = line_id.notna()
    exact_dup = keyed & pd.DataFrame({"k": line_id, "f": fp}).duplicated(keep="first")
    rc.add(exact_dup, "DUPLICATE_RECORD", "exact duplicate of an earlier row (first kept)")
    distinct = pd.DataFrame({"k": line_id, "f": fp})[keyed & ~exact_dup]
    conflicting = distinct["k"].duplicated(keep=False).reindex(frame.index, fill_value=False)
    rc.add(conflicting, "CONFLICTING_DUPLICATE_KEY", "order_line_id repeated with different content (all copies rejected)")

    # One order belongs to exactly one customer (within batch and vs. warehouse).
    oid, cid = frame["order_id"], frame["customer_id"]
    pairs = pd.DataFrame({"order_id": oid, "customer_id": cid}).dropna()
    if existing_order_customers is not None and len(existing_order_customers):
        pairs = pd.concat([pairs, existing_order_customers[existing_order_customers["order_id"].isin(pairs["order_id"])]])
    n_cust = pairs.drop_duplicates().groupby("order_id")["customer_id"].nunique()
    bad_orders = set(n_cust[n_cust > 1].index)
    rc.add(oid.isin(bad_orders), "INCONSISTENT_ORDER_CUSTOMER", "order_id is associated with more than one customer_id")

    reasons = rc.reasons()
    bad = rc.any_mask()
    good = frame[~bad].copy()
    good["quantity"] = qty[~bad].astype("int64")
    good["unit_price"] = good["unit_price"].astype(str).str.lstrip("+")
    good["order_status"] = status[~bad]
    good["order_timestamp"] = ts[~bad].dt.tz_convert(None)
    check_accounting(frame, good, bad, ["order_line_id"])  # never drop rows silently
    return ValidationResult(valid=good.reset_index(drop=True), rejected=_build_rejects(frame, reasons, bad, ORDERS, source))
