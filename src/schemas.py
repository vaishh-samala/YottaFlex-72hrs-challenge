"""Source contracts: expected columns, nullability and allowed business values.

These contracts are the single source of truth used by ingestion (schema-drift detection)
and validation (record-level rules). Documented in docs/data_dictionary.md.
"""
from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class SourceContract:
    name: str
    primary_key: str
    columns: tuple[str, ...]          # all expected business columns (snake_case)
    required: tuple[str, ...]         # must be non-null for a record to be valid
    optional: tuple[str, ...] = ()    # may be absent from the file entirely

    @property
    def mandatory_columns(self) -> tuple[str, ...]:
        """Columns whose absence from the header is a batch-level failure."""
        return tuple(c for c in self.columns if c not in self.optional)


ORDER_STATUSES = frozenset({"pending", "paid", "shipped", "delivered", "cancelled", "returned"})
# Only these statuses count as recognised revenue (see docs/design_decisions.md).
REVENUE_STATUSES = ("paid", "shipped", "delivered")
PRODUCT_STATUSES = frozenset({"active", "discontinued", "out_of_stock"})

MAX_QUANTITY = 1000            # inclusive upper bound for a single order line
MAX_PRICE = 1_000_000.00       # inclusive upper bound for unit/list price
MIN_TIMESTAMP = "2000-01-01T00:00:00Z"
FUTURE_TOLERANCE_HOURS = 24    # events may not be stamped further than this into the future

ORDERS = SourceContract(
    name="orders",
    primary_key="order_line_id",
    columns=(
        "order_line_id", "order_id", "customer_id", "product_id", "order_timestamp",
        "quantity", "unit_price", "order_status", "source_file",
    ),
    required=(
        "order_line_id", "order_id", "customer_id", "product_id", "order_timestamp",
        "quantity", "unit_price", "order_status",
    ),
    optional=("source_file",),
)

PRODUCTS = SourceContract(
    name="products",
    primary_key="product_id",
    columns=(
        "product_id", "product_name", "category", "brand", "list_price",
        "product_status", "updated_at", "source_file",
    ),
    required=("product_id", "product_name", "category", "list_price", "product_status", "updated_at"),
    optional=("source_file",),
)

CONTRACTS = {"orders": ORDERS, "products": PRODUCTS}
