# Data Dictionary

All timestamps in the warehouse are **UTC** (`TIMESTAMP` without zone). Money is `DECIMAL`, never float.

## 1. Source contracts

Defined in `src/schemas.py`. Headers are normalised to `snake_case` before checking
(`Order Line ID`, `orderLineId`, `ORDER-LINE-ID` → `order_line_id`). Every value is read as text and typed
during validation, so a bad value becomes a quarantined record rather than a crashed run.

Null tokens: empty string, whitespace-only, `null`, `NULL`, `None`, `NaN`, `N/A` (case variants listed in
`src/ingestion/reader.py`).

### Orders (`orders*.csv`) — primary key `order_line_id`

| Column | Type after validation | Required | Rules |
|---|---|---|---|
| order_line_id | VARCHAR | yes | Unique within the batch. Exact duplicate rows → first kept, others `DUPLICATE_RECORD`. Same id with different content → **all** copies `CONFLICTING_DUPLICATE_KEY`. |
| order_id | VARCHAR | yes | All lines of one order must have the same `customer_id` (in the batch and vs. the warehouse) else `INCONSISTENT_ORDER_CUSTOMER` for every line of that order. |
| customer_id | VARCHAR | yes | |
| product_id | VARCHAR | yes | Must exist among valid products of this batch or in `dim_product` → else `INVALID_PRODUCT_REFERENCE`. |
| order_timestamp | TIMESTAMP (UTC) | yes | ISO-8601; `Z`, numeric offsets (`+05:30`) and space separator accepted; naive values are taken as UTC. Unparseable → `INVALID_TIMESTAMP`. Before 2000-01-01 or > 24 h in the future → `TIMESTAMP_OUT_OF_RANGE`. |
| quantity | INTEGER | yes | Integer text (`INVALID_DATA_TYPE` for `3.5`, `two`). `< 0` → `NEGATIVE_QUANTITY`; `= 0` → `ZERO_QUANTITY`; `> 1000` → `QUANTITY_OUT_OF_RANGE`. 1 and 1000 are valid boundaries. |
| unit_price | DECIMAL(12,2) | yes | Numeric text; `> 0` and `≤ 1,000,000` (`INVALID_PRICE`); at most 2 decimals (`INVALID_PRICE_PRECISION`). The **transaction price** — used for revenue. |
| order_status | VARCHAR | yes | Trimmed + lower-cased, then one of `pending, paid, shipped, delivered, cancelled, returned` (`INVALID_STATUS`). |
| source_file | VARCHAR | no | Optional in the file; always overwritten with the actual file name read (lineage). |

### Products (`products*.csv`) — primary key `product_id` (+ `updated_at` for history)

| Column | Type after validation | Required | Rules |
|---|---|---|---|
| product_id | VARCHAR | yes | Several rows for one id in a file: latest `updated_at` wins, older rows `SUPERSEDED_DUPLICATE`; same id + same `updated_at` + different content → all `CONFLICTING_DUPLICATE_KEY`; exact copies → `DUPLICATE_RECORD`. |
| product_name | VARCHAR | yes | |
| category | VARCHAR | yes | |
| brand | VARCHAR | **no** | Nullable. |
| list_price | DECIMAL(12,2) | yes | Same numeric rules as `unit_price`. Catalog price only — never used for revenue. |
| product_status | VARCHAR | yes | `active, discontinued, out_of_stock` (normalised). |
| updated_at | TIMESTAMP (UTC) | yes | Becomes the version's `valid_from`. Re-delivery of an already-loaded (`product_id`, `updated_at`) with different attributes → `CONFLICTING_PRODUCT_VERSION`. |
| source_file | VARCHAR | no | As for orders. |

## 2. Batch-level failures (whole run rejected, nothing loaded)

| Code | Trigger |
|---|---|
| `SOURCE_UNAVAILABLE` | Input path missing or not a file. |
| `SCHEMA_MISMATCH` | A mandatory column is missing, duplicate column names after normalisation, not UTF-8, unparseable CSV. |
| `REJECT_THRESHOLD_EXCEEDED` | More than `max_reject_ratio` (default 50 %) of a file's rows rejected — treated as a bad delivery. Rejected rows are still written to `quarantine` for diagnosis. |

Non-fatal, recorded in `pipeline_runs.warnings`: `EMPTY_FILE`, `SCHEMA_DRIFT` (extra columns ignored),
`COLUMNS_NORMALISED`, `SOURCE_FILE_OVERRIDDEN`, "already processed; skipped".

## 3. Warehouse (DuckDB, `sql/ddl/warehouse.sql`)

### fact_sales — grain: one row per order line

| Column | Type | Description |
|---|---|---|
| sales_key | BIGINT PK | `md5("sale|" + order_line_id)` → first 60 bits. Deterministic. |
| order_line_id | VARCHAR UNIQUE | Business key; upsert key. |
| order_id | VARCHAR | Degenerate dimension. |
| product_key | BIGINT | → `dim_product` version valid at `order_timestamp` (as-of join). |
| customer_key | BIGINT | → `dim_customer`. |
| customer_id | VARCHAR | Business key kept on the fact (spec). |
| date_key | INTEGER | → `dim_date` (`YYYYMMDD` of the UTC order date). |
| order_timestamp | TIMESTAMP | UTC. |
| quantity | INTEGER | |
| unit_price | DECIMAL(12,2) | Transaction price as delivered. |
| total_amount | DECIMAL(18,2) | `quantity × unit_price`, exact decimal arithmetic. |
| order_status | VARCHAR | Normalised status. |
| row_hash | VARCHAR | Hash of business attributes; detects changed re-deliveries. |
| source_file | VARCHAR | File that supplied the current version of the row. |
| source_seq | BIGINT | Arrival order of that file; an older file can never overwrite a newer version. |
| run_id | VARCHAR | Run that last inserted/changed the row. |
| first_run_id | VARCHAR | Run that first inserted it. |
| loaded_at | TIMESTAMP | |

### dim_product — Type 2 SCD (one row per product version)

| Column | Type | Description |
|---|---|---|
| product_key | BIGINT PK | `md5("product|" + product_id + "|" + valid_from)`. |
| product_id | VARCHAR | Business key. `UNIQUE(product_id, valid_from)`. |
| product_name, category, brand | VARCHAR | Attributes of this version. |
| list_price | DECIMAL(12,2) | Catalog price of this version. |
| product_status | VARCHAR | |
| valid_from | TIMESTAMP | Source `updated_at`. |
| valid_to | TIMESTAMP | Next version's `valid_from`; `NULL` for the current version. |
| is_current | BOOLEAN | Exactly one per product. |
| row_hash, source_file, created_run_id | | Lineage / change detection. |

### dim_customer — Type 1

| Column | Type | Description |
|---|---|---|
| customer_key | BIGINT PK | `md5("customer|" + customer_id)`. |
| customer_id | VARCHAR UNIQUE | |
| first_order_date | DATE | Earliest order seen; moved back by late-arriving orders. |
| created_run_id | VARCHAR | |

### dim_date

`date_key` (INTEGER PK, `YYYYMMDD`), `full_date`, `day`, `month`, `month_name`, `quarter`, `year`, `weekday`,
`is_weekend`. Populated as a contiguous calendar covering every order date; extended (never rebuilt) when
late or future-dated orders arrive.

### Views

| View | Purpose |
|---|---|
| `v_sales` | Fact joined to product version and date; adds `is_revenue` (`paid`, `shipped`, `delivered`). |
| `v_current_file_runs` | Latest successful processing of each source file (prevents double counting after `--force`). |
| `v_quarantine_current` | Quarantine rows belonging to those processings. |
| `v_quarantine_reasons` | One row per (rejected record, reason code). |

## 4. Operational tables

### pipeline_runs (one row per execution)

`run_id`, `start_time`, `end_time`, `duration_seconds`, `source_files` (JSON), `input_rows`, `valid_rows`,
`rejected_rows`, `output_rows` (facts inserted + updated), `execution_status`
(`RUNNING | SUCCESS | FAILED | SKIPPED`), `error_message` (`[batch|system:CODE] message`), `warnings` (JSON),
`trigger_source` (`cli | dashboard | test | benchmark`), `forced`, per-source input/valid/rejected counts,
`facts_inserted`, `facts_updated`, `facts_unchanged`, `facts_stale_skipped`, `product_versions_inserted`,
`customers_inserted`, and stage timings `ingestion_seconds`, `validation_seconds`, `transformation_seconds`,
`load_seconds`.

### run_files

Per run and source: `file_name`, `file_hash` (SHA-256), `action`
(`PROCESSED | SKIPPED_ALREADY_PROCESSED | FAILED`), `input_rows`, `valid_rows`, `rejected_rows`.

### processed_files (idempotency ledger)

Key (`file_hash`, `source_type`): `file_name`, `row_count`, `status` (`SUCCESS | FAILED`), `last_run_id`,
`first_seen_at`, `last_processed_at`, `attempts`, `last_error`, `file_seq` (per-source arrival order).

### quarantine

`quarantine_id` (deterministic), `run_id`, `source_type`, `source_file`, `file_hash`, `source_row_number`
(physical line in the file, header = 1), `record_id` (as delivered, may be null), `primary_reason`,
`rejection_reasons` (`"CODE: detail; CODE: detail"`), `raw_record` (JSON of the parsed field values after null-token
normalisation — `NULL`, `N/A`, … appear as `null` — or `{"raw_line": ...}` for malformed rows), `rejected_at`,
`source_line` (the exact original CSV text of the record, line terminator removed; `NULL` only for rows written
by warehouses that predate the column). `source_row_number` is the first physical line of the record. A Parquet copy per run is written to
`data/quarantine/<run_id>/`, and the untouched input file is kept in `data/raw/<run_id>/`.

## 5. Reason codes → dashboard categories

| Category | Codes |
|---|---|
| Missing values | `MISSING_REQUIRED_FIELD` |
| Duplicate records | `DUPLICATE_RECORD`, `CONFLICTING_DUPLICATE_KEY`, `SUPERSEDED_DUPLICATE`, `CONFLICTING_PRODUCT_VERSION` |
| Invalid references | `INVALID_PRODUCT_REFERENCE`, `INCONSISTENT_ORDER_CUSTOMER` |
| Invalid types / malformed | `INVALID_DATA_TYPE`, `INVALID_TIMESTAMP`, `MALFORMED_RECORD` |
| Business rule violations | `NEGATIVE_QUANTITY`, `ZERO_QUANTITY`, `QUANTITY_OUT_OF_RANGE`, `INVALID_PRICE`, `INVALID_PRICE_PRECISION`, `INVALID_STATUS`, `TIMESTAMP_OUT_OF_RANGE` |
