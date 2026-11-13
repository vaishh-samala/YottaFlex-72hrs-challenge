# Design Decisions

Each decision lists the choice, the reason, and what it costs.

## D1. Read everything as text, type later
Ingestion uses Python's `csv` module and keeps every value as a string. Typing happens in validation with
explicit rules. *Why:* a `pandas.read_csv` with dtypes either crashes on `"two"` in `quantity` or silently
coerces it to NaN — both lose the record. Text-first makes every bad value a traceable quarantine row.
*Cost:* a little more memory; measured ingestion is ~0.5 s per 100k rows.

## D2. Record-level vs batch-level failures
Record problems (bad values, duplicates, unknown products, wrong field count) quarantine the row and the
batch continues. Structural problems (missing file, missing mandatory column, undecodable file) or a
rejection rate above `max_reject_ratio` (50 %) fail the **whole batch** before anything is loaded.
*Why:* a file where most rows are bad is almost always a broken export, and partially loading it would
create misleading numbers. Extra columns are *drift*, not failure: ignored, kept in the raw copy, warned.

## D3. Duplicate rules
* Exact duplicate rows (same key, same content): keep the first physical occurrence, quarantine the rest.
* Same `order_line_id`, different content in one file: quarantine **all** copies — there is no basis for
  picking one, and guessing would corrupt revenue.
* Products: several versions of a product in one file is plausible for a master-data export, so the latest
  `updated_at` wins and older rows are quarantined as `SUPERSEDED_DUPLICATE`.
* An order whose lines name different customers is rejected as a whole (`INCONSISTENT_ORDER_CUSTOMER`) —
  see D14 for the rule, its impact and the alternatives considered.
* Validation never removes a row itself: every row is either valid or quarantined, and
  `check_accounting()` fails the batch if that split is broken or a duplicate key survives (see D15).

## D4. Revenue uses the transaction price; status decides recognition
`total_amount = quantity × unit_price` from the order line, computed in DuckDB with `DECIMAL(18,2)` (no
float drift — tested with 1,000 × 0.10 × 3). The catalog `list_price` lives on the product version and is
never written to the fact. Revenue counts `paid`, `shipped`, `delivered`; `pending` (not yet paid),
`cancelled` and `returned` lines stay in the fact table but are excluded from revenue and reported
separately. *Assumption:* there is no refund amount field, so a return removes the whole line value.

## D5. dim_product is Type 2; dim_customer is Type 1
Price, status, name and category change over time, and category revenue must reflect the category the
product had **when it was sold**. Each (`product_id`, `updated_at`) becomes a version with
`valid_from`/`valid_to`/`is_current`. Facts are linked with an `ASOF JOIN` to the version valid at
`order_timestamp`. Windows are recomputed with `LEAD()` whenever a product changes, so an update that arrives
out of order (back-dated) is slotted into history correctly and existing facts are re-pointed to the right
version. Orders older than a product's first catalog row map to its earliest version (the product existed;
the catalog row is just newer) instead of being rejected. A re-delivery of an existing version with different
attributes is quarantined (`CONFLICTING_PRODUCT_VERSION`) rather than silently rewriting history.
`dim_customer` has no descriptive attributes in the source, so Type 1 (only `first_order_date`, which
late orders can move earlier) is sufficient.

## D6. Deterministic surrogate keys
Keys are the first 60 bits of `md5(prefix | business key)` as positive BIGINT. The same input yields the same
keys in any warehouse, in any order, on any DuckDB version (DuckDB's own `hash()` is not guaranteed stable
across versions). `date_key` is the readable `YYYYMMDD`. *Cost:* theoretical collision risk at 2^60 —
negligible at this scale and caught by the primary-key constraints if it ever happened.

## D7. Idempotency in three layers
1. **File ledger** – `processed_files` keyed by SHA-256 of content: an identical file (even renamed) is
   skipped. Failed files are *not* marked done, so a retry just reruns.
2. **Upsert by business key** – facts are matched on `order_line_id`. Identical rows are left untouched
   (`row_hash` equal → not even `run_id` changes), changed rows are updated in place, new rows inserted.
   Dimensions are insert-if-absent by natural key. A `--force` rerun therefore produces zero inserts and
   zero updates (verified in tests and in the benchmark at every size).
3. **Arrival order guard** – every orders file gets a per-source `file_seq` the first time it is seen.
   A changed row only overwrites a fact if it comes from a file at least as recent as the one that wrote the
   current version. Replaying an old file during a backfill cannot undo a later correction
   (e.g. `pending → shipped`); such rows are counted as `facts_stale_skipped`.
   *Assumption:* files are delivered in business order. Out-of-order *delivery* of corrections would need a
   source-provided version/`updated_at` on orders, which the contract does not have.

## D8. One transaction per batch; integrity checked before commit
Dimensions, facts, quarantine rows and the ledger entry are written in a single DuckDB transaction.
Referential integrity (no fact without product/customer/date, one current version per product, no
overlapping windows, `total_amount = quantity × unit_price`, unique order lines) is checked *inside* the
transaction and violations abort the commit. A crash at any point leaves the previous state intact; the
test-suite injects a failure after all writes and before commit to prove it. DuckDB foreign-key constraints
are deliberately not declared because they make in-place updates of referenced rows awkward in DuckDB;
integrity is enforced by the explicit checks instead.

## D9. Quarantine is append-only per run
Each run records its own rejects, so a run's numbers always reconcile (`input = valid + rejected`,
`rejected = quarantine rows`). Views (`v_current_file_runs`, `v_quarantine_current`) select the latest
successful processing of each file so forced reruns are not double counted on the dashboard. Rejects of a
batch that failed the threshold are also stored (diagnostics), but never counted as "current".

## D10. Set-based loading (performance)
All loads register a pandas frame with DuckDB (Arrow scan) and run one `INSERT … SELECT` / `UPDATE … FROM`.
Measured (2026-11-12, Apple M1, `outputs/benchmark_report.md`): 20,000 rows took 3.965 s with row-by-row
`executemany` versus 0.0045 s set-based (886×, single measurement). At 100k order lines the load stage is the
smallest one (0.37 s median); the largest remaining cost is Python-side work in validation (0.63 s) and key
hashing — computing keys in SQL would be the next optimisation.

## D11. Timestamps
All accepted spellings are converted to UTC; naive timestamps are assumed to be UTC. `date_key` is the UTC
calendar date (an order at 02:00 +05:30 belongs to the previous UTC day — tested). Ambiguous formats such as
`31/12/2026` are rejected rather than guessed.

## D12. Dashboard is presentation only
The dashboard calls the same SQL files as the CLI report, uses short-lived read-only connections, and starts
the pipeline as a subprocess. Simultaneous runs are prevented by the pipeline's own lock (the button is also
disabled while a run holds the lock), so the guarantee holds regardless of how many browser tabs are open.

## D13. Scope choices
* Local files + DuckDB, no orchestrator: the run/ledger/lock design maps directly onto an Airflow/Dagster
  task if needed later.
* Raw input is preserved per run (a copy per run, including forced reruns). For very large daily volumes
  you would deduplicate raw storage by hash.
* `future-dated` tolerance is 24 h relative to the run time; the sample data is dated Jan–Aug 2026 and is
  valid when run after that.

## D14. Whole-order rejection for an order with more than one customer (kept unchanged, 2026-11-13)
**Rule.** An `order_id` belongs to exactly one `customer_id`. If the order lines of a batch, together with
the (order_id, customer_id) pairs already in `fact_sales`, associate an `order_id` with two or more customers,
**every line of that order in the current batch** is quarantined with `INCONSISTENT_ORDER_CUSTOMER`. Facts
already in the warehouse are not touched; a later line that clashes with a loaded order is rejected on its own.

**Why.** The data gives no basis for deciding which customer is right: the "odd" line may be the correct one
and the rest wrong (e.g. a guest checkout later merged into an account). Loading the majority would attribute
revenue, `first_order_date` and customer rankings to a guess. Order-level metrics (orders, AOV) also need the
whole order — loading part of it would understate order value. Quarantining the whole order is the
conservative choice: the numbers that are published are right, and everything held back is visible.

**Impact (measured).** In the 100k generated dataset, 200 injected lines with a foreign customer caused **770**
rejected rows: the 200 lines themselves (203 incl. overlaps with other defects) plus **567 otherwise-clean
sibling lines**, about 0.57 % of all order lines and 20 % of all rejected order rows. Every one of them is in
`quarantine` with its exact source line, so they can be released once the source system fixes the order.
All lines of the order share the same reason; the quarantine row does not mark which line was the outlier.

**Alternatives considered (not adopted).**
1. Reject only the minority-customer lines — picks a winner by vote; wrong when the minority is correct, and
   ambiguous with a 1:1 split.
2. Load all lines and flag the order — puts disputed revenue into published totals.
3. Make customer an attribute of the line, not the order — contradicts the source contract (`order_id`
   identifies one customer purchase).

Changing the rule is a business decision (finance/data owner). It is isolated in `validate_orders`
(`src/validation/rules.py`) and covered by `test_order_with_two_customers_rejected_as_a_whole` and
`test_order_customer_conflict_with_warehouse_detected`.

## D15. Traceability and row accounting
* Every rejected row stores `source_file`, `source_row_number` (first physical line of the record, header = 1),
  `run_id`, `primary_reason`, `rejection_reasons`, `raw_record` (parsed fields as JSON) and **`source_line` —
  the exact original CSV text** (quotes, whitespace and null tokens such as `NULL` unchanged; only the line
  terminator removed; multi-line quoted records keep their embedded newlines). The reader captures the text
  while `csv.reader` consumes it, so it is exact for well-formed and malformed rows alike.
* Warehouses created before `source_line` existed are upgraded in place (`ALTER TABLE … ADD COLUMN IF NOT
  EXISTS`); their older quarantine rows have `source_line = NULL` — the run's copy in `data/raw/<run_id>/`
  plus `source_row_number` still locates them.
* Blank lines carry no data; they are skipped but counted in a `BLANK_LINES` run warning.
* Validation used to end with a "safety" `drop_duplicates` that could silently delete a row if a rule ever
  regressed. It was replaced by `check_accounting()`: valid + rejected must equal the input rows and valid
  rows must be unique on the target grain, otherwise `ValidationInvariantError` fails the batch (rolled back,
  recorded as `FAILED`).

