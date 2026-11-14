# Testing and Validation

This document describes how DataFlow is tested and lists the results that were actually produced.
Every number below comes from a run whose date and environment are stated next to it.

## 1. How to run the tests

```bash
# macOS / Linux, from the repository root, virtual environment active
python -m pytest            # full suite (pyproject.toml sets testpaths = tests, pythonpath = .)
python -m pytest -v         # one line per test
python -m pytest tests/test_warehouse.py -k idempot   # a subset
```

```powershell
# Windows PowerShell: the commands are the same
python -m pytest
python -m pytest -v
```

**Isolation.** Each test gets its own temporary warehouse, raw/processed/quarantine folders and log
folder (`tests/conftest.py`, `Settings.for_root(tmp_path)`). The test suite never reads or writes
`data/warehouse.duckdb`, `data/raw/`, `logs/` or `outputs/`.

## 2. Test inventory (102 test cases from 71 test functions)

| Module | Cases | What it covers |
|---|---:|---|
| `tests/test_ingestion.py` | 16 | Normal load; unique run ids and persisted history; header-only file; zero-byte file (empty, not a schema error); missing file is a batch failure; malformed rows quarantined, not fatal; missing required column fails the batch and can be retried; extra column treated as a schema-drift warning; header normalisation to snake_case (6 parametrised spellings incl. BOM); duplicate columns after normalisation |
| `tests/test_validation.py` | 41 | Every required field missing (7); missing primary key traced by line number; null tokens (`NULL`, `null`, `None`, `NaN`, `N/A`, whitespace); optional `brand`; exact vs conflicting duplicates; latest product version wins; invalid product reference (also when the product was rejected in the same batch, and resolved from an earlier batch); negative/zero/non-integer/out-of-range quantity; price type, zero, negative, >2 decimals, above max; invalid and out-of-range timestamps; status normalisation; several reasons on one record; full quarantine lineage; boundary values accepted |
| `tests/test_warehouse.py` | 24 | Late-arriving events; customer `first_order_date` moved back by a late order; SCD Type 2 new version with history kept; out-of-order catalog update re-points facts; conflicting re-delivery of a product version; **rollback after a failure injected just before commit, then a successful retry**; interrupted run marked FAILED on next start; identical rerun skipped; forced rerun gives no duplicates and no changes; same content under a new filename recognised; surrogate keys identical across separate warehouses; referential integrity; integrity violation blocks commit; revenue calculation; decimal precision; cancelled/returned excluded from revenue; whole-order rejection (within the batch and against the warehouse); timezone offsets normalised to the UTC date; status correction updates the fact in place; replaying an older file does not undo a newer correction; reject threshold fails the batch; **simultaneous execution prevented (process lock and threads)** |
| `tests/test_traceability.py` | 8 | Exact original CSV text kept for rejected rows (`\n` and `\r\n` line endings); quarantine Parquet carries `source_line`; blank lines counted and reported; an old warehouse without `source_line` is upgraded; `check_accounting` catches lost rows and duplicate keys; a duplicate that slips past the rules fails the batch instead of disappearing; every input row accounted for when duplicates are present |
| `tests/test_generator_and_analytics.py` | 8 | Generator is deterministic; defect labels never mixed into pipeline inputs; generator scales to 100k; sample end-to-end result matches the injected labels; every analytics query runs on real tables; rejected % and reconciliation with rejects; full reconciliation equation on a generated dataset; SKIPPED runs not counted as ingested |
| `tests/test_dashboard.py` | 5 | Each of the four dashboard pages renders from a warehouse (Streamlit `AppTest`); empty state shown when there is no warehouse |

## 3. Recorded results

### 3.1 Latest full run: 2026-11-14 (documentation audit)

* macOS (Apple Silicon), Python 3.14.3, pytest 9.2.0, duckdb 2.0.0, pandas 3.1.1
* Command: `python -m pytest -v -p no:cacheprovider`
* Result: **`102 passed in 18.26s`**: 0 failed, 0 skipped, 0 errors

### 3.2 Committed evidence: 2026-11-13

[`outputs/test_results.txt`](../outputs/test_results.txt) holds the full `-v` output from the previous
verification session: **`102 passed in 13.73s`** on the same machine and environment.

## 4. Reconciliation checks

### 4.1 What is checked

The SQL in `sql/reconciliation/` checks the following for every run and file, and for the warehouse
as a whole:

```text
input_rows    = valid_rows + rejected_rows                          (per run and per file)
rejected_rows = rows in quarantine for that run/file
rejected_rows = malformed + duplicates + missing_values + invalid_types + business_rule + invalid_references
valid order rows = facts_inserted + facts_updated + facts_unchanged + facts_stale_skipped
fact_rows = distinct order_line_id = Σ facts_inserted over successful runs
Σ total_amount = Σ quantity × unit_price;  no orphan product / customer / date keys
```

`scripts/run_analytics.py` runs every check and prints `Reconciliation: ALL PASS`, or lists the
failures and exits with code 1. The same checks also run inside the warehouse transaction
(`check_integrity()` in `src/warehouse/loader.py`), and `check_accounting()` in
`src/validation/rules.py` stops a batch from losing rows silently during validation.

### 4.2 Sample data (re-run 2026-11-14 in an isolated scratch directory)

Fresh warehouse: batch 1, then batch 2, then batch 1 again.

| Step | Status | Input | Valid | Rejected | Facts inserted / updated |
|---|---|---:|---:|---:|---|
| `orders.csv` + `products.csv` | SUCCESS | 667 | 642 | 25 | 582 / 0 |
| `orders_batch_2.csv` + `products_update.csv` | SUCCESS | 164 | 162 | 2 | 150 / 5 |
| `orders.csv` + `products.csv` again | SKIPPED | n/a | n/a | n/a | n/a |

Result: `Reconciliation: ALL PASS`. The rejection breakdown per file was identical to the committed
[`outputs/analytics/reconciliation__04_reject_breakdown.csv`](../outputs/analytics/reconciliation__04_reject_breakdown.csv)
(only the run ids differ):

| File | Input | Valid | Rejected | Malformed | Duplicates | Missing | Invalid types | Business rule | Invalid refs |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| orders.csv | 602 | 582 | 20 | 1 | 3 | 4 | 3 | 6 | 3 |
| orders_batch_2.csv | 156 | 155 | 1 | 0 | 0 | 0 | 0 | 0 | 1 |
| products.csv | 65 | 60 | 5 | 0 | 1 | 1 | 1 | 2 | 0 |
| products_update.csv | 8 | 7 | 1 | 0 | 0 | 1 | 0 | 0 | 0 |

### 4.3 Generated 100,000-line dataset (re-run 2026-11-14)

`python scripts/generate_data.py --orders 100000 --products 2000 --seed 42` produced 100,400 order rows
and 2,080 product rows. One pipeline run into a fresh warehouse gave:

| Source | Input | Valid | Rejected | Malformed | Duplicates | Missing | Invalid types | Business rule | Invalid refs |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| orders | 100,400 | 96,630 | 3,770 | 200 | 600 | 400 | 502 | 1,098 | 970 |
| products | 2,080 | 2,000 | 80 | 0 | 16 | 16 | 16 | 32 | 0 |
| **Total** | **102,480** | **98,630** | **3,850** | | | | | | |

Warehouse check: 96,630 fact rows = 96,630 distinct order lines = 96,630 inserted.
Σ `total_amount` = Σ `quantity × unit_price` = 43,968,852.14. Orphan keys: 0. `all_checks_pass = true`.

Why 100,400 rows: the generator adds 200 extra exact copies and 200 conflicting copies to the
100,000 requested lines. In the "duplicates" column, 600 = the 200 extra exact copies plus *both*
copies of each of the 200 conflicting pairs, because all copies of a conflicting key are rejected.
"Invalid refs" (970) includes 567 otherwise-clean lines rejected because their order named more than
one customer (see `docs/design_decisions.md` D14).

## 5. Idempotency (re-run 2026-11-14)

The same 1,000-line dataset (`--rows 1000`, 60 products, seed 42) was run three times against one
warehouse:

| Table | After run 1 | After run 2 (same files) | After run 3 (`--force`) |
|---|---:|---:|---:|
| fact_sales | 967 | 967 | 967 |
| dim_product | 60 | 60 | 60 |
| dim_customer | 114 | 114 | 114 |
| dim_date | 181 | 181 | 181 |
| pipeline_runs | 1 | 2 | 3 |
| processed_files | 2 | 2 | 2 |
| quarantine | 42 | 42 | 84 |

Run 2 status: `SKIPPED`. Run 3 status: `SUCCESS` with 0 inserted, 0 updated and 967 unchanged.
Quarantine grows on a forced run because it is append-only per run (design decision D9). The
dashboard and the analytics use `v_quarantine_current`, so forced runs are not counted twice.

## 6. Quarantine traceability

Each rejected record in `quarantine` (and in `data/quarantine/<run_id>/*.parquet`) has `run_id`,
`source_type`, `source_file`, `file_hash`, `source_row_number` (physical line, header = 1),
`record_id`, `primary_reason`, `rejection_reasons`, `raw_record` (parsed fields as JSON) and
`source_line` (the exact original CSV text). The untouched input file is also copied to
`data/raw/<run_id>/`. Tests: `test_quarantine_record_has_full_lineage`,
`test_rejected_records_keep_exact_original_text[...]`, `test_quarantine_parquet_carries_source_line`.

## 7. Dashboard verification

| Check | When | How | Result |
|---|---|---|---|
| All four pages render from a warehouse; empty state | 2026-11-14 | `tests/test_dashboard.py` (Streamlit `AppTest`) | 5 passed |
| Server starts and answers HTTP | 2026-11-14 | `streamlit run dashboard/app.py` on a scratch warehouse; `GET /_stcore/health` and `GET /` | `ok`, HTTP 200 |
| Browser check of pages, charts, run button and lock | 2026-11-13 | Playwright + Chrome, run from the previous Claude Code session (visible in its transcript) | Screenshots in [`docs/screenshots/`](screenshots/) |

The 2026-11-14 audit did not repeat the browser check.

## 8. What is not covered

* No line-coverage measurement (`pytest-cov` is not a dependency).
* All test runs listed above were on macOS. The Windows commands in the README have not been run on a
  Windows machine.
* Benchmarks and tests ran on one laptop. There is no CI pipeline.
