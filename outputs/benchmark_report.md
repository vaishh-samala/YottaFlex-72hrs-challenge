# DataFlow benchmark results

## Environment

* Measured 2026-11-12 18:47:20 UTC by `python scripts/benchmark.py --sizes 1000 100000 --repeats 5`
* macOS-26.6.2-arm64-arm-64bit-Mach-O, Apple M1, 8 logical CPUs
* Python 3.14.3, DuckDB 2.0.0, pandas 3.1.1
* 1-minute load average: 4.01 at start, 4.45 at end

## Method

* For every size and repeat: generate the dataset (seed 42; products = max(200, lines / 50)) into a fresh directory, run the full pipeline into a **fresh warehouse**, then run it again with `--force` on the same files.
* Stage timings are read from `pipeline_runs` (the pipeline's own instrumentation). `total_seconds` is the run's `duration_seconds`: it also covers locking, schema creation, raw-file copies, Parquet exports and run bookkeeping, so it is larger than the sum of the four stages.
* `rows_per_second` = input rows (orders + products, incl. rejected) / median total. `generation_seconds` is reported separately and is not part of the pipeline time.
* Every timing is the **median of 5 repeats**; `total_min`/`total_max` show the spread.

## Results (seconds)

| requested_order_lines | products_requested | input_rows | rejected_rows | generation_seconds | ingestion_seconds | validation_seconds | transformation_seconds | load_seconds | total_seconds | total_min | total_max | rows_per_second | forced_rerun_seconds |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1,000 | 200 | 1,212 | 47 | 0.033 | 0.011 | 0.022 | 0.015 | 0.044 | 0.118 | 0.112 | 0.367 | 10,271 | 0.113 |
| 100,000 | 2,000 | 102,480 | 3,850 | 0.833 | 0.458 | 0.629 | 0.459 | 0.367 | 2.056 | 2.003 | 2.74 | 49,844 | 2.303 |

Idempotency under load: forced reruns inserted 0 facts and the fact-table row count was unchanged in every repeat.

## Optimisation: set-based loading

Inserting 20,000 rows one statement at a time (`executemany`) took 3.965 s; registering the DataFrame with DuckDB (zero-copy Arrow scan) and loading with a single `INSERT ... SELECT` took 0.0045 s — **886.1x faster** (single measurement). The pipeline uses the set-based path for every table (dimensions, facts, quarantine).

## Limitations

* Wall-clock timings on a shared laptop: other processes, thermal state and disk cache change results; compare the min-max spread and the load average before drawing conclusions.
* Small sizes are dominated by fixed per-run costs (connection, DDL, Parquet snapshot), so their rows/s is not comparable with large sizes.
* One machine, one seed, one defect rate (3 %); a different rejection mix changes validation time.
