# Performance

This document summarises the committed benchmark and the optimisations it measured. The raw numbers are
in [`outputs/benchmark_report.md`](../outputs/benchmark_report.md) and
[`outputs/benchmark_results.json`](../outputs/benchmark_results.json).

## 1. How to reproduce

```bash
# macOS / Linux
python scripts/benchmark.py --sizes 1000 100000 --repeats 5
```

```powershell
# Windows PowerShell
python scripts/benchmark.py --sizes 1000 100000 --repeats 5
```

| Option | Default | Meaning |
|---|---|---|
| `--sizes N [N ...]` | `10000 50000 100000 250000` | Requested order lines per dataset |
| `--repeats N` | 3 | Runs per size; the median is reported |
| `--seed N` | 42 | Generator seed |
| `--opt-rows N` | 20000 | Row count for the row-by-row vs set-based load comparison |
| `--work DIR` | `data/benchmark` | Scratch directory for generated data and warehouses (git-ignored) |
| `--out DIR` | `outputs` | Where `benchmark_report.md` and `benchmark_results.json` are written |

**Warning:** with the default `--out`, the command **overwrites** the committed benchmark report. To keep
the committed evidence, pass a different directory, for example `--out /tmp/bench`.

For each size and repeat, the script generates a dataset, runs the full pipeline into a **fresh**
warehouse, and then runs it again with `--force`. Stage timings come from the pipeline's own
`pipeline_runs` instrumentation.

## 2. Recorded results (committed evidence)

Measured **2026-11-12 18:47 UTC** with `--sizes 1000 100000 --repeats 5` on an Apple M1 (8 logical
CPUs), macOS 26.6.2, Python 3.14.3, DuckDB 2.0.0, pandas 3.1.1. The 1-minute load average was 4.01 at the
start and 4.45 at the end. Each figure is the median of 5 runs.

| Metric | 1,000 requested lines | 100,000 requested lines |
|---|---:|---:|
| Physical input rows (orders + products) | 1,212 | 102,480 |
| Rejected rows | 47 | 3,850 |
| Data generation (not part of the pipeline) | 0.033 s | 0.833 s |
| Ingestion | 0.011 s | 0.458 s |
| Validation | 0.022 s | 0.629 s |
| Transformation | 0.015 s | 0.459 s |
| Load (single transaction) | 0.044 s | 0.367 s |
| **Total pipeline run** | **0.118 s** (min 0.112, max 0.367) | **2.056 s** (min 2.003, max 2.74) |
| Throughput | 10,271 rows/s | 49,844 rows/s |
| Forced rerun (`--force`) | 0.113 s | 2.303 s |

`total` is the run's `duration_seconds`. It also includes locking, DDL, raw-file copies, Parquet exports
and bookkeeping, so it is larger than the sum of the four stages. The benchmark run with 200 products
has different row counts from the idempotency check in `testing_and_validation.md`, which used 60
products. Forced reruns inserted 0 facts in every repeat.

**Spot check on 2026-11-14 (not a benchmark).** A single 100k pipeline run during the documentation audit
took 2.211 s, close to the committed median. A one-repeat smoke run of `benchmark.py` (1,000 lines) was
also run to confirm the script works. With one repeat on a busy machine its timings are not comparable
and are not reported here.

## 3. Optimisation that was measured: set-based loading

| Strategy | 20,000 rows | Notes |
|---|---:|---|
| Row-by-row `executemany` INSERT | 3.965 s | Rejected approach |
| Register the DataFrame with DuckDB (Arrow scan) + one `INSERT … SELECT` | 0.0045 s | Used for every table |

That is about **886× faster**, from a single measurement (2026-11-12). The pipeline uses the set-based
method for dimensions, facts and quarantine, and uses `UPDATE … FROM` for upserts. As a result, the
load stage is the smallest stage at 100k rows.

Other design choices that help performance:

* **Text-first ingestion with Python `csv`** (D1). This keeps exact source text for traceability at a
  measured cost of about 0.46 s per 100k rows.
* **Vectorised validation.** Every rule is a pandas boolean mask (`_Collector` in
  `src/validation/rules.py`), not a Python loop over rows.
* **Content hash ledger.** An unchanged file is skipped after hashing, so an identical re-delivery
  costs only the ingestion step (about 0.04 s for the sample, as recorded in `pipeline_runs`).

## 4. Bottlenecks and next steps

At 100k lines, the Python-side stages take most of the time: validation 0.63 s, transformation 0.46 s
(which includes MD5 surrogate-key hashing in a Python loop; its share of the stage was not profiled
separately) and ingestion 0.46 s. Possible next steps, **none
implemented**:

1. Compute surrogate keys in DuckDB SQL (`md5()`), not in a Python list comprehension.
2. Move the per-row rules into DuckDB SQL over a staged text table.
3. Store raw copies once per file hash, not once per run.
4. Partition the Parquet exports for larger volumes.

## 5. Limitations

* One machine, one seed and one defect rate (3 %). A different mix of rejections changes the validation time.
* Wall-clock timings on a shared laptop. Compare the min–max spread and the load average before drawing
  conclusions.
* At small sizes, fixed per-run costs dominate, so rows/s at 1,000 lines cannot be compared with rows/s
  at 100,000.
* Datasets larger than 100k were not benchmarked for the committed report.
