"""Reproducible performance benchmark.

For each size: generate data (fixed seed) -> run the full pipeline into a fresh warehouse ->
read the stage timings the pipeline itself recorded in pipeline_runs -> rerun with --force
to measure an idempotent re-load. Also measures one optimisation in isolation (set-based
Arrow load vs row-by-row INSERT).

    python scripts/benchmark.py                                  # 10k, 50k, 100k, 250k; 3 repeats each
    python scripts/benchmark.py --sizes 1000 100000 --repeats 5   # what README section 15 reports

Each repeat generates the same data (fixed seed) into a fresh warehouse; the report shows the median of
every timing and the min-max of the total, so background load on the machine is visible.
"""
from __future__ import annotations

import argparse
import json
import os
import platform
import shutil
import statistics
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import duckdb  # noqa: E402
import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402

from src.config import PROJECT_ROOT, Settings  # noqa: E402
from src.datagen import generate_dataset  # noqa: E402
from src.pipeline import run_pipeline  # noqa: E402

STAGES = ["ingestion_seconds", "validation_seconds", "transformation_seconds", "load_seconds"]


def bench_size(n: int, work: Path, seed: int) -> dict:
    data_dir, run_dir = work / f"data_{n}", work / f"run_{n}"
    shutil.rmtree(data_dir, ignore_errors=True)
    shutil.rmtree(run_dir, ignore_errors=True)
    t = time.perf_counter()
    gen = generate_dataset(data_dir, data_dir / "labels", n_orders=n, n_products=max(200, n // 50), seed=seed)
    gen_seconds = time.perf_counter() - t

    settings = Settings.for_root(run_dir)
    first = run_pipeline(settings, gen.files["orders"].path, gen.files["products"].path, trigger="benchmark",
                         console_log=False)
    if first.status != "SUCCESS":
        raise RuntimeError(f"benchmark run failed: {first.error_message}")
    con = duckdb.connect(str(settings.db_path), read_only=True)
    row = con.execute(f"SELECT duration_seconds, input_rows, valid_rows, rejected_rows, output_rows, "
                      f"{', '.join(STAGES)} FROM pipeline_runs WHERE run_id = ?", [first.run_id]).df().iloc[0]
    facts_before = con.execute("SELECT count(*) FROM fact_sales").fetchall()[0][0]
    con.close()

    rerun = run_pipeline(settings, gen.files["orders"].path, gen.files["products"].path, force=True,
                         trigger="benchmark", console_log=False)
    con = duckdb.connect(str(settings.db_path), read_only=True)
    facts_after = con.execute("SELECT count(*) FROM fact_sales").fetchall()[0][0]
    con.close()

    total = float(row.duration_seconds)
    result = {
        "order_rows": int(gen.files["orders"].rows), "product_rows": int(gen.files["products"].rows),
        "generation_seconds": round(gen_seconds, 3),
        "input_rows": int(row.input_rows), "valid_rows": int(row.valid_rows), "rejected_rows": int(row.rejected_rows),
        "warehouse_rows_written": int(row.output_rows),
        **{s: round(float(row[s]), 3) for s in STAGES},
        "total_seconds": round(total, 3),
        "rows_per_second": round(int(row.input_rows) / total) if total else None,
        "forced_rerun_seconds": round(rerun.duration_seconds, 3),
        "forced_rerun_facts_inserted": rerun.metrics.get("facts_inserted"),
        "forced_rerun_facts_unchanged": rerun.metrics.get("facts_unchanged"),
        "fact_rows_before_rerun": int(facts_before), "fact_rows_after_rerun": int(facts_after),
    }
    shutil.rmtree(data_dir, ignore_errors=True)
    shutil.rmtree(run_dir, ignore_errors=True)
    return result


TIMINGS = STAGES + ["generation_seconds", "total_seconds", "forced_rerun_seconds"]


def aggregate(runs: list[dict]) -> dict:
    """Median of every timing over the repeats; counts must be identical (deterministic data)."""
    counts = ["order_rows", "product_rows", "input_rows", "valid_rows", "rejected_rows", "warehouse_rows_written"]
    for r in runs[1:]:
        if any(r[c] != runs[0][c] for c in counts):
            raise RuntimeError("benchmark repeats produced different row counts")
    out = {c: runs[0][c] for c in counts}
    out.update({t: round(statistics.median(r[t] for r in runs), 3) for t in TIMINGS})
    totals = [r["total_seconds"] for r in runs]
    out.update({"repeats": len(runs), "total_min": min(totals), "total_max": max(totals),
                "rows_per_second": round(out["input_rows"] / out["total_seconds"]) if out["total_seconds"] else None,
                "forced_rerun_facts_inserted": max(r["forced_rerun_facts_inserted"] or 0 for r in runs),
                "facts_unchanged_after_rerun": all(r["fact_rows_before_rerun"] == r["fact_rows_after_rerun"]
                                                   for r in runs)})
    return out


def cpu_model() -> str:
    try:
        return subprocess.run(["sysctl", "-n", "machdep.cpu.brand_string"], capture_output=True, text=True,
                              timeout=5).stdout.strip() or platform.processor()
    except (OSError, subprocess.SubprocessError):
        return platform.processor() or platform.machine()


def bench_load_strategy(n: int) -> dict:
    """Optimisation evidence: row-by-row INSERT vs. set-based INSERT ... SELECT from an Arrow-registered frame."""
    rng = np.random.default_rng(0)
    frame = pd.DataFrame({
        "order_line_id": [f"OL{i}" for i in range(n)],
        "quantity": rng.integers(1, 6, n).astype("int64"),
        "unit_price": np.round(rng.uniform(1, 500, n), 2),
        "order_status": rng.choice(["paid", "shipped", "delivered"], n),
    })
    ddl = "CREATE TABLE t (order_line_id VARCHAR, quantity INTEGER, unit_price DECIMAL(12,2), order_status VARCHAR)"

    con = duckdb.connect()
    con.execute(ddl)
    t = time.perf_counter()
    con.execute("BEGIN")
    con.executemany("INSERT INTO t VALUES (?, ?, ?, ?)", frame.values.tolist())
    con.execute("COMMIT")
    row_by_row = time.perf_counter() - t
    assert con.execute("SELECT count(*) FROM t").fetchall()[0][0] == n
    con.close()

    con = duckdb.connect()
    con.execute(ddl)
    t = time.perf_counter()
    con.register("stg", frame)
    con.execute("INSERT INTO t SELECT * FROM stg")
    set_based = time.perf_counter() - t
    assert con.execute("SELECT count(*) FROM t").fetchall()[0][0] == n
    con.close()
    return {"rows": n, "row_by_row_seconds": round(row_by_row, 3), "set_based_seconds": round(set_based, 4),
            "speedup_x": round(row_by_row / set_based, 1)}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--sizes", type=int, nargs="+", default=[10_000, 50_000, 100_000, 250_000])
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--work", type=Path, default=PROJECT_ROOT / "data" / "benchmark")
    parser.add_argument("--opt-rows", type=int, default=20_000, help="Rows for the load-strategy comparison")
    parser.add_argument("--out", type=Path, default=PROJECT_ROOT / "outputs")
    parser.add_argument("--repeats", type=int, default=3, help="Runs per size; the median is reported (default 3)")
    args = parser.parse_args(argv)
    if args.repeats < 1:
        parser.error("--repeats must be >= 1")

    env = {"python": platform.python_version(), "duckdb": duckdb.__version__, "pandas": pd.__version__,
           "platform": platform.platform(), "cpu": cpu_model(), "cpu_count": os.cpu_count(),
           "run_at_utc": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
           "load_avg_1m_start": round(os.getloadavg()[0], 2), "repeats": args.repeats, "seed": args.seed}
    results = []
    for n in args.sizes:
        print(f"benchmarking {n:,} order lines x {args.repeats} ...", flush=True)
        r = aggregate([bench_size(n, args.work, args.seed) for _ in range(args.repeats)])
        r["requested_order_lines"] = n
        r["products_requested"] = max(200, n // 50)
        results.append(r)
        print(f"  median total {r['total_seconds']}s (min {r['total_min']}, max {r['total_max']})  "
              f"{r['rows_per_second']:,} rows/s  rerun {r['forced_rerun_seconds']}s")
    print(f"load-strategy comparison on {args.opt_rows:,} rows ...", flush=True)
    opt = bench_load_strategy(args.opt_rows)
    print(f"  row-by-row {opt['row_by_row_seconds']}s vs set-based {opt['set_based_seconds']}s ({opt['speedup_x']}x)")
    env["load_avg_1m_end"] = round(os.getloadavg()[0], 2)
    try:
        args.work.rmdir()  # only if empty: per-size folders are removed by bench_size; never delete user data
    except OSError:
        pass

    args.out.mkdir(parents=True, exist_ok=True)
    payload = {"environment": env, "pipeline": results, "optimisation_load_strategy": opt}
    (args.out / "benchmark_results.json").write_text(json.dumps(payload, indent=2))

    cols = ["requested_order_lines", "products_requested", "input_rows", "rejected_rows", "generation_seconds",
            "ingestion_seconds", "validation_seconds", "transformation_seconds", "load_seconds", "total_seconds",
            "total_min", "total_max", "rows_per_second", "forced_rerun_seconds"]
    lines = ["# DataFlow benchmark results", "",
             "## Environment", "",
             f"* Measured {env['run_at_utc']} UTC by `python scripts/benchmark.py --sizes "
             f"{' '.join(str(n) for n in args.sizes)} --repeats {args.repeats}`",
             f"* {env['platform']}, {env['cpu']}, {env['cpu_count']} logical CPUs",
             f"* Python {env['python']}, DuckDB {env['duckdb']}, pandas {env['pandas']}",
             f"* 1-minute load average: {env['load_avg_1m_start']} at start, {env['load_avg_1m_end']} at end", "",
             "## Method", "",
             f"* For every size and repeat: generate the dataset (seed {args.seed}; products = max(200, lines / 50)) "
             "into a fresh directory, run the full pipeline into a **fresh warehouse**, then run it again with "
             "`--force` on the same files.",
             "* Stage timings are read from `pipeline_runs` (the pipeline's own instrumentation). `total_seconds` is "
             "the run's `duration_seconds`: it also covers locking, schema creation, raw-file copies, Parquet exports "
             "and run bookkeeping, so it is larger than the sum of the four stages.",
             "* `rows_per_second` = input rows (orders + products, incl. rejected) / median total. "
             "`generation_seconds` is reported separately and is not part of the pipeline time.",
             f"* Every timing is the **median of {args.repeats} repeats**; `total_min`/`total_max` show the spread.", "",
             "## Results (seconds)", "",
             "| " + " | ".join(cols) + " |", "|" + "---:|" * len(cols)]
    for r in results:
        lines.append("| " + " | ".join(f"{r[c]:,}" if isinstance(r[c], int) else str(r[c]) for c in cols) + " |")
    lines += ["", "Idempotency under load: forced reruns inserted "
              f"{max(r['forced_rerun_facts_inserted'] for r in results)} facts and the fact-table row count was "
              + ("unchanged in every repeat." if all(r["facts_unchanged_after_rerun"] for r in results)
                 else "**NOT unchanged - investigate**."),
              "", "## Optimisation: set-based loading", "",
              f"Inserting {opt['rows']:,} rows one statement at a time (`executemany`) took "
              f"{opt['row_by_row_seconds']} s; registering the DataFrame with DuckDB (zero-copy Arrow scan) and "
              f"loading with a single `INSERT ... SELECT` took {opt['set_based_seconds']} s — "
              f"**{opt['speedup_x']}x faster** (single measurement). The pipeline uses the set-based path for every "
              "table (dimensions, facts, quarantine).",
              "", "## Limitations", "",
              "* Wall-clock timings on a shared laptop: other processes, thermal state and disk cache change "
              "results; compare the min-max spread and the load average before drawing conclusions.",
              "* Small sizes are dominated by fixed per-run costs (connection, DDL, Parquet snapshot), so their "
              "rows/s is not comparable with large sizes.",
              "* One machine, one seed, one defect rate (3 %); a different rejection mix changes validation time."]
    (args.out / "benchmark_report.md").write_text("\n".join(lines) + "\n")
    print(f"wrote {args.out / 'benchmark_report.md'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
