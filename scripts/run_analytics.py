"""Run every analytical and reconciliation query against the warehouse and save the outputs.

    python scripts/run_analytics.py                       # all queries -> outputs/analytics/*.csv + report
    python scripts/run_analytics.py --query 07_top_10_products
    python scripts/run_analytics.py --list
"""
from __future__ import annotations

import argparse
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.analytics.queries import list_queries, query_sql, reconciliation_passes, run_query  # noqa: E402
from src.config import PROJECT_ROOT, Settings  # noqa: E402
from src.warehouse.database import read_connection  # noqa: E402


def _md_table(df, max_rows: int = 15) -> str:
    if df.empty:
        return "_(no rows)_"
    shown = df.head(max_rows)
    lines = ["| " + " | ".join(map(str, shown.columns)) + " |", "|" + "---|" * len(shown.columns)]
    for row in shown.itertuples(index=False):
        lines.append("| " + " | ".join("" if v is None else str(v) for v in row) + " |")
    if len(df) > max_rows:
        lines.append(f"\n_{len(df) - max_rows} more row(s) in the CSV_")
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--db", type=Path, default=Settings().db_path)
    parser.add_argument("--out", type=Path, default=PROJECT_ROOT / "outputs")
    parser.add_argument("--query", help="Run a single analytics/reconciliation query and print it")
    parser.add_argument("--list", action="store_true", help="List available queries")
    args = parser.parse_args(argv)

    if args.list:
        for group in ("analytics", "reconciliation", "monitoring"):
            print(f"[{group}]", *list_queries(group), sep="\n  ")
        return 0
    if not Path(args.db).exists():
        print(f"Warehouse not found at {args.db}. Run the pipeline first: python -m src.pipeline", file=sys.stderr)
        return 1

    with read_connection(args.db) as con:
        if args.query:
            group = next((g for g in ("analytics", "reconciliation", "monitoring") if args.query in list_queries(g)), None)
            if group is None:
                print(f"Unknown query {args.query}; use --list", file=sys.stderr)
                return 1
            print(run_query(con, group, args.query).to_string(index=False))
            return 0

        out_dir = args.out / "analytics"
        out_dir.mkdir(parents=True, exist_ok=True)
        sections = [f"# DataFlow analytics report\n\nGenerated {datetime.now(timezone.utc):%Y-%m-%d %H:%M:%S} UTC "
                    f"from `{Path(args.db).name}` by `scripts/run_analytics.py`. Every number below is the output of "
                    f"the SQL file named in its heading.\n"]
        for group in ("analytics", "reconciliation"):
            sections.append(f"\n## {group.title()}\n")
            for name in list_queries(group):
                df = run_query(con, group, name)
                df.to_csv(out_dir / f"{group}__{name}.csv", index=False)
                sections.append(f"\n### `sql/{group}/{name}.sql`\n\n{_md_table(df)}\n")
                print(f"{group}/{name}: {len(df)} row(s)")
        sections.append("\n## Validation report\n")
        for name, title in (("rejection_categories", "Failures by category"),
                            ("rejection_reasons", "Failures by reason code"),
                            ("quarantine_current", "Rejected records (latest processing of each file)"),
                            ("runs", "Pipeline runs")):
            df = run_query(con, "monitoring", name)
            df.to_csv(out_dir / f"monitoring__{name}.csv", index=False)
            if name == "runs":
                df = df[["run_id", "execution_status", "input_rows", "valid_rows", "rejected_rows", "output_rows",
                         "duration_seconds", "error_message"]]
            if name == "quarantine_current":
                df = df[["source_file", "source_row_number", "record_id", "rejection_reasons"]]
            sections.append(f"\n### {title} (`sql/monitoring/{name}.sql`)\n\n{_md_table(df, max_rows=40)}\n")
            print(f"monitoring/{name}: {len(df)} row(s)")
        checks = reconciliation_passes(con)
        sections.append("\n## Reconciliation verdict\n\n" + "\n".join(
            f"- {'PASS' if ok else 'FAIL'} — {name}" for name, ok in checks.items()) + "\n")
    report = args.out / "analytics_report.md"
    report.write_text("".join(sections))
    print(f"Report: {report}")
    print("Reconciliation:", "ALL PASS" if all(checks.values()) else f"FAILURES {checks}")
    return 0 if all(checks.values()) else 1


if __name__ == "__main__":
    raise SystemExit(main())
