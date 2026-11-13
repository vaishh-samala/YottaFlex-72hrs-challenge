"""Generate DataFlow synthetic input data.

Examples
--------
    python scripts/generate_data.py --sample                        # data/sample (+ second batch)
    python scripts/generate_data.py --orders 100000 --products 2000 --out data/generated/100k
    python scripts/generate_data.py --rows 1000                     # --rows is an alias for --orders
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.config import PROJECT_ROOT  # noqa: E402
from src.datagen import generate_dataset  # noqa: E402


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--sample", action="store_true", help="Write the small demo dataset to data/sample")
    parser.add_argument("--orders", "--rows", dest="orders", type=int, default=600,
                        help="Number of order lines to generate (default 600). --rows is an alias.")
    parser.add_argument("--products", type=int, default=60, help="Number of products (default 60)")
    parser.add_argument("--seed", type=int, default=42, help="Random seed (default 42)")
    parser.add_argument("--defect-rate", type=float, default=0.03, help="Share of order lines with injected defects")
    parser.add_argument("--out", type=Path, default=None, help="Output directory for input CSVs")
    parser.add_argument("--labels", type=Path, default=None, help="Directory for expected-defect labels")
    parser.add_argument("--with-increment", action="store_true", help="Also write products_update.csv + orders_batch_2.csv")
    args = parser.parse_args(argv)
    if args.orders < 1 or args.products < 1:
        parser.error("--orders/--rows and --products must be positive integers")

    if args.sample:
        out = args.out or PROJECT_ROOT / "data" / "sample"
        labels = args.labels or PROJECT_ROOT / "data" / "labels" / "sample"
        with_increment = True
    else:
        out = args.out or PROJECT_ROOT / "data" / "generated" / f"{args.orders}"
        labels = args.labels or PROJECT_ROOT / "data" / "labels" / out.name
        with_increment = args.with_increment

    result = generate_dataset(out, labels, n_orders=args.orders, n_products=args.products, seed=args.seed,
                              defect_rate=args.defect_rate, with_increment=with_increment)
    for name, info in result.files.items():
        print(f"{name:<16} {info.rows:>9,} rows -> {info.path}")
    print(f"{'labels':<16} {len(result.labels):>9,} rows -> {result.labels_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
