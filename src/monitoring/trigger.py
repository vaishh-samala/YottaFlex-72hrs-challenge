"""Launch the pipeline in a separate process (used by the dashboard's "Run pipeline" button).

Running out-of-process means the dashboard never holds a DuckDB write connection, and the
pipeline's own lock file guarantees only one execution at a time.
"""
from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

from src.config import PROJECT_ROOT, Settings
from src.monitoring.lock import read_lock


def is_running(settings: Settings | None = None) -> dict | None:
    """Metadata of the live run holding the lock, or None."""
    return read_lock((settings or Settings()).lock_path)


def trigger_pipeline(orders: Path, products: Path, force: bool = False, db_path: Path | None = None,
                     timeout: int = 1800) -> dict:
    cmd = [sys.executable, "-m", "src.pipeline", "--orders", str(orders), "--products", str(products),
           "--trigger", "dashboard"]
    if force:
        cmd.append("--force")
    if db_path:
        cmd += ["--db", str(db_path)]
    proc = subprocess.run(cmd, cwd=PROJECT_ROOT, capture_output=True, text=True, timeout=timeout)
    stdout = proc.stdout.strip()
    try:
        start = stdout.index("{")
        payload = json.loads(stdout[start:])
    except ValueError:
        payload = {"status": "ERROR", "error_message": (proc.stderr or stdout)[-2000:]}
    payload["exit_code"] = proc.returncode
    payload["log_tail"] = proc.stderr.strip().splitlines()[-15:]
    return payload
