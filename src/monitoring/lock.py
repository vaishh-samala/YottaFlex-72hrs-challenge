"""Single-writer pipeline lock (prevents simultaneous executions, e.g. double clicks)."""
from __future__ import annotations

import json
import os
import time
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterator


class PipelineLockedError(RuntimeError):
    """Raised when another pipeline execution currently holds the lock."""


def _pid_alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def read_lock(lock_path: Path) -> dict | None:
    """Return lock metadata if a *live* lock exists, else None (stale locks are ignored)."""
    try:
        info = json.loads(Path(lock_path).read_text())
    except (FileNotFoundError, json.JSONDecodeError, OSError):
        return None
    pid = int(info.get("pid", -1))
    if pid > 0 and _pid_alive(pid):
        return info
    return None


@contextmanager
def pipeline_lock(lock_path: Path, run_id: str) -> Iterator[None]:
    lock_path = Path(lock_path)
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    payload = json.dumps({"pid": os.getpid(), "run_id": run_id, "acquired_at": datetime.now(timezone.utc).isoformat()})
    for attempt in range(2):
        try:
            fd = os.open(lock_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
            break
        except FileExistsError:
            holder = read_lock(lock_path)
            if holder is not None:
                raise PipelineLockedError(
                    f"Pipeline already running (run_id={holder.get('run_id')}, pid={holder.get('pid')})"
                )
            # Stale lock left by a crashed process: remove and retry once.
            try:
                lock_path.unlink()
            except FileNotFoundError:
                pass
            time.sleep(0.05)
    else:  # pragma: no cover - extremely unlikely race
        raise PipelineLockedError("Could not acquire pipeline lock")
    try:
        os.write(fd, payload.encode())
        os.close(fd)
        yield
    finally:
        try:
            lock_path.unlink()
        except FileNotFoundError:
            pass
