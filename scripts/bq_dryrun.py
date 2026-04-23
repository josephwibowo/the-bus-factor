"""Dry-run the *hot-path* BigQuery queries (free, 0 bytes billed).

Hot path = the queries the weekly pipeline issues.  Universe selection is
pre-computed via ``scripts/refresh_universe.py`` so it is intentionally NOT
measured here.
"""

from __future__ import annotations

import json
import os
import sys
import time
from datetime import UTC, datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

LOG_PATH = ROOT / ".cache" / "bq_dryrun.log"
LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
LOG_FILE = LOG_PATH.open("w", buffering=1)


def log(msg: str) -> None:
    stamp = datetime.now(UTC).strftime("%H:%M:%S")
    line = f"[{stamp}] {msg}"
    print(line, flush=True)
    LOG_FILE.write(line + "\n")
    LOG_FILE.flush()


def fmt_bytes(n: int) -> str:
    units = ["B", "KB", "MB", "GB", "TB"]
    v = float(n)
    for u in units:
        if v < 1024 or u == "TB":
            return f"{v:,.2f} {u}"
        v /= 1024
    return f"{n} B"


def _load_seed(ecosystem: str) -> list[str]:
    path = ROOT / "pipeline" / "data" / "universe" / f"{ecosystem}.json"
    payload = json.loads(path.read_text())
    return list(payload["packages"])


def main() -> int:
    os.environ.setdefault("GCP_PROJECT_ID", "bus-factor-494119")
    from pipeline.assets.raw.raw_deps_dev import _query as deps_dev_sql
    from pipeline.lib import bq

    # Hardcoded to avoid the expensive Snapshots lookup during preflight.
    snapshot = os.environ.get("DRYRUN_SNAPSHOT", "2026-04-14")
    log(f"snapshot = {snapshot}")

    npm = _load_seed("npm")
    pypi = _load_seed("pypi")
    log(f"npm seed size = {len(npm)}")
    log(f"pypi seed size = {len(pypi)}")

    # Sample at full seed size — this is what the pipeline actually runs.
    queries = [
        (f"deps_dev npm ({len(npm)} pkgs)", deps_dev_sql("NPM", npm, snapshot)),
        (f"deps_dev pypi ({len(pypi)} pkgs)", deps_dev_sql("PYPI", pypi, snapshot)),
    ]

    total = 0
    for label, sql in queries:
        log(f"dry-run: {label}")
        t0 = time.time()
        try:
            n = bq.dry_run_bytes(sql)
            total += n
            log(f"  -> {fmt_bytes(n)}  ({time.time() - t0:.1f}s)")
        except Exception as exc:
            log(f"  -> ERROR: {exc}")

    log(f"TOTAL weekly-run hot-path estimate: {fmt_bytes(total)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
