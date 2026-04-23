"""Refresh the committed universe seeds for npm and PyPI.

Run manually (or from a dedicated GitHub Action) every few weeks. Pass
``--ecosystem`` to refresh only one side. Pass ``--bootstrap`` to use a
free HTTP source for npm instead of hitting deps.dev BigQuery — useful for
the very first commit when BigQuery access isn't set up yet.

Examples::

    python scripts/refresh_universe.py --ecosystem pypi --limit 500
    python scripts/refresh_universe.py --ecosystem npm --limit 500
    python scripts/refresh_universe.py --ecosystem npm --bootstrap --limit 500

Logs to ``.cache/refresh_universe.log`` in append mode for easy tailing.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import re
import sys
from datetime import UTC, datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from pipeline.lib.http import HttpClient  # noqa: E402
from pipeline.lib.universe import (  # noqa: E402
    ECOSYSTEMS,
    UNIVERSE_SEED_DIR,
    UniversePackage,
    _serialise_packages,
    refresh_seed,
)

LOG_PATH = ROOT / ".cache" / "refresh_universe.log"
LOG_PATH.parent.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[
        logging.FileHandler(LOG_PATH, mode="a"),
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger("refresh_universe")

NPM_BOOTSTRAP_URL = (
    "https://raw.githubusercontent.com/wooorm/npm-high-impact/main/lib/top-download.js"
)
NPM_STRING_RE = re.compile(r"'([^']+)'")


async def _bootstrap_npm(limit: int) -> list[UniversePackage]:
    """Free fallback: pull the npm-high-impact download ranking.

    The npm-high-impact list is ordered by download count and does NOT
    expose a per-package dependent count, so bootstrap entries are written
    without ``dependent_count``. A subsequent non-bootstrap refresh (which
    hits deps.dev BigQuery) will fill in the real counts.
    """

    async with HttpClient(window="refresh-universe-bootstrap") as client:
        payload = await client.get_json(NPM_BOOTSTRAP_URL)
    if isinstance(payload, list):
        names = [str(x) for x in payload][:limit]
    else:
        text = str(payload)
        matches = NPM_STRING_RE.findall(text)
        if matches:
            names = matches[:limit]
        else:
            names = [ln.strip() for ln in text.splitlines() if ln.strip()][:limit]
    return [UniversePackage(name=n) for n in names]


def _write_bootstrap(
    ecosystem: str, packages: list[UniversePackage], source: str, seed_dir: Path
) -> None:
    refreshed_at = datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
    payload = {
        "ecosystem": ecosystem,
        "source": source,
        "refreshed_at": refreshed_at,
        "packages": _serialise_packages(packages),
    }
    seed_dir.mkdir(parents=True, exist_ok=True)
    path = seed_dir / f"{ecosystem}.json"
    tmp = path.with_suffix(".tmp")
    with tmp.open("w", encoding="utf-8") as fh:
        json.dump(payload, fh, indent=2, sort_keys=True)
        fh.write("\n")
    tmp.replace(path)
    log.info("wrote %s (%d packages, source=%s)", path, len(packages), source)


def _refresh(ecosystem: str, limit: int, *, bootstrap: bool) -> None:
    if bootstrap and ecosystem == "npm":
        log.info("bootstrapping npm via %s", NPM_BOOTSTRAP_URL)
        pkgs = asyncio.run(_bootstrap_npm(limit))
        _write_bootstrap("npm", pkgs, "wooorm/npm-high-impact (bootstrap)", UNIVERSE_SEED_DIR)
        return
    log.info("refreshing %s (limit=%d)", ecosystem, limit)
    result = refresh_seed(ecosystem, limit)
    log.info(
        "refreshed %s -> %d packages from %s (overlap=%s)",
        ecosystem,
        len(result.packages),
        result.source,
        result.overlap_with_previous,
    )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--ecosystem",
        choices=[*list(ECOSYSTEMS), "all"],
        default="all",
        help="which ecosystem to refresh (default: all)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=500,
        help="number of top packages to persist (default: 500)",
    )
    parser.add_argument(
        "--bootstrap",
        action="store_true",
        help="use a free HTTP fallback (npm-high-impact) instead of deps.dev BigQuery",
    )
    args = parser.parse_args()

    targets = ECOSYSTEMS if args.ecosystem == "all" else (args.ecosystem,)
    os.environ.setdefault("GCP_PROJECT_ID", "bus-factor-494119")
    for eco in targets:
        _refresh(eco, args.limit, bootstrap=args.bootstrap)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
