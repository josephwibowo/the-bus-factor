"""Thin wrapper around :mod:`google.cloud.bigquery` for raw assets.

Reasons to have this in its own module:
  * Every query must honour ``BQ_MAX_BYTES_BILLED`` so a pathological scan
    can't burn the monthly free-tier quota.
  * Credential resolution has three supported modes (ADC, service-account
    file, inline JSON env) and it's simpler to keep that logic in one place.
  * Tests can monkeypatch :func:`make_client` to a fake.
"""

from __future__ import annotations

import json
import logging
import os
import tempfile
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

DEFAULT_MAX_BYTES_BILLED = (
    100_000_000_000  # 100 GB (weekly budget ~400 GB/month, well within 1 TB free tier)
)


def max_bytes_billed() -> int:
    raw = os.environ.get("BQ_MAX_BYTES_BILLED")
    if raw:
        try:
            return int(raw)
        except ValueError:
            logger.warning("invalid BQ_MAX_BYTES_BILLED=%s; falling back to default", raw)
    return DEFAULT_MAX_BYTES_BILLED


def project_id() -> str | None:
    direct = (
        os.environ.get("GCP_PROJECT_ID")
        or os.environ.get("GOOGLE_CLOUD_PROJECT")
        or os.environ.get("BQ_PROJECT_ID")
    )
    if direct:
        return direct
    # Bruin injects the named connection JSON into an env var keyed by the
    # connection name (e.g. ``bigquery_default``).  Fall back to that so local
    # dev never has to export GCP_PROJECT_ID by hand.
    blob = os.environ.get("bigquery_default") or ""  # noqa: SIM112 - Bruin connection env var
    if not blob:
        return None
    try:
        cfg = json.loads(blob)
    except json.JSONDecodeError:
        return None
    pid = cfg.get("project_id") or cfg.get("project") if isinstance(cfg, dict) else None
    return pid if isinstance(pid, str) and pid else None


def _materialise_sa_json(json_payload: str) -> Path:
    """Persist an inline service-account JSON blob to a temp file and return the path."""

    tmp = Path(tempfile.gettempdir()) / "bus-factor-sa.json"
    tmp.write_text(json_payload)
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = str(tmp)
    return tmp


def make_client() -> Any:
    """Return a :class:`google.cloud.bigquery.Client` with the right creds."""

    from google.cloud import bigquery  # local import so fixture runs don't import

    sa_inline = os.environ.get("GCP_SERVICE_ACCOUNT_JSON")
    if sa_inline and sa_inline.strip().startswith("{"):
        try:
            json.loads(sa_inline)
            _materialise_sa_json(sa_inline)
        except json.JSONDecodeError:
            logger.warning("GCP_SERVICE_ACCOUNT_JSON looks non-JSON; ignoring")
    return bigquery.Client(project=project_id())


def dry_run_bytes(sql: str) -> int:
    """Return the BigQuery ``total_bytes_processed`` estimate without running.

    Dry-run queries are **free** and do not count against quota, so callers
    can cheaply sanity-check a query's scan size before paying for it.
    """

    from google.cloud import bigquery

    client = make_client()
    job_config = bigquery.QueryJobConfig(dry_run=True, use_query_cache=False)
    job = client.query(sql, job_config=job_config)
    return int(job.total_bytes_processed or 0)


def run_query(sql: str, *, job_description: str = "") -> list[dict[str, Any]]:
    """Run ``sql`` with the cost guardrail enforced and return row dicts."""

    from google.cloud import bigquery

    client = make_client()
    job_config = bigquery.QueryJobConfig(
        use_query_cache=True,
        maximum_bytes_billed=max_bytes_billed(),
    )
    if job_description:
        logger.info("BigQuery job: %s", job_description)
    job = client.query(sql, job_config=job_config)
    return [dict(row) for row in job.result()]


_LATEST_DEPS_DEV_SNAPSHOT: str | None = None


def latest_deps_dev_snapshot_date() -> str:
    """Return the most-recent snapshot date (``YYYY-MM-DD``) from deps.dev.

    The deps.dev ``*Latest`` relations are **views** that full-scan their base
    tables, so callers should pin the partition explicitly (the base tables are
    ``PARTITION BY DATE(SnapshotAt)``). We cache the result per-process because
    the snapshot cadence is daily.
    """

    global _LATEST_DEPS_DEV_SNAPSHOT
    if _LATEST_DEPS_DEV_SNAPSHOT is not None:
        return _LATEST_DEPS_DEV_SNAPSHOT
    rows = run_query(
        "SELECT FORMAT_DATE('%Y-%m-%d', DATE(MAX(Time))) AS d "
        "FROM `bigquery-public-data.deps_dev_v1.Snapshots`",
        job_description="deps.dev latest snapshot",
    )
    if not rows or not rows[0].get("d"):
        raise RuntimeError("could not determine deps.dev latest snapshot")
    _LATEST_DEPS_DEV_SNAPSHOT = str(rows[0]["d"])
    return _LATEST_DEPS_DEV_SNAPSHOT
