"""
anomaly/store.py — Anomaly Parquet store

Persists detected anomalies to data/anomalies/anomalies.parquet so the API
can serve a stable incident feed (with stable IDs for /diagnose/:id) across
detection runs and restarts. Deduplicates by anomaly_id: the detector emits
deterministic per-(type, hour) IDs, so re-running detection is idempotent.

Anomalies come from exactly two real sources: the rule-based detector
(detector.py) and the manual live-CI snapshot (evidence.build_snapshot_anomaly).
There is deliberately no synthetic/demo seeding here — the incident feed only
ever shows numbers that came from real data.
"""

import json
import os
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import structlog
from dotenv import load_dotenv

load_dotenv()

log = structlog.get_logger(__name__)

ANOMALY_DIR: Path = Path(os.getenv("ANOMALY_DIR", "data/anomalies"))
ANOMALIES_FILENAME: str = "anomalies.parquet"

ANOMALY_SCHEMA = pa.schema(
    [
        pa.field("anomaly_id",  pa.string(),                  nullable=False),
        pa.field("type",        pa.string(),                  nullable=False),
        pa.field("title",       pa.string(),                  nullable=False),
        pa.field("severity",    pa.string(),                  nullable=False),
        pa.field("description", pa.string(),                  nullable=True),
        pa.field("metric_json", pa.string(),                  nullable=True),
        pa.field("repo",        pa.string(),                  nullable=True),
        pa.field("detected_at", pa.timestamp("us", tz="UTC"), nullable=False),
        pa.field("is_demo",     pa.bool_(),                   nullable=False),
    ]
)

def _path(anomaly_dir: Path) -> Path:
    return anomaly_dir / ANOMALIES_FILENAME


def _to_row(anomaly: dict) -> dict:
    """Detector dict → Parquet row (metric dict serialized to JSON)."""
    row = dict(anomaly)
    row["metric_json"] = json.dumps(row.pop("metric", {}))
    return row


def _from_row(row: dict) -> dict:
    """Parquet row → API-facing dict (metric JSON parsed back)."""
    anomaly = dict(row)
    anomaly["metric"] = json.loads(anomaly.pop("metric_json") or "{}")
    return anomaly


def load_anomalies(anomaly_dir: Path = ANOMALY_DIR) -> list[dict]:
    """All stored anomalies, newest first. Empty list when none exist."""
    path = _path(anomaly_dir)
    if not path.exists():
        return []
    rows = pq.read_table(path).to_pylist()
    anomalies = sorted(
        (_from_row(row) for row in rows),
        key=lambda a: a["detected_at"],
        reverse=True,
    )
    return anomalies


def save_anomalies(anomalies: list[dict], anomaly_dir: Path = ANOMALY_DIR) -> list[dict]:
    """
    Merge new anomalies into the store, deduplicating by anomaly_id
    (existing entries win — first detection timestamp is kept).
    Returns the merged, newest-first list.
    """
    existing = {a["anomaly_id"]: a for a in load_anomalies(anomaly_dir)}
    added = 0
    for anomaly in anomalies:
        if anomaly["anomaly_id"] not in existing:
            existing[anomaly["anomaly_id"]] = anomaly
            added += 1

    merged = sorted(existing.values(), key=lambda a: a["detected_at"], reverse=True)
    anomaly_dir.mkdir(parents=True, exist_ok=True)
    table = pa.Table.from_pylist([_to_row(a) for a in merged], schema=ANOMALY_SCHEMA)
    pq.write_table(table, _path(anomaly_dir))
    log.info("anomalies_saved", added=added, total=len(merged))
    return merged


