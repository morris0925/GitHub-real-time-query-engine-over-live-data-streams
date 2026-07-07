"""
anomaly/store.py — Anomaly Parquet store + demo seeding

Persists detected anomalies to data/anomalies/anomalies.parquet so the API
can serve a stable incident feed (with stable IDs for /diagnose/:id) across
detection runs and restarts. Deduplicates by anomaly_id: the detector emits
deterministic per-(type, hour) IDs, so re-running detection is idempotent.

Also home of seed_demo_anomaly() — live GitHub events may not conveniently
produce an anomaly during a live demo, so the dashboard has a button that
plants a realistic synthetic one, clearly flagged is_demo=True.
"""

import json
import os
import uuid
from datetime import datetime, timezone
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

# Realistic synthetic anomalies for the demo button, one per type.
_DEMO_TEMPLATES: dict[str, dict] = {
    "ci_failure_spike": {
        "title": "CI failure rate spiked to 46%",
        "severity": "high",
        "description": (
            "CI failure rate over the last 24h is 46% (61 runs), "
            "vs an 8% baseline."
        ),
        "metric": {
            "recent_failure_rate": 0.46,
            "baseline_failure_rate": 0.08,
            "recent_runs": 61,
            "baseline_runs": 412,
        },
    },
    "merge_time_anomaly": {
        "title": "PR merge time 2.4x above baseline",
        "severity": "medium",
        "description": (
            "PRs merged in the last 24h averaged 31.2h open-to-merge, "
            "vs a 13.0h baseline."
        ),
        "metric": {
            "recent_avg_merge_hours": 31.2,
            "baseline_avg_merge_hours": 13.0,
            "recent_merged": 9,
            "ratio": 2.4,
        },
    },
    "commit_drought": {
        "title": "Commit activity dropped to 0.4/h",
        "severity": "low",
        "description": (
            "PushEvents averaged 0.4/h over the last 24h, "
            "vs a 3.1/h baseline."
        ),
        "metric": {
            "recent_pushes_per_hour": 0.4,
            "baseline_pushes_per_hour": 3.1,
        },
    },
}


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


def seed_demo_anomaly(
    anomaly_type: str = "ci_failure_spike",
    anomaly_dir: Path = ANOMALY_DIR,
) -> dict:
    """
    Plant a synthetic anomaly (unique ID, is_demo=True) and persist it.
    Raises KeyError for unknown types — the API maps that to a 400.
    """
    template = _DEMO_TEMPLATES[anomaly_type]
    anomaly = {
        "anomaly_id": f"demo-{uuid.uuid4().hex[:8]}",
        "type": anomaly_type,
        "title": template["title"],
        "severity": template["severity"],
        "description": template["description"],
        "metric": dict(template["metric"]),
        "repo": None,
        "detected_at": datetime.now(tz=timezone.utc),
        "is_demo": True,
    }
    save_anomalies([anomaly], anomaly_dir)
    log.info("demo_anomaly_seeded", anomaly_id=anomaly["anomaly_id"], type=anomaly_type)
    return anomaly


def demo_anomaly_types() -> list[str]:
    """Types the demo button can seed."""
    return list(_DEMO_TEMPLATES)
