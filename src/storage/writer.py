"""
storage/writer.py — Write GitHub events to date-partitioned Parquet files

Partition layout on disk:
    data/events/date=2026-06-25/part-<uuid>.parquet   ← normal events
    data/events/date=2026-06-24/part-<uuid>.parquet   ← slightly late (still within watermark)
    data/events/date=late/part-<uuid>.parquet          ← very late events (beyond watermark)

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
The Late-Arriving Events Problem
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

In any streaming pipeline, some events arrive later than expected. With
GitHub's API, events are usually fresh (seconds old), but occasionally:

  - GitHub's API has a backlog and surfaces old events
  - Our consumer was down for hours and re-reads from Kafka from an old offset
  - Clock skew between GitHub's servers and ours

The naive fix: always partition by ingestion date (today). Simple, but:
  If an event has created_at=yesterday and we write it to date=today,
  then a query like "SELECT * WHERE date='yesterday'" will miss it.
  The data is "in the wrong drawer."

Our fix: partition by event time (created_at date), with a watermark.

  Watermark = LATE_EVENT_THRESHOLD_HOURS (default: 24h)

  created_at is RECENT (within watermark)  →  write to date=<created_at date>
  created_at is OLD    (beyond watermark)  →  write to date=late/

The watermark trades correctness against complexity. If we accepted events
into their "correct" partition forever, compaction and downstream consumers
would never know when a partition is "sealed" and safe to finalize. The
watermark says: "after 24 hours, a partition is closed." Late arrivals get
quarantined in date=late/ where they can be inspected or reprocessed.

This is the same mechanism Flink, Spark Structured Streaming, and Apache
Beam use — they call it the "watermark" and the late partition a "side output."

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Why write_batch now returns list[Path]:
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
A single batch from Kafka may contain events from different dates —
e.g. a consumer restart replaying yesterday's and today's events together.
We group them by partition key and write one file per group.
"""

import json
import os
import uuid
from collections import defaultdict
from datetime import datetime, timezone, timedelta
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import structlog
from dotenv import load_dotenv

from storage.schema import GITHUB_EVENT_SCHEMA

load_dotenv()
log = structlog.get_logger(__name__)

# ── Configuration ─────────────────────────────────────────────────────────────

DEFAULT_DATA_DIR = Path("data/events")

# Events older than this many hours go to the date=late/ partition.
# After this window, we consider the date partition "sealed."
LATE_EVENT_THRESHOLD_HOURS: int = int(os.getenv("LATE_EVENT_THRESHOLD_HOURS", "24"))

# Sentinel name for the late-arrival partition
LATE_PARTITION_KEY = "late"


class StorageWriteError(Exception):
    """Raised when a batch of events cannot be written to Parquet."""


# ── Helpers ───────────────────────────────────────────────────────────────────

def flatten_event(event: dict, ingested_at: datetime) -> dict:
    """
    Convert one raw GitHub API event dict into a flat row matching our schema.

    Args:
        event:       Raw dict from the GitHub Events API (via Kafka).
        ingested_at: The timestamp when our consumer received this event.

    Returns:
        A plain dict whose keys match GITHUB_EVENT_SCHEMA column names.
    """
    actor = event.get("actor") or {}
    repo  = event.get("repo")  or {}

    created_at_str: str = event.get("created_at", "")
    try:
        created_at = datetime.fromisoformat(created_at_str.replace("Z", "+00:00"))
    except (ValueError, AttributeError):
        log.warning("bad_created_at", raw_value=created_at_str, event_id=event.get("id"))
        created_at = ingested_at

    return {
        "event_id":     str(event.get("id", "")),
        "event_type":   str(event.get("type", "")),
        "actor_id":     int(actor["id"]) if actor.get("id") is not None else None,
        "actor_login":  actor.get("login"),
        "repo_id":      int(repo["id"])  if repo.get("id")  is not None else None,
        "repo_name":    repo.get("name", ""),
        "payload_json": json.dumps(event.get("payload")) if event.get("payload") else None,
        "public":       bool(event.get("public")) if event.get("public") is not None else None,
        "created_at":   created_at,
        "ingested_at":  ingested_at,
    }


def _partition_key(
    created_at: datetime,
    ingested_at: datetime,
    threshold_hours: int = LATE_EVENT_THRESHOLD_HOURS,
) -> str:
    """
    Decide which date partition this event belongs in.

    Returns "YYYY-MM-DD" for events within the watermark, or "late" for
    events older than threshold_hours.

    Args:
        created_at:      When GitHub recorded the event.
        ingested_at:     When our consumer received it.
        threshold_hours: How old an event can be before it's "too late."

    Examples:
        created_at=today, ingested_at=today          → "2026-06-25"
        created_at=yesterday, ingested_at=today      → "2026-06-24"  (within 24h)
        created_at=3 days ago, ingested_at=today     → "late"
    """
    age = ingested_at - created_at
    if age > timedelta(hours=threshold_hours):
        return LATE_PARTITION_KEY
    return created_at.strftime("%Y-%m-%d")


def _write_rows_to_partition(
    rows: list[dict],
    partition_dir: Path,
) -> Path:
    """
    Write a list of flat row dicts to a new Parquet file in partition_dir.

    Returns the path of the written file.
    Raises StorageWriteError on failure.
    """
    partition_dir.mkdir(parents=True, exist_ok=True)
    file_path = partition_dir / f"part-{uuid.uuid4()}.parquet"

    try:
        table: pa.Table = pa.Table.from_pylist(rows, schema=GITHUB_EVENT_SCHEMA)
        pq.write_table(table, file_path, compression="snappy")
        return file_path
    except Exception as exc:
        log.error("write_rows_failed", error=str(exc), path=str(file_path))
        raise StorageWriteError(f"Failed to write to {file_path}: {exc}") from exc


# ── Public API ────────────────────────────────────────────────────────────────

def write_batch(
    events: list[dict],
    data_dir: Path = DEFAULT_DATA_DIR,
    threshold_hours: int = LATE_EVENT_THRESHOLD_HOURS,
) -> list[Path]:
    """
    Flatten a batch of raw GitHub events and write them to Parquet,
    grouping by each event's created_at date.

    A single batch may span multiple date partitions (e.g. after a consumer
    restart that replays yesterday's and today's events together). We group
    by partition key and write one file per group.

    Late events (created_at older than threshold_hours) go to date=late/.

    Args:
        events:          List of raw GitHub event dicts from Kafka.
        data_dir:        Root directory for Parquet output. Override in tests.
        threshold_hours: Watermark in hours. Events older than this → date=late/.

    Returns:
        List of Paths to files written (one per date partition touched).
        Empty list if events is empty.

    Raises:
        StorageWriteError: If any write fails.
    """
    if not events:
        log.info("write_batch_skipped", reason="empty batch")
        return []

    ingested_at = datetime.now(tz=timezone.utc)

    # ── Step 1: flatten all events ────────────────────────────────────────────
    rows: list[dict] = [flatten_event(e, ingested_at) for e in events]

    # ── Step 2: group rows by partition key ───────────────────────────────────
    # defaultdict(list) means groups["2026-06-25"] auto-initialises to []
    groups: dict[str, list[dict]] = defaultdict(list)
    for row in rows:
        key = _partition_key(row["created_at"], ingested_at, threshold_hours)
        groups[key].append(row)

    # Log any late arrivals so we can monitor them
    if LATE_PARTITION_KEY in groups:
        late_count = len(groups[LATE_PARTITION_KEY])
        log.warning(
            "late_events_detected",
            count=late_count,
            threshold_hours=threshold_hours,
            destination=f"date={LATE_PARTITION_KEY}",
        )

    # ── Step 3: write one file per partition ──────────────────────────────────
    written: list[Path] = []
    for key, partition_rows in groups.items():
        partition_dir = data_dir / f"date={key}"
        path = _write_rows_to_partition(partition_rows, partition_dir)
        written.append(path)
        log.info(
            "batch_written",
            rows=len(partition_rows),
            path=str(path),
            partition=key,
        )

    return written
