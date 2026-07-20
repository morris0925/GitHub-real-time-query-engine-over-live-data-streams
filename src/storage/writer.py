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

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Delivery-semantics guarantee: at-least-once ingest, idempotent write
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
consumer.py commits Kafka offsets only AFTER write_batch() returns
successfully (see its "Offset commit ordering" docstring). If the process
crashes between the write and the commit, Kafka replays the un-committed
messages on restart — the same event_id can reach write_batch() twice,
either within one batch (a Kafka poll() returning a redelivered message
alongside new ones) or across two separate batches (a full restart).

write_batch() absorbs that redelivery so storage stays exactly-once even
though ingest is only at-least-once:
  1. Rows with an event_id already seen earlier in the SAME batch are
     dropped, keeping the first occurrence.
  2. Before writing a partition's rows, we read back the event_ids already
     on disk in that date=.../ directory (cheap: only the event_id column)
     and drop any row that's already stored there.
A partition whose rows are entirely duplicates is skipped — no empty file
is written, and it doesn't appear in the returned path list.

Net guarantee: **at-least-once Kafka delivery + idempotent Parquet write
= each GitHub event_id is stored at most once.**
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


def _existing_event_ids(partition_dir: Path) -> set[str]:
    """
    Return the event_id values already written to partition_dir.

    Reads only the event_id column of each existing Parquet file, so this
    stays cheap even as a partition accumulates many files between
    compaction runs. Used to make write_batch idempotent across batches
    (see the "Delivery-semantics guarantee" note at the top of this file).
    """
    if not partition_dir.exists():
        return set()

    ids: set[str] = set()
    for file_path in partition_dir.glob("*.parquet"):
        try:
            ids.update(pq.read_table(file_path, columns=["event_id"])["event_id"].to_pylist())
        except Exception as exc:
            log.warning("dedupe_scan_failed", path=str(file_path), error=str(exc))
    return ids


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

    # ── Step 1b: drop duplicates within this batch (keep first occurrence) ────
    # A redelivered Kafka message can land in the same poll() as new events.
    seen_in_batch: set[str] = set()
    deduped_rows: list[dict] = []
    for row in rows:
        if row["event_id"] in seen_in_batch:
            continue
        seen_in_batch.add(row["event_id"])
        deduped_rows.append(row)

    dropped_in_batch = len(rows) - len(deduped_rows)
    if dropped_in_batch:
        log.warning("duplicate_events_in_batch", count=dropped_in_batch)

    # ── Step 2: group rows by partition key ───────────────────────────────────
    # defaultdict(list) means groups["2026-06-25"] auto-initialises to []
    groups: dict[str, list[dict]] = defaultdict(list)
    for row in deduped_rows:
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

    # ── Step 3: write one file per partition, skipping already-stored rows ────
    written: list[Path] = []
    for key, partition_rows in groups.items():
        partition_dir = data_dir / f"date={key}"

        already_stored = _existing_event_ids(partition_dir)
        new_rows = [r for r in partition_rows if r["event_id"] not in already_stored]

        dropped_already_stored = len(partition_rows) - len(new_rows)
        if dropped_already_stored:
            log.warning(
                "duplicate_events_already_stored",
                count=dropped_already_stored,
                partition=key,
            )

        if not new_rows:
            continue

        path = _write_rows_to_partition(new_rows, partition_dir)
        written.append(path)
        log.info(
            "batch_written",
            rows=len(new_rows),
            path=str(path),
            partition=key,
        )

    return written
