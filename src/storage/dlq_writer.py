"""
storage/dlq_writer.py — Dead Letter Queue for invalid GitHub events

What is a DLQ?
──────────────────────────────────────────────────────────────────────────────
A Dead Letter Queue is a holding area for messages that couldn't be
processed. In our pipeline, "couldn't be processed" means a processor
raised a ValidationError — the event was structurally broken (missing
required fields, wrong shape).

Before Day 10, ValidationError → log warning + skip. The broken event
was acknowledged and forgotten. That's fine for a prototype, but in
production you want to:

  1. Retain the raw event so you can reprocess it later if the bug was in
     your code (not in the data)
  2. Monitor DLQ growth — a sudden spike means something upstream changed
  3. Audit what's being dropped (GitHub API format changes, bots, etc.)

Our DLQ design:
──────────────────────────────────────────────────────────────────────────────
- Storage: Parquet files in data/dlq/  (separate from data/events/)
- Schema: 5 columns — event_id, event_type, error_reason, raw_json, failed_at
- Access: reader.inspect_dlq() + `python src/cli.py dlq`
- Trigger: consumer.py catches ValidationError → write_dlq_entry()

Why Parquet (not a separate Kafka topic)?
- Same tooling: DuckDB can query it with the same SQL
- Durable: survives restarts
- Inspectable: any Parquet reader can open it
- For production systems you'd use a separate Kafka DLQ topic; for this
  portfolio project Parquet is simpler and shows the same design intent.

Schema deliberately minimal:
- We store raw_json so the full event can be replayed if we fix the bug
- We don't try to parse the broken event — that's what caused the failure
"""

import json
import uuid
from datetime import datetime, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import structlog

log = structlog.get_logger(__name__)

# ── DLQ Schema ────────────────────────────────────────────────────────────────

DLQ_SCHEMA = pa.schema([
    pa.field("event_id",     pa.string(),                  nullable=True),
    pa.field("event_type",   pa.string(),                  nullable=True),
    pa.field("error_reason", pa.string(),                  nullable=False),
    pa.field("raw_json",     pa.string(),                  nullable=False),
    pa.field("failed_at",    pa.timestamp("us", tz="UTC"), nullable=False),
])

# Default DLQ directory — separate from data/events/ to avoid confusion
DEFAULT_DLQ_DIR = Path("data/dlq")


class DLQWriteError(Exception):
    """Raised when a DLQ Parquet write fails."""


def write_dlq_entry(
    event: dict,
    reason: str,
    dlq_dir: Path = DEFAULT_DLQ_DIR,
) -> Path:
    """
    Write one invalid event to the DLQ Parquet store.

    Each call writes a single-row Parquet file. This is intentionally simple:
    DLQ volume should be low (invalid events are the exception, not the rule).
    If you start seeing high DLQ write rates, that's the real problem to fix.

    Args:
        event:   The raw event dict that failed validation.
        reason:  Human-readable explanation of why it failed.
        dlq_dir: Directory to write DLQ Parquet files into.

    Returns:
        Path to the written Parquet file.

    Raises:
        DLQWriteError: If the write fails.
    """
    dlq_dir.mkdir(parents=True, exist_ok=True)

    now = datetime.now(timezone.utc)

    row = {
        "event_id":     str(event.get("id", "")),
        "event_type":   str(event.get("type", "unknown")),
        "error_reason": reason,
        "raw_json":     json.dumps(event, default=str),
        "failed_at":    now,
    }

    table = pa.table(
        {col: [row[col]] for col in DLQ_SCHEMA.names},
        schema=DLQ_SCHEMA,
    )

    filename = f"dlq-{uuid.uuid4()}.parquet"
    out_path = dlq_dir / filename

    try:
        pq.write_table(table, out_path, compression="snappy")
        log.debug(
            "dlq_entry_written",
            event_id=row["event_id"],
            event_type=row["event_type"],
            reason=reason,
            path=str(out_path),
        )
        return out_path
    except Exception as exc:
        raise DLQWriteError(f"Failed to write DLQ entry: {exc}") from exc
