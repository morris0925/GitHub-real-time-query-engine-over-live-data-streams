"""
storage/jsonl_writer.py — Write GitHub events to newline-delimited JSON (JSONL)

This module provides the baseline row-oriented writer used by the benchmark
(docs/benchmark.md) to compare query performance against the Parquet writer.

File layout on disk:
    data/jsonl/date=2026-06-25/part-<uuid>.jsonl

Each line is a flat JSON object matching the same flattened schema as
storage/writer.py, so DuckDB can query both with identical SQL.

Why JSON-lines?
-   The simplest possible streaming storage format: one append per event.
-   Widely used as a row-store baseline when evaluating columnar formats.
-   DuckDB supports read_json_auto() for zero-config querying.
"""

import json
import uuid
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

import structlog

from storage.writer import flatten_event, _partition_key, LATE_EVENT_THRESHOLD_HOURS

log = structlog.get_logger(__name__)

DEFAULT_JSONL_DIR = Path("data/jsonl")


class JSONLWriteError(Exception):
    """Raised when a batch of events cannot be written to JSONL."""


def _serialize_row(row: dict) -> str:
    """
    Serialize a flattened event row to a JSON string.

    Timestamps are converted to ISO-8601 strings so they survive a
    JSON round-trip and remain queryable via DuckDB's read_json_auto().
    """
    serializable = {}
    for k, v in row.items():
        if isinstance(v, datetime):
            serializable[k] = v.isoformat()
        else:
            serializable[k] = v
    return json.dumps(serializable, ensure_ascii=False)


def _write_rows_to_partition(rows: list[dict], partition_dir: Path) -> Path:
    """
    Append a list of flat row dicts to a new JSONL file in partition_dir.

    Returns the path of the written file.
    Raises JSONLWriteError on failure.
    """
    partition_dir.mkdir(parents=True, exist_ok=True)
    file_path = partition_dir / f"part-{uuid.uuid4()}.jsonl"

    try:
        with open(file_path, "w", encoding="utf-8") as fh:
            for row in rows:
                fh.write(_serialize_row(row) + "\n")
        return file_path
    except Exception as exc:
        log.error("jsonl_write_failed", error=str(exc), path=str(file_path))
        raise JSONLWriteError(f"Failed to write to {file_path}: {exc}") from exc


def write_batch_jsonl(
    events: list[dict],
    jsonl_dir: Path = DEFAULT_JSONL_DIR,
    threshold_hours: int = LATE_EVENT_THRESHOLD_HOURS,
) -> list[Path]:
    """
    Flatten a batch of raw GitHub events and write them to JSONL files,
    grouping by each event's created_at date (same partitioning as Parquet).

    Args:
        events:          List of raw GitHub event dicts.
        jsonl_dir:       Root directory for JSONL output.
        threshold_hours: Late-event watermark (mirrors Parquet writer).

    Returns:
        List of Paths to files written (one per date partition).
        Empty list if events is empty.

    Raises:
        JSONLWriteError: If any write fails.
    """
    if not events:
        return []

    ingested_at = datetime.now(tz=timezone.utc)

    rows: list[dict] = [flatten_event(e, ingested_at) for e in events]

    groups: dict[str, list[dict]] = defaultdict(list)
    for row in rows:
        key = _partition_key(row["created_at"], ingested_at, threshold_hours)
        groups[key].append(row)

    written: list[Path] = []
    for key, partition_rows in groups.items():
        partition_dir = jsonl_dir / f"date={key}"
        path = _write_rows_to_partition(partition_rows, partition_dir)
        written.append(path)
        log.info("jsonl_batch_written", rows=len(partition_rows), path=str(path))

    return written
