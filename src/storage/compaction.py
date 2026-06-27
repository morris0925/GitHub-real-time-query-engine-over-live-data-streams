"""
storage/compaction.py — Merge small Parquet files into larger ones

The Problem: Small Files
─────────────────────────
Every time our consumer flushes a micro-batch (e.g. every 30 seconds),
it writes a new Parquet file:

    data/events/date=2026-06-25/part-a1b2.parquet   ← 50 rows
    data/events/date=2026-06-25/part-c3d4.parquet   ← 50 rows
    data/events/date=2026-06-25/part-e5f6.parquet   ← 50 rows
    ... (288 files if flushing every 5 minutes all day)

This is called the "small file problem". Each file has overhead:
  - DuckDB opens and reads the footer metadata of EVERY file, even for
    a simple COUNT(*) — hundreds of file-open syscalls instead of one.
  - Object storage (S3, GCS) charges per API request; many small files
    means many requests per query.
  - File system inodes get eaten up.

The Fix: Compaction
────────────────────
Compaction reads all the small files for a partition, concatenates the
data into one big PyArrow Table, writes a single merged file, then
deletes the originals:

    data/events/date=2026-06-25/compacted-<uuid>.parquet  ← 14,400 rows

After compaction, DuckDB opens one file per day partition instead of
hundreds. Queries get faster without changing the data at all.

When to run:
  - Once per day on previous day's partition (after midnight, it won't grow)
  - Or on any past partition that has many files
  - Safe to run on today's partition too — it compacts whatever exists,
    and the consumer just creates new files afterward

Real-world analogy:
  This is what Apache Spark's "optimize" command, Delta Lake's auto-compaction,
  and Apache Iceberg's "rewrite data files" procedure all do under the hood.
  Same problem, same solution, different scale.
"""

import uuid
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import structlog

from storage.schema import GITHUB_EVENT_SCHEMA

log = structlog.get_logger(__name__)

DEFAULT_DATA_DIR = Path("data/events")

# Only compact a partition if it has more than this many files.
# If it's already one file, compaction gains nothing.
MIN_FILES_TO_COMPACT = 2


class CompactionError(Exception):
    """Raised when a compaction run fails."""


def get_partition_dirs(data_dir: Path = DEFAULT_DATA_DIR) -> list[Path]:
    """
    Return all date partition directories under data_dir, sorted oldest first.

    Example: [Path("data/events/date=2026-06-24"), Path("data/events/date=2026-06-25")]
    """
    if not data_dir.exists():
        return []
    return sorted(
        p for p in data_dir.iterdir()
        if p.is_dir() and p.name.startswith("date=")
    )


def compact_partition(partition_dir: Path, min_files: int = MIN_FILES_TO_COMPACT) -> dict:
    """
    Merge all Parquet files in a single date partition into one file.

    Steps:
      1. Find all .parquet files in the partition directory
      2. Skip if there are fewer than min_files (nothing to merge)
      3. Read all files into PyArrow Tables, concatenate them
      4. Write the merged table as a new 'compacted-<uuid>.parquet'
      5. Delete the original files

    Args:
        partition_dir: Path to one date partition, e.g. data/events/date=2026-06-25/
        min_files:     Only compact if at least this many files exist.

    Returns:
        A summary dict with keys: partition, files_before, rows, output_path, skipped
    """
    partition_name = partition_dir.name

    # Find all existing Parquet files in the partition directory.
    # This includes any previously compacted files — running compaction
    # a second time simply re-merges them with any new small files that
    # arrived since the last run. Safe and idempotent.
    all_files = sorted(partition_dir.glob("*.parquet"))

    if len(all_files) < min_files:
        log.info(
            "compaction_skipped",
            partition=partition_name,
            files=len(all_files),
            reason=f"fewer than {min_files} files",
        )
        return {
            "partition": partition_name,
            "skipped": True,
            "files_before": len(all_files),
            "rows": 0,
            "output_path": None,
        }

    log.info("compaction_starting", partition=partition_name, files=len(all_files))

    try:
        # ── Step 1: Read all files into memory ──────────────────────────────
        # pq.read_table() reads one Parquet file into a PyArrow Table.
        # pa.concat_tables() stacks them vertically (like SQL UNION ALL).
        tables: list[pa.Table] = [pq.read_table(f) for f in all_files]
        merged: pa.Table = pa.concat_tables(tables)
        total_rows = merged.num_rows

        log.info(
            "compaction_merged",
            partition=partition_name,
            input_files=len(tables),
            total_rows=total_rows,
        )

        # ── Step 2: Write the merged table ───────────────────────────────────
        # Use a distinct "compacted-" prefix so it's easy to identify.
        output_path = partition_dir / f"compacted-{uuid.uuid4()}.parquet"
        pq.write_table(merged, output_path, compression="snappy")

        # ── Step 3: Delete originals ─────────────────────────────────────────
        # Only delete AFTER the new file is successfully written.
        # If write_table() raised above, we never reach here — originals safe.
        for f in all_files:
            f.unlink()
            log.debug("deleted_original", file=f.name)

        log.info(
            "compaction_complete",
            partition=partition_name,
            files_removed=len(all_files),
            rows=total_rows,
            output=output_path.name,
        )

        return {
            "partition": partition_name,
            "skipped": False,
            "files_before": len(all_files),
            "rows": total_rows,
            "output_path": output_path,
        }

    except Exception as exc:
        log.error("compaction_failed", partition=partition_name, error=str(exc))
        raise CompactionError(f"Failed to compact {partition_name}: {exc}") from exc


def compact_all(
    data_dir: Path = DEFAULT_DATA_DIR,
    min_files: int = MIN_FILES_TO_COMPACT,
) -> list[dict]:
    """
    Compact every date partition under data_dir.

    Typically you'd run this once a day (e.g. via cron at 01:00) to clean up
    the previous day's many small files.

    Returns:
        List of summary dicts from compact_partition(), one per partition.
    """
    partitions = get_partition_dirs(data_dir)

    if not partitions:
        log.info("no_partitions_found", data_dir=str(data_dir))
        return []

    log.info("compact_all_starting", partitions=len(partitions))
    results = [compact_partition(p, min_files=min_files) for p in partitions]

    compacted = sum(1 for r in results if not r["skipped"])
    skipped   = sum(1 for r in results if r["skipped"])
    total_rows = sum(r["rows"] for r in results)

    log.info(
        "compact_all_done",
        compacted=compacted,
        skipped=skipped,
        total_rows=total_rows,
    )
    return results


# ── CLI entry point ───────────────────────────────────────────────────────────

if __name__ == "__main__":
    import os
    from dotenv import load_dotenv
    load_dotenv()

    data_dir = Path(os.getenv("DATA_DIR", "data/events"))
    print(f"Compacting partitions in: {data_dir}\n")

    results = compact_all(data_dir=data_dir)

    for r in results:
        if r["skipped"]:
            print(f"  SKIP  {r['partition']}  ({r['files_before']} file(s), below threshold)")
        else:
            print(f"  OK    {r['partition']}  {r['files_before']} → 1 file, {r['rows']:,} rows")

    print("\nDone.")
