"""
tests/test_compaction.py — Unit tests for the compaction and lag metric

Run with:
    pytest tests/test_compaction.py -v
"""

from datetime import datetime, timezone, timedelta
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from storage.writer import write_batch
from storage.compaction import (
    compact_partition,
    compact_all,
    get_partition_dirs,
    CompactionError,
    MIN_FILES_TO_COMPACT,
)
from storage import reader


# ── Helpers ───────────────────────────────────────────────────────────────────

def make_event(event_id: str, repo: str = "alice/repo", lag_seconds: float = 10.0) -> dict:
    """
    Build a raw GitHub event dict. lag_seconds controls how far in the past
    created_at is set relative to now — used to test lag metric calculations.
    """
    now = datetime.now(tz=timezone.utc)
    created_at = now - timedelta(seconds=lag_seconds)
    return {
        "id": event_id,
        "type": "PushEvent",
        "actor": {"id": 1, "login": "alice"},
        "repo": {"id": 99, "name": repo},
        "payload": {},
        "public": True,
        "created_at": created_at.strftime("%Y-%m-%dT%H:%M:%SZ"),
    }


def write_n_files(tmp_path: Path, n: int, events_per_file: int = 5) -> list[Path]:
    """Write n separate Parquet files, each with events_per_file rows."""
    paths = []
    for i in range(n):
        batch = [make_event(f"e{i}-{j}") for j in range(events_per_file)]
        path = write_batch(batch, data_dir=tmp_path)
        paths.append(path)
    return paths


# ── get_partition_dirs tests ──────────────────────────────────────────────────

class TestGetPartitionDirs:
    def test_returns_empty_for_nonexistent_dir(self, tmp_path: Path):
        assert get_partition_dirs(tmp_path / "nonexistent") == []

    def test_finds_date_partitions(self, tmp_path: Path):
        (tmp_path / "date=2026-06-24").mkdir()
        (tmp_path / "date=2026-06-25").mkdir()
        (tmp_path / "other_dir").mkdir()     # should be ignored

        dirs = get_partition_dirs(tmp_path)
        names = [d.name for d in dirs]
        assert "date=2026-06-24" in names
        assert "date=2026-06-25" in names
        assert "other_dir" not in names

    def test_sorted_oldest_first(self, tmp_path: Path):
        (tmp_path / "date=2026-06-25").mkdir()
        (tmp_path / "date=2026-06-23").mkdir()
        (tmp_path / "date=2026-06-24").mkdir()

        dirs = get_partition_dirs(tmp_path)
        assert [d.name for d in dirs] == [
            "date=2026-06-23", "date=2026-06-24", "date=2026-06-25"
        ]


# ── compact_partition tests ───────────────────────────────────────────────────

class TestCompactPartition:
    def _partition_dir(self, tmp_path: Path) -> Path:
        """Write files into a date partition and return that directory."""
        write_n_files(tmp_path, n=3, events_per_file=4)
        # write_batch creates date=YYYY-MM-DD/ automatically — find it
        dirs = list(tmp_path.glob("date=*"))
        assert len(dirs) == 1, "Expected exactly one date partition"
        return dirs[0]

    def test_single_output_file_after_compaction(self, tmp_path: Path):
        """After compaction, the partition should have exactly one Parquet file."""
        partition_dir = self._partition_dir(tmp_path)
        compact_partition(partition_dir)

        remaining = list(partition_dir.glob("*.parquet"))
        assert len(remaining) == 1

    def test_output_named_compacted(self, tmp_path: Path):
        """The merged file should start with 'compacted-'."""
        partition_dir = self._partition_dir(tmp_path)
        result = compact_partition(partition_dir)
        assert result["output_path"].name.startswith("compacted-")

    def test_row_count_preserved(self, tmp_path: Path):
        """Total rows after compaction must equal total rows before."""
        n_files, rows_per_file = 3, 4
        write_n_files(tmp_path, n=n_files, events_per_file=rows_per_file)
        partition_dir = list(tmp_path.glob("date=*"))[0]

        result = compact_partition(partition_dir)

        merged = pq.read_table(result["output_path"])
        assert merged.num_rows == n_files * rows_per_file

    def test_original_files_deleted(self, tmp_path: Path):
        """All part-<uuid>.parquet originals must be deleted after compaction."""
        partition_dir = self._partition_dir(tmp_path)
        original_files = sorted(partition_dir.glob("part-*.parquet"))
        assert len(original_files) > 0

        compact_partition(partition_dir)

        for f in original_files:
            assert not f.exists(), f"Original file should have been deleted: {f.name}"

    def test_skips_when_single_file(self, tmp_path: Path):
        """A partition with one file should be skipped — nothing to merge."""
        write_n_files(tmp_path, n=1, events_per_file=5)
        partition_dir = list(tmp_path.glob("date=*"))[0]

        result = compact_partition(partition_dir, min_files=2)

        assert result["skipped"] is True
        # Original file should still exist
        assert len(list(partition_dir.glob("*.parquet"))) == 1

    def test_result_dict_structure(self, tmp_path: Path):
        """compact_partition should return a dict with the expected keys."""
        partition_dir = self._partition_dir(tmp_path)
        result = compact_partition(partition_dir)

        assert "partition" in result
        assert "skipped" in result
        assert "files_before" in result
        assert "rows" in result
        assert "output_path" in result

    def test_files_before_count(self, tmp_path: Path):
        """files_before should reflect the number of original files."""
        n = 4
        write_n_files(tmp_path, n=n, events_per_file=2)
        partition_dir = list(tmp_path.glob("date=*"))[0]

        result = compact_partition(partition_dir)
        assert result["files_before"] == n


# ── compact_all tests ─────────────────────────────────────────────────────────

class TestCompactAll:
    def test_empty_data_dir(self, tmp_path: Path):
        """compact_all on an empty dir should return [] without error."""
        assert compact_all(data_dir=tmp_path) == []

    def test_compacts_multiple_partitions(self, tmp_path: Path):
        """compact_all should process every partition it finds."""
        # Write 3 files into each of two manually-named partition dirs.
        # We create the dirs explicitly so we control the names; write_batch
        # will place files inside whichever dir already exists for today.
        for date in ["date=2026-06-24", "date=2026-06-25"]:
            part_dir = tmp_path / date
            part_dir.mkdir(exist_ok=True)
            for i in range(3):
                batch = [make_event(f"{date}-e{i}")]
                # Write directly into the named partition dir (bypass write_batch's
                # auto-date logic by passing the partition dir itself as data_dir)
                write_batch(batch, data_dir=tmp_path / "_staging")
                # Move the written file into our target partition
                staged = list((tmp_path / "_staging").glob("**/*.parquet"))
                for f in staged:
                    f.rename(part_dir / f.name)

        results = compact_all(data_dir=tmp_path)
        # Both partitions processed (not skipped), since each has 3 files
        assert len(results) == 2
        assert all(not r["skipped"] for r in results)


# ── Lag metric tests ──────────────────────────────────────────────────────────

class TestLagMetric:
    def test_lag_returns_none_with_no_data(self, tmp_path: Path):
        """No Parquet files → get_avg_lag returns None, not an error."""
        assert reader.get_avg_lag(data_dir=tmp_path) is None

    def test_lag_approximately_correct(self, tmp_path: Path):
        """
        Write events where created_at is exactly 20s before ingested_at.
        The lag should be approximately 20s (within a few seconds tolerance
        to allow for test execution time).
        """
        # make_event sets created_at = now - lag_seconds
        # flatten_event sets ingested_at = now (at write time)
        # So lag ≈ lag_seconds
        events = [make_event(f"e{i}", lag_seconds=20.0) for i in range(5)]
        write_batch(events, data_dir=tmp_path)

        lag = reader.get_avg_lag(since_minutes=99999, data_dir=tmp_path)

        assert lag is not None
        assert "avg_lag_seconds" in lag
        assert "sample_size" in lag
        assert lag["sample_size"] == 5
        # Allow a few seconds of tolerance for test execution time
        assert 15.0 <= lag["avg_lag_seconds"] <= 30.0

    def test_lag_result_has_all_keys(self, tmp_path: Path):
        """get_avg_lag result should have avg, min, max, and sample_size."""
        events = [make_event(f"e{i}", lag_seconds=10.0) for i in range(3)]
        write_batch(events, data_dir=tmp_path)

        lag = reader.get_avg_lag(since_minutes=99999, data_dir=tmp_path)
        assert lag is not None
        for key in ("avg_lag_seconds", "min_lag_seconds", "max_lag_seconds", "sample_size"):
            assert key in lag, f"Missing key: {key}"
