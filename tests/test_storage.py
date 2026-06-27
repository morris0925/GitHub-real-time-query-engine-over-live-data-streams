"""
tests/test_storage.py — Unit tests for the storage layer

Tests cover:
  1. Schema — correct columns and non-nullable constraints
  2. flatten_event — raw GitHub dict → flat row dict
  3. write_batch — write to a temp dir, read back, verify contents
  4. reader functions — end-to-end: write some rows, query them back

We use pytest's `tmp_path` fixture for a temporary directory that is
automatically cleaned up after each test. This means tests never touch
the real data/ directory.

Run with:
    pytest tests/test_storage.py -v
"""

import json
from datetime import datetime, timezone, timedelta
from pathlib import Path

import pyarrow.parquet as pq
import pytest

# We import from the src/ package. Make sure to run pytest from the repo root
# (or with PYTHONPATH=src set) so these imports resolve correctly.
from storage.schema import GITHUB_EVENT_SCHEMA, SCHEMA_COLUMNS
from storage.writer import flatten_event, write_batch, StorageWriteError
from storage import reader


# ── Fixtures ──────────────────────────────────────────────────────────────────

def make_raw_event(
    event_id: str = "evt-001",
    event_type: str = "PushEvent",
    actor_login: str = "alice",
    actor_id: int = 42,
    repo_name: str = "alice/myrepo",
    repo_id: int = 99,
    created_at: str | None = None,
) -> dict:
    """
    Helper that builds a minimal raw GitHub event dict — the same shape
    the GitHub API (and thus our Kafka producer) would produce.
    """
    # Default to "1 minute ago" so time-windowed SQL queries always include it
    ts = created_at or (
        datetime.now(tz=timezone.utc) - timedelta(minutes=1)
    ).strftime("%Y-%m-%dT%H:%M:%SZ")
    return {
        "id": event_id,
        "type": event_type,
        "actor": {"id": actor_id, "login": actor_login},
        "repo": {"id": repo_id, "name": repo_name},
        "payload": {"commits": [{"message": "fix bug"}]},
        "public": True,
        "created_at": ts,
    }


FAKE_INGESTED_AT = datetime(2024, 1, 15, 10, 31, 0, tzinfo=timezone.utc)


# ── 1. Schema tests ───────────────────────────────────────────────────────────

class TestSchema:
    def test_schema_has_expected_columns(self):
        """Every column we documented should exist in the schema."""
        expected = {
            "event_id", "event_type", "actor_id", "actor_login",
            "repo_id", "repo_name", "payload_json", "public",
            "created_at", "ingested_at",
        }
        assert set(SCHEMA_COLUMNS) == expected

    def test_schema_column_list_matches_schema_object(self):
        """SCHEMA_COLUMNS must stay in sync with GITHUB_EVENT_SCHEMA."""
        assert SCHEMA_COLUMNS == [f.name for f in GITHUB_EVENT_SCHEMA]

    def test_event_id_is_not_nullable(self):
        """event_id is our primary identifier — it must never be null."""
        field = GITHUB_EVENT_SCHEMA.field("event_id")
        assert not field.nullable

    def test_created_at_is_timestamp_utc(self):
        """created_at must be a UTC-aware timestamp so time math works."""
        import pyarrow as pa
        field = GITHUB_EVENT_SCHEMA.field("created_at")
        assert pa.types.is_timestamp(field.type)
        assert field.type.tz == "UTC"


# ── 2. flatten_event tests ────────────────────────────────────────────────────

class TestFlattenEvent:
    def test_basic_flatten(self):
        """Core fields are extracted to the top level correctly."""
        raw = make_raw_event()
        row = flatten_event(raw, FAKE_INGESTED_AT)

        assert row["event_id"] == "evt-001"
        assert row["event_type"] == "PushEvent"
        assert row["actor_login"] == "alice"
        assert row["actor_id"] == 42
        assert row["repo_name"] == "alice/myrepo"
        assert row["repo_id"] == 99
        assert row["public"] is True
        assert row["ingested_at"] == FAKE_INGESTED_AT

    def test_payload_serialized_to_json_string(self):
        """payload_json must be a string (JSON-encoded), not a dict."""
        raw = make_raw_event()
        row = flatten_event(raw, FAKE_INGESTED_AT)

        assert isinstance(row["payload_json"], str)
        decoded = json.loads(row["payload_json"])
        assert decoded == {"commits": [{"message": "fix bug"}]}

    def test_created_at_parsed_correctly(self):
        """GitHub's 'Z' suffix should be understood as UTC."""
        raw = make_raw_event(created_at="2025-06-01T08:00:00Z")
        row = flatten_event(raw, FAKE_INGESTED_AT)

        assert row["created_at"] == datetime(2025, 6, 1, 8, 0, 0, tzinfo=timezone.utc)

    def test_bad_created_at_falls_back_to_ingested_at(self):
        """If created_at is malformed, don't crash — fall back to ingested_at."""
        raw = make_raw_event(created_at="not-a-date")
        row = flatten_event(raw, FAKE_INGESTED_AT)

        assert row["created_at"] == FAKE_INGESTED_AT

    def test_missing_actor_fields_produce_none(self):
        """Events with no actor dict should not crash — fields default to None."""
        raw = make_raw_event()
        raw.pop("actor")
        row = flatten_event(raw, FAKE_INGESTED_AT)

        assert row["actor_id"] is None
        assert row["actor_login"] is None

    def test_missing_payload_produces_none(self):
        """Events with no payload should set payload_json to None."""
        raw = make_raw_event()
        raw.pop("payload")
        row = flatten_event(raw, FAKE_INGESTED_AT)

        assert row["payload_json"] is None


# ── 3. write_batch tests ──────────────────────────────────────────────────────

class TestWriteBatch:
    def test_write_creates_parquet_file(self, tmp_path: Path):
        """write_batch should return a list with at least one written path."""
        events = [make_raw_event(event_id=f"e{i}") for i in range(3)]
        paths = write_batch(events, data_dir=tmp_path)

        assert len(paths) >= 1
        assert all(p.suffix == ".parquet" for p in paths)
        assert all(p.exists() for p in paths)

    def test_written_file_has_correct_schema(self, tmp_path: Path):
        """The Parquet file's schema must match GITHUB_EVENT_SCHEMA."""
        events = [make_raw_event()]
        paths = write_batch(events, data_dir=tmp_path)

        table = pq.read_table(paths[0])
        assert set(table.schema.names) == set(SCHEMA_COLUMNS)

    def test_written_row_count_matches_input(self, tmp_path: Path):
        """Total rows across all written files must equal the number of events."""
        n = 5
        events = [make_raw_event(event_id=f"e{i}") for i in range(n)]
        paths = write_batch(events, data_dir=tmp_path)

        total_rows = sum(pq.read_table(p).num_rows for p in paths)
        assert total_rows == n

    def test_file_placed_in_date_partition(self, tmp_path: Path):
        """Output file must live inside a date=YYYY-MM-DD/ subdirectory."""
        events = [make_raw_event()]
        paths = write_batch(events, data_dir=tmp_path)

        assert all(p.parent.name.startswith("date=") for p in paths)

    def test_empty_batch_returns_empty_list(self, tmp_path: Path):
        """An empty event list should return [] and create no files."""
        result = write_batch([], data_dir=tmp_path)
        assert result == []
        assert list(tmp_path.glob("**/*.parquet")) == []

    def test_snappy_compression_used(self, tmp_path: Path):
        """We require Snappy compression as per the storage conventions."""
        events = [make_raw_event()]
        paths = write_batch(events, data_dir=tmp_path)

        meta = pq.read_metadata(paths[0])
        for rg in range(meta.num_row_groups):
            for col in range(meta.num_columns):
                compression = meta.row_group(rg).column(col).compression
                assert compression == "SNAPPY"


# ── 4. Reader tests ───────────────────────────────────────────────────────────

class TestReader:
    """
    These tests write real Parquet files to a temp dir, then query them
    through the reader functions. They exercise the full storage round-trip.
    """

    @pytest.fixture(autouse=True)
    def _reset_duckdb(self):
        """
        DuckDB's singleton connection is module-level in reader.py.
        We don't need to reset it between tests — DuckDB reads from files,
        not from in-memory state — but we do need to ensure each test uses
        its own temp dir (handled by tmp_path).
        """
        yield

    def _write_events(self, tmp_path: Path, events: list[dict]) -> None:
        """Helper: write a batch to tmp_path using our writer."""
        write_batch(events, data_dir=tmp_path)

    def test_get_total_event_count(self, tmp_path: Path):
        """get_total_event_count should return the number of rows written."""
        self._write_events(tmp_path, [make_raw_event(event_id=f"e{i}") for i in range(7)])
        assert reader.get_total_event_count(data_dir=tmp_path) == 7

    def test_get_total_event_count_no_data(self, tmp_path: Path):
        """When no Parquet files exist, total count should be 0 (not an error)."""
        assert reader.get_total_event_count(data_dir=tmp_path) == 0

    def test_get_event_counts_by_type(self, tmp_path: Path):
        """Should group and count events by type correctly."""
        events = (
            [make_raw_event(event_id=f"p{i}", event_type="PushEvent") for i in range(4)]
            + [make_raw_event(event_id=f"w{i}", event_type="WatchEvent") for i in range(2)]
        )
        self._write_events(tmp_path, events)

        # Use a large window so all test events are included
        results = reader.get_event_counts_by_type(since_minutes=99999, data_dir=tmp_path)
        by_type = {r["event_type"]: r["event_count"] for r in results}

        assert by_type["PushEvent"] == 4
        assert by_type["WatchEvent"] == 2

    def test_get_event_counts_no_data(self, tmp_path: Path):
        """Empty data dir should return [] not raise an exception."""
        results = reader.get_event_counts_by_type(data_dir=tmp_path)
        assert results == []

    def test_get_top_repos(self, tmp_path: Path):
        """Top repos should be ordered by event count descending."""
        events = (
            [make_raw_event(event_id=f"a{i}", repo_name="popular/repo") for i in range(5)]
            + [make_raw_event(event_id=f"b{i}", repo_name="quiet/repo") for i in range(1)]
        )
        self._write_events(tmp_path, events)

        results = reader.get_top_repos(since_minutes=99999, limit=10, data_dir=tmp_path)
        assert results[0]["repo_name"] == "popular/repo"
        assert results[0]["event_count"] == 5

    def test_get_recent_events_returns_limit(self, tmp_path: Path):
        """get_recent_events should respect the limit parameter."""
        self._write_events(tmp_path, [make_raw_event(event_id=f"e{i}") for i in range(10)])
        results = reader.get_recent_events(limit=3, data_dir=tmp_path)
        assert len(results) == 3

    def test_get_recent_events_returns_dicts(self, tmp_path: Path):
        """Each result row should be a plain dict with the expected keys."""
        self._write_events(tmp_path, [make_raw_event()])
        results = reader.get_recent_events(limit=1, data_dir=tmp_path)
        assert isinstance(results, list)
        assert isinstance(results[0], dict)
        assert "event_type" in results[0]
        assert "repo_name" in results[0]


# ── 5. Late-arriving event tests ──────────────────────────────────────────────

class TestLateArrivingEvents:
    """
    Tests for the watermark / late-event partitioning logic in write_batch.
    """

    def _recent_event(self, event_id: str) -> dict:
        """Event created 1 minute ago — well within any watermark."""
        ts = (datetime.now(tz=timezone.utc) - timedelta(minutes=1)).strftime(
            "%Y-%m-%dT%H:%M:%SZ"
        )
        return make_raw_event(event_id=event_id, created_at=ts)

    def _late_event(self, event_id: str, hours_old: int = 48) -> dict:
        """Event created hours_old hours ago — beyond the 24h watermark."""
        ts = (datetime.now(tz=timezone.utc) - timedelta(hours=hours_old)).strftime(
            "%Y-%m-%dT%H:%M:%SZ"
        )
        return make_raw_event(event_id=event_id, created_at=ts)

    def test_recent_events_go_to_date_partition(self, tmp_path: Path):
        """Fresh events should land in a date=YYYY-MM-DD/ partition."""
        paths = write_batch([self._recent_event("e1")], data_dir=tmp_path)
        assert len(paths) == 1
        assert paths[0].parent.name != "date=late"
        assert paths[0].parent.name.startswith("date=")

    def test_late_events_go_to_late_partition(self, tmp_path: Path):
        """Events older than the watermark must go to date=late/."""
        paths = write_batch(
            [self._late_event("e1", hours_old=48)],
            data_dir=tmp_path,
            threshold_hours=24,
        )
        assert len(paths) == 1
        assert paths[0].parent.name == "date=late"

    def test_mixed_batch_splits_into_two_partitions(self, tmp_path: Path):
        """A batch with both recent and late events should write two files."""
        events = [self._recent_event("r1"), self._late_event("l1", hours_old=48)]
        paths = write_batch(events, data_dir=tmp_path, threshold_hours=24)

        partition_names = {p.parent.name for p in paths}
        assert len(paths) == 2
        assert "date=late" in partition_names
        assert any(n.startswith("date=") and n != "date=late" for n in partition_names)

    def test_late_partition_row_count(self, tmp_path: Path):
        """All late events should be accounted for in date=late/."""
        n_late = 3
        events = [self._late_event(f"l{i}", hours_old=72) for i in range(n_late)]
        paths = write_batch(events, data_dir=tmp_path, threshold_hours=24)

        assert len(paths) == 1
        table = pq.read_table(paths[0])
        assert table.num_rows == n_late

    def test_event_just_inside_watermark_not_late(self, tmp_path: Path):
        """An event 23h old (threshold=24h) should go to its date partition, not late."""
        ts = (datetime.now(tz=timezone.utc) - timedelta(hours=23)).strftime(
            "%Y-%m-%dT%H:%M:%SZ"
        )
        event = make_raw_event(event_id="e1", created_at=ts)
        paths = write_batch([event], data_dir=tmp_path, threshold_hours=24)
        assert paths[0].parent.name != "date=late"
