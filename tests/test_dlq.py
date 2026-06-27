"""
tests/test_dlq.py — Unit tests for the Dead Letter Queue layer

Tests cover:
  1. write_dlq_entry() — writes a valid Parquet file with DLQ schema
  2. write_dlq_entry() — handles events with missing/None fields gracefully
  3. inspect_dlq() — reads DLQ entries back via DuckDB
  4. inspect_dlq() — returns empty list when dir missing or empty
  5. DLQWriteError — raised on I/O failure

Run with:
    pytest tests/test_dlq.py -v
"""

import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch, MagicMock

import pyarrow.parquet as pq
import pytest

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from storage.dlq_writer import write_dlq_entry, DLQWriteError, DLQ_SCHEMA
from storage.reader import inspect_dlq


# ── Helpers ───────────────────────────────────────────────────────────────────

def make_event(event_id: str = "evt-1", event_type: str = "PushEvent") -> dict:
    return {
        "id":         event_id,
        "type":       event_type,
        "actor":      {"login": "alice"},
        "repo":       {"name": "alice/repo"},
        "created_at": "2026-06-28T10:00:00Z",
    }


# ── write_dlq_entry ───────────────────────────────────────────────────────────

class TestWriteDlqEntry:
    def test_creates_parquet_file(self, tmp_path):
        event = make_event()
        path = write_dlq_entry(event, reason="test reason", dlq_dir=tmp_path)
        assert path.exists()
        assert path.suffix == ".parquet"

    def test_filename_starts_with_dlq(self, tmp_path):
        event = make_event()
        path = write_dlq_entry(event, reason="test", dlq_dir=tmp_path)
        assert path.name.startswith("dlq-")

    def test_file_has_correct_schema(self, tmp_path):
        event = make_event()
        path = write_dlq_entry(event, reason="missing ref", dlq_dir=tmp_path)
        table = pq.read_table(path)
        for field_name in ["event_id", "event_type", "error_reason", "raw_json", "failed_at"]:
            assert field_name in table.schema.names

    def test_event_id_stored(self, tmp_path):
        event = make_event(event_id="evt-abc")
        path = write_dlq_entry(event, reason="broken", dlq_dir=tmp_path)
        table = pq.read_table(path)
        assert table.column("event_id")[0].as_py() == "evt-abc"

    def test_event_type_stored(self, tmp_path):
        event = make_event(event_type="WatchEvent")
        path = write_dlq_entry(event, reason="broken", dlq_dir=tmp_path)
        table = pq.read_table(path)
        assert table.column("event_type")[0].as_py() == "WatchEvent"

    def test_reason_stored(self, tmp_path):
        event = make_event()
        path = write_dlq_entry(event, reason="payload.ref is missing", dlq_dir=tmp_path)
        table = pq.read_table(path)
        assert table.column("error_reason")[0].as_py() == "payload.ref is missing"

    def test_raw_json_is_valid_json(self, tmp_path):
        event = make_event()
        path = write_dlq_entry(event, reason="broken", dlq_dir=tmp_path)
        table = pq.read_table(path)
        raw = table.column("raw_json")[0].as_py()
        parsed = json.loads(raw)
        assert parsed["id"] == "evt-1"

    def test_failed_at_is_utc_timestamp(self, tmp_path):
        event = make_event()
        path = write_dlq_entry(event, reason="broken", dlq_dir=tmp_path)
        table = pq.read_table(path)
        failed_at = table.column("failed_at")[0].as_py()
        assert failed_at is not None
        # The timestamp should be recent (within last 5 seconds)
        now = datetime.now(timezone.utc)
        delta = abs((now - failed_at).total_seconds())
        assert delta < 5

    def test_creates_dlq_dir_if_missing(self, tmp_path):
        nested = tmp_path / "new" / "dlq"
        assert not nested.exists()
        write_dlq_entry(make_event(), reason="test", dlq_dir=nested)
        assert nested.exists()

    def test_event_with_no_id_is_handled(self, tmp_path):
        """Events without 'id' should write an empty string, not crash."""
        event = {"type": "PushEvent"}
        path = write_dlq_entry(event, reason="no id", dlq_dir=tmp_path)
        table = pq.read_table(path)
        assert table.column("event_id")[0].as_py() == ""

    def test_event_with_no_type_is_handled(self, tmp_path):
        event = {"id": "e1"}
        path = write_dlq_entry(event, reason="no type", dlq_dir=tmp_path)
        table = pq.read_table(path)
        assert table.column("event_type")[0].as_py() == "unknown"

    def test_multiple_entries_create_multiple_files(self, tmp_path):
        for i in range(3):
            write_dlq_entry(make_event(event_id=f"evt-{i}"), reason="broken", dlq_dir=tmp_path)
        parquet_files = list(tmp_path.glob("*.parquet"))
        assert len(parquet_files) == 3

    def test_raises_dlq_write_error_on_io_failure(self, tmp_path):
        """If the write fails, DLQWriteError should be raised."""
        event = make_event()
        with patch("storage.dlq_writer.pq.write_table", side_effect=OSError("disk full")):
            with pytest.raises(DLQWriteError) as exc_info:
                write_dlq_entry(event, reason="test", dlq_dir=tmp_path)
        assert "disk full" in str(exc_info.value)


# ── inspect_dlq ───────────────────────────────────────────────────────────────

class TestInspectDlq:
    def test_returns_empty_list_when_dir_missing(self, tmp_path):
        missing = tmp_path / "nonexistent"
        result = inspect_dlq(dlq_dir=missing)
        assert result == []

    def test_returns_empty_list_when_dir_empty(self, tmp_path):
        result = inspect_dlq(dlq_dir=tmp_path)
        assert result == []

    def test_returns_entries_after_writing(self, tmp_path):
        write_dlq_entry(make_event("evt-1"), reason="ref missing", dlq_dir=tmp_path)
        write_dlq_entry(make_event("evt-2"), reason="no commits", dlq_dir=tmp_path)
        rows = inspect_dlq(dlq_dir=tmp_path)
        assert len(rows) == 2

    def test_rows_have_expected_keys(self, tmp_path):
        write_dlq_entry(make_event(), reason="broken", dlq_dir=tmp_path)
        rows = inspect_dlq(dlq_dir=tmp_path)
        assert len(rows) == 1
        row = rows[0]
        for key in ("event_id", "event_type", "error_reason", "raw_json", "failed_at"):
            assert key in row

    def test_event_id_roundtrip(self, tmp_path):
        write_dlq_entry(make_event("evt-roundtrip"), reason="test", dlq_dir=tmp_path)
        rows = inspect_dlq(dlq_dir=tmp_path)
        assert rows[0]["event_id"] == "evt-roundtrip"

    def test_reason_roundtrip(self, tmp_path):
        write_dlq_entry(make_event(), reason="payload.ref is missing", dlq_dir=tmp_path)
        rows = inspect_dlq(dlq_dir=tmp_path)
        assert rows[0]["error_reason"] == "payload.ref is missing"

    def test_limit_is_respected(self, tmp_path):
        for i in range(5):
            write_dlq_entry(make_event(f"evt-{i}"), reason="broken", dlq_dir=tmp_path)
        rows = inspect_dlq(limit=3, dlq_dir=tmp_path)
        assert len(rows) <= 3

    def test_ordered_by_failed_at_desc(self, tmp_path):
        """Most recent failure should appear first."""
        for i in range(3):
            write_dlq_entry(make_event(f"evt-{i}"), reason="broken", dlq_dir=tmp_path)
        rows = inspect_dlq(dlq_dir=tmp_path)
        # Timestamps should be descending (or equal, since all written in same second)
        timestamps = [r["failed_at"] for r in rows]
        assert timestamps == sorted(timestamps, reverse=True)
