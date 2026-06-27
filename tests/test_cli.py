"""
tests/test_cli.py — Unit tests for the CLI query interface (src/cli.py)

We use Click's CliRunner to invoke CLI commands in-process without
spawning a subprocess. This is the standard approach for testing Click apps.

All reader functions are mocked so tests don't need real Parquet files.
This keeps the tests fast and isolated — the reader layer has its own
tests in test_storage.py.

Run with:
    pytest tests/test_cli.py -v
"""

import sys
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

import pytest
from click.testing import CliRunner

# Ensure src/ is on the path when running from project root
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from cli import cli


# ── Fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture
def runner() -> CliRunner:
    # Wide terminal so Rich doesn't truncate table cells with "…"
    return CliRunner(mix_stderr=False)


SAMPLE_EVENTS = [
    {
        "event_type":  "PushEvent",
        "actor_login": "alice",
        "repo_name":   "alice/myrepo",
        "created_at":  datetime(2026, 6, 28, 10, 0, 0, tzinfo=timezone.utc),
    },
    {
        "event_type":  "WatchEvent",
        "actor_login": "bob",
        "repo_name":   "torvalds/linux",
        "created_at":  datetime(2026, 6, 28, 9, 55, 0, tzinfo=timezone.utc),
    },
]

SAMPLE_STATS = [
    {"event_type": "PushEvent",  "event_count": 10},
    {"event_type": "WatchEvent", "event_count": 5},
]

SAMPLE_REPOS = [
    {"repo_name": "torvalds/linux", "event_count": 20, "unique_actors": 5},
    {"repo_name": "django/django",  "event_count": 8,  "unique_actors": 3},
]

SAMPLE_LAG = {
    "avg_lag_seconds": 25.0,
    "min_lag_seconds": 5.0,
    "max_lag_seconds": 45.0,
    "sample_size":     100,
}

SAMPLE_DLQ = [
    {
        "event_id":     "evt-broken-1",
        "event_type":   "PushEvent",
        "error_reason": "payload.ref is missing",
        "raw_json":     '{"id": "evt-broken-1"}',
        "failed_at":    datetime(2026, 6, 28, 10, 5, 0, tzinfo=timezone.utc),
    }
]


# ── events command ────────────────────────────────────────────────────────────

class TestEventsCommand:
    def test_events_shows_table(self, runner):
        with patch("cli.reader.get_recent_events", return_value=SAMPLE_EVENTS):
            result = runner.invoke(cli, ["events"])
        assert result.exit_code == 0
        assert "PushEvent" in result.output
        assert "alice" in result.output

    def test_events_shows_repo_name(self, runner):
        with patch("cli.reader.get_recent_events", return_value=SAMPLE_EVENTS):
            result = runner.invoke(cli, ["events"])
        # Rich may truncate long repo names — check prefix is present
        assert "alice/myr" in result.output

    def test_events_filter_by_type(self, runner):
        with patch("cli.reader.get_recent_events", return_value=SAMPLE_EVENTS):
            result = runner.invoke(cli, ["events", "--type", "PushEvent"])
        assert result.exit_code == 0
        assert "PushEvent" in result.output
        # WatchEvent should be filtered out
        assert "WatchEvent" not in result.output

    def test_events_empty_result(self, runner):
        with patch("cli.reader.get_recent_events", return_value=[]):
            result = runner.invoke(cli, ["events"])
        assert result.exit_code == 0
        assert "No events" in result.output

    def test_events_respects_limit_option(self, runner):
        """--limit should be passed to reader (we check it's accepted)."""
        with patch("cli.reader.get_recent_events", return_value=SAMPLE_EVENTS) as mock:
            result = runner.invoke(cli, ["events", "--limit", "5"])
        assert result.exit_code == 0

    def test_events_shows_count_footer(self, runner):
        with patch("cli.reader.get_recent_events", return_value=SAMPLE_EVENTS):
            result = runner.invoke(cli, ["events"])
        assert "2 events" in result.output


# ── stats command ─────────────────────────────────────────────────────────────

class TestStatsCommand:
    def test_stats_shows_event_types(self, runner):
        with patch("cli.reader.get_event_counts_by_type", return_value=SAMPLE_STATS):
            result = runner.invoke(cli, ["stats"])
        assert result.exit_code == 0
        assert "PushEvent" in result.output
        assert "WatchEvent" in result.output

    def test_stats_shows_counts(self, runner):
        with patch("cli.reader.get_event_counts_by_type", return_value=SAMPLE_STATS):
            result = runner.invoke(cli, ["stats"])
        assert "10" in result.output
        assert "5" in result.output

    def test_stats_shows_total_footer(self, runner):
        with patch("cli.reader.get_event_counts_by_type", return_value=SAMPLE_STATS):
            result = runner.invoke(cli, ["stats"])
        assert "15" in result.output   # total = 10 + 5

    def test_stats_empty(self, runner):
        with patch("cli.reader.get_event_counts_by_type", return_value=[]):
            result = runner.invoke(cli, ["stats"])
        assert result.exit_code == 0
        assert "No events" in result.output

    def test_stats_since_option_accepted(self, runner):
        with patch("cli.reader.get_event_counts_by_type", return_value=SAMPLE_STATS) as mock:
            result = runner.invoke(cli, ["stats", "--since", "30"])
        assert result.exit_code == 0


# ── repos command ─────────────────────────────────────────────────────────────

class TestReposCommand:
    def test_repos_shows_repo_names(self, runner):
        with patch("cli.reader.get_top_repos", return_value=SAMPLE_REPOS):
            result = runner.invoke(cli, ["repos"])
        assert result.exit_code == 0
        assert "torvalds/linux" in result.output
        assert "django/django" in result.output

    def test_repos_shows_event_counts(self, runner):
        with patch("cli.reader.get_top_repos", return_value=SAMPLE_REPOS):
            result = runner.invoke(cli, ["repos"])
        assert "20" in result.output

    def test_repos_shows_rank_numbers(self, runner):
        with patch("cli.reader.get_top_repos", return_value=SAMPLE_REPOS):
            result = runner.invoke(cli, ["repos"])
        assert "1" in result.output
        assert "2" in result.output

    def test_repos_empty(self, runner):
        with patch("cli.reader.get_top_repos", return_value=[]):
            result = runner.invoke(cli, ["repos"])
        assert result.exit_code == 0
        assert "No data" in result.output

    def test_repos_top_option_accepted(self, runner):
        with patch("cli.reader.get_top_repos", return_value=SAMPLE_REPOS):
            result = runner.invoke(cli, ["repos", "--top", "5"])
        assert result.exit_code == 0


# ── lag command ───────────────────────────────────────────────────────────────

class TestLagCommand:
    def test_lag_shows_avg(self, runner):
        with patch("cli.reader.get_avg_lag", return_value=SAMPLE_LAG):
            result = runner.invoke(cli, ["lag"])
        assert result.exit_code == 0
        assert "25.0s" in result.output

    def test_lag_shows_min_max(self, runner):
        with patch("cli.reader.get_avg_lag", return_value=SAMPLE_LAG):
            result = runner.invoke(cli, ["lag"])
        assert "5.0s" in result.output   # min
        assert "45.0s" in result.output  # max

    def test_lag_healthy_status(self, runner):
        """avg < 30s → green / healthy."""
        with patch("cli.reader.get_avg_lag", return_value=SAMPLE_LAG):
            result = runner.invoke(cli, ["lag"])
        assert "healthy" in result.output

    def test_lag_elevated_status(self, runner):
        """30 ≤ avg < 60s → yellow / elevated."""
        elevated = {**SAMPLE_LAG, "avg_lag_seconds": 45.0}
        with patch("cli.reader.get_avg_lag", return_value=elevated):
            result = runner.invoke(cli, ["lag"])
        assert "elevated" in result.output

    def test_lag_high_status(self, runner):
        """avg ≥ 60s → red / high."""
        high = {**SAMPLE_LAG, "avg_lag_seconds": 90.0}
        with patch("cli.reader.get_avg_lag", return_value=high):
            result = runner.invoke(cli, ["lag"])
        assert "high" in result.output

    def test_lag_no_data(self, runner):
        with patch("cli.reader.get_avg_lag", return_value=None):
            result = runner.invoke(cli, ["lag"])
        assert result.exit_code == 0
        assert "No lag data" in result.output


# ── dlq command ───────────────────────────────────────────────────────────────

class TestDlqCommand:
    def test_dlq_empty(self, runner):
        with patch("cli.reader.inspect_dlq", return_value=[]):
            result = runner.invoke(cli, ["dlq"])
        assert result.exit_code == 0
        assert "empty" in result.output.lower()

    def test_dlq_shows_entries(self, runner):
        with patch("cli.reader.inspect_dlq", return_value=SAMPLE_DLQ):
            result = runner.invoke(cli, ["dlq"])
        assert result.exit_code == 0
        assert "evt-broken-1" in result.output

    def test_dlq_shows_reason(self, runner):
        with patch("cli.reader.inspect_dlq", return_value=SAMPLE_DLQ):
            result = runner.invoke(cli, ["dlq"])
        # Rich may wrap long text across lines — check the key phrase is present
        assert "payload.ref" in result.output

    def test_dlq_shows_count(self, runner):
        with patch("cli.reader.inspect_dlq", return_value=SAMPLE_DLQ):
            result = runner.invoke(cli, ["dlq"])
        assert "1" in result.output

    def test_dlq_limit_option_accepted(self, runner):
        with patch("cli.reader.inspect_dlq", return_value=SAMPLE_DLQ):
            result = runner.invoke(cli, ["dlq", "--limit", "5"])
        assert result.exit_code == 0


# ── global options ────────────────────────────────────────────────────────────

class TestGlobalOptions:
    def test_help_flag_works(self, runner):
        result = runner.invoke(cli, ["--help"])
        assert result.exit_code == 0
        assert "StreamLens" in result.output

    def test_events_help_shows_options(self, runner):
        result = runner.invoke(cli, ["events", "--help"])
        assert result.exit_code == 0
        assert "--limit" in result.output
        assert "--type" in result.output

    def test_stats_help_shows_since(self, runner):
        result = runner.invoke(cli, ["stats", "--help"])
        assert "--since" in result.output

    def test_repos_help_shows_top(self, runner):
        result = runner.invoke(cli, ["repos", "--help"])
        assert "--top" in result.output
