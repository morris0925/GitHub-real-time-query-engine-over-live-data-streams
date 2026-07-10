"""
tests/test_anomaly.py — Unit tests for anomaly/detector.py, store.py, ci_fetch.py

Tests cover:
  1. CI failure spike — fires on a genuine spike, silent on healthy/thin data
  2. Merge-time anomaly — fires when recent avg stretches past 1.5x baseline
  3. Commit drought — fires when push rate collapses
  4. pipeline_signal — ok/warn/alert/unknown component statuses
  5. store — save/load round trip, dedup by anomaly_id, demo seeding

Fixtures write real Parquet files (events with raw payload JSON, CI runs)
into tmp_path — the same shapes the pipeline produces — and the detector
queries them through the same .sql files used in production.

Run with:
    PYTHONPATH=src pytest tests/test_anomaly.py -v
"""

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from anomaly import detector, store
from anomaly.ci_fetch import CI_RUN_SCHEMA, RUNS_FILENAME, write_runs
from storage.schema import GITHUB_EVENT_SCHEMA


NOW = datetime.now(tz=timezone.utc)


# ── Fixture builders ──────────────────────────────────────────────────────────

def hours_ago(hours: float) -> datetime:
    return NOW - timedelta(hours=hours)


def write_ci_runs(tmp_path: Path, recent_failures: int, recent_total: int,
                  baseline_failures: int, baseline_total: int) -> Path:
    """CI runs parquet: recent = last 12h, baseline = 48-120h ago."""
    runs = []
    run_id = 0
    for count, failures, age in [
        (recent_total, recent_failures, 12),
        (baseline_total, baseline_failures, 48),
    ]:
        for i in range(count):
            run_id += 1
            runs.append({
                "run_id": run_id,
                "repo": "acme/widgets",
                "workflow_name": "ci",
                "status": "completed",
                "conclusion": "failure" if i < failures else "success",
                "created_at": hours_ago(age + i * 0.01),
            })
    ci_dir = tmp_path / "ci_runs"
    write_runs(runs, ci_dir=ci_dir)
    return ci_dir


def write_events(tmp_path: Path, rows: list[dict]) -> Path:
    """Events parquet in a date partition, matching the pipeline layout."""
    data_dir = tmp_path / "events"
    partition = data_dir / "date=2026-07-07"
    partition.mkdir(parents=True, exist_ok=True)
    full_rows = [
        {
            "event_id": f"evt-{i}",
            "event_type": row["event_type"],
            "actor_id": 1,
            "actor_login": "alice",
            "repo_id": 99,
            "repo_name": row.get("repo_name", "acme/widgets"),
            "payload_json": row.get("payload_json"),
            "public": True,
            "created_at": row["created_at"],
            "ingested_at": row["created_at"],
        }
        for i, row in enumerate(rows)
    ]
    table = pa.Table.from_pylist(full_rows, schema=GITHUB_EVENT_SCHEMA)
    pq.write_table(table, partition / "batch.parquet")
    return data_dir


def merged_pr_row(event_age_hours: float, merge_duration_hours: float) -> dict:
    """A PullRequestEvent for a PR merged after merge_duration_hours."""
    merged_at = hours_ago(event_age_hours)
    created_at = merged_at - timedelta(hours=merge_duration_hours)
    payload = {
        "action": "closed",
        "number": 1,
        "pull_request": {
            "merged": True,
            "created_at": created_at.isoformat(),
            "merged_at": merged_at.isoformat(),
        },
    }
    return {
        "event_type": "PullRequestEvent",
        "payload_json": json.dumps(payload),
        "created_at": merged_at,
    }


def push_row(event_age_hours: float) -> dict:
    return {"event_type": "PushEvent", "payload_json": "{}", "created_at": hours_ago(event_age_hours)}


# ── CI failure spike ──────────────────────────────────────────────────────────

def test_ci_spike_detected(tmp_path: Path) -> None:
    ci_dir = write_ci_runs(tmp_path, recent_failures=6, recent_total=10,
                           baseline_failures=4, baseline_total=40)
    anomaly = detector.detect_ci_failure_spike(ci_dir)
    assert anomaly is not None
    assert anomaly["type"] == "ci_failure_spike"
    assert anomaly["severity"] == "high"  # 60% vs 10% baseline
    assert anomaly["metric"]["recent_failure_rate"] == pytest.approx(0.6)
    assert anomaly["is_demo"] is False


def test_ci_spike_silent_when_healthy(tmp_path: Path) -> None:
    ci_dir = write_ci_runs(tmp_path, recent_failures=1, recent_total=10,
                           baseline_failures=4, baseline_total=40)
    assert detector.detect_ci_failure_spike(ci_dir) is None


def test_ci_spike_silent_on_thin_data(tmp_path: Path) -> None:
    ci_dir = write_ci_runs(tmp_path, recent_failures=2, recent_total=3,
                           baseline_failures=0, baseline_total=10)
    assert detector.detect_ci_failure_spike(ci_dir) is None  # < MIN_CI_RUNS


def test_ci_spike_silent_without_data(tmp_path: Path) -> None:
    assert detector.detect_ci_failure_spike(tmp_path) is None


# ── Merge-time anomaly ────────────────────────────────────────────────────────

def test_merge_time_anomaly_detected(tmp_path: Path) -> None:
    rows = (
        [merged_pr_row(age, 20.0) for age in (2, 5, 8)]          # recent: 20h avg
        + [merged_pr_row(age, 5.0) for age in (30, 50, 70, 90)]  # baseline: 5h avg
    )
    data_dir = write_events(tmp_path, rows)
    # repo=None everywhere below: rule behavior must not depend on the
    # developer's .env (ANOMALY_REPO); scoping is tested separately.
    anomaly = detector.detect_merge_time_anomaly(data_dir, repo=None)
    assert anomaly is not None
    assert anomaly["type"] == "merge_time_anomaly"
    assert anomaly["severity"] == "high"  # 4x baseline
    assert anomaly["metric"]["ratio"] == pytest.approx(4.0, rel=0.05)


def test_merge_time_silent_when_stable(tmp_path: Path) -> None:
    rows = [merged_pr_row(age, 6.0) for age in (2, 5, 8, 30, 50, 70)]
    assert detector.detect_merge_time_anomaly(write_events(tmp_path, rows), repo=None) is None


def test_merge_time_silent_on_thin_data(tmp_path: Path) -> None:
    rows = [merged_pr_row(2, 20.0)] + [merged_pr_row(age, 5.0) for age in (30, 50, 70)]
    assert detector.detect_merge_time_anomaly(write_events(tmp_path, rows), repo=None) is None


# ── Commit drought ────────────────────────────────────────────────────────────

def test_commit_drought_detected(tmp_path: Path) -> None:
    # Baseline: 216 pushes across 24-168h ago (1.5/h). Recent: none.
    rows = [push_row(24 + i * (144 / 216)) for i in range(216)]
    data_dir = write_events(tmp_path, rows)
    anomaly = detector.detect_commit_drought(data_dir, repo=None)
    assert anomaly is not None
    assert anomaly["type"] == "commit_drought"
    assert anomaly["severity"] == "high"  # zero recent pushes
    assert anomaly["metric"]["recent_pushes_per_hour"] == 0.0


def test_commit_drought_silent_when_active(tmp_path: Path) -> None:
    rows = [push_row(24 + i * (144 / 216)) for i in range(216)]
    rows += [push_row(i * 0.5) for i in range(48)]  # 2/h recent
    assert detector.detect_commit_drought(write_events(tmp_path, rows), repo=None) is None


def test_commit_drought_silent_on_quiet_baseline(tmp_path: Path) -> None:
    rows = [push_row(30 + i * 10) for i in range(10)]  # ~0.07/h baseline
    assert detector.detect_commit_drought(write_events(tmp_path, rows), repo=None) is None


# ── Repo scoping ──────────────────────────────────────────────────────────────

def test_commit_drought_repo_filter_ignores_other_repos(tmp_path: Path) -> None:
    # acme/widgets: healthy baseline, zero recent pushes → drought.
    rows = [push_row(24 + i * (144 / 216)) for i in range(216)]
    # other/repo: constant recent activity that would mask the drought
    # if the filter leaked.
    rows += [
        {**push_row(i * 0.5), "repo_name": "other/repo"} for i in range(48)
    ]
    data_dir = write_events(tmp_path, rows)

    unscoped = detector.detect_commit_drought(data_dir, repo=None)
    scoped = detector.detect_commit_drought(data_dir, repo="acme/widgets")

    assert unscoped is None                    # other/repo's pushes mask it
    assert scoped is not None                  # filter isolates the drought
    assert scoped["repo"] == "acme/widgets"
    assert scoped["metric"]["recent_pushes_per_hour"] == 0.0


def test_merge_time_repo_filter(tmp_path: Path) -> None:
    rows = (
        [merged_pr_row(age, 20.0) for age in (2, 5, 8)]
        + [merged_pr_row(age, 5.0) for age in (30, 50, 70, 90)]
    )
    data_dir = write_events(tmp_path, rows)
    # Filtering to a repo with no events → silent, never an error.
    assert detector.detect_merge_time_anomaly(data_dir, repo="other/repo") is None
    assert detector.detect_merge_time_anomaly(data_dir, repo="acme/widgets") is not None


# ── detect_all + pipeline_signal ──────────────────────────────────────────────

def test_detect_all_combines_rules(tmp_path: Path) -> None:
    ci_dir = write_ci_runs(tmp_path, recent_failures=6, recent_total=10,
                           baseline_failures=4, baseline_total=40)
    rows = [push_row(24 + i * (144 / 216)) for i in range(216)]
    data_dir = write_events(tmp_path, rows)
    types = {a["type"] for a in detector.detect_all(data_dir, ci_dir, repo=None)}
    assert types == {"ci_failure_spike", "commit_drought"}


def test_pipeline_signal_statuses(tmp_path: Path) -> None:
    ci_dir = write_ci_runs(tmp_path, recent_failures=6, recent_total=10,
                           baseline_failures=4, baseline_total=40)
    rows = [push_row(24 + i * (144 / 216)) for i in range(216)]
    data_dir = write_events(tmp_path, rows)

    signal = detector.pipeline_signal(data_dir, ci_dir, repo=None)
    assert signal["ci_stability"]["status"] == "alert"       # 60% failure rate
    assert signal["commit_cadence"]["status"] == "alert"     # zero recent pushes
    assert signal["pr_velocity"]["status"] == "unknown"      # no PR events at all
    assert "not a live production health check" in signal["caption"]


def test_pipeline_signal_unknown_without_any_data(tmp_path: Path) -> None:
    signal = detector.pipeline_signal(tmp_path / "events", tmp_path / "ci", repo=None)
    assert all(
        signal[key]["status"] == "unknown"
        for key in ("ci_stability", "pr_velocity", "commit_cadence")
    )


# ── Store ─────────────────────────────────────────────────────────────────────

def test_store_round_trip_and_dedup(tmp_path: Path) -> None:
    anomaly = detector._make_anomaly(
        "ci_failure_spike", "CI spiked", "high", "desc", {"rate": 0.5}
    )
    store.save_anomalies([anomaly], anomaly_dir=tmp_path)
    store.save_anomalies([anomaly], anomaly_dir=tmp_path)  # same id → dedup

    loaded = store.load_anomalies(anomaly_dir=tmp_path)
    assert len(loaded) == 1
    assert loaded[0]["anomaly_id"] == anomaly["anomaly_id"]
    assert loaded[0]["metric"] == {"rate": 0.5}  # JSON round trip


def test_store_empty_dir(tmp_path: Path) -> None:
    assert store.load_anomalies(anomaly_dir=tmp_path) == []


def test_seed_demo_anomaly(tmp_path: Path) -> None:
    seeded = store.seed_demo_anomaly("merge_time_anomaly", anomaly_dir=tmp_path)
    assert seeded["is_demo"] is True
    assert seeded["anomaly_id"].startswith("demo-")

    loaded = store.load_anomalies(anomaly_dir=tmp_path)
    assert loaded[0]["anomaly_id"] == seeded["anomaly_id"]
    assert loaded[0]["metric"]["recent_avg_merge_hours"] == pytest.approx(31.2)


def test_seed_demo_anomaly_unknown_type(tmp_path: Path) -> None:
    with pytest.raises(KeyError):
        store.seed_demo_anomaly("nonsense_type", anomaly_dir=tmp_path)


def test_ci_run_schema_columns() -> None:
    assert CI_RUN_SCHEMA.names == [
        "run_id", "repo", "workflow_name", "status", "conclusion", "created_at"
    ]
    assert RUNS_FILENAME == "runs.parquet"
