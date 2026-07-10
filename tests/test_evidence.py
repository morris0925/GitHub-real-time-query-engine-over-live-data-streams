"""
tests/test_evidence.py — Unit tests for anomaly/evidence.py

Tests cover:
  1. top_failing_workflows — per-workflow ranking, min-runs guard
  2. recent_merged_prs — PR extraction from payload JSON, repo filter, order
  3. pipeline_snapshot + format_snapshot — real numbers in, honest text out,
     missing data stated plainly
  4. build_snapshot_anomaly — severity thresholds, None without CI data

Run with:
    PYTHONPATH=src pytest tests/test_evidence.py -v
"""

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from anomaly import evidence
from anomaly.ci_fetch import write_runs
from storage.schema import GITHUB_EVENT_SCHEMA


NOW = datetime.now(tz=timezone.utc)


def hours_ago(hours: float) -> datetime:
    return NOW - timedelta(hours=hours)


def make_ci(tmp_path: Path) -> Path:
    """Recent runs: e2e-storage failing hard, unit mostly green, lint 1 run."""
    runs, run_id = [], 0
    for name, conclusions, age in [
        ("e2e-storage", ["failure"] * 4 + ["success"], 2),
        ("unit", ["failure"] + ["success"] * 7, 3),
        ("lint", ["failure"], 4),  # single run — must be excluded (min 2)
    ]:
        for i, conclusion in enumerate(conclusions):
            run_id += 1
            runs.append({
                "run_id": run_id, "repo": "acme/widgets", "workflow_name": name,
                "status": "completed", "conclusion": conclusion,
                "created_at": hours_ago(age + i * 0.01),
            })
    ci_dir = tmp_path / "ci_runs"
    write_runs(runs, ci_dir=ci_dir)
    return ci_dir


def make_events(tmp_path: Path) -> Path:
    """Two merged PRs for acme/widgets (one older), one for another repo."""
    def pr(number: int, title: str, age_hours: float, repo: str) -> dict:
        payload = {
            "action": "closed", "number": number,
            "pull_request": {"merged": True, "title": title},
        }
        return {
            "event_id": f"evt-{number}", "event_type": "PullRequestEvent",
            "actor_id": 1, "actor_login": "alice", "repo_id": 9,
            "repo_name": repo, "payload_json": json.dumps(payload),
            "public": True, "created_at": hours_ago(age_hours),
            "ingested_at": hours_ago(age_hours),
        }

    rows = [
        pr(101, "scheduler: fix retry backoff", 1.0, "acme/widgets"),
        pr(102, "storage: bump csi driver", 5.0, "acme/widgets"),
        pr(999, "unrelated change", 2.0, "other/repo"),
        pr(100, "ancient change", 500.0, "acme/widgets"),  # outside window
    ]
    data_dir = tmp_path / "events"
    partition = data_dir / "date=2026-07-10"
    partition.mkdir(parents=True)
    pq.write_table(pa.Table.from_pylist(rows, schema=GITHUB_EVENT_SCHEMA),
                   partition / "batch.parquet")
    return data_dir


# ── top_failing_workflows ─────────────────────────────────────────────────────

def test_failing_workflows_ranked_and_guarded(tmp_path: Path) -> None:
    workflows = evidence.top_failing_workflows(make_ci(tmp_path))
    names = [w["workflow_name"] for w in workflows]
    assert names == ["e2e-storage", "unit"]        # lint excluded (1 run)
    assert workflows[0]["failure_rate"] == pytest.approx(0.8)
    assert workflows[0]["failures"] == 4


def test_failing_workflows_no_data(tmp_path: Path) -> None:
    assert evidence.top_failing_workflows(tmp_path) == []


# ── recent_merged_prs ─────────────────────────────────────────────────────────

def test_recent_merged_prs_filtered_and_ordered(tmp_path: Path) -> None:
    prs = evidence.recent_merged_prs(make_events(tmp_path), repo="acme/widgets")
    assert [p["pr_number"] for p in prs] == ["101", "102"]   # newest first,
    assert prs[0]["title"] == "scheduler: fix retry backoff"  # window + repo scoped


def test_recent_merged_prs_no_data(tmp_path: Path) -> None:
    assert evidence.recent_merged_prs(tmp_path / "none", repo=None) == []


# ── snapshot + formatting ─────────────────────────────────────────────────────

def test_snapshot_and_format_with_data(tmp_path: Path) -> None:
    snapshot = evidence.pipeline_snapshot(
        make_events(tmp_path), make_ci(tmp_path), repo="acme/widgets"
    )
    text = evidence.format_snapshot(snapshot)
    assert "acme/widgets" in text
    assert "e2e-storage: 4/5 failed (80%)" in text
    assert "#101 scheduler: fix retry backoff (@alice)" in text
    assert "computed, not generated" in text


def test_format_snapshot_states_missing_data(tmp_path: Path) -> None:
    snapshot = evidence.pipeline_snapshot(
        tmp_path / "events", tmp_path / "ci", repo="acme/widgets"
    )
    text = evidence.format_snapshot(snapshot)
    assert "no workflow-run data" in text
    assert "none observed in the stream yet" in text


# ── build_snapshot_anomaly ────────────────────────────────────────────────────

def test_snapshot_anomaly_from_real_data(tmp_path: Path) -> None:
    snapshot = evidence.pipeline_snapshot(
        make_events(tmp_path), make_ci(tmp_path), repo="acme/widgets"
    )
    anomaly = evidence.build_snapshot_anomaly(snapshot)
    assert anomaly is not None
    assert anomaly["is_demo"] is True
    assert anomaly["anomaly_id"].startswith("snap-")
    assert anomaly["severity"] == "high"            # 5/13 failures ≈ 38% ≥ 0.35
    assert "real data, not a detected anomaly" in anomaly["description"]
    assert "e2e-storage" in anomaly["description"]  # worst workflow named


def test_snapshot_anomaly_none_without_ci(tmp_path: Path) -> None:
    snapshot = evidence.pipeline_snapshot(tmp_path / "e", tmp_path / "c", repo=None)
    assert evidence.build_snapshot_anomaly(snapshot) is None
