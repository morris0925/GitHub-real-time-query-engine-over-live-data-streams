"""
anomaly/evidence.py — Live pipeline evidence for grounded diagnosis

The fix for "the LLM has nothing real to reason about": before every
diagnosis or free-text query, compute a snapshot of what is actually
happening in the pipeline right now, straight from the data we already
ingest —

- which workflows are failing most (data/ci_runs/, per-workflow rates)
- which PRs merged inside the window (the "what changed" suspect list)
- current CI failure rate and push cadence vs baseline

The snapshot is (1) formatted into the LLM prompt so summaries can cite
specific workflows and PR numbers, (2) prepended to the raw-evidence panel
so the engineer sees exactly the same facts, and (3) the basis of the
"live snapshot" demo anomaly, which replaces invented numbers with the
repo's real current state.

Everything here is computed — DuckDB over Parquet, no LLM.
"""

import uuid
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import structlog

from anomaly.ci_fetch import CI_DIR, CI_REPO, RUNS_FILENAME
from anomaly.detector import (
    ANOMALY_REPO,
    DATA_DIR,
    WINDOW_HOURS,
    _load_sql,
    _repo_filter,
    _windows_glob,
    ci_failure_rates,
    push_rates,
)

log = structlog.get_logger(__name__)

TOP_WORKFLOWS: int = 5
RECENT_PRS: int = 10


def _rows(sql: str) -> list[dict]:
    cursor = duckdb.connect().execute(sql)
    columns = [desc[0] for desc in cursor.description]
    return [dict(zip(columns, row)) for row in cursor.fetchall()]


# ── Evidence queries ──────────────────────────────────────────────────────────

def top_failing_workflows(
    ci_dir: Path = CI_DIR,
    window_hours: int = WINDOW_HOURS,
    limit: int = TOP_WORKFLOWS,
) -> list[dict]:
    """Workflows ranked by failure rate in the window; [] when no data."""
    path = ci_dir / RUNS_FILENAME
    if not path.exists():
        return []
    return _rows(_load_sql("ci_failing_workflows.sql").format(
        ci_glob=path, since_hours=window_hours, limit=limit))


def recent_merged_prs(
    data_dir: Path = DATA_DIR,
    repo: str | None = ANOMALY_REPO,
    window_hours: int = WINDOW_HOURS,
    limit: int = RECENT_PRS,
) -> list[dict]:
    """PRs merged in the window, newest first; [] when no data."""
    glob = _windows_glob(data_dir)
    if glob is None:
        return []
    return _rows(_load_sql("recent_merged_prs.sql").format(
        data_glob=glob, repo_filter=_repo_filter(repo),
        since_hours=window_hours, limit=limit))


def pipeline_snapshot(
    data_dir: Path = DATA_DIR,
    ci_dir: Path = CI_DIR,
    repo: str | None = ANOMALY_REPO,
) -> dict:
    """Everything the diagnosis should know about the pipeline right now."""
    ci_recent, ci_base = ci_failure_rates(ci_dir)
    push_recent, push_base = push_rates(data_dir, repo)
    return {
        "repo": repo or CI_REPO,
        "window_hours": WINDOW_HOURS,
        "ci_failure_rate": ci_recent.get("failure_rate"),
        "ci_runs": ci_recent.get("run_count"),
        "ci_baseline_failure_rate": ci_base.get("failure_rate"),
        "failing_workflows": top_failing_workflows(ci_dir),
        "merged_prs": recent_merged_prs(data_dir, repo),
        "pushes_per_hour": push_recent.get("pushes_per_hour"),
        "baseline_pushes_per_hour": push_base.get("pushes_per_hour"),
    }


def format_snapshot(snapshot: dict) -> str:
    """
    The snapshot as a text block — used verbatim both in the LLM prompt and
    in the raw-evidence panel, so the model and the engineer see the same
    facts. Missing data is stated plainly, never papered over.
    """
    lines: list[str] = [
        f"Live pipeline snapshot — {snapshot['repo']}, "
        f"last {snapshot['window_hours']}h (computed, not generated)"
    ]

    rate, runs = snapshot.get("ci_failure_rate"), snapshot.get("ci_runs")
    if rate is None:
        lines.append("CI: no workflow-run data in the window")
    else:
        base = snapshot.get("ci_baseline_failure_rate")
        base_text = f" (baseline {base:.0%})" if base is not None else ""
        lines.append(f"CI failure rate: {rate:.0%} over {runs} runs{base_text}")

    workflows = snapshot.get("failing_workflows") or []
    if workflows:
        lines.append("Most-failing workflows:")
        lines.extend(
            f"  - {w['workflow_name']}: {w['failures']}/{w['runs']} failed "
            f"({w['failure_rate']:.0%})"
            for w in workflows
        )

    prs = snapshot.get("merged_prs") or []
    if prs:
        lines.append(f"PRs merged in the window ({len(prs)} most recent):")
        lines.extend(
            f"  - #{p['pr_number']} {p['title']} (@{p['actor_login']})"
            for p in prs
        )
    else:
        lines.append("PRs merged in the window: none observed in the stream yet")

    pushes = snapshot.get("pushes_per_hour")
    if pushes is not None:
        base = snapshot.get("baseline_pushes_per_hour")
        base_text = f" (baseline {base:.1f}/h)" if base else ""
        lines.append(f"Push rate: {pushes:.1f}/h{base_text}")

    return "\n".join(lines)


# ── Live-snapshot demo anomaly ────────────────────────────────────────────────

def build_snapshot_anomaly(snapshot: dict) -> dict | None:
    """
    A demo anomaly built from the repo's REAL current CI state instead of
    invented numbers. Severity follows the signal-bar thresholds. Returns
    None when there is no CI data to snapshot (caller falls back to the
    canned template).
    """
    rate = snapshot.get("ci_failure_rate")
    runs = snapshot.get("ci_runs")
    if rate is None or not runs:
        return None

    severity = "high" if rate >= 0.35 else "medium" if rate >= 0.15 else "low"
    worst = (snapshot.get("failing_workflows") or [None])[0]
    worst_text = (
        f" Worst workflow: {worst['workflow_name']} "
        f"({worst['failures']}/{worst['runs']} failed)."
        if worst else ""
    )
    return {
        "anomaly_id": f"snap-{uuid.uuid4().hex[:8]}",
        "type": "ci_failure_spike",
        "title": f"CI snapshot: {rate:.0%} failure rate ({runs} runs)",
        "severity": severity,
        "description": (
            f"Manually triggered snapshot of {snapshot['repo']}'s current CI "
            f"state over the last {snapshot['window_hours']}h — real data, "
            f"not a detected anomaly.{worst_text}"
        ),
        "metric": {
            "recent_failure_rate": rate,
            "baseline_failure_rate": snapshot.get("ci_baseline_failure_rate"),
            "recent_runs": runs,
        },
        "repo": snapshot.get("repo"),
        "detected_at": datetime.now(tz=timezone.utc),
        "is_demo": True,
    }
