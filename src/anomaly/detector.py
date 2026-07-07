"""
anomaly/detector.py — Tier 1 rule-based anomaly detection + Dev Pipeline Signal

Three rules, each comparing a recent window against a longer baseline window
(design proposal §3 Tier 1 — development-process proxies, not production
health):

1. ci_failure_spike    — CI failure rate (from data/ci_runs/) jumps vs baseline
2. merge_time_anomaly  — average PR merge duration stretches vs baseline
3. commit_drought      — PushEvent rate collapses vs baseline

Also computes the three Dev Pipeline Signal components (CI stability /
PR velocity / commit cadence) shown side by side at the top of the dashboard,
deliberately NOT collapsed into one number — "CI is failing" and "everyone's
on vacation" must stay distinguishable.

Everything here is deterministic SQL over Parquet — no LLM involvement.
Anomaly IDs are deterministic per (type, hour) so re-running detection within
the same hour dedupes instead of stuttering out duplicates.
"""

import hashlib
import os
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import structlog
from dotenv import load_dotenv

from anomaly.ci_fetch import CI_DIR, RUNS_FILENAME

load_dotenv()

log = structlog.get_logger(__name__)

# ── Configuration ─────────────────────────────────────────────────────────────

DATA_DIR: Path = Path(os.getenv("DATA_DIR", "data/events"))

WINDOW_HOURS:   int = int(os.getenv("ANOMALY_WINDOW_HOURS", "24"))
BASELINE_HOURS: int = int(os.getenv("ANOMALY_BASELINE_HOURS", "168"))

# Minimum sample sizes below which a rule stays silent rather than guessing.
MIN_CI_RUNS:   int = 5
MIN_MERGED_PRS: int = 3
MIN_BASELINE_PUSH_RATE: float = 1.0  # pushes/hour

_QUERIES_DIR = Path(__file__).parent.parent / "storage" / "queries"

SIGNAL_CAPTION: str = (
    "Based on CI/PR/commit signals — not a live production health check"
)


# ── Internal helpers ──────────────────────────────────────────────────────────

def _load_sql(filename: str) -> str:
    return (_QUERIES_DIR / filename).read_text(encoding="utf-8")


def _one_row(sql: str) -> dict:
    """Run SQL on a fresh in-memory connection, return the single row as a dict."""
    cursor = duckdb.connect().execute(sql)
    columns = [desc[0] for desc in cursor.description]
    row = cursor.fetchone()
    return dict(zip(columns, row)) if row else {}


def _anomaly_id(anomaly_type: str, now: datetime) -> str:
    """Deterministic per (type, hour) — repeated detection dedupes."""
    bucket = now.strftime("%Y-%m-%dT%H")
    return hashlib.sha1(f"{anomaly_type}:{bucket}".encode("utf-8")).hexdigest()[:12]


def _make_anomaly(
    anomaly_type: str,
    title: str,
    severity: str,
    description: str,
    metric: dict,
    repo: str | None = None,
) -> dict:
    now = datetime.now(tz=timezone.utc)
    return {
        "anomaly_id": _anomaly_id(anomaly_type, now),
        "type": anomaly_type,
        "title": title,
        "severity": severity,
        "description": description,
        "metric": metric,
        "repo": repo,
        "detected_at": now,
        "is_demo": False,
    }


def _windows_glob(data_dir: Path) -> str | None:
    """Events glob, or None when no Parquet exists yet (rules stay silent)."""
    if not list(data_dir.glob("**/*.parquet")):
        return None
    return str(data_dir / "**" / "*.parquet")


# ── Metric queries (shared by rules and the signal bar) ───────────────────────

def ci_failure_rates(ci_dir: Path = CI_DIR) -> tuple[dict, dict]:
    """(recent, baseline) failure-rate rows; empty dicts when no data."""
    path = ci_dir / RUNS_FILENAME
    if not path.exists():
        return {}, {}
    recent = _one_row(_load_sql("ci_failure_rate.sql").format(
        ci_glob=path, since_hours=WINDOW_HOURS, until_hours=0))
    baseline = _one_row(_load_sql("ci_failure_rate.sql").format(
        ci_glob=path, since_hours=BASELINE_HOURS, until_hours=WINDOW_HOURS))
    return recent, baseline


def merge_times(data_dir: Path = DATA_DIR) -> tuple[dict, dict]:
    """(recent, baseline) avg PR merge-duration rows; empty when no data."""
    glob = _windows_glob(data_dir)
    if glob is None:
        return {}, {}
    recent = _one_row(_load_sql("pr_merge_times.sql").format(
        data_glob=glob, since_hours=WINDOW_HOURS, until_hours=0))
    baseline = _one_row(_load_sql("pr_merge_times.sql").format(
        data_glob=glob, since_hours=BASELINE_HOURS, until_hours=WINDOW_HOURS))
    return recent, baseline


def push_rates(data_dir: Path = DATA_DIR) -> tuple[dict, dict]:
    """(recent, baseline) PushEvent-rate rows; empty when no data."""
    glob = _windows_glob(data_dir)
    if glob is None:
        return {}, {}
    recent = _one_row(_load_sql("push_rate.sql").format(
        data_glob=glob, since_hours=WINDOW_HOURS, until_hours=0,
        window_hours=WINDOW_HOURS))
    baseline = _one_row(_load_sql("push_rate.sql").format(
        data_glob=glob, since_hours=BASELINE_HOURS, until_hours=WINDOW_HOURS,
        window_hours=BASELINE_HOURS - WINDOW_HOURS))
    return recent, baseline


# ── Rules ─────────────────────────────────────────────────────────────────────

def detect_ci_failure_spike(ci_dir: Path = CI_DIR) -> dict | None:
    """CI failure rate spiked: recent > max(1.5 × baseline, 0.2)."""
    recent, baseline = ci_failure_rates(ci_dir)
    rate = recent.get("failure_rate")
    if rate is None or (recent.get("run_count") or 0) < MIN_CI_RUNS:
        return None

    base_rate = baseline.get("failure_rate") or 0.0
    threshold = max(base_rate * 1.5, 0.2)
    if rate <= threshold:
        return None

    ratio = rate / base_rate if base_rate > 0 else float("inf")
    severity = "high" if (ratio >= 3 or rate >= 0.5) else "medium" if ratio >= 2 else "low"
    return _make_anomaly(
        "ci_failure_spike",
        f"CI failure rate spiked to {rate:.0%}",
        severity,
        f"CI failure rate over the last {WINDOW_HOURS}h is {rate:.0%} "
        f"({recent['run_count']} runs), vs a {base_rate:.0%} baseline.",
        {
            "recent_failure_rate": rate,
            "baseline_failure_rate": base_rate,
            "recent_runs": recent.get("run_count"),
            "baseline_runs": baseline.get("run_count"),
        },
    )


def detect_merge_time_anomaly(data_dir: Path = DATA_DIR) -> dict | None:
    """Average PR merge duration stretched: recent > 1.5 × baseline."""
    recent, baseline = merge_times(data_dir)
    recent_avg = recent.get("avg_merge_hours")
    base_avg = baseline.get("avg_merge_hours")
    if recent_avg is None or base_avg is None or base_avg <= 0:
        return None
    if (recent.get("merged_count") or 0) < MIN_MERGED_PRS:
        return None
    if (baseline.get("merged_count") or 0) < MIN_MERGED_PRS:
        return None

    ratio = recent_avg / base_avg
    if ratio <= 1.5:
        return None

    severity = "high" if ratio >= 3 else "medium" if ratio >= 2 else "low"
    return _make_anomaly(
        "merge_time_anomaly",
        f"PR merge time {ratio:.1f}x above baseline",
        severity,
        f"PRs merged in the last {WINDOW_HOURS}h averaged {recent_avg:.1f}h "
        f"open-to-merge, vs a {base_avg:.1f}h baseline.",
        {
            "recent_avg_merge_hours": recent_avg,
            "baseline_avg_merge_hours": base_avg,
            "recent_merged": recent.get("merged_count"),
            "ratio": ratio,
        },
    )


def detect_commit_drought(data_dir: Path = DATA_DIR) -> dict | None:
    """PushEvent rate collapsed: recent < 0.3 × baseline."""
    recent, baseline = push_rates(data_dir)
    recent_rate = recent.get("pushes_per_hour")
    base_rate = baseline.get("pushes_per_hour")
    if recent_rate is None or base_rate is None:
        return None
    if base_rate < MIN_BASELINE_PUSH_RATE:
        return None
    if recent_rate >= base_rate * 0.3:
        return None

    severity = (
        "high" if recent_rate == 0
        else "medium" if recent_rate < base_rate * 0.15
        else "low"
    )
    return _make_anomaly(
        "commit_drought",
        f"Commit activity dropped to {recent_rate:.1f}/h",
        severity,
        f"PushEvents averaged {recent_rate:.1f}/h over the last {WINDOW_HOURS}h, "
        f"vs a {base_rate:.1f}/h baseline.",
        {
            "recent_pushes_per_hour": recent_rate,
            "baseline_pushes_per_hour": base_rate,
        },
    )


def detect_all(data_dir: Path = DATA_DIR, ci_dir: Path = CI_DIR) -> list[dict]:
    """Run every rule; silent rules simply contribute nothing."""
    anomalies = [
        anomaly
        for anomaly in (
            detect_ci_failure_spike(ci_dir),
            detect_merge_time_anomaly(data_dir),
            detect_commit_drought(data_dir),
        )
        if anomaly is not None
    ]
    log.info("detection_ran", found=len(anomalies))
    return anomalies


# ── Dev Pipeline Signal (§3 project-level indicator) ──────────────────────────

def _component(status: str, detail: dict) -> dict:
    return {"status": status, **detail}


def pipeline_signal(data_dir: Path = DATA_DIR, ci_dir: Path = CI_DIR) -> dict:
    """
    The three signal-bar components, each ok/warn/alert/unknown.

    Kept separate (never one number) so "CI is actually failing" and
    "commit frequency naturally dropped" stay distinguishable.
    """
    ci_recent, ci_base = ci_failure_rates(ci_dir)
    rate = ci_recent.get("failure_rate")
    if rate is None:
        ci = _component("unknown", {"failure_rate": None, "runs": 0})
    else:
        status = "alert" if rate >= 0.35 else "warn" if rate >= 0.15 else "ok"
        ci = _component(status, {
            "failure_rate": rate,
            "baseline_failure_rate": ci_base.get("failure_rate"),
            "runs": ci_recent.get("run_count"),
        })

    m_recent, m_base = merge_times(data_dir)
    recent_avg, base_avg = m_recent.get("avg_merge_hours"), m_base.get("avg_merge_hours")
    if recent_avg is None or base_avg is None or base_avg <= 0:
        pr = _component("unknown", {"avg_merge_hours": recent_avg})
    else:
        ratio = recent_avg / base_avg
        status = "alert" if ratio >= 3 else "warn" if ratio >= 1.5 else "ok"
        pr = _component(status, {
            "avg_merge_hours": recent_avg,
            "baseline_avg_merge_hours": base_avg,
        })

    p_recent, p_base = push_rates(data_dir)
    push_rate, push_base = p_recent.get("pushes_per_hour"), p_base.get("pushes_per_hour")
    if push_rate is None or push_base is None or push_base < MIN_BASELINE_PUSH_RATE:
        commits = _component("unknown", {"pushes_per_hour": push_rate})
    else:
        fraction = push_rate / push_base
        status = "alert" if fraction < 0.15 else "warn" if fraction < 0.5 else "ok"
        commits = _component(status, {
            "pushes_per_hour": push_rate,
            "baseline_pushes_per_hour": push_base,
        })

    return {
        "ci_stability": ci,
        "pr_velocity": pr,
        "commit_cadence": commits,
        "caption": SIGNAL_CAPTION,
    }
