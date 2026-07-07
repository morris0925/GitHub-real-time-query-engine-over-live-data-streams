"""
knowledge/outcomes.py — Tier 2 historical-outcome estimate (computed, not generated)

Given the similar cases the retriever found, answer: "how much did situations
like this actually hurt, historically?" — via DuckDB aggregation over the
label + revert-linkage metadata in kb.parquet. No LLM involved anywhere in
this module (design proposal §3): this is the factual counterweight that sits
next to the LLM summary in the UI, and it is deliberately NOT under the
AI-content trust labeling (§4).

Signals:
- revert_rate            — fraction of similar PRs that were later reverted
- avg_time_to_resolve    — mean close time of similar cases, in hours
- severity_labels        — severity/priority-ish labels seen across the set
- sample_size            — how many cases the numbers are based on; small
                           samples must be shown as weak evidence in the UI
"""

from pathlib import Path

import duckdb
import structlog

from knowledge.embeddings import KB_DIR
from knowledge.ingest import KB_FILENAME

log = structlog.get_logger(__name__)

# Label prefixes that indicate severity/priority metadata worth surfacing.
SEVERITY_LABEL_MARKERS: tuple[str, ...] = ("severity", "priority", "p0", "p1", "hotfix", "critical")


def estimate_outcomes(case_ids: list[str], kb_dir: Path = KB_DIR) -> dict:
    """
    Aggregate Tier 2 outcome signals over the given similar cases.

    Returns:
        {
            "sample_size": 5,
            "pr_count": 4,                      # revert_rate denominator
            "revert_rate": 0.25,                # None if no PRs in the set
            "avg_time_to_resolve_hours": 37.2,  # None if no closed cases
            "severity_labels": ["severity:high", "priority/critical"],
        }

    Computed entirely in DuckDB from kb.parquet metadata. Returns zeroed/None
    fields when case_ids is empty or the KB file is missing — the API layer
    translates that into "insufficient historical data" rather than a number.
    """
    empty: dict = {
        "sample_size": 0,
        "pr_count": 0,
        "revert_rate": None,
        "avg_time_to_resolve_hours": None,
        "severity_labels": [],
    }
    kb_path = kb_dir / KB_FILENAME
    if not case_ids or not kb_path.exists():
        return empty

    conn = duckdb.connect()
    placeholders = ", ".join("?" for _ in case_ids)

    row = conn.execute(
        f"SELECT count(*), "
        f"  count(*) FILTER (kind = 'pr'), "
        f"  avg(CASE WHEN was_reverted THEN 1.0 ELSE 0.0 END) FILTER (kind = 'pr'), "
        f"  avg(time_to_resolve_hours) "
        f"FROM read_parquet('{kb_path}') WHERE case_id IN ({placeholders})",
        case_ids,
    ).fetchone()
    if row is None or row[0] == 0:
        return empty
    sample_size, pr_count, revert_rate, avg_resolve = row

    label_rows = conn.execute(
        f"SELECT DISTINCT lower(label) FROM ("
        f"  SELECT unnest(labels) AS label FROM read_parquet('{kb_path}') "
        f"  WHERE case_id IN ({placeholders})) "
        f"ORDER BY 1",
        case_ids,
    ).fetchall()
    severity_labels = [
        label for (label,) in label_rows
        if any(marker in label for marker in SEVERITY_LABEL_MARKERS)
    ]

    result = {
        "sample_size": int(sample_size),
        "pr_count": int(pr_count),
        "revert_rate": float(revert_rate) if revert_rate is not None else None,
        "avg_time_to_resolve_hours": float(avg_resolve) if avg_resolve is not None else None,
        "severity_labels": severity_labels,
    }
    log.info("outcomes_estimated", **result)
    return result
