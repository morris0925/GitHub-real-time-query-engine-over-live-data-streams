"""
tests/test_outcomes.py — Unit tests for knowledge/outcomes.py

Tests cover:
  1. revert_rate over PRs only (issues excluded from the denominator)
  2. avg_time_to_resolve across the selected cases
  3. severity-ish label extraction
  4. Empty inputs / missing KB → zeroed result, never an exception

Run with:
    PYTHONPATH=src pytest tests/test_outcomes.py -v
"""

from pathlib import Path

import pytest

from knowledge import ingest
from knowledge.outcomes import estimate_outcomes


def make_kb(tmp_path: Path) -> Path:
    """KB with 3 PRs (one reverted) + 1 issue, known resolve times."""
    def item(n: int, title: str, is_pr: bool, labels: list[str], body: str | None = None,
             closed: str = "2026-01-11T00:00:00Z") -> dict:
        base: dict = {
            "number": n, "title": title, "body": body,
            "html_url": f"https://github.com/acme/widgets/issues/{n}",
            "labels": [{"name": name} for name in labels],
            "created_at": "2026-01-10T00:00:00Z", "closed_at": closed,
        }
        if is_pr:
            base["pull_request"] = {"url": "..."}
        return base

    items = [
        item(1, "Add scheduler retry", True, ["severity:high"]),                 # reverted, 24h
        item(2, "Refactor consumer", True, ["kind/cleanup"],
             closed="2026-01-12T00:00:00Z"),                                     # 48h
        item(3, 'Revert "Add scheduler retry"', True, [],
             body="Reverts acme/widgets#1"),                                     # the revert, 24h
        item(4, "CI flaky on arm64", False, ["priority/critical", "kind/bug"]),  # issue, 24h
    ]
    ingest.write_kb(ingest.build_cases(items), kb_dir=tmp_path)
    return tmp_path


def test_revert_rate_counts_prs_only(tmp_path: Path) -> None:
    kb = make_kb(tmp_path)
    result = estimate_outcomes(["pr-1", "pr-2", "issue-4"], kb_dir=kb)
    assert result["sample_size"] == 3
    assert result["pr_count"] == 2
    assert result["revert_rate"] == pytest.approx(0.5)  # pr-1 of {pr-1, pr-2}


def test_avg_time_to_resolve(tmp_path: Path) -> None:
    kb = make_kb(tmp_path)
    result = estimate_outcomes(["pr-1", "pr-2"], kb_dir=kb)
    assert result["avg_time_to_resolve_hours"] == pytest.approx(36.0)  # (24+48)/2


def test_severity_labels_extracted(tmp_path: Path) -> None:
    kb = make_kb(tmp_path)
    result = estimate_outcomes(["pr-1", "pr-2", "issue-4"], kb_dir=kb)
    assert result["severity_labels"] == ["priority/critical", "severity:high"]
    assert "kind/cleanup" not in result["severity_labels"]


def test_issues_only_has_no_revert_rate(tmp_path: Path) -> None:
    kb = make_kb(tmp_path)
    result = estimate_outcomes(["issue-4"], kb_dir=kb)
    assert result["pr_count"] == 0
    assert result["revert_rate"] is None
    assert result["avg_time_to_resolve_hours"] == pytest.approx(24.0)


def test_empty_case_ids(tmp_path: Path) -> None:
    kb = make_kb(tmp_path)
    result = estimate_outcomes([], kb_dir=kb)
    assert result["sample_size"] == 0
    assert result["revert_rate"] is None


def test_missing_kb_file(tmp_path: Path) -> None:
    result = estimate_outcomes(["pr-1"], kb_dir=tmp_path)  # no kb.parquet here
    assert result["sample_size"] == 0
    assert result["severity_labels"] == []


def test_unknown_case_ids(tmp_path: Path) -> None:
    kb = make_kb(tmp_path)
    result = estimate_outcomes(["pr-999"], kb_dir=kb)
    assert result["sample_size"] == 0
