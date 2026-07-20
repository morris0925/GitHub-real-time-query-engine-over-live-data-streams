"""
tests/test_eval_retrieval.py — Unit tests for scripts/eval_retrieval.py

Tests cover:
  1. build_eval_set — deterministic sampling, title-length filtering
  2. evaluate — recall@k / MRR arithmetic, using HashEmbeddings (offline)

evaluate()'s ranking math is provider-agnostic, so we test it with exact
case-text queries against the HashEmbeddings stub — the same "identical
text retrieves its own case" contract used in test_retriever.py. That
isolates the metric arithmetic from semantic quality, which only a real
provider (Voyage) can demonstrate.

Run with:
    PYTHONPATH=src pytest tests/test_eval_retrieval.py -v
"""

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))

from eval_retrieval import build_eval_set, evaluate  # noqa: E402

from knowledge import ingest
from knowledge.embeddings import HashEmbeddings, build_embeddings_file
from knowledge.ingest import case_text
from knowledge.retriever import Retriever


# ── Fixtures ──────────────────────────────────────────────────────────────────

def make_kb(kb_dir: Path) -> Path:
    """Write a small kb.parquet + kb_embeddings.parquet into kb_dir."""
    items = [
        {
            "number": n,
            "title": title,
            "body": body,
            "html_url": f"https://github.com/acme/widgets/pull/{n}",
            "labels": [],
            "created_at": "2026-01-10T00:00:00Z",
            "closed_at": "2026-01-11T00:00:00Z",
            "pull_request": {"url": "..."},
        }
        for n, title, body in [
            (1, "CI pipeline failing on flaky scheduler test", "The e2e suite times out"),
            (2, "Slow merge queue after infra change", "PRs waiting 6 hours"),
            (3, "Fix memory leak in kafka consumer", "Heap grows unbounded"),
            (4, "x", "too short to be sampled"),
        ]
    ]
    cases = ingest.build_cases(items)
    ingest.write_kb(cases, kb_dir=kb_dir)
    build_embeddings_file(kb_dir=kb_dir, provider=HashEmbeddings(dim=64))
    return kb_dir


# ── build_eval_set ────────────────────────────────────────────────────────────

def test_build_eval_set_returns_query_and_expected_id(tmp_path: Path) -> None:
    kb_dir = make_kb(tmp_path)
    eval_set = build_eval_set(kb_dir, n=3, seed=1)

    assert len(eval_set) == 3
    for pair in eval_set:
        assert "query" in pair and "expected_case_id" in pair
        assert pair["expected_case_id"].startswith("pr-")


def test_build_eval_set_filters_short_titles(tmp_path: Path) -> None:
    kb_dir = make_kb(tmp_path)
    eval_set = build_eval_set(kb_dir, n=10, seed=1)

    # Only 3 of the 4 fixture cases have a title >= MIN_TITLE_CHARS ("x" is excluded).
    assert len(eval_set) == 3
    assert "pr-4" not in {pair["expected_case_id"] for pair in eval_set}


def test_build_eval_set_deterministic_for_same_seed(tmp_path: Path) -> None:
    kb_dir = make_kb(tmp_path)
    first = build_eval_set(kb_dir, n=2, seed=7)
    second = build_eval_set(kb_dir, n=2, seed=7)
    assert first == second


def test_build_eval_set_missing_kb_raises(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError):
        build_eval_set(tmp_path, n=5)


# ── evaluate ──────────────────────────────────────────────────────────────────

def test_evaluate_perfect_recall_and_mrr_on_exact_text(tmp_path: Path) -> None:
    """Querying with a case's own exact embedded text must rank it #1."""
    kb_dir = make_kb(tmp_path)
    retriever = Retriever(kb_dir=kb_dir, provider=HashEmbeddings(dim=64))

    cases = [
        {"case_id": "pr-1", "title": "CI pipeline failing on flaky scheduler test",
         "body_snippet": "The e2e suite times out"},
        {"case_id": "pr-2", "title": "Slow merge queue after infra change",
         "body_snippet": "PRs waiting 6 hours"},
    ]
    eval_set = [
        {"query": case_text(c), "expected_case_id": c["case_id"]} for c in cases
    ]

    report = evaluate(retriever, eval_set, k_values=(1, 3))

    assert report["n"] == 2
    assert report["recall_at_k"][1] == 1.0
    assert report["recall_at_k"][3] == 1.0
    assert report["mrr"] == 1.0
    assert all(d["rank"] == 1 for d in report["detail"])


def test_evaluate_scores_zero_for_unknown_expected_id(tmp_path: Path) -> None:
    """An expected_case_id that doesn't exist in the KB must count as a miss, not crash."""
    kb_dir = make_kb(tmp_path)
    retriever = Retriever(kb_dir=kb_dir, provider=HashEmbeddings(dim=64))

    eval_set = [{"query": "anything", "expected_case_id": "pr-999-does-not-exist"}]
    report = evaluate(retriever, eval_set, k_values=(1, 5))

    assert report["recall_at_k"][1] == 0.0
    assert report["recall_at_k"][5] == 0.0
    assert report["mrr"] == 0.0
    assert report["detail"][0]["rank"] is None


def test_evaluate_mixed_hits_and_misses(tmp_path: Path) -> None:
    """One guaranteed hit (exact text) + one guaranteed miss (unknown id) → 0.5/0.5."""
    kb_dir = make_kb(tmp_path)
    retriever = Retriever(kb_dir=kb_dir, provider=HashEmbeddings(dim=64))

    eval_set = [
        {"query": case_text({"title": "CI pipeline failing on flaky scheduler test",
                              "body_snippet": "The e2e suite times out"}),
         "expected_case_id": "pr-1"},
        {"query": "anything", "expected_case_id": "pr-999-does-not-exist"},
    ]
    report = evaluate(retriever, eval_set, k_values=(1, 3))

    assert report["recall_at_k"][1] == 0.5
    assert report["mrr"] == 0.5
    assert report["detail"][0]["rank"] == 1
    assert report["detail"][1]["rank"] is None
