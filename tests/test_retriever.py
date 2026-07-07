"""
tests/test_retriever.py — Unit tests for knowledge/embeddings.py + retriever.py

Tests cover:
  1. HashEmbeddings — determinism, unit length, dimension
  2. build_embeddings_file — Parquet output aligned with kb.parquet
  3. Retriever — exact-match retrieval, band mapping, metadata passthrough,
     graceful behavior when KB files are missing

All tests use the offline HashEmbeddings provider — no network, no keys.
Semantic quality isn't testable with the stub, but the mechanical contract
is: identical text must retrieve its own case with similarity ≈ 1.0.

Run with:
    PYTHONPATH=src pytest tests/test_retriever.py -v
"""

import math
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from knowledge import ingest
from knowledge.embeddings import HashEmbeddings, build_embeddings_file
from knowledge.retriever import Retriever, similarity_band


# ── Fixtures ──────────────────────────────────────────────────────────────────

def make_kb(tmp_path: Path) -> Path:
    """Write a small kb.parquet + kb_embeddings.parquet into tmp_path."""
    items = [
        {
            "number": n,
            "title": title,
            "body": body,
            "html_url": f"https://github.com/acme/widgets/pull/{n}",
            "labels": [{"name": "kind/bug"}],
            "created_at": "2026-01-10T00:00:00Z",
            "closed_at": "2026-01-11T00:00:00Z",
            "pull_request": {"url": "..."},
        }
        for n, title, body in [
            (1, "CI pipeline failing on flaky scheduler test", "The e2e suite times out"),
            (2, "Slow merge queue after infra change", "PRs waiting 6 hours"),
            (3, "Fix memory leak in kafka consumer", "Heap grows unbounded"),
        ]
    ]
    cases = ingest.build_cases(items)
    ingest.write_kb(cases, kb_dir=tmp_path)
    build_embeddings_file(kb_dir=tmp_path, provider=HashEmbeddings(dim=64))
    return tmp_path


# ── HashEmbeddings ────────────────────────────────────────────────────────────

def test_hash_embeddings_deterministic() -> None:
    provider = HashEmbeddings(dim=64)
    a1, a2 = provider.embed(["same text", "same text"])
    assert a1 == a2


def test_hash_embeddings_unit_length_and_dim() -> None:
    (vector,) = HashEmbeddings(dim=64).embed(["hello"])
    assert len(vector) == 64
    assert math.sqrt(sum(v * v for v in vector)) == pytest.approx(1.0)


def test_hash_embeddings_distinct_texts_differ() -> None:
    v1, v2 = HashEmbeddings(dim=64).embed(["alpha", "beta"])
    assert v1 != v2


# ── build_embeddings_file ─────────────────────────────────────────────────────

def test_build_embeddings_file_aligned_with_kb(tmp_path: Path) -> None:
    kb_dir = make_kb(tmp_path)
    emb = pq.read_table(kb_dir / "kb_embeddings.parquet").to_pylist()
    kb = pq.read_table(kb_dir / "kb.parquet").to_pylist()
    assert {e["case_id"] for e in emb} == {c["case_id"] for c in kb}
    assert all(len(e["embedding"]) == 64 for e in emb)


# ── similarity_band ───────────────────────────────────────────────────────────

def test_similarity_bands() -> None:
    assert similarity_band(0.9) == "high"
    assert similarity_band(0.6) == "medium"
    assert similarity_band(0.1) == "low"


# ── Retriever ─────────────────────────────────────────────────────────────────

def test_retriever_finds_exact_match_with_high_band(tmp_path: Path) -> None:
    retriever = Retriever(kb_dir=make_kb(tmp_path), provider=HashEmbeddings(dim=64))
    assert retriever.ready

    # Query with the exact embedded text of case pr-1 (title\nbody).
    results = retriever.search(
        "CI pipeline failing on flaky scheduler test\nThe e2e suite times out", top_k=3
    )
    assert results[0]["case_id"] == "pr-1"
    assert results[0]["similarity"] == pytest.approx(1.0, abs=1e-5)
    assert results[0]["similarity_band"] == "high"


def test_retriever_carries_case_metadata(tmp_path: Path) -> None:
    retriever = Retriever(kb_dir=make_kb(tmp_path), provider=HashEmbeddings(dim=64))
    (top,) = retriever.search("anything", top_k=1)
    # Tier 2 metadata must survive the round trip into search results.
    assert top["labels"] == ["kind/bug"]
    assert "was_reverted" in top and "time_to_resolve_hours" in top
    assert "embedding" not in top  # raw vectors don't belong in results


def test_retriever_respects_top_k(tmp_path: Path) -> None:
    retriever = Retriever(kb_dir=make_kb(tmp_path), provider=HashEmbeddings(dim=64))
    assert len(retriever.search("query", top_k=2)) == 2


def test_retriever_missing_kb_is_not_ready(tmp_path: Path) -> None:
    retriever = Retriever(kb_dir=tmp_path, provider=HashEmbeddings(dim=64))
    assert not retriever.ready
    assert retriever.search("query") == []


def test_retriever_provider_name_surfaced(tmp_path: Path) -> None:
    retriever = Retriever(kb_dir=make_kb(tmp_path), provider=HashEmbeddings(dim=64))
    assert retriever.provider_name == "hash-stub"
