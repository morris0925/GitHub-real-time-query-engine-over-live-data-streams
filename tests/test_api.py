"""
tests/test_api.py — Endpoint tests for the FastAPI diagnostic service

Tests cover:
  1. /health — provider names surfaced honestly (stub/hash in tests)
  2. /demo/anomaly + /anomalies — seeding shows up in the feed; bad type → 400
  3. /diagnose/{id} — full response shape (§2), trust fields (§4), caching,
     404 on unknown IDs, stub-LLM parse path
  4. /query — RAG + LLM round trip; empty question → 400
  5. /signal — three components + honest caption

Everything runs offline: hash embeddings, stub LLM, tmp_path Parquet dirs
injected through create_app().

Run with:
    PYTHONPATH=src pytest tests/test_api.py -v
"""

import json
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from api.main import create_app
from api.diagnosis import StubLLM, _parse_llm_json
from api.schemas import AI_NOTICE, DISCLAIMER
from knowledge import ingest
from knowledge.embeddings import HashEmbeddings, build_embeddings_file


# ── Fixtures ──────────────────────────────────────────────────────────────────

def make_kb(kb_dir: Path) -> None:
    """Tiny KB: 2 PRs (one reverted) + 1 issue, embedded with the hash stub."""
    items = [
        {
            "number": 1,
            "title": "CI failing after runner image bump",
            "body": "All e2e jobs started failing",
            "html_url": "https://github.com/acme/widgets/pull/1",
            "labels": [{"name": "severity:high"}],
            "created_at": "2026-01-10T00:00:00Z",
            "closed_at": "2026-01-11T00:00:00Z",
            "pull_request": {"url": "..."},
        },
        {
            "number": 2,
            "title": 'Revert "CI failing after runner image bump"',
            "body": "Reverts acme/widgets#1",
            "html_url": "https://github.com/acme/widgets/pull/2",
            "labels": [],
            "created_at": "2026-01-11T00:00:00Z",
            "closed_at": "2026-01-11T06:00:00Z",
            "pull_request": {"url": "..."},
        },
        {
            "number": 3,
            "title": "Flaky scheduler e2e test",
            "body": "Timeout in CI on arm64",
            "html_url": "https://github.com/acme/widgets/issues/3",
            "labels": [{"name": "kind/flake"}],
            "created_at": "2026-01-09T00:00:00Z",
            "closed_at": "2026-01-10T00:00:00Z",
        },
    ]
    ingest.write_kb(ingest.build_cases(items), kb_dir=kb_dir)
    build_embeddings_file(kb_dir=kb_dir, provider=HashEmbeddings(dim=64))


@pytest.fixture()
def client(tmp_path: Path) -> TestClient:
    kb_dir = tmp_path / "knowledge"
    kb_dir.mkdir()
    make_kb(kb_dir)
    app = create_app(
        kb_dir=kb_dir,
        data_dir=tmp_path / "events",       # empty → rules stay silent
        ci_dir=tmp_path / "ci_runs",
        anomaly_dir=tmp_path / "anomalies",
        embedding_provider=HashEmbeddings(dim=64),
        llm=StubLLM(),
    )
    return TestClient(app)


# ── /health ───────────────────────────────────────────────────────────────────

def test_health_reports_providers(client: TestClient) -> None:
    body = client.get("/health").json()
    assert body["status"] == "ok"
    assert body["kb_ready"] is True
    assert body["embedding_provider"] == "hash-stub"
    assert body["llm_provider"] == "stub"
    assert body["disclaimer"] == DISCLAIMER


# ── /demo/anomaly + /anomalies ────────────────────────────────────────────────

def test_empty_feed_initially(client: TestClient) -> None:
    assert client.get("/anomalies").json() == []


def test_demo_anomaly_appears_in_feed(client: TestClient) -> None:
    created = client.post("/demo/anomaly", json={"type": "ci_failure_spike"})
    assert created.status_code == 201
    anomaly = created.json()
    assert anomaly["is_demo"] is True
    assert anomaly["severity"] == "high"

    feed = client.get("/anomalies").json()
    assert [a["anomaly_id"] for a in feed] == [anomaly["anomaly_id"]]


def test_demo_anomaly_unknown_type_is_400(client: TestClient) -> None:
    response = client.post("/demo/anomaly", json={"type": "nonsense"})
    assert response.status_code == 400
    assert "Valid types" in response.json()["detail"]


# ── /diagnose/{id} ────────────────────────────────────────────────────────────

def test_diagnose_full_shape(client: TestClient) -> None:
    anomaly = client.post("/demo/anomaly", json={"type": "ci_failure_spike"}).json()
    body = client.get(f"/diagnose/{anomaly['anomaly_id']}").json()

    # §2 ordering blocks all present
    assert body["anomaly"]["anomaly_id"] == anomaly["anomaly_id"]
    assert body["generated"]["summary"]
    assert body["generated"]["confidence"] in ("high", "medium", "low")
    assert body["generated"]["ai_notice"] == AI_NOTICE          # §4 per-block label
    assert body["outcome_estimate"]["sample_size"] == 3
    assert body["outcome_estimate"]["revert_rate"] == pytest.approx(0.5)
    assert len(body["similar_cases"]) == 3
    assert all(c["similarity_band"] in ("high", "medium", "low")
               for c in body["similar_cases"])                  # bands, never %
    assert len(body["raw_evidence"]) == 3
    assert body["meta"]["llm_provider"] == "stub"               # honest meta
    assert body["meta"]["disclaimer"] == DISCLAIMER


def test_diagnose_is_cached(client: TestClient) -> None:
    anomaly = client.post("/demo/anomaly", json={"type": "commit_drought"}).json()
    first = client.get(f"/diagnose/{anomaly['anomaly_id']}").json()
    second = client.get(f"/diagnose/{anomaly['anomaly_id']}").json()
    assert first["meta"]["generated_at"] == second["meta"]["generated_at"]


def test_diagnose_unknown_id_is_404(client: TestClient) -> None:
    assert client.get("/diagnose/nope").status_code == 404


# ── /query ────────────────────────────────────────────────────────────────────

def test_query_round_trip(client: TestClient) -> None:
    response = client.post("/query", json={"question": "why did CI fail rate spike?"})
    assert response.status_code == 200
    body = response.json()
    assert body["question"] == "why did CI fail rate spike?"
    assert body["generated"]["ai_notice"] == AI_NOTICE
    assert len(body["similar_cases"]) == 3
    assert body["outcome_estimate"]["sample_size"] == 3


def test_query_empty_question_is_400(client: TestClient) -> None:
    assert client.post("/query", json={"question": "   "}).status_code == 400


# ── /signal ───────────────────────────────────────────────────────────────────

def test_signal_shape_and_caption(client: TestClient) -> None:
    body = client.get("/signal").json()
    for key in ("ci_stability", "pr_velocity", "commit_cadence"):
        assert body[key]["status"] == "unknown"  # no data dirs in fixture
    assert "not a live production health check" in body["caption"]


# ── LLM output parsing ────────────────────────────────────────────────────────

def test_parse_llm_json_plain() -> None:
    parsed = _parse_llm_json(json.dumps(
        {"summary": "Likely related to X", "suggested_actions": ["Check Y"], "confidence": "medium"}
    ))
    assert parsed["confidence"] == "medium"
    assert parsed["suggested_actions"] == ["Check Y"]


def test_parse_llm_json_with_fences() -> None:
    raw = '```json\n{"summary": "s", "suggested_actions": [], "confidence": "LOW"}\n```'
    assert _parse_llm_json(raw)["confidence"] == "low"


def test_parse_llm_json_garbage_degrades() -> None:
    parsed = _parse_llm_json("I think the root cause is definitely X.")
    assert parsed["confidence"] == "low"
    assert parsed["suggested_actions"] == []
    assert "root cause" in parsed["summary"]  # raw text preserved, not lost
