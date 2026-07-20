"""
tests/test_api.py — Endpoint tests for the FastAPI diagnostic service

Tests cover:
  1. /health — provider names surfaced honestly (stub/hash in tests)
  2. /anomalies — stored (real) anomalies show up in the feed
  3. /snapshot — 503 when there is no CI data (no synthetic fallback)
  4. /diagnose/{id} — full response shape (§2), trust fields (§4), caching,
     404 on unknown IDs, stub-LLM parse path
  5. /query — RAG + LLM round trip; empty question → 400
  6. /signal — three components + honest caption
  7. get_llm / get_provider — raise loudly when their API key is missing

Everything runs offline: hash embeddings, stub LLM, tmp_path Parquet dirs
injected through create_app(). No canned/synthetic anomalies exist anymore;
tests that need an anomaly persist a real-shaped one via store.save_anomalies.

Run with:
    PYTHONPATH=src pytest tests/test_api.py -v
"""

import json
from datetime import datetime, timezone
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from anomaly import store
from anomaly.ci_fetch import write_runs
from api.main import create_app
from api.diagnosis import StubLLM, _parse_llm_json, get_llm
from api.schemas import AI_NOTICE, DISCLAIMER
from knowledge import ingest
from knowledge.embeddings import HashEmbeddings, build_embeddings_file, get_provider
from storage.writer import write_batch


def _save_real_anomaly(anomaly_dir: Path, anomaly_id: str = "ci-2026071812") -> dict:
    """
    Persist a real-shaped anomaly (as the detector / CI snapshot would emit)
    so endpoint tests have something to diagnose without any synthetic seeding.
    """
    anomaly = {
        "anomaly_id": anomaly_id,
        "type": "ci_failure_spike",
        "title": "CI failure rate 40% (10 runs)",
        "severity": "high",
        "description": "Recent CI failure rate is 40% over 10 runs.",
        "metric": {"recent_failure_rate": 0.4, "recent_runs": 10},
        "repo": "acme/widgets",
        "detected_at": datetime.now(tz=timezone.utc),
        "is_demo": False,
    }
    anomaly_dir.mkdir(parents=True, exist_ok=True)
    store.save_anomalies([anomaly], anomaly_dir=anomaly_dir)
    return anomaly


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
    assert body["kb_case_count"] == 3
    assert body["embedding_provider"] == "hash-stub"
    assert body["llm_provider"] == "stub"
    assert body["disclaimer"] == DISCLAIMER


def test_health_freshness_null_when_no_data(client: TestClient) -> None:
    """No events or CI runs ingested yet → freshness fields are null, not fake."""
    body = client.get("/health").json()
    assert body["latest_event_at"] is None
    assert body["latest_ci_run_at"] is None


def test_health_freshness_reflects_real_data(client: TestClient, tmp_path: Path) -> None:
    event_time = datetime(2026, 7, 18, 12, 0, 0, tzinfo=timezone.utc)
    write_batch(
        [
            {
                "id": "e1",
                "type": "PushEvent",
                "actor": {"id": 1, "login": "alice"},
                "repo": {"id": 1, "name": "acme/widgets"},
                "payload": {},
                "public": True,
                "created_at": event_time.strftime("%Y-%m-%dT%H:%M:%SZ"),
            }
        ],
        data_dir=tmp_path / "events",
    )
    ci_dir = tmp_path / "ci_runs"
    ci_run_time = datetime(2026, 7, 18, 13, 0, 0, tzinfo=timezone.utc)
    write_runs(
        [
            {
                "run_id": 1,
                "repo": "acme/widgets",
                "workflow_name": "ci",
                "status": "completed",
                "conclusion": "success",
                "created_at": ci_run_time,
            }
        ],
        ci_dir=ci_dir,
    )

    body = client.get("/health").json()
    # DuckDB returns timestamps in the local session timezone, so compare by
    # instant (parsed datetime) rather than exact string.
    assert datetime.fromisoformat(body["latest_event_at"]) == event_time
    assert datetime.fromisoformat(body["latest_ci_run_at"]) == ci_run_time


# ── /anomalies + /snapshot ────────────────────────────────────────────────────

def test_empty_feed_initially(client: TestClient) -> None:
    assert client.get("/anomalies").json() == []


def test_saved_anomaly_appears_in_feed(client: TestClient, tmp_path: Path) -> None:
    anomaly = _save_real_anomaly(tmp_path / "anomalies")
    feed = client.get("/anomalies").json()
    assert anomaly["anomaly_id"] in [a["anomaly_id"] for a in feed]


def test_snapshot_without_ci_data_is_503(client: TestClient) -> None:
    # Fixture has no CI data → snapshot is impossible → 503, never a canned
    # synthetic anomaly. The feed must not gain an invented incident.
    response = client.post("/snapshot")
    assert response.status_code == 503
    assert "No CI data" in response.json()["detail"]
    assert client.get("/anomalies").json() == []


# ── /diagnose/{id} ────────────────────────────────────────────────────────────

def test_diagnose_full_shape(client: TestClient, tmp_path: Path) -> None:
    anomaly = _save_real_anomaly(tmp_path / "anomalies")
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
    assert len(body["raw_evidence"]) == 4                       # snapshot + 3 cases
    assert body["raw_evidence"][0].startswith("[live pipeline snapshot]")
    assert body["meta"]["llm_provider"] == "stub"               # honest meta
    assert body["meta"]["disclaimer"] == DISCLAIMER


def test_diagnose_is_cached(client: TestClient, tmp_path: Path) -> None:
    anomaly = _save_real_anomaly(tmp_path / "anomalies")
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


# ── Live evidence wiring ──────────────────────────────────────────────────────

def test_query_includes_live_snapshot_in_evidence(client: TestClient) -> None:
    body = client.post("/query", json={"question": "what is failing?"}).json()
    assert body["raw_evidence"][0].startswith("[live pipeline snapshot]")
    # Empty fixture dirs → the snapshot must say so, not invent numbers.
    assert "no workflow-run data" in body["raw_evidence"][0]


# ── Loud failure when a real provider key is missing ──────────────────────────

def test_get_llm_raises_without_key(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    with pytest.raises(RuntimeError, match="ANTHROPIC_API_KEY"):
        get_llm()


def test_get_provider_raises_without_key(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("VOYAGE_API_KEY", raising=False)
    with pytest.raises(RuntimeError, match="VOYAGE_API_KEY"):
        get_provider()


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
