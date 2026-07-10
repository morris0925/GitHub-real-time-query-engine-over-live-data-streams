"""
api/routes.py — HTTP endpoints for the AI diagnostic layer

GET  /anomalies     — incident feed (runs detection, throttled; merged store)
GET  /signal        — Dev Pipeline Signal components (CI / PR / commits)
GET  /diagnose/{id} — cached RAG + Haiku diagnosis + Tier 2 outcome estimate
POST /query         — free-text question → RAG + Haiku
POST /demo/anomaly  — seed a synthetic anomaly for live demos
GET  /health        — liveness + which providers are active

Detection throttling: the frontend polls /anomalies every 5-10s; running
the detection SQL on every poll is wasteful, so sweeps are at most one per
DETECTION_INTERVAL_SECONDS and polls in between serve the stored feed.
"""

import time

import structlog
from fastapi import APIRouter, HTTPException, Request

from anomaly import detector, evidence, store
from api import schemas
from api.diagnosis import anomaly_subject, diagnose

log = structlog.get_logger(__name__)

router = APIRouter()

DETECTION_INTERVAL_SECONDS: float = 5.0


def _sweep_and_load(request: Request) -> list[dict]:
    """Run detection (throttled), persist results, return the merged feed."""
    app_state = request.app.state
    now = time.monotonic()
    if now - app_state.anomalies_ran_at >= DETECTION_INTERVAL_SECONDS:
        app_state.anomalies_ran_at = now
        detected = detector.detect_all(app_state.data_dir, app_state.ci_dir)
        if detected:
            store.save_anomalies(detected, anomaly_dir=app_state.anomaly_dir)
    return store.load_anomalies(anomaly_dir=app_state.anomaly_dir)


@router.get("/anomalies", response_model=list[schemas.Anomaly])
def list_anomalies(request: Request) -> list[dict]:
    return _sweep_and_load(request)


@router.get("/signal", response_model=schemas.PipelineSignal)
def pipeline_signal(request: Request) -> dict:
    return detector.pipeline_signal(request.app.state.data_dir, request.app.state.ci_dir)


@router.get("/diagnose/{anomaly_id}", response_model=schemas.Diagnosis)
def diagnose_anomaly(anomaly_id: str, request: Request) -> dict:
    app_state = request.app.state

    cached = app_state.diagnosis_cache.get(anomaly_id)
    if cached is not None:
        return cached

    anomalies = {a["anomaly_id"]: a for a in store.load_anomalies(app_state.anomaly_dir)}
    anomaly = anomalies.get(anomaly_id)
    if anomaly is None:
        raise HTTPException(status_code=404, detail=f"Unknown anomaly: {anomaly_id}")

    snapshot = evidence.pipeline_snapshot(app_state.data_dir, app_state.ci_dir)
    body = diagnose(
        anomaly_subject(anomaly),
        retriever=app_state.retriever,
        llm=app_state.llm,
        kb_dir=app_state.kb_dir,
        live_context=evidence.format_snapshot(snapshot),
    )
    result = {"anomaly": anomaly, **body}
    app_state.diagnosis_cache[anomaly_id] = result
    log.info("diagnosis_cached", anomaly_id=anomaly_id)
    return result


@router.post("/query", response_model=schemas.QueryResponse)
def free_text_query(payload: schemas.QueryRequest, request: Request) -> dict:
    question = payload.question.strip()
    if not question:
        raise HTTPException(status_code=400, detail="Question must not be empty")

    app_state = request.app.state
    snapshot = evidence.pipeline_snapshot(app_state.data_dir, app_state.ci_dir)
    body = diagnose(
        question,
        retriever=app_state.retriever,
        llm=app_state.llm,
        kb_dir=app_state.kb_dir,
        live_context=evidence.format_snapshot(snapshot),
    )
    return {"question": question, **body}


@router.post("/demo/anomaly", response_model=schemas.Anomaly, status_code=201)
def trigger_demo_anomaly(payload: schemas.DemoAnomalyRequest, request: Request) -> dict:
    app_state = request.app.state

    if payload.type == "snapshot":
        # Preferred demo path: a snapshot of the repo's REAL current CI state.
        snapshot = evidence.pipeline_snapshot(app_state.data_dir, app_state.ci_dir)
        anomaly = evidence.build_snapshot_anomaly(snapshot)
        if anomaly is not None:
            store.save_anomalies([anomaly], anomaly_dir=app_state.anomaly_dir)
            return anomaly
        # No CI data to snapshot — fall back to the canned template, which
        # is at least honestly labeled is_demo.
        log.warning("snapshot_demo_no_ci_data", fallback="ci_failure_spike template")
        return store.seed_demo_anomaly("ci_failure_spike", anomaly_dir=app_state.anomaly_dir)

    try:
        return store.seed_demo_anomaly(payload.type, anomaly_dir=app_state.anomaly_dir)
    except KeyError:
        raise HTTPException(
            status_code=400,
            detail=f"Unknown demo type: {payload.type!r}. "
                   f"Valid types: {['snapshot', *store.demo_anomaly_types()]}",
        )


@router.get("/health")
def health(request: Request) -> dict:
    app_state = request.app.state
    return {
        "status": "ok",
        "kb_ready": app_state.retriever.ready,
        "embedding_provider": app_state.retriever.provider_name,
        "llm_provider": app_state.llm.name,
        "disclaimer": schemas.DISCLAIMER,
    }
