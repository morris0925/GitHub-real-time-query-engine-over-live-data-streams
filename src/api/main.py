"""
api/main.py — FastAPI app factory for the AI diagnostic layer

Run locally:
    PYTHONPATH=src uvicorn api.main:app --host 127.0.0.1 --port 8000

All heavy state (retriever with its in-memory DuckDB table, LLM client,
diagnosis cache) lives on app.state. The embedding provider and LLM client
are resolved lazily on first use (both raise without their API key, and we
must not trigger that at import time), so create_app() itself never needs a
key. Tests inject tmp dirs, the hash embedding provider, and the stub LLM
explicitly through create_app()'s overrides.
"""

import os
from pathlib import Path

from dotenv import load_dotenv
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from anomaly.ci_fetch import CI_DIR
from anomaly.detector import DATA_DIR
from anomaly.store import ANOMALY_DIR
from api.diagnosis import LLMClient
from api.routes import router
from knowledge.embeddings import KB_DIR, EmbeddingProvider
from knowledge.retriever import Retriever

load_dotenv()

FRONTEND_ORIGIN: str = os.getenv("FRONTEND_ORIGIN", "http://localhost:3000")


def create_app(
    kb_dir: Path = KB_DIR,
    data_dir: Path = DATA_DIR,
    ci_dir: Path = CI_DIR,
    anomaly_dir: Path = ANOMALY_DIR,
    embedding_provider: EmbeddingProvider | None = None,
    llm: LLMClient | None = None,
) -> FastAPI:
    app = FastAPI(title="StreamLens AI Diagnostic API", version="0.1.0")

    app.add_middleware(
        CORSMiddleware,
        allow_origins=[FRONTEND_ORIGIN],
        allow_methods=["*"],
        allow_headers=["*"],
    )

    app.state.kb_dir = kb_dir
    app.state.data_dir = data_dir
    app.state.ci_dir = ci_dir
    app.state.anomaly_dir = anomaly_dir
    app.state.retriever = Retriever(kb_dir=kb_dir, provider=embedding_provider)
    # Resolved lazily on first diagnose/query (get_llm raises without a key);
    # building it here would fail at import time in keyless CI. Tests inject.
    app.state.llm = llm
    app.state.diagnosis_cache = {}   # anomaly_id → Diagnosis dict
    app.state.anomalies_ran_at = 0.0  # monotonic ts of last detection sweep

    app.include_router(router)
    return app


app = create_app()
