"""
api/diagnosis.py — RAG + Haiku orchestration

The one place where retrieval, the Tier 2 outcome estimate, and the LLM call
come together:

    anomaly/question → retriever.search() → estimate_outcomes()
                     → Haiku (hedged system prompt) → Diagnosis dict

LLM access is behind a small protocol so tests (and keyless demos) run with
StubLLM — which returns clearly-labeled canned hedged text rather than
failing, and reports provider="stub" in the response meta so the UI can
surface that no real model was involved.
"""

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Protocol

import structlog
from dotenv import load_dotenv

from api.prompts import DIAGNOSIS_SYSTEM_PROMPT
from knowledge.embeddings import KB_DIR
from knowledge.outcomes import estimate_outcomes
from knowledge.retriever import Retriever

load_dotenv()

log = structlog.get_logger(__name__)

ANTHROPIC_MODEL: str = os.getenv("ANTHROPIC_MODEL", "claude-haiku-4-5-20251001")
TOP_K: int = 5
MAX_TOKENS: int = 600


# ── LLM providers ─────────────────────────────────────────────────────────────

class LLMClient(Protocol):
    """Anything that can answer a (system, user) prompt pair with text."""

    name: str
    model: str | None

    def complete(self, system: str, user: str) -> str:
        ...


class AnthropicLLM:
    """Claude Haiku via the Anthropic SDK. Requires ANTHROPIC_API_KEY."""

    name = "anthropic"

    def __init__(self, api_key: str, model: str = ANTHROPIC_MODEL) -> None:
        import anthropic  # deferred so the stub path needs no SDK at all

        self._client = anthropic.Anthropic(api_key=api_key)
        self.model: str | None = model

    def complete(self, system: str, user: str) -> str:
        response = self._client.messages.create(
            model=self.model,
            max_tokens=MAX_TOKENS,
            system=system,
            messages=[{"role": "user", "content": user}],
        )
        return response.content[0].text


class StubLLM:
    """
    Keyless fallback. Returns canned, on-format, hedged JSON so the demo and
    tests exercise the full pipeline; meta.llm_provider="stub" keeps it honest.
    """

    name = "stub"
    model: str | None = None

    def complete(self, system: str, user: str) -> str:
        return json.dumps(
            {
                "summary": (
                    "No LLM key is configured, so this is placeholder text. "
                    "Based on retrieval alone, the anomaly is possibly related "
                    "to the similar historical cases listed below — review "
                    "them directly before drawing conclusions."
                ),
                "suggested_actions": [
                    "Review the similar historical cases below",
                    "Set ANTHROPIC_API_KEY to enable real diagnoses",
                ],
                "confidence": "low",
            }
        )


def get_llm() -> LLMClient:
    """Anthropic when a key is configured, otherwise the loud stub."""
    api_key = os.getenv("ANTHROPIC_API_KEY")
    if api_key:
        return AnthropicLLM(api_key=api_key)
    log.warning("no_anthropic_key", fallback="stub LLM — diagnoses are placeholders")
    return StubLLM()


# ── Prompt assembly / response parsing ────────────────────────────────────────

def _evidence_blocks(cases: list[dict]) -> list[str]:
    """The exact chunks shown in 'raw evidence' AND fed to the LLM (§2 item 8)."""
    blocks: list[str] = []
    for case in cases:
        labels = ", ".join(case.get("labels") or []) or "none"
        reverted = "yes" if case.get("was_reverted") else "no"
        blocks.append(
            f"[{case['case_id']}] {case['title']}\n"
            f"labels: {labels} | reverted: {reverted} | "
            f"similarity: {case['similarity_band']}\n"
            f"{case.get('body_snippet') or '(no body)'}"
        )
    return blocks


def _build_user_prompt(
    subject: str, evidence: list[str], live_context: str | None = None
) -> str:
    evidence_text = "\n\n---\n\n".join(evidence) if evidence else "(no similar cases retrieved)"
    sections = [f"## Subject\n{subject}"]
    if live_context:
        sections.append(
            "## Live pipeline evidence (computed from current stream/CI data — "
            f"cite specific workflows and PR numbers from here)\n{live_context}"
        )
    sections.append(f"## Retrieved similar historical cases\n{evidence_text}")
    return "\n\n".join(sections)


def _parse_llm_json(raw: str) -> dict:
    """
    Parse the model's JSON, tolerating markdown fences. On unparseable
    output, degrade to summary=raw text with confidence=low — never 500
    because a model went off-format.
    """
    text = raw.strip()
    if text.startswith("```"):
        text = text.strip("`")
        text = text[text.find("{"):]
    try:
        parsed = json.loads(text[text.index("{"): text.rindex("}") + 1])
        return {
            "summary": str(parsed.get("summary", "")).strip(),
            "suggested_actions": [str(a) for a in parsed.get("suggested_actions", [])],
            "confidence": str(parsed.get("confidence", "low")).lower(),
        }
    except (ValueError, KeyError):
        log.warning("llm_output_not_json", preview=raw[:120])
        return {"summary": raw.strip(), "suggested_actions": [], "confidence": "low"}


# ── Orchestration ─────────────────────────────────────────────────────────────

def _trim_case(case: dict) -> dict:
    """Retriever row → SimilarCase shape for the API response."""
    return {
        "case_id": case["case_id"],
        "kind": case["kind"],
        "title": case["title"],
        "url": case.get("url"),
        "labels": case.get("labels") or [],
        "similarity_band": case["similarity_band"],
        "was_reverted": bool(case.get("was_reverted")),
        "time_to_resolve_hours": case.get("time_to_resolve_hours"),
        "snippet": (case.get("body_snippet") or "")[:200] or None,
    }


def diagnose(
    subject: str,
    retriever: Retriever,
    llm: LLMClient,
    kb_dir: Path = KB_DIR,
    live_context: str | None = None,
) -> dict:
    """
    Full RAG + LLM + Tier 2 pass for a subject (anomaly text or question).

    live_context is the formatted pipeline snapshot (anomaly/evidence.py):
    it goes into the prompt so the model can cite real workflows/PRs, and
    into raw_evidence so the engineer audits the exact same facts.

    Returns the shared body of Diagnosis/QueryResponse:
    generated, outcome_estimate, similar_cases, raw_evidence, meta.
    """
    cases = retriever.search(subject, top_k=TOP_K)
    outcome = estimate_outcomes([case["case_id"] for case in cases], kb_dir=kb_dir)
    evidence = _evidence_blocks(cases)
    if live_context:
        evidence = [f"[live pipeline snapshot]\n{live_context}", *evidence]

    raw = llm.complete(
        DIAGNOSIS_SYSTEM_PROMPT,
        _build_user_prompt(subject, _evidence_blocks(cases), live_context),
    )
    generated = _parse_llm_json(raw)

    log.info(
        "diagnosis_generated",
        cases=len(cases),
        llm=llm.name,
        confidence=generated["confidence"],
    )
    return {
        "generated": generated,
        "outcome_estimate": outcome,
        "similar_cases": [_trim_case(case) for case in cases],
        "raw_evidence": evidence,
        "meta": {
            "llm_provider": llm.name,
            "llm_model": llm.model,
            "embedding_provider": retriever.provider_name,
            "generated_at": datetime.now(tz=timezone.utc),
        },
    }


def anomaly_subject(anomaly: dict) -> str:
    """The retrieval/diagnosis text for an anomaly: title + description."""
    return f"{anomaly['title']}\n{anomaly.get('description') or ''}".strip()
