"""
knowledge/kb_schema.py — PyArrow schemas for the RAG knowledge base

Single source of truth for the knowledge-base Parquet files, mirroring the
role storage/schema.py plays for the event pipeline.

Two files, two schemas:

1. kb.parquet (KB_CASE_SCHEMA) — one row per closed issue/PR, produced by
   ingest.py. Carries the *structured metadata* the Tier 2 historical-outcome
   estimate needs (labels, revert linkage, time-to-resolve), not just text.

2. kb_embeddings.parquet (KB_EMBEDDING_SCHEMA) — one row per case with its
   text embedding, produced by embeddings.py. Kept separate so the corpus can
   be re-embedded (e.g. switching providers) without re-fetching from GitHub.
"""

import pyarrow as pa

# ── Column-level documentation ──────────────────────────────────────────────
# case_id                : stable key, "issue-<n>" or "pr-<n>"
# kind                   : "issue" | "pr"
# number                 : GitHub issue/PR number
# title / body_snippet   : embedded text source (snippet capped at ingest)
# url                    : html_url for linking from the UI
# labels                 : label names, e.g. ["kind/bug", "severity:high"]
# created_at / closed_at : GitHub timestamps (UTC)
# time_to_resolve_hours  : closed_at − created_at, Tier 2 signal
# is_revert              : this case is itself a "Revert ..." PR
# reverts_number         : PR number this revert points back to (if is_revert)
# was_reverted           : a later revert PR points at this case — Tier 2 signal
# reverted_by            : PR number of the revert that undid this case
# ────────────────────────────────────────────────────────────────────────────

KB_CASE_SCHEMA = pa.schema(
    [
        pa.field("case_id",               pa.string(),                  nullable=False),
        pa.field("kind",                  pa.string(),                  nullable=False),
        pa.field("number",                pa.int64(),                   nullable=False),
        pa.field("title",                 pa.string(),                  nullable=False),
        pa.field("body_snippet",          pa.string(),                  nullable=True),
        pa.field("url",                   pa.string(),                  nullable=True),
        pa.field("labels",                pa.list_(pa.string()),        nullable=True),
        pa.field("created_at",            pa.timestamp("us", tz="UTC"), nullable=True),
        pa.field("closed_at",             pa.timestamp("us", tz="UTC"), nullable=True),
        pa.field("time_to_resolve_hours", pa.float64(),                 nullable=True),
        pa.field("is_revert",             pa.bool_(),                   nullable=False),
        pa.field("reverts_number",        pa.int64(),                   nullable=True),
        pa.field("was_reverted",          pa.bool_(),                   nullable=False),
        pa.field("reverted_by",           pa.int64(),                   nullable=True),
    ]
)

KB_EMBEDDING_SCHEMA = pa.schema(
    [
        pa.field("case_id",   pa.string(),             nullable=False),
        pa.field("embedding", pa.list_(pa.float32()),  nullable=False),
    ]
)

KB_CASE_COLUMNS: list[str] = [field.name for field in KB_CASE_SCHEMA]
