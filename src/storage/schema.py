"""
storage/schema.py — PyArrow schema definitions for StreamLens

This file is the single source of truth for what a GitHub event looks like
when stored as a Parquet file. Everything that writes or reads Parquet must
use the schema defined here.

Why flatten the nested GitHub API response?
-------------------------------------------
The raw GitHub event looks like this (simplified):
    {
        "id": "12345",
        "type": "PushEvent",
        "actor": {"id": 1, "login": "alice"},
        "repo": {"id": 99, "name": "alice/myrepo"},
        "payload": { ... lots of nested stuff ... },
        "public": true,
        "created_at": "2024-01-15T10:30:00Z"
    }

Storing nested dicts in Parquet is possible but makes SQL queries awkward.
Instead, we "flatten" it: pull the fields we care about up to the top level,
and store the complex `payload` as a raw JSON string.

The result is a clean table with one row per event, easy to query with DuckDB.
"""

import pyarrow as pa

# ── Column-level documentation ──────────────────────────────────────────────
# event_id     : GitHub's own unique ID for this event (string, not int)
# event_type   : "PushEvent", "WatchEvent", "CreateEvent", etc.
# actor_id     : numeric GitHub user ID of who triggered the event
# actor_login  : GitHub username (e.g. "alice")
# repo_id       : numeric GitHub repository ID
# repo_name    : "owner/repo" format (e.g. "alice/myrepo")
# payload_json : raw JSON string of the event-specific payload — kept as a
#                blob so we don't have to model every event type's structure
# public       : whether the event appeared on the public event stream
# created_at   : when GitHub recorded the event (UTC)
# ingested_at  : when our consumer wrote this row (UTC) — useful for
#                measuring pipeline lag (ingested_at − created_at)
# ────────────────────────────────────────────────────────────────────────────

GITHUB_EVENT_SCHEMA = pa.schema(
    [
        pa.field("event_id",     pa.string(),                  nullable=False),
        pa.field("event_type",   pa.string(),                  nullable=False),
        pa.field("actor_id",     pa.int64(),                   nullable=True),
        pa.field("actor_login",  pa.string(),                  nullable=True),
        pa.field("repo_id",      pa.int64(),                   nullable=True),
        pa.field("repo_name",    pa.string(),                  nullable=False),
        pa.field("payload_json", pa.string(),                  nullable=True),
        pa.field("public",       pa.bool_(),                   nullable=True),
        pa.field("created_at",   pa.timestamp("us", tz="UTC"), nullable=False),
        pa.field("ingested_at",  pa.timestamp("us", tz="UTC"), nullable=False),
    ]
)

# Convenience: the column names as a list, useful for validation in tests
SCHEMA_COLUMNS: list[str] = [field.name for field in GITHUB_EVENT_SCHEMA]
