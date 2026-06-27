#!/usr/bin/env bash
# StreamLens — rewrite git history into logical commits
#
# Run this from the repo root:
#   bash scripts/rewrite_git_history.sh
#
# What it does:
#   The big "Days 4-9" commit has already been soft-reset (your working tree
#   is unchanged). This script commits the files in 7 logical groups so that
#   git log tells a coherent story.

set -e  # exit on any error

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$REPO_ROOT"

echo "=== StreamLens: splitting commits ==="
echo "Working in: $REPO_ROOT"
echo ""

# Safety check — make sure the big commit is already reset
if git show HEAD --stat | grep -q "Days 4-9"; then
    echo "ERROR: The big commit is still HEAD. Please run:"
    echo "  git reset HEAD~1"
    echo "then re-run this script."
    exit 1
fi

# ── Commit 1: Storage layer ────────────────────────────────────────────────────
echo "[1/7] Storage layer (schema, writer, reader, SQL queries)..."
git add requirements.txt \
    src/storage/__init__.py \
    src/storage/schema.py \
    src/storage/writer.py \
    src/storage/reader.py \
    src/storage/queries/event_counts_by_type.sql \
    src/storage/queries/top_repos.sql \
    src/storage/queries/recent_events.sql \
    src/storage/queries/avg_lag.sql \
    src/storage/CLAUDE.md \
    tests/test_storage.py

git commit -m "feat: storage layer — PyArrow schema, Parquet writer, DuckDB reader

- schema.py: 10-field PyArrow schema (source of truth for all layers)
- writer.py: flatten_event() + write_batch() with event-time partitioning
  and 24h watermark (late events → date=late/)
- reader.py: DuckDB singleton + 5 query functions (event counts, top repos,
  recent events, total count, avg lag)
- queries/: SQL files for queries > 5 lines
- tests: 28 tests covering schema, writer, reader, late-arriving events"

# ── Commit 2: Kafka consumer ───────────────────────────────────────────────────
echo "[2/7] Kafka consumer..."
git add src/consumer.py

git commit -m "feat: Kafka consumer — micro-batch flush and at-least-once delivery

- Reads from Kafka topic, accumulates events in micro-batches
- Flushes when batch hits BATCH_SIZE OR FLUSH_INTERVAL_SECONDS elapses
- Offset commit happens AFTER successful Parquet write (at-least-once)
- NoBrokersAvailable handled gracefully on startup"

# ── Commit 3: Dashboard ────────────────────────────────────────────────────────
echo "[3/7] Rich terminal dashboard..."
git add src/dashboard/__init__.py \
    src/dashboard/dashboard.py \
    src/dashboard/CLAUDE.md

git commit -m "feat: dashboard — Rich 4-panel terminal UI with live refresh

Layout:
  - Header: pipeline status indicator + topic name + refresh rate
  - Left panel: scrolling live event feed (last 20 events, color by type)
  - Top-right: event type breakdown with ASCII bar chart
  - Bottom-right: top repositories by event count
  - Footer: total events, pipeline lag metric (green/yellow/red), timestamp

Refreshes every REFRESH_INTERVAL seconds (default: 4s)"

# ── Commit 4: Compaction + lag metric ─────────────────────────────────────────
echo "[4/7] Compaction + lag metric..."
git add src/storage/compaction.py \
    tests/test_compaction.py

git commit -m "feat: compaction — merge small Parquet files + pipeline lag metric

Compaction:
  - compact_partition(): read all small files → pa.concat_tables() → write
    one merged file → delete originals (write-then-delete for crash safety)
  - compact_all(): run across all date partitions, oldest-first
  - Idempotent: safe to run multiple times on the same partition

Lag metric (reader.py):
  - get_avg_lag(): avg/min/max lag in seconds over a configurable window
  - Uses avg(ingested_at - created_at) as pipeline health signal
  - Lag coloring in dashboard: green < 30s, yellow 30-60s, red > 60s

Tests: 15 compaction tests"

# ── Commit 5: Producer refactor ────────────────────────────────────────────────
echo "[5/7] Producer refactor..."
git add src/producer.py

git commit -m "refactor: producer — env vars, structlog, ETag conditional requests

- All config moved to env vars via python-dotenv (no hardcoded values)
- Replaced print() with structlog structured logging
- ETag support: If-None-Match header avoids re-publishing unchanged events
  (saves rate limit quota — 60 req/hr unauthenticated, 5000 with token)
- GITHUB_TOKEN support for higher rate limits
- Graceful handling of ConnectionError, Timeout, HTTPError"

# ── Commit 6: Processors layer ────────────────────────────────────────────────
echo "[6/7] Processors layer..."
git add src/processors/__init__.py \
    src/processors/base.py \
    src/processors/default.py \
    src/processors/push_event.py \
    src/processors/watch_event.py \
    src/processors/pull_request_event.py \
    tests/test_processors.py

git commit -m "feat: processors — per-event-type validation and enrichment

Pattern: Strategy + Registry
  - EventProcessor ABC: every processor must implement process() → ProcessorResult
  - Registry in __init__.py: maps event type strings to processor classes
  - DefaultProcessor: catch-all so unknown event types are never silently dropped
  - get_processor() returns cached singleton instances

Processors implemented:
  - PushEventProcessor: validates ref + commits, extracts branch + commit count
  - WatchEventProcessor: validates action field
  - PullRequestEventProcessor: validates action/number, detects merged vs closed

Consumer change: ValidationError → log + skip (never crashes the pipeline)

Tests: 32 tests across registry, all 4 processor types, ValidationError structure"

# ── Commit 7: Docs, CI, env example ───────────────────────────────────────────
echo "[7/7] Docs, CI, env example..."
git add README.md \
    .env.example \
    docs/interview_narrative.md \
    docs/schema_changelog.md \
    docs/devlog.md \
    CLAUDE.md \
    src/CLAUDE.md \
    .github/workflows/ci.yml

git commit -m "docs: README, interview narrative, schema changelog, CI workflow

- README.md: architecture diagram, stack table, Quick Start, Engineering
  Design Decisions (Kafka rationale, Parquet+DuckDB, event-time partitioning,
  small file compaction, at-least-once delivery)
- docs/interview_narrative.md: 8 interview Q&As in first-person
- docs/schema_changelog.md: v1.0.0 and v1.1.0 schema history + migration guide
- .env.example: all environment variables with defaults and comments
- .github/workflows/ci.yml: GitHub Actions — pytest on every push/PR"

echo ""
echo "=== Done! ==="
echo ""
git log --oneline
echo ""
echo "Now push with:"
echo "  git push origin main --force"
