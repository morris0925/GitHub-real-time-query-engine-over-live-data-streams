# StreamLens — Dev Log

Daily record of what was built, why decisions were made, and what was learned.

---

## Day 1 — Environment Setup & Docker

**Goal:** Get Kafka running locally.

**What was done:**
- Created the `streamlens/` project folder structure
- Wrote `docker-compose.yml`: two services — Zookeeper (Kafka's dependency) and the Kafka broker
- Added a health check to the Kafka container to ensure it's truly ready before marking it as started
- Used a named volume so Kafka data persists across `docker-compose down`
- Verified: `docker-compose up -d` → both containers healthy → `docker-compose down`

**What was learned:**
- Kafka requires Zookeeper to manage broker metadata (who is the leader, which partitions are where)
- Docker health checks allow `depends_on` to wait until Kafka is genuinely ready before starting dependent services
- Named volumes vs bind mounts: named volumes are managed by Docker and cleaner for this use case

**Key configuration (docker-compose.yml):**
- `KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092` — tells producers/consumers where to connect
- `KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1` — single-node dev setup, no replication needed

---

## Day 2 — GitHub Events API

**Goal:** Hit the GitHub API with Python and understand the data structure.

**What was done:**
- Wrote a simple script to call `https://api.github.com/events`
- Explored the response structure: each event has `id`, `type`, `actor`, `repo`, `payload`, `created_at`
- Discovered GitHub's ETag mechanism: a second call for unchanged data returns 304 Not Modified
- Implemented ETag caching: store the last ETag and pass it in `If-None-Match` on subsequent requests

**What was learned:**
- The GitHub Events API is public — no token required (but rate-limited to 60 req/hr)
- ETag is HTTP's conditional request mechanism: the server says "this resource's version is XYZ," the client asks next time "is it still XYZ?" — if yes, 304 + empty body
- Most common GitHub event types: `PushEvent`, `WatchEvent` (star), `CreateEvent` (new branch/repo)

**Data structure (simplified):**
```json
{
  "id": "12345678901",
  "type": "PushEvent",
  "actor": { "id": 1, "login": "alice" },
  "repo": { "id": 99, "name": "alice/myrepo" },
  "payload": { "commits": [...] },
  "public": true,
  "created_at": "2024-01-15T10:30:00Z"
}
```

---

## Day 3 — GitHub Events → Kafka Producer

**Goal:** Push API events into a Kafka topic.

**What was done:**
- Wrote `src/producer.py`: polls GitHub every 5 seconds, sends each event to the `github-events` topic
- Used `kafka-python`'s `KafkaProducer` with a `value_serializer` that automatically converts dicts to JSON bytes
- Message key = `event["id"]` (GitHub's event ID), so Kafka routes events with the same ID to the same partition
- Error handling: `NoBrokersAvailable` prints a hint (Docker not running)
- Config: broker address and topic name hardcoded in this version (TODO: switch to env vars)

**What was learned:**
- Kafka's core concept: Producer → Topic → Consumer
- A topic is a named log that can have multiple partitions
- `producer.flush()` ensures buffered messages are actually sent before the process exits
- Serialization: Kafka stores only bytes, so Python dicts must be `json.dumps()`-ed then `.encode("utf-8")`-ed

**Why each event is a separate message:**
The GitHub API returns up to 30 events per call as an array, but we publish each event as an independent Kafka message. This lets the consumer process them individually and distributes them more evenly across partitions.

---

## Day 4 — Storage Layer + Kafka Consumer

**Goal:** Write Kafka events to Parquet files and build the DuckDB query layer.

### `src/storage/schema.py`
- Defined `GITHUB_EVENT_SCHEMA`: a PyArrow schema with 10 fields
- Flattened nested JSON fields (`actor.login`, `repo.name`) into top-level columns
- Stored complex `payload` as a JSON string (avoids needing a different schema per event type)
- Added `ingested_at` field (when the consumer wrote the event) to enable pipeline lag calculation

### `src/storage/writer.py`
- `flatten_event()`: converts one raw GitHub event dict into a flat dict matching the schema
- `write_batch()`: a list of events → PyArrow Table → Parquet file
- Date partitioning: `data/events/date=2026-06-25/part-<uuid>.parquet`
- Snappy compression (fast, moderate compression ratio — appropriate for analytics workloads)
- UUID filenames prevent collisions from concurrent writes

### `src/storage/reader.py`
- DuckDB module-level singleton connection — one connection reused for all queries
- `get_recent_events()` — most recent N events, for the dashboard feed
- `get_event_counts_by_type()` — grouped counts by type, for the stats panel
- `get_top_repos()` — most active repositories, for the top repos panel
- `get_total_event_count()` — total count, for the status bar
- Queries use a DuckDB glob: `read_parquet('data/events/**/*.parquet')` scans all partitions at once

### `src/storage/queries/` — three SQL files
- `event_counts_by_type.sql`
- `top_repos.sql`
- `recent_events.sql`
- SQL queries longer than 5 lines are extracted to `.sql` files and loaded with `Path.read_text()`

### `src/consumer.py`
- `enable_auto_commit=False` — offsets committed manually, only after a successful Parquet write
- Micro-batch: flushes when 100 messages are accumulated OR 30 seconds have elapsed, whichever comes first
- Error handling: `StorageWriteError` → do not commit (let Kafka redeliver); other exceptions → raise
- `auto_offset_reset="earliest"` — on first run, start from the beginning of the topic

### `tests/test_storage.py`
- 23 unit tests, all passing
- Coverage: schema validation, `flatten_event` edge cases (bad timestamps, missing actor/payload), `write_batch` round-trip, all reader functions
- Uses pytest `tmp_path` fixture — tests never touch the real `data/` directory

**Key design decisions:**
- Why date partitioning? DuckDB can do partition pruning — when querying only today's data, it skips all other directories entirely
- Why `ingested_at`? Enables pipeline lag measurement (`ingested_at - created_at`)
- Why store payload as a JSON string rather than a nested struct? GitHub has 30+ event types each with a different payload structure; a unified string is simplest, with `json.loads()` when field-level access is needed

---

## Day 5 — Rich Terminal Dashboard

**Goal:** Turn DuckDB query results into an auto-refreshing terminal UI using Rich.

### `src/dashboard/dashboard.py`
Four-panel layout:
```
┌─ ● StreamLens  │  topic: github-events  │  ↻ every 4s ─────────────┐
├──── Live Event Feed (last 20) ───┬─── Event Types (last 60 min) ────┤
│  PushEvent  alice  torvalds/...  │  PushEvent    3  ██████████████  │
│  WatchEvent diana  django/...    │  WatchEvent   2  ████████        │
│  ForkEvent  bob    python/...    │  ForkEvent    1  ████            │
│                                  ├─── Top Repositories ─────────────┤
│                                  │  1  torvalds/linux   3 events    │
│                                  │  2  django/django    2 events    │
╰──────────────────────────────────┴──────────────────────────────────╯
╭─ Total events: 7  │  Updated: 14:17:49 UTC  │  Ctrl+C to exit ──────╮
```

**Details:**
- `rich.live.Live(screen=True)` — takes over the full terminal; restores it on exit
- Data refreshes every 4 seconds via `time.sleep(4)`
- Each event type has its own color (`PushEvent` = green, `WatchEvent` = yellow, `PullRequestEvent` = blue...)
- Stats panel includes an ASCII bar chart (`█` characters) so relative magnitudes are immediately visible
- Empty data shows "no data yet" — no crashes
- Header dot `●`: green when data is present, gray when not

**Rich Layout structure:**
```
root (vertical)
├── header   (size=3, fixed height)
├── body     (fills remaining space)
│   ├── left   (ratio=55, event feed)
│   └── right  (ratio=45)
│       ├── counts (ratio=45, event type stats)
│       └── repos  (ratio=55, top repos)
└── footer   (size=3, fixed height)
```

**How to run the full pipeline:**
```bash
# Terminal 1
docker-compose up -d

# Terminal 2
PYTHONPATH=src python src/producer.py

# Terminal 3
PYTHONPATH=src python src/consumer.py

# Terminal 4
PYTHONPATH=src python src/dashboard/dashboard.py
```

---

## Day 6 — Refactor + Compaction + Lag Monitoring

**Goal:** Address three gaps: hardcoded producer config, compaction for production readiness, and a quantifiable pipeline health metric.

### 1. Refactor producer.py — env vars + structlog

**What changed:**
- Replaced three hardcoded constants (`KAFKA_BROKER = "localhost:9092"` etc.) with `os.getenv()` + `python-dotenv`
- Added `GITHUB_TOKEN` env var support (raises rate limit from 60 to 5,000 req/hr)
- Replaced all `print()` calls with `structlog` for consistent structured output

**Why:** Hardcoded broker addresses make the producer impossible to deploy. Env vars let the same image target any environment.

---

### 2. storage/compaction.py — small file merging

**The problem (Small File Problem):**
The consumer flushes every 30 seconds — 2,880 small Parquet files per partition per day. Every DuckDB query must open and read the footer metadata of all 2,880 files before touching any actual data. The syscall overhead far exceeds the time spent reading data.

**The solution:**
```
Before:
  date=2026-06-25/
    part-a1b2.parquet   (50 rows)
    part-c3d4.parquet   (50 rows)
    ... × 2880

After compact_partition():
  date=2026-06-25/
    compacted-uuid.parquet  (144,000 rows)
```

**Key details:**
- Write the new file first; delete originals only after a successful write — no data loss on crash
- `min_files=2`: only compact when more than one file exists
- `compacted-` prefix: makes merged files visually distinguishable from originals
- `compact_all()` scans all date partitions; typically scheduled as a nightly cron job

This is the same operation as Delta Lake's `OPTIMIZE`, Apache Iceberg's `rewrite_data_files`, and Spark's `coalesce()`.

---

### 3. Lag monitoring — reader.py + dashboard

**New `get_avg_lag()`:**
- Computes `AVG(ingested_at - created_at)` — the average time from GitHub recording an event to us writing it to Parquet
- SQL lives in `storage/queries/avg_lag.sql` (consistent with the "5-line rule")
- Returns min, max, and sample_size alongside the average

**Dashboard status bar update:**
```
Before:  Total events: 1,234  │  Last updated: 14:17:36 UTC
After:   Total: 1,234 events  │  Lag: 28.4s avg (n=847)  │  Updated: ...
```

Lag color coding:
- Green: < 30s (healthy — close to the flush interval)
- Yellow: 30–60s (elevated — possible Kafka backlog)
- Red: > 60s (problem)

**Why `ingested_at - created_at` is meaningful:**
- `created_at` = when GitHub recorded the event
- `ingested_at` = when our consumer wrote it to Parquet
- The difference = end-to-end pipeline latency from source to storage

---

**Tests:** `tests/test_compaction.py` — 15 tests. Total: 38 passing (23 existing + 15 new).

---

## Day 7 — Late-Arriving Events + README

**Goal:** Fix the partition design flaw in writer.py, and package the project as a portfolio-grade README.

### 1. writer.py rewrite — Event-Time Partitioning + Watermark

**The original problem:**

Day 4's writer placed all events in today's partition:
```python
today_str = ingested_at.strftime("%Y-%m-%d")   # based on processing time
partition_dir = data_dir / f"date={today_str}"
```

This meant:
- An event with `created_at=yesterday` landed in `date=today/`
- A query for "yesterday's data" had DuckDB skip yesterday's partition entirely via pruning — the event disappeared

**Fix — Event-Time Partitioning with Watermark:**
```
Event age ≤ 24 hours  →  write to date=<created_at date>/
Event age > 24 hours  →  write to date=late/  (quarantine)
```

The core function `_partition_key(created_at, ingested_at, threshold_hours)` decides which partition each event belongs in.

**API change:**
`write_batch()` now returns `list[Path]` instead of a single `Path`, because a single batch may span multiple dates (e.g. a consumer restart replaying both yesterday's and today's events).

**Why "watermark":**
"Watermark" is the streaming systems term for "we consider all events before this point to have arrived." Late events beyond the watermark go to a side output (`date=late/`). Apache Flink, Spark Structured Streaming, and Apache Beam all use this mechanism.

**New tests `TestLateArrivingEvents` (5 tests):**
- Recent event → date partition
- Event past watermark → `date=late/`
- Mixed batch → two separate files
- Late partition row count is correct
- Event just inside the watermark is not classified as late

---

### 2. README.md

Complete rewrite covering:
- ASCII architecture diagram
- Quick Start (three terminals to run the full pipeline)
- Environment variable reference table
- Full project structure
- **Engineering Design Decisions** — the key section: explains why each technical choice was made, not just what was used

Design decisions cover four questions that come up in data engineering interviews:
1. **Why Kafka?** — decoupling, rate limit protection, replay buffer
2. **Why Parquet + DuckDB instead of a database?** — analytics vs transactional, columnar storage advantages, S3 + Athena analogy
3. **Why partition by event time instead of ingestion time?** — correctness vs complexity, watermark tradeoff
4. **Why compact?** — small file problem, 2,880 files → 1 file

---

**Tests:** 43 passing (+5 late-event tests).

---

## Day 8 — processors/ Layer

**Goal:** Build the last missing architectural block from the plan: a per-event-type processor layer.

### processors/ layer

**Background:**
GitHub's Events API has 30+ event types, each with a completely different `payload` structure. Putting all validation and enrichment logic into consumer.py produces a tangle of `if event_type == "PushEvent": ... elif event_type == "WatchEvent": ...` that's hard to maintain and test.

**Solution: Strategy Pattern**

One processor class per event type, all inheriting from the same abstract base class:

```
processors/
├── __init__.py           # Registry + get_processor()
├── base.py               # EventProcessor ABC, ValidationError, ProcessorResult
├── push_event.py         # PushEventProcessor
├── watch_event.py        # WatchEventProcessor
├── pull_request_event.py # PullRequestEventProcessor
└── default.py            # DefaultProcessor (fallback for unknown types)
```

**What each processor does:**
1. **Validate** — check that required fields are present; raise `ValidationError` if not
2. **Enrich** — extract useful metrics from the payload (not written to Parquet; used for logging/monitoring)

**`ProcessorResult` dataclass:**
```python
@dataclass
class ProcessorResult:
    event:   dict   # original event (possibly enriched)
    metrics: dict   # extracted metrics (commit_count, branch, is_merged...)
    skipped: bool   # if True, consumer does not store this event
```

**Registry + singleton:**
`get_processor("PushEvent")` returns a cached `PushEventProcessor` instance. Unknown types return `DefaultProcessor` (pass-through — no data dropped). Adding a new processor requires only: write the class, add to `REGISTRY`, write tests. consumer.py never changes.

**`PushEventProcessor` metrics example:**
```python
{
    "commit_count":      3,
    "branch":            "main",         # stripped "refs/heads/"
    "is_default_branch": True,
    "distinct_size":     3,
}
```

**`PullRequestEventProcessor` `is_merged` logic:**
GitHub has no separate "MergedEvent". Detecting whether a PR was merged vs closed:
```python
is_merged = (action == "closed") and bool(pr.get("merged"))
```

**consumer.py change:**
Added the processor layer inside the poll loop:
```python
result = get_processor(event["type"]).process(raw_event)
# ValidationError → log + skip (do not add to batch)
# result.skipped → also skip
# success → batch.append(result.event)
```
Offset commit logic unchanged: only commit after write_batch succeeds.

---

**Tests:** `tests/test_processors.py` — 32 new tests covering the registry, every processor's valid/invalid paths, and `ValidationError` structure.

Total: **75 passing** (43 existing + 32 new).

---

## Day 9 — Interview Narrative + Schema Changelog + Git

**Goal:** Wrap up the portfolio — write interview preparation docs, add schema history, push everything to GitHub.

### docs/design-faq.md
Complete interview preparation document with answers in first person. Each question has a short version (15-second answer) and an expanded version (3–5 minute deep dive).

Questions covered:
- "Tell me about a project you built." — full pipeline overview
- "Why Kafka?" — decoupling, rate limit protection, replay, scalability
- "Why Parquet + DuckDB?" — analytics vs transactional, columnar format, S3+Athena analogy
- "How do you handle late-arriving events?" — event-time partitioning + watermark + side output
- "What's the small file problem?" — syscall overhead + compaction solution, Delta Lake OPTIMIZE analogy
- "How do you ensure data isn't lost?" — at-least-once, offset commit ordering, deduplication strategy
- "What would you do differently?" — asyncio producer, schema registry, Prometheus metrics
- "What did you learn?" — Kafka partition mechanics, Parquet footer/predicate pushdown, watermark as a tradeoff

### docs/schema_changelog.md
Records:
- **v1.0.0** (Day 4): initial 10-field schema, `ingested_at` for lag calculation, rationale for JSON string payload
- **v1.1.0** (Day 7): schema unchanged but partition strategy switched to event-time + watermark
- **Candidate future changes**: `actor_type` (human vs bot), `org_login` (organization)
- **Migration strategies**: forward-only (add nullable fields) vs backfill (change types)

### Git
Pushed to `morris0925/GitHub-real-time-query-engine-over-live-data-streams`. 75 tests passing.

---

## Day 10 — CLI, Extended Processors, Dead Letter Queue

**Goal:** Upgrade the portfolio from "runs correctly" to "usable, observable, and extensible."

### 1. src/cli.py — interactive query interface

**Why a CLI:**
The Rich dashboard is good for continuous monitoring. For quick one-off questions — "what events came in the last 10 minutes?" or "what's the lag right now?" — a CLI is faster and more scriptable. It wraps the DuckDB reader as a one-shot query tool.

**Five subcommands:**
```bash
python src/cli.py events              # most recent 20 events
python src/cli.py events --type PushEvent --limit 50
python src/cli.py stats --since 30   # event counts for the last 30 minutes
python src/cli.py repos --top 5      # top 5 most active repos
python src/cli.py lag                # pipeline lag statistics
python src/cli.py dlq                # inspect the Dead Letter Queue
```

**Why Click over argparse:**
- Auto-generated `--help` is better formatted
- `@click.pass_context` threads global options (`--data-dir`) through all subcommands cleanly
- `CliRunner` makes unit testing trivial (no subprocess needed)
- Declarative API is more readable than argparse's `add_argument` style

**Output uses Rich:**
Already a dependency. Rich Table output matches the dashboard's visual style — same color mapping per event type so users aren't relearning the UI when switching tools.

---

### 2. Extended processors/ — IssuesEvent, ForkEvent, CreateEvent

**Why add these:**
Day 8's REGISTRY covered only 3 types. The top 6 most frequent types in GitHub's public event stream are:
1. `PushEvent` ✅
2. `CreateEvent` ← added
3. `WatchEvent` ✅
4. `IssuesEvent` ← added
5. `PullRequestEvent` ✅
6. `ForkEvent` ← added

`DefaultProcessor` (pass-through) means no data is dropped for these types, but we also get no metrics. Adding typed processors means the consumer log now carries `action`, `issue_number`, `is_closed`, and `label_count` for every IssuesEvent, `fork_full_name` and `fork_owner` for ForkEvent, and `ref_type` and `is_semver_tag` for CreateEvent.

**Design highlights per processor:**

`IssuesEventProcessor`:
- `is_closed` uses `action == "closed"`, not `issue.state == "closed"` — because the event's `action` field reflects what just happened, not the current state
- `label_count` tracking: label changes are a meaningful signal in an issue's lifecycle

`ForkEventProcessor`:
- A fork is a stronger signal than a star: star = interest, fork = intent to work
- `is_private` tracking: private forks suggest commercial use

`CreateEventProcessor`:
- `ref_type` can be branch, tag, or repository
- `is_semver_tag`: regex detection of `v1.2.3` format → release signal
- Interview angle: "To track release frequency per repo, filter `CreateEvent` where `is_semver_tag=True` and group by `repo_name`."

Adding a processor requires only: write the class, add to `REGISTRY`. consumer.py is never modified — that's the value of the Strategy Pattern.

---

### 3. Dead Letter Queue — storage/dlq_writer.py

**The original problem:**
`ValidationError` in the consumer was handled as:
```python
except ValidationError as exc:
    log.warning("event_validation_failed", ...)
    total_skipped += 1
    continue
```
The event was logged and skipped, but **the raw event was permanently gone**. If the bug was in our processor code (not the data itself), we had no way to reprocess those events.

**DLQ design:**
```
data/
├── events/       ← normal pipeline Parquet (event-time partitioned)
└── dlq/          ← events that failed validation (separate schema)
    ├── dlq-<uuid>.parquet
    └── ...
```

DLQ schema (5 fields):
```
event_id     string
event_type   string
error_reason string
raw_json     string   ← full raw event JSON for replay
failed_at    timestamp(UTC)
```

**Consumer change:**
```python
# Before: log + skip, event gone forever
log.warning("event_validation_failed", ...)
total_skipped += 1

# After: log + write to DLQ, event retained
log.warning("event_validation_failed", ...)
write_dlq_entry(raw_event, reason=str(exc), dlq_dir=DLQ_DIR)
total_dlq += 1
```

DLQ writes can fail (disk full, permission error). In that case we `log.error` but do not raise — a DLQ write failure should not bring down the main consumer pipeline.

**`inspect_dlq()` + CLI:**
```bash
python src/cli.py dlq
```

Output:
```
⚠  3 invalid events in DLQ
  Failed At         Type        Event ID     Reason
  06-28 10:05:00    PushEvent   evt-1234     payload.ref is missing
  06-28 09:58:00    WatchEvent  evt-5678     payload.action is missing

Tip: check data/dlq/*.parquet for the full raw_json payload
```

When the DLQ is empty: `✓ DLQ is empty — no invalid events.`

**Why Parquet instead of a DLQ Kafka topic:**
Production systems typically use a DLQ Kafka topic, but Parquet works better for this portfolio:
- Same toolchain (DuckDB can query it directly)
- No extra Docker container
- One-line inspection: `python src/cli.py dlq`

The design intent is the same: retain problem events for later review.

---

**Tests:**
- `test_cli.py`: 31 new tests using Click's `CliRunner` (in-process, no subprocess)
- `test_dlq.py`: 19 new tests covering write, schema, roundtrip, edge cases
- `test_processors.py`: expanded to cover IssuesEvent (10), ForkEvent (7), CreateEvent (8)
- Total: **155 passing** (75 existing + 80 new)

---

## Day 11 — Parquet vs JSON-Lines Benchmark

**Goal:** Produce real numbers to back up the claim that "columnar is faster than row-oriented."

**Background:**
The project's interview pitch includes "I benchmarked query latency against row vs columnar storage." Before Day 11, that claim was unverified. Day 11 makes it true.

### New files

- `src/storage/jsonl_writer.py` — parallel implementation of the Parquet writer, same API: `write_batch_jsonl(events, jsonl_dir)` → `list[Path]`
- `scripts/benchmark.py` — 7 trials × 3 scales (10k / 100k / 500k events) × 5 queries × 2 formats
- `results/benchmark_results.json` — raw benchmark data (7 trials per query per format per scale)
- `docs/benchmark.md` — benchmark report with charts and analysis

**`jsonl_writer.py` design:**
- Reuses `flatten_event()` and `_partition_key()` from `writer.py` — no copy-paste
- Same date partitioning (`date=YYYY-MM-DD/part-*.jsonl`) for a fair comparison
- Uses `json.dumps()` with datetime → ISO-8601 string conversion so `DuckDB read_json_auto()` can parse directly

**The 5 benchmark queries:**

| Query | What it tests |
|-------|---------------|
| Q1: top repos | GROUP BY + ORDER BY LIMIT — the most common dashboard query |
| Q2: event type distribution | Low-cardinality column scan |
| Q3: time range filter | WHERE created_at >= ... — Parquet's strongest predicate pushdown case |
| Q4: actor activity | Multi-column aggregation + COUNT DISTINCT |
| Q5: push stats | JSON blob extraction — both formats must parse JSON |

**Measured results (median latency, 100k events):**

| Query | Parquet (ms) | JSONL (ms) | Speedup |
|-------|:---:|:---:|:---:|
| Q1 top repos | 3.3 | 77.4 | 23× |
| Q2 event type dist | 2.2 | 69.5 | 32× |
| Q3 recent filter | 1.8 | 102.7 | **56×** |
| Q4 actor activity | 5.2 | 88.1 | 17× |
| Q5 push stats | 12.9 | 37.2 | 3× |

**Why Q3 is the most extreme (56×):**
Parquet stores min/max statistics per column in each row group footer. DuckDB reads the footer and skips row groups that can't satisfy `WHERE created_at >= NOW() - INTERVAL 1 HOUR` — it never reads the data. JSON-lines must parse every row to evaluate the condition. This is predicate pushdown + columnar storage at its most powerful.

**Why Q5 has the smallest gap (3×):**
Q5 extracts `.size` from the `payload_json` string column. Regardless of format, DuckDB must parse a JSON blob per row. The bottleneck shifts from I/O to CPU JSON parsing — both formats slow down, and the gap narrows.

**Why speedup decreases slightly from 100k → 500k:**
At 10k events, overhead dominates (connection setup, SQL parsing, I/O). At 100k, the true format difference becomes visible and speedup peaks. At 500k, both formats slow proportionally, but Parquet's per-row-group skipping becomes slightly less effective when each partition has only one large file. In a real production scenario with many small files per partition, Parquet's advantage would only increase.

**Updated interview pitch:**
The earlier brief said "measured a 4x difference." The actual result is **6–56×**, depending on query pattern:
- Aggregation queries (GROUP BY): 10–30×
- Time filter queries (predicate pushdown): 35–56×
- JSON blob extraction queries: 2–3×

"On 100k events, Parquet is 17–56× faster than JSON-lines for aggregation queries. The filter queries are most extreme because Parquet's row-group statistics let DuckDB skip most of the data without reading it at all."

---

## Day 12 — AI Diagnostic Layer MVP (FastAPI + RAG + Next.js)

**Goal:** Build the §6 "1-week minimal demo" scope from docs/ai-interface-design-proposal.md: a FastAPI diagnostic service (RAG over closed issues/PRs + Claude Haiku) and a Next.js dashboard, as a peer layer that leaves the core pipeline and Rich terminal dashboard untouched.

**What was done:**
- `src/knowledge/` — knowledge base: GitHub REST ingestion of closed issues/PRs into `kb.parquet`, storing labels, time-to-resolve, and revert linkage (a later PR titled "Revert ..." pointing back at the original), not just embeddable text; Voyage AI embeddings (deterministic hash-stub fallback when no key); DuckDB retriever with best-effort VSS HNSW index and brute-force `array_cosine_similarity` fallback
- `src/knowledge/outcomes.py` — Tier 2 historical-outcome estimate: revert rate, avg time-to-resolve, severity-label rollup. Pure DuckDB aggregation, no LLM — the factual counterweight rendered beside the generated summary
- `src/anomaly/` — Tier 1 rule-based detection (CI failure spike / merge-time anomaly / commit drought), each a recent-window vs baseline comparison with min-sample guards; CI signal fetched from the GitHub Actions API since the public Events feed has no CI events; anomaly Parquet store with deterministic per-(type, hour) IDs; demo seeder for live demos
- `src/api/` — FastAPI: `GET /anomalies`, `GET /signal`, `GET /diagnose/{id}` (cached), `POST /query`, `POST /demo/anomaly`, `GET /health`
- `frontend/` — Next.js 15 single page: Dev Pipeline Signal bar (three components side by side, honest caption), incident feed polling every 7s, diagnosis panel in the exact §2 order, active-query box, demo trigger

**Trust labeling (§4), enforced at every layer:**
- Hedged language required in the Haiku system prompt itself ("likely related to…", causal claims forbidden) — the highest-leverage control, since the generated text must not overclaim regardless of UI
- Violet reserved exclusively for AI content; severity keeps red/orange/yellow; signal bar gets teal
- Sparkle + "AI-generated · verify before acting" on every generated block individually, plus a page-footer disclaimer backstop
- Confidence and similarity as qualitative bands (high/medium/low), never percentages
- Keyless/stub runs are labeled as placeholders in the UI and reported honestly in response meta

**What was learned:**
- The public GitHub Events feed contains no CI/status events — "CI failure rate" needs the Actions API (or webhooks) as a separate source. Worth stating in interviews: know what your data source can't tell you
- GitHub's revert button auto-inserts "Reverts owner/repo#N" into PR bodies, which makes revert-linkage mining a two-pass join over data we already fetch — Tier 2 impact estimates cost zero extra API calls
- DuckDB ≥1.0 has `array_cosine_similarity` in core; the VSS extension only adds the HNSW index. For a few hundred KB cases, brute force is plenty — the index is an optimization, not a dependency
- Degrade loudly, not silently: hash-stub embeddings and the stub LLM keep the demo running without keys, but every response carries the provider name so nothing fake can pass as real

**Not built (explicitly out of scope):** CLI changes, Slack/Teams push, auth, persisted feedback, metric chart in the panel, Tier 3 production telemetry, evaluation framework.
