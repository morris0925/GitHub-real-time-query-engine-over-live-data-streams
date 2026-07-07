# StreamLens — Project Guide for Claude Code

## What This Project Is
A real-time data pipeline that polls the GitHub Events API, pushes events into Kafka, stores them as Parquet files via PyArrow, queries them with DuckDB, and displays metrics in a Rich terminal dashboard. Built as a portfolio project demonstrating data engineering fundamentals (streaming, storage, distributed systems).

## Tech Stack
| Layer | Technology |
|-------|-----------|
| Data source | GitHub Events API (polled every 5 seconds) |
| Message broker | Apache Kafka (`kafka-python`) |
| Storage | PyArrow → Parquet files |
| Query engine | DuckDB (reads Parquet directly, no separate DB server) |
| Terminal dashboard | Rich (Python) |
| AI diagnostic API | FastAPI + DuckDB VSS + Claude Haiku (see docs/ai-interface-design-proposal.md) |
| AI dashboard | Next.js (`frontend/`, separate from the Python core) |
| Container | Docker + Docker Compose (Kafka + Zookeeper) |

## Current Project State
Days 1–11 complete and pushed to GitHub. All pipeline layers implemented and tested (155 tests passing).
In progress: AI diagnostic layer (docs/ai-interface-design-proposal.md §6 MVP) — `src/knowledge/`, `src/anomaly/`, `src/api/`, `frontend/`.

```
streamlens/
├── src/
│   ├── producer.py           # GitHub Events → Kafka ✅
│   ├── consumer.py           # Kafka → Parquet (micro-batch) + DLQ ✅
│   ├── cli.py                # Click CLI — events/stats/repos/lag/dlq ✅
│   ├── processors/           # Per-event-type validation + enrichment ✅
│   ├── storage/
│   │   ├── schema.py         # PyArrow schema (source of truth) ✅
│   │   ├── writer.py         # Event-time partitioning + watermark ✅
│   │   ├── reader.py         # DuckDB query functions ✅
│   │   ├── compaction.py     # Small file merging ✅
│   │   ├── dlq_writer.py     # Dead Letter Queue Parquet writer ✅
│   │   ├── jsonl_writer.py   # JSONL writer (benchmark comparison) ✅
│   │   └── queries/          # SQL files > 5 lines ✅
│   └── dashboard/
│       └── dashboard.py      # Rich 4-panel terminal UI ✅
├── tests/                    # 155 tests ✅
├── scripts/
│   └── benchmark.py          # Parquet vs JSONL benchmark ✅
├── results/
│   └── benchmark_results.json
├── docs/
│   ├── devlog.md             # Daily engineering log (English)
│   ├── design-faq.md         # Engineering Q&A for interviews (English)
│   ├── schema_changelog.md   # Schema version history (English)
│   └── benchmark.md          # Benchmark report
├── .github/workflows/ci.yml  # GitHub Actions (pytest on push) ✅
├── .env.example
├── docker-compose.yml
└── requirements.txt
```

## Language Rules

**All output must be in English.** This is a portfolio project targeting US companies.
- All code comments, docstrings, and inline documentation: English only
- All docs/ files (devlog, schema changelog, benchmark, etc.): English only
- Commit messages: English only
- README and any user-facing text: English only

The only exception is this CLAUDE.md file itself and private local notes — but even those should prefer English.

## Coding Rules

### Python (all layers)
- Python 3.11, type hints on every function signature
- Use `pyarrow` and `duckdb` directly — never pandas
- Kafka: use `kafka-python` library (`from kafka import KafkaProducer`, `KafkaConsumer`)
- Environment variables via `python-dotenv` — never hardcode broker address, ports, or credentials
  - Note: early scripts (producer.py) have hardcoded values — refactor these when building consumer/storage layer
- Error handling: use `structlog` for logging (not bare `print()`), don't swallow exceptions silently
- Naming: `snake_case` for functions/variables, `PascalCase` for classes, `UPPER_SNAKE` for constants

### General
- Every new module needs a corresponding test file in `tests/`
- Commit messages: `feat:`, `fix:`, `refactor:`, `test:`, `docs:` prefixes
- Never commit `.env` files

## Git Workflow Rules (learned the hard way)

**Commit per logical unit, not per day.**
Each feature or layer gets its own commit with a clear message. Don't accumulate multiple days of work into one giant commit — it makes the git log useless and looks like the code was generated all at once.

**Push at the end of every session.**
`git push` after every working session. Forgetting this means the remote is stale and you may have to force-push later (which rewrites history and is risky on shared branches).

**Commit message format:**
```
feat: short description of what was added

- Bullet 1: key design decision or non-obvious detail
- Bullet 2: what tests cover this
```

**Suggested commit cadence for this project:**
- After each new module (schema → writer → reader → consumer → dashboard → etc.)
- After a refactor (producer env-var refactor = its own commit)
- After adding tests for a layer
- After docs updates

**`.git/index.lock` or `.git/HEAD.lock` errors:**
These appear when a previous git process crashed without cleanup. Fix:
```bash
rm .git/index.lock   # if it exists
rm .git/HEAD.lock    # if it exists
```
Then retry the git command.

**Never batch-commit multiple features in one shot.**
Even if development was done in one session, stage and commit feature by feature before pushing. The commit history is part of the portfolio — it tells the story of how the project was built.

## Docker
- `docker-compose.yml` at root runs Kafka + Zookeeper
- Use named volumes for data persistence
- Health checks required on kafka container
- No web server container in docker-compose — the FastAPI service and Next.js dev server run directly on the host for the demo

## What NOT to Do
- Don't use pandas (use PyArrow/DuckDB instead)
- Don't hardcode config values — everything via env vars
- Don't write raw SQL strings longer than 5 lines inline — extract to `src/storage/queries/`
- Don't add new Python dependencies without updating `requirements.txt`
- Don't mix concerns: pipeline logic, storage logic, and dashboard logic stay in separate files
- Don't build web UI for the core pipeline — the pipeline dashboard is a Rich terminal app. The AI diagnostic layer is the one exception: it ships a Next.js frontend (`frontend/`) talking to `src/api/` over HTTP, per docs/ai-interface-design-proposal.md. Never mix the two: the Rich dashboard stays untouched.
- Don't let the Next.js app reimplement data logic — it only calls the FastAPI service
- AI-generated content must follow the trust rules in docs/ai-interface-design-proposal.md §4 (violet accent, per-block labels, qualitative confidence bands, hedged prompt language)
