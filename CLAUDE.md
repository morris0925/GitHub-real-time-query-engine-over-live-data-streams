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
| Container | Docker + Docker Compose (Kafka + Zookeeper) |

## Current Project State
Days 1–9 complete and pushed to GitHub. All layers implemented and tested (75 tests passing).

```
streamlens/
├── src/
│   ├── producer.py           # GitHub Events → Kafka ✅
│   ├── consumer.py           # Kafka → Parquet (micro-batch) ✅
│   ├── processors/           # Per-event-type validation + enrichment ✅
│   ├── storage/
│   │   ├── schema.py         # PyArrow schema (source of truth) ✅
│   │   ├── writer.py         # Event-time partitioning + watermark ✅
│   │   ├── reader.py         # DuckDB query functions ✅
│   │   ├── compaction.py     # Small file merging ✅
│   │   └── queries/          # SQL files > 5 lines ✅
│   └── dashboard/
│       └── dashboard.py      # Rich 4-panel terminal UI ✅
├── tests/                    # 75 tests ✅
├── docs/
│   ├── devlog.md             # Daily engineering log
│   ├── interview_narrative.md
│   └── schema_changelog.md
├── .github/workflows/ci.yml  # GitHub Actions (pytest on push) ✅
├── .env.example
├── docker-compose.yml
└── requirements.txt
```

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
- No web server container — this is a terminal-only project

## What NOT to Do
- Don't use pandas (use PyArrow/DuckDB instead)
- Don't hardcode config values — everything via env vars
- Don't write raw SQL strings longer than 5 lines inline — extract to `src/storage/queries/`
- Don't add new Python dependencies without updating `requirements.txt`
- Don't mix concerns: pipeline logic, storage logic, and dashboard logic stay in separate files
- Don't build a web frontend — the dashboard is a Rich terminal app
