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
The project is in active development. Current real structure:
```
streamlens/
├── src/
│   └── producer.py       # GitHub Events API → Kafka producer ✅ done
├── data/                 # Parquet storage (date-partitioned, gitignored)
├── docs/                 # Architecture notes, schema changelog
├── docker-compose.yml    # Kafka + Zookeeper ✅ done
└── README.md
```

Planned structure as development continues:
```
streamlens/
├── src/
│   ├── producer.py           # GitHub Events → Kafka
│   ├── consumer.py           # Kafka → storage writer
│   ├── processors/           # One file per event type (transform, validate)
│   ├── storage/
│   │   ├── writer.py         # PyArrow → Parquet write
│   │   ├── reader.py         # DuckDB query functions
│   │   ├── schema.py         # PyArrow schemas (source of truth)
│   │   └── queries/          # .sql files for queries > 5 lines
│   └── dashboard.py          # Rich terminal dashboard
├── tests/
├── data/
├── docs/
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
