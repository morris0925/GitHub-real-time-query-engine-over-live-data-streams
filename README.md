# StreamLens

A real-time data pipeline that ingests GitHub's public event stream, stores
events as partitioned Parquet files, and displays live metrics in a terminal
dashboard — built to demonstrate hands-on experience with distributed systems
problems at the core of data engineering roles.

```
GitHub Events API
      │  poll every 5s
      ▼
┌─────────────┐     JSON      ┌──────────────┐    Parquet    ┌──────────────────┐
│  producer   │ ──────────▶   │    Kafka     │ ──────────▶   │  data/events/    │
│  (Python)   │   messages    │  (Docker)    │   (consumer)  │  date=YYYY-MM-DD │
└─────────────┘               └──────────────┘               └────────┬─────────┘
                                                                      │
                                                          DuckDB glob scan
                                                                      │
                                                            ┌─────────▼──────────┐
                                                            │  Rich Terminal     │
                                                            │  Dashboard         │
                                                            │  (dashboard.py)    │
                                                            └────────────────────┘
```

## Stack

| Layer        | Technology                    | Why                                             |
|--------------|-------------------------------|-------------------------------------------------|
| Data source  | GitHub Events API             | Real-world public stream, no auth needed        |
| Broker       | Apache Kafka (`kafka-python`) | Decouples producer from consumer; replayable    |
| Storage      | PyArrow → Parquet (Snappy)    | Columnar, fast analytics, language-agnostic     |
| Query engine | DuckDB                        | In-process SQL over Parquet — no server needed  |
| Dashboard    | Rich (Python)                 | Full-screen terminal UI, no web server          |
| Container    | Docker + Docker Compose       | Reproducible local Kafka + Zookeeper            |

## Quick Start

**Prerequisites:** Python 3.11+, Docker, Docker Compose

```bash
# 1. Clone and install dependencies
git clone <repo>
cd streamlens
pip install -r requirements.txt

# 2. Start Kafka
docker-compose up -d

# 3. Run the pipeline (three separate terminals)
PYTHONPATH=src python src/producer.py                  # polls GitHub, publishes to Kafka
PYTHONPATH=src python src/consumer.py                  # reads Kafka, writes Parquet
PYTHONPATH=src python src/dashboard/dashboard.py       # live terminal UI

# 4. (Optional) Compact yesterday's small files
PYTHONPATH=src python src/storage/compaction.py

# 5. Stop Kafka when done
docker-compose down
```

## Configuration

All settings are read from environment variables (copy `.env.example` to `.env`):

| Variable                     | Default                      | Description                                     |
|------------------------------|------------------------------|-------------------------------------------------|
| `KAFKA_BROKER`               | `localhost:9092`             | Kafka bootstrap server                          |
| `KAFKA_TOPIC`                | `github-events`              | Topic name                                      |
| `KAFKA_GROUP_ID`             | `streamlens-events-consumer` | Consumer group                                  |
| `POLL_INTERVAL`              | `5`                          | Seconds between GitHub API polls                |
| `GITHUB_TOKEN`               | _(none)_                     | Personal access token (60 → 5,000 req/hr)       |
| `BATCH_SIZE`                 | `100`                        | Flush after this many messages                  |
| `FLUSH_INTERVAL_SECONDS`     | `30`                         | Or after this many seconds, whichever is first  |
| `DATA_DIR`                   | `data/events`                | Root directory for Parquet files                |
| `LATE_EVENT_THRESHOLD_HOURS` | `24`                         | Events older than this → `date=late/`           |
| `STATS_WINDOW_MINUTES`       | `60`                         | Look-back window for dashboard stats            |

## Project Structure

```
streamlens/
├── src/
│   ├── producer.py              # GitHub API → Kafka
│   ├── consumer.py              # Kafka → Parquet (micro-batch)
│   ├── storage/
│   │   ├── schema.py            # PyArrow schema (source of truth)
│   │   ├── writer.py            # Flatten + write to Parquet
│   │   ├── reader.py            # DuckDB query functions
│   │   ├── compaction.py        # Merge small files into large ones
│   │   └── queries/             # SQL files (> 5 lines)
│   │       ├── event_counts_by_type.sql
│   │       ├── top_repos.sql
│   │       ├── recent_events.sql
│   │       └── avg_lag.sql
│   └── dashboard/
│       └── dashboard.py         # Rich 4-panel terminal UI
├── tests/
│   ├── test_storage.py          # Schema, writer, reader, late-event tests
│   └── test_compaction.py       # Compaction + lag metric tests
├── data/                        # Parquet files (gitignored)
│   └── events/
│       ├── date=2026-06-25/
│       │   └── part-<uuid>.parquet
│       └── date=late/           # Late-arriving events (beyond watermark)
├── docs/
│   └── devlog.md                # Daily engineering log
├── docker-compose.yml
└── requirements.txt
```

## Engineering Design Decisions

### Why Kafka between the API and storage?

The GitHub API enforces a rate limit (60 req/hr unauthenticated). If the
storage layer or downstream consumers are slow, a direct API→storage design
would block or drop data. Kafka decouples the two: the producer runs at the
API's pace; the consumer runs at storage's pace. The topic also acts as a
replay buffer — if the consumer crashes, it restarts and re-reads from its last
committed offset without any data loss.

This mirrors how production systems at scale (Kinesis → S3, Pub/Sub → BigQuery)
are designed.

### Why Parquet + DuckDB instead of a database?

A traditional database (PostgreSQL, MySQL) requires a running server, schema
migrations, and connection pool management. For an analytics workload where
rows are written once and read many times, columnar Parquet files are a better
fit: DuckDB reads only the columns a query needs, skips partitions via pruning,
and runs entirely in-process with no server to manage.

The pattern (object storage + query engine) is identical to the AWS
S3 + Athena stack used at production scale, just local.

### Why date-partition by `created_at`, not ingestion time?

Partitioning by ingestion date is simpler but breaks time-based queries:
an event with `created_at=yesterday` written today would go in `date=today`,
making it invisible to `WHERE date='yesterday'` queries.

We partition by event time (`created_at`) with a **24-hour watermark**:

- Events within 24h of their `created_at` → correct date partition
- Events older than 24h → `date=late/` (quarantined for inspection)

This is the same "event-time windowing with watermarks" mechanism used by
Apache Flink, Spark Structured Streaming, and Apache Beam.

### Why compact small files?

The consumer writes a new Parquet file every ~30 seconds. After one full day:
~2,880 files per partition. DuckDB must open and read the footer metadata of
every file for any query — even a simple `COUNT(*)`. At thousands of files,
this overhead dominates query time.

The compaction script merges all files in a partition into one — reducing
2,880 file-open syscalls to 1. Run it once per day on the previous day's
(now "sealed") partition.

This is the same problem Apache Iceberg calls "small file compaction" and
Delta Lake solves with its `OPTIMIZE` command.

### Offset commit ordering (at-least-once delivery)

```python
write_batch(batch)   # 1. write to Parquet
consumer.commit()    # 2. commit Kafka offset  ← only after write succeeds
```

If the process crashes between steps 1 and 2, the Kafka offset is not
advanced. On restart, the consumer re-reads and re-processes those messages.
The batch may be processed twice — but it will never be silently dropped.
This is the standard "at-least-once" delivery guarantee.

### Idempotent write (making storage exactly-once on top of at-least-once ingest)

At-least-once delivery means `write_batch()` can see the same `event_id`
more than once — either redelivered within one batch, or in a fresh batch
after a crash-and-replay. `storage/writer.py` absorbs both cases before
anything is written to Parquet:

1. **Within a batch** — rows with an `event_id` already seen earlier in the
   same batch are dropped, keeping the first occurrence.
2. **Across batches** — before writing a date partition, the writer reads
   back the `event_id` column already stored in that `date=.../` directory
   and drops any row that's already there. A partition left with zero new
   rows is skipped entirely (no empty file).

Net result: **at-least-once Kafka delivery + idempotent Parquet write =
each GitHub `event_id` is stored at most once.** Covered by
`TestDedupe` in `tests/test_storage.py`, including a test that replays the
same event across two separate `write_batch()` calls and asserts exactly
one row lands on disk.

## Dashboard

```
╭─ ● StreamLens  │  topic: github-events  │  ↻ every 4s ────────────────────────╮
├──── Live Event Feed (last 20) ────────┬─── Event Types (last 60 min) ──────────┤
│  PushEvent    alice  torvalds/linux   │  PushEvent     312  ██████████████     │
│  WatchEvent   bob    django/django    │  WatchEvent    148  ████████           │
│  ForkEvent    carol  python/cpython   │  ForkEvent      41  ███                │
│  ...                                  ├─── Top Repositories ────────────────────┤
│                                       │  1  torvalds/linux    88 events         │
│                                       │  2  django/django     51 events         │
╰───────────────────────────────────────┴────────────────────────────────────────╯
╭─ Total: 12,483 events  │  Lag: 28.4s avg (n=847)  │  Updated: 14:30:01 UTC ────╮
```

The **Lag** metric shows average pipeline latency (`ingested_at − created_at`).
Color: green < 30s · yellow 30–60s · red > 60s.

## Running Tests

```bash
PYTHONPATH=src pytest tests/ -v
# 75 tests — schema, writer, reader, late-events, compaction, lag metric, processors
```
