# StreamLens — Engineering Design FAQ

Common questions about the architecture and implementation decisions behind StreamLens.
Written in first person for interview preparation, but shared here as a design reference.

---

## "Tell me about a project you built."

**Short answer:**
StreamLens is a real-time data pipeline that polls GitHub's public event stream, routes events through Kafka, stores them as date-partitioned Parquet files, and displays live metrics in a terminal dashboard — built to demonstrate hands-on experience with distributed systems problems at the core of data engineering.

**Full answer:**
I wanted to build something that traced the complete path of a streaming data system end-to-end — not just learn each tool in isolation, but understand why each piece exists and what problem it solves. I chose GitHub's public events API as the data source: it's real-world traffic, no account needed, and it produces 20+ event types which forced me to think about schema design and per-type validation.

The pipeline: a producer polls GitHub every 5 seconds and publishes events to Kafka. A consumer reads from Kafka, routes each event through a typed processor (validation + enrichment), accumulates a micro-batch, and writes date-partitioned Parquet files. DuckDB scans those files directly for queries. A Rich terminal dashboard displays live metrics with a 4-second refresh.

---

## "Why Kafka? Couldn't you just write directly to Parquet?"

**Short answer:**
Kafka solves three problems: rate limit protection, crash recovery without data loss, and independent scaling of producer and consumer.

**Full answer:**
The simplest design would be producer → Parquet directly. But that couples the two components tightly:

**Rate limiting.** GitHub's API allows 60 requests/hour unauthenticated. If the storage layer is slow or crashes, a direct design either blocks the producer or drops data. Kafka lets the producer write at its own pace and the consumer read at its own pace — they're decoupled.

**Replay on crash.** Kafka retains messages for 7 days by default. If the consumer crashes or has a bug, it restarts from its last committed offset and reprocesses. No data is lost. This is the at-least-once delivery guarantee.

**Horizontal scaling.** Today there's one consumer. If event volume grew, I could add consumer instances to the same consumer group and Kafka would rebalance partitions automatically — no code changes needed.

This mirrors production patterns: AWS Kinesis → Lambda → S3, or Google Pub/Sub → Dataflow → BigQuery. I'm reproducing that architecture locally with open-source tools.

---

## "Why Parquet + DuckDB instead of PostgreSQL?"

**Short answer:**
This is an analytics workload, not a transactional one. Parquet's columnar layout lets DuckDB skip columns and partitions it doesn't need. No server to manage, and it maps directly to the S3 + Athena pattern used at production scale.

**Full answer:**
PostgreSQL is designed for transactional workloads — many short reads and writes, row-oriented storage, ACID guarantees. For this use case: events are written once and never updated, and queries always aggregate over a time window with a few columns (`event_type`, `repo_name`, `created_at`). That's an analytics pattern.

Parquet is columnar: each column's bytes are stored contiguously on disk. A query like `SELECT event_type, COUNT(*) GROUP BY event_type` reads only the `event_type` column bytes and skips everything else. DuckDB also does partition pruning — if a query has `WHERE ingested_at >= NOW() - INTERVAL '60 MINUTE'`, it skips entire date directories without reading a single file.

DuckDB is in-process. `import duckdb` and you have a full SQL engine — no server, no connection pool, no migrations. The `read_parquet('data/events/**/*.parquet')` glob scans all partitions in one query.

Most importantly, this is the same architecture as AWS Athena (which runs on Presto/Trino) scanning S3 Parquet files. The local version is structurally identical, just smaller.

---

## "How do you handle late-arriving events?"

**Short answer:**
Event-time partitioning with a 24-hour watermark: each event goes into a partition named after its `created_at` date. Events older than 24 hours go to `date=late/` instead of their "correct" partition.

**Full answer:**
This was a real bug I hit. In the first version, the writer partitioned by ingestion date — events always went into today's folder. After a consumer restart that replayed old Kafka offsets, events with `created_at = yesterday` landed in today's partition. A query with `WHERE date = 'yesterday'` would use DuckDB's partition pruning to skip yesterday's folder entirely — those events became invisible.

The fix: partition by event time (`created_at` date). Each event now goes to the folder that matches when GitHub actually recorded it.

But this creates a new problem: if we accept late events into their "correct" partition forever, the partition never closes. A compaction job can't safely merge a partition that might still receive new files next week.

The solution is a **watermark**: events older than 24 hours go to `date=late/` instead of their dated partition. After 24 hours, a date partition is considered sealed and safe to compact.

This is the same mechanism Apache Flink calls a "watermark" and Spark Structured Streaming calls `withWatermark()`. The `date=late/` folder is what Flink calls a "side output."

The tradeoff: a longer watermark means more late events land in the right partition (more correct), but partitions take longer to seal (more storage overhead). The right value depends on how late your upstream data actually arrives.

---

## "What's the small file problem and how did you solve it?"

**Short answer:**
Flushing every 30 seconds produces 2,880 files per day per partition. DuckDB reads the footer metadata of every file on every query — thousands of syscalls instead of one. Compaction merges all files in a sealed partition into one.

**Full answer:**
Any system that writes frequently produces many small files. My consumer flushes every 30 seconds, so one partition accumulates ~2,880 files per day.

The problem: Parquet stores statistics (min/max per column, row counts) in a footer at the end of each file. Before DuckDB can decide whether to read a file's data, it must open and read its footer. For 2,880 files, that's 2,880 `open()` syscalls just to evaluate whether any data is in range — even for a `COUNT(*)`.

The fix: compaction. Read all files in a partition into PyArrow Tables, `pa.concat_tables()` them, write one merged file, then delete the originals. After compaction, one footer read per partition per query.

Critical detail: **write the new file first, then delete the originals.** If we deleted first and crashed during the write, data is permanently gone. Write-then-delete means the worst case is having both the originals and the merged file coexist briefly — no data loss.

This is exactly what Delta Lake's `OPTIMIZE` command and Apache Iceberg's `rewrite_data_files()` do at scale.

---

## "How do you ensure data isn't lost if the consumer crashes?"

**Short answer:**
Kafka offsets are committed only after a successful Parquet write. At-least-once semantics: events may be written twice on crash-then-restart, but never silently dropped.

**Full answer:**
The consumer uses `enable_auto_commit=False` and calls `consumer.commit()` manually:

```python
paths = write_batch(batch, data_dir=DATA_DIR)  # 1. write Parquet first
consumer.commit()                               # 2. advance Kafka offset
```

If the process crashes between step 1 and step 2, the Kafka offset is not advanced. On restart, the consumer re-reads from the last committed offset and reprocesses those messages. The batch may be written twice — at-least-once — but it will never be silently skipped.

For the duplicate-write case: each GitHub event has a unique `event_id`. If exactly-once semantics were required, we could deduplicate at query time with `SELECT DISTINCT ON (event_id) ...`, or deduplicate before writing using a bloom filter. For this dashboard use case, a <0.01% duplication rate doesn't affect the metrics meaningfully, so I documented the tradeoff rather than adding complexity.

---

## "What would you do differently if you rebuilt this?"

**Three main changes:**

**1. Async producer with `asyncio`**
The current producer uses synchronous `requests` + `time.sleep()`, which blocks for the full poll interval. With `aiohttp` + `asyncio`, the producer could handle multiple topics or backfill jobs concurrently while waiting on the GitHub API response.

**2. Schema registry**
Right now the schema lives in `schema.py`, a Python file. If multiple consumers in different languages need to agree on the schema, a centralized schema registry (Confluent Schema Registry with Avro or Protobuf) gives schema versioning, compatibility enforcement, and language-agnostic contracts.

**3. Prometheus + Grafana for observability**
The lag metric is displayed in the dashboard but not stored or alerted on. A production system would push lag, error rates, and event counts to Prometheus, build a Grafana dashboard, and page on-call when lag exceeds a threshold. The structured logging (structlog) is already in place — adding a Prometheus exporter would be straightforward.

---

## "What did you learn building this?"

The biggest insight was developing intuition for *why* distributed systems are designed the way they are:

**Kafka consumer groups:** I understood partitions abstractly before, but implementing it made the constraint concrete — you can't have more consumer instances than partitions and expect all of them to be active. The partition count sets the ceiling for parallelism.

**Parquet footers:** I thought of Parquet as "a faster CSV." Building this taught me that the file footer — which stores row group statistics (min/max per column) — is what makes predicate pushdown possible. DuckDB reads the footer to decide whether a row group can contain matching rows before reading any actual data. Without footers, every byte of every file would need to be scanned.

**Watermarks as a correctness/latency tradeoff:** There's no universally correct watermark. A longer watermark increases correctness (more late events land in the right partition) at the cost of latency (partitions take longer to seal before compaction can run). The right value is determined by how late your upstream data actually arrives in practice — you measure it, you don't guess it.
