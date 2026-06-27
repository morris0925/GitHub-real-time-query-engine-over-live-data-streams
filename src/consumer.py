"""
Kafka Consumer → Processor → Parquet Storage

Pipeline position:
    GitHub API → producer.py → [Kafka topic] → consumer.py → Parquet files
                                                              ↑ we are here

Flow per message:
    1. Deserialize JSON from Kafka
    2. Route to the correct EventProcessor via get_processor(event["type"])
    3. Processor validates + enriches the event; raises ValidationError if broken
    4. Accumulate valid events in a micro-batch
    5. When batch is full OR timer fires → write_batch() → commit offsets

Batching strategy (micro-batch):
    Flush when EITHER:
    - batch has reached BATCH_SIZE messages, OR
    - FLUSH_INTERVAL_SECONDS have passed since the last flush
    This balances throughput (big batches = fewer Parquet files) against
    latency (we don't wait forever for a full batch).

Offset commit ordering (at-least-once):
    commit() is called AFTER write_batch() succeeds. If the process crashes
    between write and commit, Kafka replays the messages on restart — possible
    duplicate writes, but never silent data loss.
"""

import json
import os
import time
from pathlib import Path

import structlog
from dotenv import load_dotenv
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable

from processors import get_processor, ValidationError
from storage.writer import write_batch, StorageWriteError

load_dotenv()
log = structlog.get_logger(__name__)

# ── Configuration ─────────────────────────────────────────────────────────────

KAFKA_BROKER:           str   = os.getenv("KAFKA_BROKER", "localhost:9092")
KAFKA_TOPIC:            str   = os.getenv("KAFKA_TOPIC", "github-events")
KAFKA_GROUP_ID:         str   = os.getenv("KAFKA_GROUP_ID", "streamlens-events-consumer")
BATCH_SIZE:             int   = int(os.getenv("BATCH_SIZE", "100"))
FLUSH_INTERVAL_SECONDS: float = float(os.getenv("FLUSH_INTERVAL_SECONDS", "30.0"))
DATA_DIR:               Path  = Path(os.getenv("DATA_DIR", "data/events"))


# ── Consumer setup ────────────────────────────────────────────────────────────

def create_consumer() -> KafkaConsumer:
    """
    Connect to Kafka and return a consumer with manual offset commits.

    enable_auto_commit=False: we call commit() ourselves, only after a
    successful Parquet write (at-least-once guarantee).

    auto_offset_reset="earliest": on first run, start from the beginning
    of the topic so we don't miss events that arrived before startup.
    """
    return KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        group_id=KAFKA_GROUP_ID,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
        value_deserializer=lambda b: json.loads(b.decode("utf-8")),
    )


# ── Main loop ─────────────────────────────────────────────────────────────────

def run_consumer() -> None:
    """Run the consumer loop until interrupted with Ctrl+C."""
    log.info("consumer_starting", broker=KAFKA_BROKER, topic=KAFKA_TOPIC)

    try:
        consumer = create_consumer()
        log.info("consumer_connected", group_id=KAFKA_GROUP_ID)
    except NoBrokersAvailable:
        log.error("no_brokers", broker=KAFKA_BROKER, hint="Is docker-compose up?")
        return

    batch:           list[dict] = []
    last_flush_time: float      = time.monotonic()
    total_written:   int        = 0
    total_skipped:   int        = 0

    try:
        while True:
            records = consumer.poll(timeout_ms=1000)

            for partition_messages in records.values():
                for message in partition_messages:
                    raw_event: dict = message.value

                    # ── Processor layer ───────────────────────────────────
                    event_type = raw_event.get("type", "unknown")
                    try:
                        result = get_processor(event_type).process(raw_event)
                    except ValidationError as exc:
                        # Broken event — log and skip. Do NOT add to batch.
                        # The offset will be committed with the next successful
                        # flush, so Kafka won't replay this broken message.
                        log.warning(
                            "event_validation_failed",
                            event_type=event_type,
                            event_id=str(raw_event.get("id", "")),
                            reason=str(exc),
                        )
                        total_skipped += 1
                        continue

                    if result.skipped:
                        total_skipped += 1
                        continue

                    if result.metrics:
                        log.debug("event_metrics", event_type=event_type, **result.metrics)

                    batch.append(result.event)
                    # ─────────────────────────────────────────────────────

            # Decide whether to flush
            elapsed = time.monotonic() - last_flush_time
            should_flush = len(batch) >= BATCH_SIZE or (
                batch and elapsed >= FLUSH_INTERVAL_SECONDS
            )

            if should_flush:
                log.info("flushing_batch", size=len(batch), elapsed_seconds=round(elapsed, 1))
                try:
                    # ── At-least-once pattern ──────────────────────────────
                    paths = write_batch(batch, data_dir=DATA_DIR)  # 1. write
                    consumer.commit()                               # 2. commit
                    # ──────────────────────────────────────────────────────
                    total_written += len(batch)
                    log.info(
                        "flush_complete",
                        total_written=total_written,
                        total_skipped=total_skipped,
                        files_written=len(paths),
                    )
                    batch = []
                    last_flush_time = time.monotonic()

                except StorageWriteError as exc:
                    log.error("storage_write_failed", error=str(exc))
                    # do NOT commit — Kafka will re-deliver on next restart

                except Exception as exc:
                    log.error("unexpected_error", error=str(exc))
                    raise

    except KeyboardInterrupt:
        log.info("consumer_stopping", total_written=total_written, total_skipped=total_skipped)
        print(f"\n[consumer] Stopped. Written: {total_written}, Skipped: {total_skipped}")
    finally:
        consumer.close()
        log.info("consumer_closed")


if __name__ == "__main__":
    print("=== StreamLens Kafka Consumer ===")
    run_consumer()
