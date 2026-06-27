# src/ — Pipeline & Source Code Guide

## Responsibilities
Everything between the data source and storage lives here:
GitHub Events API → producer → Kafka topic → consumer → processor → storage/

## What's Here
- `producer.py` — polls GitHub Events API, publishes each event to Kafka topic `github-events`
- `consumer.py` _(planned)_ — reads from Kafka, calls processor, hands off to storage
- `processors/` _(planned)_ — one file per event type (PushEvent, WatchEvent, etc.)
- `storage/` _(planned)_ — DuckDB + Parquet layer (see storage/CLAUDE.md when created)
- `dashboard.py` _(planned)_ — Rich terminal dashboard

## Kafka Conventions
- Library: `kafka-python` (`from kafka import KafkaProducer, KafkaConsumer`)
- Consumer group naming: `streamlens-{purpose}` e.g. `streamlens-events-consumer`
- Topic naming: `{project}-{domain}` e.g. `github-events`
- Always commit offsets **after** successful write to storage, not before

## Error Handling Pattern
```python
# Preferred pattern for consumer loop
try:
    process_message(msg)
    write_to_storage(msg)
    consumer.commit()           # commit only after storage write succeeds
except StorageWriteError as e:
    structlog.error("storage_write_failed", error=str(e))
    # do NOT commit — let Kafka retry from this offset
except Exception as e:
    structlog.error("unexpected_error", error=str(e))
    raise                       # bubble up, let process restart
```

## What Does NOT Belong Here
- DuckDB queries → `src/storage/reader.py`
- SQL files → `src/storage/queries/`
- Test files → `tests/`
