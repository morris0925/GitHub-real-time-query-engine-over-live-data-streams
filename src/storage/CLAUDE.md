# src/storage/ — DuckDB + Parquet Layer Guide

## Responsibilities
Persist processed events to Parquet files and expose query functions to the dashboard and any other consumers. DuckDB is the query engine — it reads directly from Parquet files, no separate DB server needed.

## File Layout
```
storage/
├── writer.py          # PyArrow → Parquet write logic
├── reader.py          # DuckDB query functions
├── schema.py          # PyArrow schemas (source of truth for column types)
└── queries/           # SQL strings longer than 5 lines go here as .sql files
```

## Parquet Conventions
- Partition by date: `data/events/date=YYYY-MM-DD/part-{uuid}.parquet`
- Use Snappy compression: `compression='snappy'`
- Always write via the schema defined in `schema.py` — never infer schema from data

## DuckDB Conventions
- Use DuckDB's native Parquet scanning: `SELECT * FROM read_parquet('data/events/**/*.parquet')`
- Keep the DuckDB connection as a module-level singleton — don't open/close per query
- Long SQL queries (5+ lines) live in `storage/queries/*.sql`, loaded at runtime with `Path(...).read_text()`
- Return type from query functions: `list[dict]` for dashboard consumption, `pa.Table` for internal pipeline use

## Schema Changes
- Update `schema.py` first
- Write a migration note in `docs/schema_changelog.md`
- Parquet files are immutable — old partitions keep old schema, new partitions use new schema
