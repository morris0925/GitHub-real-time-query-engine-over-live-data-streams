# src/dashboard — Rich Terminal Dashboard Guide

## Responsibilities
Display real-time and historical metrics from DuckDB in a terminal UI. This is a Python script using the `rich` library — no web server, no frontend build, no browser required.

## Stack
- Python 3.11
- `rich` library — `Live`, `Layout`, `Table`, `Panel`, `Text`, `Spinner`
- DuckDB (via `src/storage/reader.py` — never query DuckDB directly from dashboard)

## Dashboard Layout
The terminal is divided into panels:
- **Event Feed** — scrolling live view of latest events (type, repo name, timestamp)
- **Stats Panel** — event counts per type (PushEvent, WatchEvent, CreateEvent, etc.)
- **Top Repos** — most active repositories in the last N minutes
- **Status Bar** — Kafka connection status, total events processed, last updated time

## Rules
- Use `rich.live.Live` with `refresh_per_second=4` — don't go higher
- Layout defined with `rich.layout.Layout` — no hardcoded terminal size assumptions
- Data fetched from `src/storage/reader.py` at each refresh cycle — no caching in the dashboard layer
- Keep dashboard.py a single file unless it exceeds ~300 lines
- Handle `KeyboardInterrupt` (Ctrl+C) gracefully — print a clean exit message

## Running
```bash
python src/dashboard.py
```

## What Does NOT Belong Here
- DuckDB queries → `src/storage/reader.py`
- Kafka consumer logic → `src/consumer.py`
- Any HTTP server or API endpoint — this is terminal-only
