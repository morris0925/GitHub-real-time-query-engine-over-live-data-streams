"""
storage/reader.py — DuckDB query layer for StreamLens

Responsibility: provide query functions that the dashboard (and tests) can
call to get data out of the Parquet files.

Key design decisions:
─────────────────────
1. Module-level singleton connection
   DuckDB can run in-process (no separate server). We create ONE connection
   when this module is first imported and reuse it for every query. Opening
   and closing a connection per query would add unnecessary overhead.

2. Return type: list[dict]
   The dashboard doesn't need PyArrow tables — it just iterates rows. A
   list of plain Python dicts is the simplest contract. We convert from
   DuckDB's result using .fetchdf() or .fetchall() + column names.

3. SQL in .sql files
   Queries longer than 5 lines live in storage/queries/*.sql and are loaded
   at runtime with Path.read_text(). This keeps Python code clean and makes
   SQL easy to edit without touching Python logic.

4. No caching here
   The dashboard calls these functions on every refresh tick. Caching would
   add complexity; DuckDB is fast enough on small Parquet files that it's
   not needed yet.
"""

import duckdb
from pathlib import Path
import structlog

log = structlog.get_logger(__name__)

# ── Paths ────────────────────────────────────────────────────────────────────

# Where our Parquet files live. read_parquet() uses this glob to find ALL
# partitions at once: data/events/**/*.parquet
DEFAULT_DATA_DIR = Path("data/events")

# Directory containing .sql query files
_QUERIES_DIR = Path(__file__).parent / "queries"

# ── Singleton DuckDB connection ───────────────────────────────────────────────
# duckdb.connect() with no arguments creates an in-memory database.
# We use it purely as a query engine over our Parquet files — we never
# store anything inside DuckDB itself.
_conn: duckdb.DuckDBPyConnection = duckdb.connect()


# ── Internal helpers ──────────────────────────────────────────────────────────

def _load_sql(filename: str) -> str:
    """
    Read a .sql file from the queries/ directory and return it as a string.

    Using files (instead of inline strings) keeps our SQL formatted and
    editable without modifying Python code.
    """
    path = _QUERIES_DIR / filename
    return path.read_text(encoding="utf-8")


def _data_glob(data_dir: Path) -> str:
    """
    Build the glob string DuckDB needs to scan all Parquet partitions.

    Example: "data/events/**/*.parquet"

    The ** means "any subdirectory" — DuckDB will find files in
    date=2024-01-15/, date=2024-01-16/, etc. all at once.
    """
    return str(data_dir / "**" / "*.parquet")


def _execute(sql: str) -> list[dict]:
    """
    Run a SQL string against the DuckDB singleton and return rows as dicts.

    DuckDB's .fetchall() returns a list of tuples; .description gives
    column names. We zip them together into dicts so callers get
    {"event_type": "PushEvent", "event_count": 42} style results.
    """
    try:
        cursor = _conn.execute(sql)
        columns = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
        return [dict(zip(columns, row)) for row in rows]
    except Exception as exc:
        log.error("query_failed", error=str(exc), sql_preview=sql[:120])
        raise


# ── Public query functions ────────────────────────────────────────────────────

def get_event_counts_by_type(
    since_minutes: int = 60,
    data_dir: Path = DEFAULT_DATA_DIR,
) -> list[dict]:
    """
    Return event counts grouped by type for the last `since_minutes` minutes.

    Example return value:
        [
            {"event_type": "PushEvent",   "event_count": 120},
            {"event_type": "WatchEvent",  "event_count": 43},
            {"event_type": "CreateEvent", "event_count": 11},
        ]

    The dashboard uses this to populate its Stats Panel.
    """
    glob = _data_glob(data_dir)

    # Check if any data files exist — DuckDB raises an error if the glob
    # matches nothing, so we handle it gracefully here.
    if not list(data_dir.glob("**/*.parquet")):
        log.warning("no_parquet_files", data_dir=str(data_dir))
        return []

    sql = _load_sql("event_counts_by_type.sql").format(
        data_glob=glob,
        since_minutes=since_minutes,
    )
    return _execute(sql)


def get_top_repos(
    since_minutes: int = 60,
    limit: int = 10,
    data_dir: Path = DEFAULT_DATA_DIR,
) -> list[dict]:
    """
    Return the most active repositories in the last `since_minutes` minutes.

    Example return value:
        [
            {
                "repo_name":     "torvalds/linux",
                "event_count":   88,
                "unique_actors": 14,
                "last_seen_at":  datetime(2024, 1, 15, 10, 30, tzinfo=utc),
            },
            ...
        ]

    The dashboard uses this for its Top Repos panel.
    """
    glob = _data_glob(data_dir)

    if not list(data_dir.glob("**/*.parquet")):
        log.warning("no_parquet_files", data_dir=str(data_dir))
        return []

    sql = _load_sql("top_repos.sql").format(
        data_glob=glob,
        since_minutes=since_minutes,
        limit=limit,
    )
    return _execute(sql)


def get_recent_events(
    limit: int = 20,
    data_dir: Path = DEFAULT_DATA_DIR,
) -> list[dict]:
    """
    Return the `limit` most recently created events.

    Example return value:
        [
            {
                "event_type":  "PushEvent",
                "actor_login": "alice",
                "repo_name":   "alice/myrepo",
                "created_at":  datetime(2024, 1, 15, 10, 30, tzinfo=utc),
            },
            ...
        ]

    The dashboard uses this for its scrolling Event Feed panel.
    """
    glob = _data_glob(data_dir)

    if not list(data_dir.glob("**/*.parquet")):
        log.warning("no_parquet_files", data_dir=str(data_dir))
        return []

    sql = _load_sql("recent_events.sql").format(
        data_glob=glob,
        limit=limit,
    )
    return _execute(sql)


def get_total_event_count(data_dir: Path = DEFAULT_DATA_DIR) -> int:
    """
    Return the total number of events ever stored.

    Used by the dashboard's Status Bar to show "X events processed".
    Short enough to inline (< 5 lines of SQL), so no .sql file needed.
    """
    glob = _data_glob(data_dir)

    if not list(data_dir.glob("**/*.parquet")):
        return 0

    result = _execute(f"SELECT COUNT(*) AS n FROM read_parquet('{glob}')")
    return result[0]["n"] if result else 0


DEFAULT_DLQ_DIR = Path("data/dlq")


def inspect_dlq(
    limit: int = 20,
    dlq_dir: Path = DEFAULT_DLQ_DIR,
) -> list[dict]:
    """
    Return the most recent DLQ entries (events that failed validation).

    Returns an empty list if the DLQ directory is empty or doesn't exist.

    Example return value:
        [
            {
                "event_id":     "12345678901",
                "event_type":   "PushEvent",
                "error_reason": "payload.ref is missing",
                "raw_json":     "{...}",
                "failed_at":    datetime(2026-06-28, ...),
            },
            ...
        ]

    Use the CLI command `python src/cli.py dlq` for a formatted view.
    """
    if not dlq_dir.exists() or not list(dlq_dir.glob("*.parquet")):
        return []

    glob = str(dlq_dir / "*.parquet")
    sql = (
        f"SELECT event_id, event_type, error_reason, raw_json, failed_at "
        f"FROM read_parquet('{glob}') "
        f"ORDER BY failed_at DESC "
        f"LIMIT {limit}"
    )
    return _execute(sql)


def get_avg_lag(
    since_minutes: int = 60,
    data_dir: Path = DEFAULT_DATA_DIR,
) -> dict | None:
    """
    Return pipeline lag statistics for the last `since_minutes` minutes.

    Lag = seconds between when GitHub recorded an event (created_at) and
    when our consumer wrote it to Parquet (ingested_at). A healthy pipeline
    has avg_lag_seconds roughly equal to FLUSH_INTERVAL_SECONDS (≈30s).

    Example return value:
        {
            "avg_lag_seconds": 28.4,
            "min_lag_seconds": 5.1,
            "max_lag_seconds": 62.3,
            "sample_size": 847,
        }

    Returns None if there is no data yet.
    """
    if not list(data_dir.glob("**/*.parquet")):
        return None

    glob = _data_glob(data_dir)
    sql = _load_sql("avg_lag.sql").format(
        data_glob=glob,
        since_minutes=since_minutes,
    )
    result = _execute(sql)
    if not result or result[0]["avg_lag_seconds"] is None:
        return None
    return result[0]
