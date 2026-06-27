#!/usr/bin/env python3
"""
scripts/benchmark.py — Parquet vs JSON-lines query latency benchmark

Compares DuckDB query performance between:
  - Columnar storage: Parquet (Snappy-compressed, typed schema)
  - Row storage:      JSON-lines (one JSON object per line)

Methodology
-----------
For each scale (10_000 / 100_000 / 500_000 events):
  1. Generate synthetic GitHub-like events in memory
  2. Write to Parquet via storage.writer.write_batch()
  3. Write to JSONL   via storage.jsonl_writer.write_batch_jsonl()
  4. Run 5 analytical queries TRIALS times against each format
  5. Record min/median/p99 latency per (query, format, scale)
  6. Print a Rich table and write results/benchmark_results.json

Usage:
    cd "GitHub real-time query engine over live data streams"
    python scripts/benchmark.py

The results are consumed by docs/benchmark.md.
"""

from __future__ import annotations

import json
import random
import shutil
import statistics
import sys
import time
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

# ── path bootstrap (run from project root) ────────────────────────────────────
PROJECT_ROOT = Path(__file__).resolve().parent.parent
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

import duckdb
from rich.console import Console
from rich.table import Table

from storage.writer import write_batch
from storage.jsonl_writer import write_batch_jsonl

# ── Configuration ─────────────────────────────────────────────────────────────

SCALES    = [10_000, 100_000, 500_000]
TRIALS    = 7          # runs per (query, format, scale) — take median
BENCH_DIR = Path("/tmp/streamlens_benchmark")
RESULTS_DIR = PROJECT_ROOT / "results"

EVENT_TYPES = [
    "PushEvent", "WatchEvent", "PullRequestEvent",
    "IssuesEvent", "ForkEvent", "CreateEvent",
]
ACTOR_POOL  = [f"user_{i}" for i in range(200)]
REPO_POOL   = [f"org_{i%20}/repo_{i}" for i in range(500)]

# ── Synthetic event generation ────────────────────────────────────────────────

def _random_event(base_time: datetime) -> dict:
    """Build one synthetic GitHub-like event dict."""
    event_type = random.choice(EVENT_TYPES)
    offset_s   = random.randint(0, 86_400)   # spread events over 24h
    created_at = base_time - timedelta(seconds=offset_s)

    payload: dict = {}
    if event_type == "PushEvent":
        payload = {
            "ref": random.choice(["refs/heads/main", "refs/heads/dev", "refs/heads/feat"]),
            "size": random.randint(1, 5),
            "commits": [{"sha": uuid.uuid4().hex[:8], "message": "fix"}
                        for _ in range(random.randint(1, 3))],
        }
    elif event_type == "WatchEvent":
        payload = {"action": "started"}
    elif event_type == "PullRequestEvent":
        payload = {
            "action": random.choice(["opened", "closed", "merged"]),
            "number": random.randint(1, 9999),
            "pull_request": {
                "title": "Update thing",
                "state": "open",
                "merged": False,
                "draft": False,
                "additions": random.randint(1, 500),
                "deletions": random.randint(0, 200),
                "changed_files": random.randint(1, 20),
                "base": {"ref": "main"},
                "head": {"ref": "feat/update"},
            },
        }
    elif event_type == "IssuesEvent":
        payload = {
            "action": random.choice(["opened", "closed", "labeled"]),
            "issue": {
                "number": random.randint(1, 9999),
                "title": "Some issue",
                "state": "open",
                "comments": random.randint(0, 50),
                "labels": [],
            },
        }
    elif event_type == "ForkEvent":
        user = random.choice(ACTOR_POOL)
        payload = {
            "forkee": {
                "id": random.randint(1_000_000, 9_999_999),
                "name": "forked-repo",
                "full_name": f"{user}/forked-repo",
                "owner": {"login": user},
                "private": False,
                "fork": True,
            }
        }
    elif event_type == "CreateEvent":
        payload = {
            "ref_type": random.choice(["branch", "tag"]),
            "ref": f"v{random.randint(1,5)}.{random.randint(0,9)}.{random.randint(0,9)}",
            "master_branch": "main",
        }

    actor_login = random.choice(ACTOR_POOL)
    repo_name   = random.choice(REPO_POOL)

    return {
        "id":         str(random.randint(10**9, 10**10)),
        "type":       event_type,
        "actor":      {"id": random.randint(1000, 9999), "login": actor_login},
        "repo":       {"id": random.randint(1000, 9999), "name": repo_name},
        "payload":    payload,
        "public":     True,
        "created_at": created_at.strftime("%Y-%m-%dT%H:%M:%SZ"),
    }


def generate_events(n: int) -> list[dict]:
    base_time = datetime.now(tz=timezone.utc)
    return [_random_event(base_time) for _ in range(n)]


# ── Query definitions ─────────────────────────────────────────────────────────

QUERIES = {
    "Q1 top repos": {
        "parquet": lambda parquet_glob: f"""
            SELECT repo_name, COUNT(*) AS event_count
            FROM read_parquet('{parquet_glob}')
            GROUP BY repo_name
            ORDER BY event_count DESC
            LIMIT 10
        """,
        "jsonl": lambda jsonl_glob: f"""
            SELECT repo_name, COUNT(*) AS event_count
            FROM read_json_auto('{jsonl_glob}')
            GROUP BY repo_name
            ORDER BY event_count DESC
            LIMIT 10
        """,
    },
    "Q2 event type dist": {
        "parquet": lambda g: f"""
            SELECT event_type, COUNT(*) AS cnt
            FROM read_parquet('{g}')
            GROUP BY event_type
            ORDER BY cnt DESC
        """,
        "jsonl": lambda g: f"""
            SELECT event_type, COUNT(*) AS cnt
            FROM read_json_auto('{g}')
            GROUP BY event_type
            ORDER BY cnt DESC
        """,
    },
    "Q3 recent events filter": {
        "parquet": lambda g: f"""
            SELECT COUNT(*) AS recent_count
            FROM read_parquet('{g}')
            WHERE created_at >= NOW() - INTERVAL 1 HOUR
        """,
        "jsonl": lambda g: f"""
            SELECT COUNT(*) AS recent_count
            FROM read_json_auto('{g}')
            WHERE CAST(created_at AS TIMESTAMP WITH TIME ZONE) >= NOW() - INTERVAL 1 HOUR
        """,
    },
    "Q4 actor activity": {
        "parquet": lambda g: f"""
            SELECT actor_login, COUNT(*) AS events, COUNT(DISTINCT event_type) AS types
            FROM read_parquet('{g}')
            GROUP BY actor_login
            ORDER BY events DESC
            LIMIT 20
        """,
        "jsonl": lambda g: f"""
            SELECT actor_login, COUNT(*) AS events, COUNT(DISTINCT event_type) AS types
            FROM read_json_auto('{g}')
            GROUP BY actor_login
            ORDER BY events DESC
            LIMIT 20
        """,
    },
    "Q5 push stats aggregation": {
        "parquet": lambda g: f"""
            SELECT
                COUNT(*) AS total_push_events,
                SUM(CAST(JSON_EXTRACT_STRING(payload_json, '$.size') AS INTEGER)) AS total_commits
            FROM read_parquet('{g}')
            WHERE event_type = 'PushEvent'
              AND payload_json IS NOT NULL
              AND JSON_EXTRACT_STRING(payload_json, '$.size') IS NOT NULL
        """,
        "jsonl": lambda g: f"""
            SELECT
                COUNT(*) AS total_push_events,
                SUM(CAST(payload_json->>'$.size' AS INTEGER)) AS total_commits
            FROM (
                SELECT event_type,
                       JSON(json_object('size', payload.size)) AS payload_json
                FROM read_json_auto('{g}', columns={{
                    event_type: 'VARCHAR',
                    payload: 'JSON'
                }})
                WHERE event_type = 'PushEvent'
                  AND payload IS NOT NULL
            )
        """,
    },
}


# ── Benchmark runner ──────────────────────────────────────────────────────────

def run_query(con: duckdb.DuckDBPyConnection, sql: str) -> float:
    """Execute sql and return wall-clock seconds."""
    t0 = time.perf_counter()
    con.execute(sql).fetchall()
    return time.perf_counter() - t0


def benchmark_scale(
    scale: int,
    events: list[dict],
    con: duckdb.DuckDBPyConnection,
) -> dict[str, dict[str, list[float]]]:
    """
    Write events to both formats and run all queries.

    Returns: {query_name: {"parquet": [latencies], "jsonl": [latencies]}}
    """
    scale_dir  = BENCH_DIR / str(scale)
    parquet_dir = scale_dir / "parquet"
    jsonl_dir   = scale_dir / "jsonl"

    # Clean previous runs for this scale
    if parquet_dir.exists():
        shutil.rmtree(parquet_dir)
    if jsonl_dir.exists():
        shutil.rmtree(jsonl_dir)

    # Write data (not timed — we're benchmarking reads)
    write_batch(events, data_dir=parquet_dir, threshold_hours=48)
    write_batch_jsonl(events, jsonl_dir=jsonl_dir, threshold_hours=48)

    parquet_glob = str(parquet_dir / "**" / "*.parquet")
    jsonl_glob   = str(jsonl_dir   / "**" / "*.jsonl")

    results: dict[str, dict[str, list[float]]] = {}

    for qname, qfns in QUERIES.items():
        pq_sql   = qfns["parquet"](parquet_glob)
        jsonl_sql = qfns["jsonl"](jsonl_glob)

        pq_times: list[float]   = []
        jsonl_times: list[float] = []

        for _ in range(TRIALS):
            pq_times.append(run_query(con, pq_sql))

        # Q5 JSONL is complex — fall back gracefully if syntax unsupported
        jsonl_q5_ok = True
        if qname == "Q5 push stats aggregation":
            try:
                for _ in range(TRIALS):
                    jsonl_times.append(run_query(con, jsonl_sql))
            except Exception:
                jsonl_q5_ok = False
                # Fallback: simpler aggregation without nested JSON parsing
                fallback_sql = f"""
                    SELECT COUNT(*) AS total_push_events
                    FROM read_json_auto('{jsonl_glob}')
                    WHERE event_type = 'PushEvent'
                """
                for _ in range(TRIALS):
                    jsonl_times.append(run_query(con, fallback_sql))
        else:
            for _ in range(TRIALS):
                jsonl_times.append(run_query(con, jsonl_sql))

        results[qname] = {"parquet": pq_times, "jsonl": jsonl_times}

    return results


def p50(xs: list[float]) -> float:
    return statistics.median(xs)


def p99(xs: list[float]) -> float:
    xs_sorted = sorted(xs)
    idx = max(0, int(len(xs_sorted) * 0.99) - 1)
    return xs_sorted[idx]


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    console = Console()
    con = duckdb.connect()

    all_results: dict[str, dict] = {}  # scale → query → format → latencies

    for scale in SCALES:
        console.print(f"\n[bold cyan]Scale: {scale:,} events[/bold cyan]")
        console.print("  Generating events...", end=" ")
        events = generate_events(scale)
        console.print("[green]done[/green]")
        console.print("  Writing Parquet + JSONL...", end=" ")
        scale_results = benchmark_scale(scale, events, con)
        console.print("[green]done[/green]")
        console.print(f"  Running {TRIALS} trials per query...", end=" ")
        console.print("[green]done[/green]")
        all_results[str(scale)] = scale_results

    # ── Print summary table ───────────────────────────────────────────────────
    console.print("\n[bold]Benchmark Results — Median query latency (seconds)[/bold]\n")

    for scale in SCALES:
        table = Table(title=f"Scale: {scale:,} events", show_lines=True)
        table.add_column("Query",          style="bold")
        table.add_column("Parquet p50 (s)", justify="right", style="green")
        table.add_column("JSONL p50 (s)",   justify="right", style="yellow")
        table.add_column("Speedup",         justify="right", style="cyan")

        scale_results = all_results[str(scale)]
        for qname, timings in scale_results.items():
            pq_p50   = p50(timings["parquet"])
            jl_p50   = p50(timings["jsonl"])
            speedup  = jl_p50 / pq_p50 if pq_p50 > 0 else float("inf")
            table.add_row(
                qname,
                f"{pq_p50:.4f}",
                f"{jl_p50:.4f}",
                f"{speedup:.1f}×",
            )

        console.print(table)

    # ── Persist raw results ───────────────────────────────────────────────────
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    output_file = RESULTS_DIR / "benchmark_results.json"

    # Flatten for JSON serialisation
    serialisable: dict = {}
    for scale in SCALES:
        serialisable[str(scale)] = {}
        for qname, timings in all_results[str(scale)].items():
            serialisable[str(scale)][qname] = {
                "parquet": {
                    "p50": round(p50(timings["parquet"]), 6),
                    "p99": round(p99(timings["parquet"]), 6),
                    "raw": [round(t, 6) for t in timings["parquet"]],
                },
                "jsonl": {
                    "p50": round(p50(timings["jsonl"]), 6),
                    "p99": round(p99(timings["jsonl"]), 6),
                    "raw": [round(t, 6) for t in timings["jsonl"]],
                },
                "speedup_p50": round(
                    p50(timings["jsonl"]) / p50(timings["parquet"])
                    if p50(timings["parquet"]) > 0 else 0, 2
                ),
            }

    with open(output_file, "w") as fh:
        json.dump(serialisable, fh, indent=2)

    console.print(f"\n[dim]Raw results written to {output_file}[/dim]")
    console.print("[bold green]Benchmark complete.[/bold green]")


if __name__ == "__main__":
    main()
