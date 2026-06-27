"""
cli.py — StreamLens command-line query interface

Usage (from project root):
    PYTHONPATH=src python src/cli.py events
    PYTHONPATH=src python src/cli.py events --limit 50 --type PushEvent
    PYTHONPATH=src python src/cli.py stats --since 30
    PYTHONPATH=src python src/cli.py repos --top 5
    PYTHONPATH=src python src/cli.py lag
    PYTHONPATH=src python src/cli.py dlq

Why a CLI?
─────────────────────────────────────────────────────────────────────────────
The Rich dashboard is great for live monitoring, but sometimes you want a
quick ad-hoc answer without launching the full TUI. This CLI exposes the
same DuckDB reader functions as one-shot queries printed to stdout.

Examples from a real debugging session:
    # Something looks off — what types are coming in?
    python src/cli.py stats --since 10

    # Which repos are active right now?
    python src/cli.py repos --top 20

    # How bad is the lag spike?
    python src/cli.py lag --since 5

    # Did any events fail validation?
    python src/cli.py dlq

Design:
─────────────────────────────────────────────────────────────────────────────
- Click for argument parsing (consistent --help, type coercion, error messages)
- Rich tables for output (same visual style as the dashboard)
- All data queries delegate to storage.reader — no SQL in this file
- --data-dir global option lets you query a different data directory
"""

import sys
from pathlib import Path

import click
from rich import box
from rich.console import Console
from rich.table import Table

# When run as `PYTHONPATH=src python src/cli.py`, storage.reader is importable.
# When run from an IDE, this ensures src/ is on the path.
sys.path.insert(0, str(Path(__file__).parent))

import storage.reader as reader

console = Console()

# ── Type → colour mapping (same as dashboard for visual consistency) ──────────

_TYPE_COLOURS: dict[str, str] = {
    "PushEvent":        "green",
    "WatchEvent":       "yellow",
    "PullRequestEvent": "blue",
    "IssuesEvent":      "magenta",
    "ForkEvent":        "cyan",
    "CreateEvent":      "bright_green",
    "DeleteEvent":      "red",
}


def _type_colour(event_type: str) -> str:
    return _TYPE_COLOURS.get(event_type, "white")


# ── CLI group ─────────────────────────────────────────────────────────────────

@click.group()
@click.option(
    "--data-dir",
    default="data/events",
    show_default=True,
    envvar="DATA_DIR",
    help="Path to the Parquet event data directory.",
)
@click.pass_context
def cli(ctx: click.Context, data_dir: str) -> None:
    """StreamLens — query your local Parquet event store."""
    ctx.ensure_object(dict)
    ctx.obj["data_dir"] = Path(data_dir)


# ── events ────────────────────────────────────────────────────────────────────

@cli.command()
@click.option("--limit", "-n", default=20, show_default=True, help="Number of events to show.")
@click.option(
    "--type", "event_type",
    default=None,
    metavar="TYPE",
    help="Filter by event type, e.g. PushEvent.",
)
@click.pass_context
def events(ctx: click.Context, limit: int, event_type: str | None) -> None:
    """Show the most recent ingested events."""
    data_dir: Path = ctx.obj["data_dir"]
    rows = reader.get_recent_events(limit=limit * 3 if event_type else limit, data_dir=data_dir)

    if event_type:
        rows = [r for r in rows if r.get("event_type") == event_type]
        rows = rows[:limit]

    if not rows:
        console.print("[dim]No events found.[/dim]")
        return

    table = Table(box=box.SIMPLE, show_header=True, header_style="bold cyan")
    table.add_column("Time (UTC)", style="dim", width=17)
    table.add_column("Type", width=22)
    table.add_column("Actor", width=18)
    table.add_column("Repo")

    for r in rows:
        created = r.get("created_at")
        time_str = (
            created.strftime("%m-%d %H:%M:%S")
            if hasattr(created, "strftime")
            else str(created)[:16]
        )
        etype = r.get("event_type", "")
        colour = _type_colour(etype)
        table.add_row(
            time_str,
            f"[{colour}]{etype}[/{colour}]",
            r.get("actor_login", ""),
            r.get("repo_name", ""),
        )

    console.print(table)
    suffix = f" (filtered: {event_type})" if event_type else ""
    console.print(f"[dim]{len(rows)} events{suffix}[/dim]")


# ── stats ─────────────────────────────────────────────────────────────────────

@cli.command()
@click.option(
    "--since", default=60, show_default=True,
    help="Time window in minutes.",
)
@click.pass_context
def stats(ctx: click.Context, since: int) -> None:
    """Show event counts by type for the last N minutes."""
    data_dir: Path = ctx.obj["data_dir"]
    rows = reader.get_event_counts_by_type(since_minutes=since, data_dir=data_dir)

    if not rows:
        console.print(f"[dim]No events in the last {since} minutes.[/dim]")
        return

    total = sum(r.get("event_count", 0) for r in rows)

    table = Table(box=box.SIMPLE, show_header=True, header_style="bold cyan")
    table.add_column("Event Type", width=25)
    table.add_column("Count", justify="right", width=7)
    table.add_column("Bar + Share", width=26)

    for r in rows:
        count = r.get("event_count", 0)
        etype = r.get("event_type", "")
        pct = count / total * 100 if total > 0 else 0
        # ASCII bar: scale 100% → 20 blocks
        bar_len = max(1, int(pct / 5))
        colour = _type_colour(etype)
        bar = f"[{colour}]{'█' * bar_len}[/{colour}]"
        table.add_row(
            f"[{colour}]{etype}[/{colour}]",
            str(count),
            f"{bar} {pct:.1f}%",
        )

    console.print(table)
    console.print(f"[dim]{total:,} events total  ·  last {since} min[/dim]")


# ── repos ─────────────────────────────────────────────────────────────────────

@cli.command()
@click.option("--since", default=60, show_default=True, help="Time window in minutes.")
@click.option("--top", default=10, show_default=True, help="Number of repositories to show.")
@click.pass_context
def repos(ctx: click.Context, since: int, top: int) -> None:
    """Show the most active repositories."""
    data_dir: Path = ctx.obj["data_dir"]
    rows = reader.get_top_repos(since_minutes=since, limit=top, data_dir=data_dir)

    if not rows:
        console.print("[dim]No data found.[/dim]")
        return

    table = Table(box=box.SIMPLE, show_header=True, header_style="bold cyan")
    table.add_column("#", width=4, justify="right", style="dim")
    table.add_column("Repository")
    table.add_column("Events", justify="right", width=8)
    table.add_column("Actors", justify="right", width=8)

    for i, r in enumerate(rows, 1):
        table.add_row(
            str(i),
            r.get("repo_name", ""),
            str(r.get("event_count", "")),
            str(r.get("unique_actors", "")),
        )

    console.print(table)
    console.print(f"[dim]Top {len(rows)} repos  ·  last {since} min[/dim]")


# ── lag ───────────────────────────────────────────────────────────────────────

@cli.command()
@click.option("--since", default=60, show_default=True, help="Time window in minutes.")
@click.pass_context
def lag(ctx: click.Context, since: int) -> None:
    """Show pipeline lag statistics (GitHub event time → our ingest time)."""
    data_dir: Path = ctx.obj["data_dir"]
    result = reader.get_avg_lag(since_minutes=since, data_dir=data_dir)

    if result is None:
        console.print("[dim]No lag data available yet.[/dim]")
        return

    avg = result.get("avg_lag_seconds", 0) or 0
    min_lag = result.get("min_lag_seconds", 0) or 0
    max_lag = result.get("max_lag_seconds", 0) or 0
    n = result.get("sample_size", 0) or 0

    # Colour-code by severity (mirrors dashboard logic)
    if avg < 30:
        avg_str = f"[green]{avg:.1f}s[/green]"
        status = "[green]● healthy[/green]"
    elif avg < 60:
        avg_str = f"[yellow]{avg:.1f}s[/yellow]"
        status = "[yellow]● elevated[/yellow]"
    else:
        avg_str = f"[red]{avg:.1f}s[/red]"
        status = "[red]● high[/red]"

    console.print()
    console.print(f"  Status   {status}")
    console.print(f"  Avg lag  {avg_str}")
    console.print(f"  Min lag  {min_lag:.1f}s")
    console.print(f"  Max lag  {max_lag:.1f}s")
    console.print(f"  Samples  {n:,}")
    console.print(f"  Window   last {since} minutes")
    console.print()


# ── dlq ───────────────────────────────────────────────────────────────────────

@cli.command()
@click.option("--limit", "-n", default=20, show_default=True, help="Max DLQ entries to show.")
@click.option(
    "--dlq-dir", default="data/dlq", show_default=True,
    envvar="DLQ_DIR",
    help="Path to the Dead Letter Queue Parquet directory.",
)
@click.pass_context
def dlq(ctx: click.Context, limit: int, dlq_dir: str) -> None:
    """Inspect the Dead Letter Queue (events that failed validation)."""
    dlq_path = Path(dlq_dir)
    rows = reader.inspect_dlq(limit=limit, dlq_dir=dlq_path)

    if not rows:
        console.print("[green]✓ DLQ is empty — no invalid events.[/green]")
        return

    table = Table(box=box.SIMPLE, show_header=True, header_style="bold red")
    table.add_column("Failed At (UTC)", width=17, style="dim")
    table.add_column("Type", width=22)
    table.add_column("Event ID", width=16)
    table.add_column("Reason")

    for r in rows:
        failed = r.get("failed_at")
        time_str = (
            failed.strftime("%m-%d %H:%M:%S")
            if hasattr(failed, "strftime")
            else str(failed)[:16]
        )
        etype = r.get("event_type", "")
        table.add_row(
            time_str,
            f"[red]{etype}[/red]",
            r.get("event_id", ""),
            r.get("error_reason", ""),
        )

    console.print(f"\n[bold red]⚠  {len(rows)} invalid events in DLQ[/bold red]")
    console.print(table)
    console.print(
        f"[dim]Tip: check data/dlq/*.parquet for the full raw_json payload[/dim]\n"
    )


# ── entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    cli(obj={})
