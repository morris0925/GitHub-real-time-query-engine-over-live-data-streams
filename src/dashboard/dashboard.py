"""
Day 5: Rich Terminal Dashboard

Goal: Display real-time metrics from our Parquet files in a terminal UI,
      refreshing every 4 seconds.

Layout (what you'll see in the terminal):
┌─────────────────────────────────────────────────────────────┐
│  ● StreamLens  │  topic: github-events  │  ↻ every 4s      │ ← header
├───────────────────────────┬─────────────────────────────────┤
│                           │  Event Types  (last 60 min)     │
│   Live Event Feed         ├─────────────────────────────────┤
│   (20 most recent events) │                                 │
│                           │  Top Repositories (last 60 min) │
│                           │                                 │
├───────────────────────────┴─────────────────────────────────┤
│  Total: 1,234 events processed  │  Updated: 10:30:45 UTC    │ ← status bar
└─────────────────────────────────────────────────────────────┘

How Rich's Live works:
───────────────────────
`rich.live.Live` takes a "renderable" (any Rich object) and re-renders it on a
timer. Every `refresh_per_second` seconds, it calls our `build_layout()`
function, which fetches fresh data from DuckDB and returns a new Layout object.
Rich then redraws only the parts that changed — the terminal doesn't flicker.

The pattern is:
    with Live(refresh_per_second=4) as live:
        while True:
            live.update(build_layout())   ← we control what triggers a refresh
            time.sleep(REFRESH_INTERVAL)

We sleep for REFRESH_INTERVAL (4s) between updates. This means we query DuckDB
at most 4 times per second — plenty frequent for a terminal dashboard.
"""

import os
import time
from datetime import datetime, timezone
from pathlib import Path

import structlog
from dotenv import load_dotenv
from rich import box
from rich.columns import Columns
from rich.console import Console
from rich.layout import Layout
from rich.live import Live
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

# We import from storage/ — run this script with PYTHONPATH=src
import sys
sys.path.insert(0, str(Path(__file__).parent.parent))

from storage import reader

load_dotenv()
log = structlog.get_logger(__name__)

# ── Configuration ─────────────────────────────────────────────────────────────

REFRESH_INTERVAL: float = float(os.getenv("REFRESH_INTERVAL", "4.0"))
STATS_WINDOW_MINUTES: int = int(os.getenv("STATS_WINDOW_MINUTES", "60"))
TOP_REPOS_LIMIT: int = int(os.getenv("TOP_REPOS_LIMIT", "8"))
FEED_LIMIT: int = int(os.getenv("FEED_LIMIT", "20"))
DATA_DIR: Path = Path(os.getenv("DATA_DIR", "data/events"))

KAFKA_TOPIC: str = os.getenv("KAFKA_TOPIC", "github-events")

# ── Color palette ─────────────────────────────────────────────────────────────
# Rich uses a CSS-like color system. These named colors work on most terminals.

COLOR_HEADER_BG = "dark_blue"
COLOR_ACCENT    = "bright_cyan"
COLOR_DIM       = "grey62"
COLOR_SUCCESS   = "bright_green"
COLOR_WARNING   = "yellow"

# One color per common GitHub event type — makes the feed easy to scan
EVENT_TYPE_COLORS: dict[str, str] = {
    "PushEvent":            "bright_green",
    "WatchEvent":           "bright_yellow",
    "CreateEvent":          "bright_cyan",
    "DeleteEvent":          "red",
    "ForkEvent":            "magenta",
    "IssuesEvent":          "orange3",
    "IssueCommentEvent":    "orange1",
    "PullRequestEvent":     "bright_blue",
    "PullRequestReviewEvent": "blue",
    "ReleaseEvent":         "bright_magenta",
    "PublicEvent":          "green",
    "MemberEvent":          "cyan",
    "CommitCommentEvent":   "grey74",
}

def event_color(event_type: str) -> str:
    """Return a color for the given event type, defaulting to white."""
    return EVENT_TYPE_COLORS.get(event_type, "white")


# ── Panel builders ────────────────────────────────────────────────────────────
# Each function returns a Rich renderable (Panel, Table, Text…).
# These are called on every refresh cycle — keep them fast.

def build_header() -> Panel:
    """
    Top bar: app name, topic, and refresh rate.
    The ● dot is green when data exists, grey when there's nothing yet.
    """
    total = reader.get_total_event_count(data_dir=DATA_DIR)
    dot_color = COLOR_SUCCESS if total > 0 else COLOR_DIM

    header_text = Text()
    header_text.append("● ", style=dot_color)
    header_text.append("StreamLens", style=f"bold {COLOR_ACCENT}")
    header_text.append("   │   ", style=COLOR_DIM)
    header_text.append(f"topic: {KAFKA_TOPIC}", style="white")
    header_text.append("   │   ", style=COLOR_DIM)
    header_text.append(f"↻ every {REFRESH_INTERVAL:.0f}s", style=COLOR_DIM)

    return Panel(header_text, style=COLOR_HEADER_BG, padding=(0, 1))


def build_event_feed() -> Panel:
    """
    Left panel: the 20 most recent events as a scrolling table.
    Each row: TYPE  │  actor  │  repo  │  time
    """
    events = reader.get_recent_events(limit=FEED_LIMIT, data_dir=DATA_DIR)

    table = Table(
        box=box.SIMPLE,
        show_header=True,
        header_style="bold " + COLOR_ACCENT,
        padding=(0, 1),
        expand=True,
    )
    table.add_column("Type",   style="bold", min_width=18, no_wrap=True)
    table.add_column("Actor",  style=COLOR_DIM, min_width=12, no_wrap=True)
    table.add_column("Repo",   min_width=20, no_wrap=True)
    table.add_column("When",   style=COLOR_DIM, min_width=8, justify="right", no_wrap=True)

    if not events:
        table.add_row(
            Text("no data yet", style=COLOR_DIM),
            "", "", "",
        )
    else:
        for row in events:
            evt_type: str = row.get("event_type", "")
            actor: str = row.get("actor_login") or "—"
            repo: str = row.get("repo_name", "")
            created_at = row.get("created_at")

            # Format the timestamp as HH:MM:SS for compactness
            if created_at and hasattr(created_at, "strftime"):
                when = created_at.strftime("%H:%M:%S")
            elif isinstance(created_at, str):
                when = created_at[11:19]   # slice "HH:MM:SS" from ISO string
            else:
                when = "—"

            table.add_row(
                Text(evt_type, style=f"bold {event_color(evt_type)}"),
                actor,
                repo,
                when,
            )

    return Panel(
        table,
        title=f"[bold {COLOR_ACCENT}]Live Event Feed[/]  [dim](last {FEED_LIMIT})[/]",
        border_style="bright_blue",
        padding=(0, 0),
    )


def build_event_counts() -> Panel:
    """
    Top-right panel: event counts by type for the last STATS_WINDOW_MINUTES.
    Includes a simple ASCII bar so you can see proportions at a glance.
    """
    counts = reader.get_event_counts_by_type(
        since_minutes=STATS_WINDOW_MINUTES,
        data_dir=DATA_DIR,
    )

    table = Table(
        box=box.SIMPLE,
        show_header=True,
        header_style="bold " + COLOR_ACCENT,
        padding=(0, 1),
        expand=True,
    )
    table.add_column("Event Type", min_width=22, no_wrap=True)
    table.add_column("Count",      min_width=6, justify="right")
    table.add_column("Bar",        min_width=12)

    if not counts:
        table.add_row(Text("no data yet", style=COLOR_DIM), "", "")
    else:
        max_count: int = counts[0]["event_count"] if counts else 1
        bar_width = 14   # max bar length in characters

        for row in counts:
            evt_type: str = row["event_type"]
            count: int = row["event_count"]
            bar_len = max(1, round(count / max_count * bar_width))
            bar = "█" * bar_len

            table.add_row(
                Text(evt_type, style=event_color(evt_type)),
                Text(str(count), style="bold white"),
                Text(bar, style=f"dim {event_color(evt_type)}"),
            )

    return Panel(
        table,
        title=f"[bold {COLOR_ACCENT}]Event Types[/]  [dim](last {STATS_WINDOW_MINUTES} min)[/]",
        border_style="bright_blue",
    )


def build_top_repos() -> Panel:
    """
    Bottom-right panel: most active repositories in the last STATS_WINDOW_MINUTES.
    """
    repos = reader.get_top_repos(
        since_minutes=STATS_WINDOW_MINUTES,
        limit=TOP_REPOS_LIMIT,
        data_dir=DATA_DIR,
    )

    table = Table(
        box=box.SIMPLE,
        show_header=True,
        header_style="bold " + COLOR_ACCENT,
        padding=(0, 1),
        expand=True,
    )
    table.add_column("#",          width=3,  justify="right", style=COLOR_DIM)
    table.add_column("Repository", min_width=26, no_wrap=True)
    table.add_column("Events",     min_width=6, justify="right")
    table.add_column("Authors",    min_width=7, justify="right", style=COLOR_DIM)

    if not repos:
        table.add_row("", Text("no data yet", style=COLOR_DIM), "", "")
    else:
        for rank, row in enumerate(repos, start=1):
            repo_name: str   = row.get("repo_name", "")
            event_count: int = row.get("event_count", 0)
            unique_actors    = row.get("unique_actors", 0)

            # Highlight the top repo
            rank_style = "bold yellow" if rank == 1 else COLOR_DIM

            table.add_row(
                Text(str(rank), style=rank_style),
                Text(repo_name, style="bright_white" if rank == 1 else "white"),
                Text(str(event_count), style="bold white"),
                str(unique_actors),
            )

    return Panel(
        table,
        title=f"[bold {COLOR_ACCENT}]Top Repositories[/]  [dim](last {STATS_WINDOW_MINUTES} min)[/]",
        border_style="bright_blue",
    )


def build_status_bar(total_events: int) -> Panel:
    """
    Bottom bar: total events, average pipeline lag, and current timestamp.

    Lag = avg(ingested_at - created_at) over the last 60 minutes.
    Color-coded: green < 30s, yellow 30–60s, red > 60s.
    """
    now_str = datetime.now(tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    lag = reader.get_avg_lag(since_minutes=STATS_WINDOW_MINUTES, data_dir=DATA_DIR)

    status = Text()
    status.append("Total: ", style=COLOR_DIM)
    status.append(f"{total_events:,} events", style=f"bold {COLOR_SUCCESS}")
    status.append("   │   ", style=COLOR_DIM)

    # Lag indicator
    if lag is None or lag["avg_lag_seconds"] is None:
        status.append("Lag: —", style=COLOR_DIM)
    else:
        avg = lag["avg_lag_seconds"]
        lag_color = COLOR_SUCCESS if avg < 30 else (COLOR_WARNING if avg < 60 else "red")
        status.append("Lag: ", style=COLOR_DIM)
        status.append(f"{avg:.1f}s avg", style=f"bold {lag_color}")
        status.append(f" (n={lag['sample_size']:,})", style=COLOR_DIM)

    status.append("   │   ", style=COLOR_DIM)
    status.append("Updated: ", style=COLOR_DIM)
    status.append(now_str, style="white")
    status.append("   │   ", style=COLOR_DIM)
    status.append("Ctrl+C to exit", style=COLOR_DIM)

    return Panel(status, style="on grey7", padding=(0, 1))


# ── Layout assembly ───────────────────────────────────────────────────────────

def build_layout() -> Layout:
    """
    Assemble all panels into a single Layout object.

    Layout tree:
        root (vertical)
        ├── header        (3 lines tall, fixed)
        ├── body          (fills remaining space)
        │   ├── left      (event feed, 55% width)
        │   └── right     (vertical)
        │       ├── counts  (event type stats)
        │       └── repos   (top repositories)
        └── footer        (3 lines tall, fixed)
    """
    layout = Layout()

    # Outer: header / body / footer stacked vertically
    layout.split_column(
        Layout(name="header", size=3),
        Layout(name="body"),
        Layout(name="footer", size=3),
    )

    # Body: event feed on the left, stats on the right
    layout["body"].split_row(
        Layout(name="left",  ratio=55),
        Layout(name="right", ratio=45),
    )

    # Right column: event counts stacked above top repos
    layout["right"].split_column(
        Layout(name="counts", ratio=45),
        Layout(name="repos",  ratio=55),
    )

    # Fetch total once (used by both header dot and status bar)
    total = reader.get_total_event_count(data_dir=DATA_DIR)

    # Populate each region
    layout["header"].update(build_header())
    layout["left"].update(build_event_feed())
    layout["counts"].update(build_event_counts())
    layout["repos"].update(build_top_repos())
    layout["footer"].update(build_status_bar(total))

    return layout


# ── Entry point ───────────────────────────────────────────────────────────────

def main() -> None:
    """
    Run the dashboard in a Live loop until the user presses Ctrl+C.

    `screen=True` makes Rich take over the full terminal window and restore
    it cleanly on exit (no leftover output). Set screen=False if you prefer
    to see output scrolling instead.
    """
    console = Console()

    console.print(
        "\n[bold bright_cyan]StreamLens Dashboard[/] starting…  "
        "[dim](Ctrl+C to exit)[/]\n"
    )

    # Brief pause so the user can read the startup message before Live takes over
    time.sleep(1.0)

    try:
        with Live(
            build_layout(),
            console=console,
            refresh_per_second=4,
            screen=True,          # full-screen takeover
        ) as live:
            while True:
                time.sleep(REFRESH_INTERVAL)
                live.update(build_layout())

    except KeyboardInterrupt:
        pass   # Clean exit — Live already restored the terminal in __exit__

    console.print("\n[bold bright_cyan]StreamLens[/] stopped. Goodbye!\n")


if __name__ == "__main__":
    main()
