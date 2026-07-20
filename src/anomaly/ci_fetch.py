"""
anomaly/ci_fetch.py — GitHub Actions workflow runs → Parquet

The public GitHub Events feed carries no CI or status events, so the CI
stability signal needs its own (small) source: the Actions API for one
configured repo. Polled on demand / by cron, written to data/ci_runs/.

Run manually:
    PYTHONPATH=src python src/anomaly/ci_fetch.py

Configuration (via .env):
    CI_REPO       repo whose workflow runs to fetch (defaults to KB_REPO)
    CI_DIR        output directory                  (default: data/ci_runs)
    GITHUB_TOKEN  optional, raises rate limit
"""

import os
from datetime import datetime
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import requests
import structlog
from dotenv import load_dotenv

load_dotenv()

log = structlog.get_logger(__name__)

# ── Configuration ─────────────────────────────────────────────────────────────

GITHUB_API_URL: str = "https://api.github.com"
CI_REPO: str = os.getenv("CI_REPO") or os.getenv("KB_REPO", "kubernetes/kubernetes")
CI_DIR:  Path = Path(os.getenv("CI_DIR", "data/ci_runs"))
GITHUB_TOKEN: str | None = os.getenv("GITHUB_TOKEN")

RUNS_FILENAME: str = "runs.parquet"
MAX_RUNS: int = 200

CI_RUN_SCHEMA = pa.schema(
    [
        pa.field("run_id",        pa.int64(),                   nullable=False),
        pa.field("repo",          pa.string(),                  nullable=False),
        pa.field("workflow_name", pa.string(),                  nullable=True),
        pa.field("status",        pa.string(),                  nullable=True),
        pa.field("conclusion",    pa.string(),                  nullable=True),
        pa.field("created_at",    pa.timestamp("us", tz="UTC"), nullable=False),
    ]
)


def fetch_workflow_runs(repo: str = CI_REPO, max_runs: int = MAX_RUNS) -> list[dict]:
    """Fetch the most recent workflow runs (paginated, newest first)."""
    headers = {
        "Accept": "application/vnd.github+json",
        **({"Authorization": f"Bearer {GITHUB_TOKEN}"} if GITHUB_TOKEN else {}),
    }
    runs: list[dict] = []
    page = 1
    while len(runs) < max_runs:
        response = requests.get(
            f"{GITHUB_API_URL}/repos/{repo}/actions/runs",
            headers=headers,
            params={"per_page": 100, "page": page},
            timeout=15,
        )
        response.raise_for_status()
        batch = response.json().get("workflow_runs", [])
        if not batch:
            break
        for run in batch:
            runs.append(
                {
                    "run_id": int(run["id"]),
                    "repo": repo,
                    "workflow_name": run.get("name"),
                    "status": run.get("status"),
                    "conclusion": run.get("conclusion"),
                    "created_at": datetime.fromisoformat(
                        run["created_at"].replace("Z", "+00:00")
                    ),
                }
            )
        page += 1
    log.info("ci_runs_fetched", repo=repo, count=min(len(runs), max_runs))
    return runs[:max_runs]


def write_runs(runs: list[dict], ci_dir: Path = CI_DIR) -> Path:
    """Write runs to <ci_dir>/runs.parquet, replacing any previous file."""
    ci_dir.mkdir(parents=True, exist_ok=True)
    path = ci_dir / RUNS_FILENAME
    table = pa.Table.from_pylist(runs, schema=CI_RUN_SCHEMA)
    pq.write_table(table, path)
    log.info("ci_runs_written", path=str(path), rows=table.num_rows)
    return path


def latest_run_time(ci_dir: Path = CI_DIR) -> datetime | None:
    """
    Return the created_at of the most recently fetched CI run.

    Returns None if runs.parquet doesn't exist yet. Used by /health to
    report CI-fetch freshness.
    """
    path = ci_dir / RUNS_FILENAME
    if not path.exists():
        return None
    result = duckdb.connect().execute(
        f"SELECT MAX(created_at) AS latest FROM read_parquet('{path}')"
    ).fetchone()
    return result[0] if result else None


def main() -> None:
    write_runs(fetch_workflow_runs())


if __name__ == "__main__":
    main()
