"""
knowledge/ingest.py — Closed issues/PRs → knowledge-base Parquet

Fetches closed issues and pull requests for KB_REPO via the GitHub REST API
and writes data/knowledge/kb.parquet (KB_CASE_SCHEMA).

Why this stores more than embeddable text
------------------------------------------
The Tier 2 historical-outcome estimate (design proposal §3) is a pure DuckDB
aggregation over *structured metadata*: labels, revert linkage, and
time-to-resolve. So ingestion must capture, per case:

1. labels        — e.g. "severity:high", "kind/bug", "priority/critical"
2. revert linkage — a later PR titled "Revert ..." pointing back at the
                    original PR. We detect revert PRs by title, extract the
                    original PR number (GitHub auto-inserts "Reverts
                    owner/repo#123" into the revert PR body), then mark the
                    original with was_reverted=True / reverted_by=<n>.
3. time_to_resolve_hours — closed_at − created_at.

Run manually:
    PYTHONPATH=src python src/knowledge/ingest.py

Configuration (via .env):
    KB_REPO       owner/repo to mine           (default: kubernetes/kubernetes)
    KB_MAX_ITEMS  max closed items to fetch    (default: 300)
    KB_DIR        output directory             (default: data/knowledge)
    GITHUB_TOKEN  optional, raises rate limit from 60 to 5000 req/hr
"""

import os
import re
from datetime import datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import requests
import structlog
from dotenv import load_dotenv

from knowledge.kb_schema import KB_CASE_SCHEMA

load_dotenv()

log = structlog.get_logger(__name__)

# ── Configuration ─────────────────────────────────────────────────────────────

GITHUB_API_URL: str = "https://api.github.com"
KB_REPO:        str = os.getenv("KB_REPO", "kubernetes/kubernetes")
KB_MAX_ITEMS:   int = int(os.getenv("KB_MAX_ITEMS", "300"))
KB_DIR:         Path = Path(os.getenv("KB_DIR", "data/knowledge"))
GITHUB_TOKEN:   str | None = os.getenv("GITHUB_TOKEN")

KB_FILENAME: str = "kb.parquet"

# How much of the issue/PR body to keep. Enough for embedding context,
# small enough that raw-evidence panels stay readable.
BODY_SNIPPET_CHARS: int = 1000

# GitHub's revert PRs are titled 'Revert "original title"'.
_REVERT_TITLE_RE = re.compile(r"^revert\b", re.IGNORECASE)

# GitHub auto-inserts 'Reverts owner/repo#123' into revert PR bodies.
_REVERTS_BODY_RE = re.compile(r"reverts\s+[\w.-]+/[\w.-]+#(\d+)", re.IGNORECASE)

# Fallback: a bare '#123' reference in the revert PR title.
_NUMBER_REF_RE = re.compile(r"#(\d+)")


# ── GitHub REST fetch ─────────────────────────────────────────────────────────

def _headers() -> dict[str, str]:
    """Standard GitHub API headers, with auth when a token is configured."""
    return {
        "Accept": "application/vnd.github+json",
        **({"Authorization": f"Bearer {GITHUB_TOKEN}"} if GITHUB_TOKEN else {}),
    }


def fetch_closed_items(
    repo: str = KB_REPO,
    max_items: int = KB_MAX_ITEMS,
) -> list[dict]:
    """
    Fetch up to `max_items` recently closed issues AND pull requests.

    Uses /repos/{repo}/issues?state=closed, which returns both — PRs are
    distinguished by the presence of a "pull_request" key. Paginates at 100
    per page (GitHub's max) until max_items is reached or pages run out.
    """
    items: list[dict] = []
    page = 1

    while len(items) < max_items:
        response = requests.get(
            f"{GITHUB_API_URL}/repos/{repo}/issues",
            headers=_headers(),
            params={
                "state": "closed",
                "sort": "updated",
                "direction": "desc",
                "per_page": 100,
                "page": page,
            },
            timeout=15,
        )
        response.raise_for_status()
        batch = response.json()
        if not batch:
            break
        items.extend(batch)
        page += 1

    log.info("kb_items_fetched", repo=repo, count=min(len(items), max_items))
    return items[:max_items]


# ── Parsing helpers ───────────────────────────────────────────────────────────

def _parse_timestamp(value: str | None) -> datetime | None:
    """Parse a GitHub ISO-8601 timestamp ('2026-01-15T10:30:00Z') or None."""
    if not value:
        return None
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def parse_reverted_number(title: str, body: str | None) -> int | None:
    """
    For a PR titled "Revert ...", find the number of the PR it reverts.

    Prefers the body ("Reverts owner/repo#123" is auto-inserted by GitHub's
    revert button); falls back to a bare "#123" in the title. Returns None
    if no reference is found — the revert is still recorded as is_revert,
    it just can't be linked back.
    """
    if body:
        match = _REVERTS_BODY_RE.search(body)
        if match:
            return int(match.group(1))
    match = _NUMBER_REF_RE.search(title)
    if match:
        return int(match.group(1))
    return None


def build_cases(items: list[dict]) -> list[dict]:
    """
    Flatten raw GitHub issue/PR dicts into KB_CASE_SCHEMA rows and resolve
    revert linkage.

    Two passes:
      1. Flatten every item; detect revert PRs by title and extract the
         original PR number they point at.
      2. Walk the revert map and mark each *original* case with
         was_reverted=True / reverted_by=<revert PR number>.
    """
    cases: list[dict] = []
    reverts: dict[int, int] = {}  # original PR number → revert PR number

    for item in items:
        is_pr = "pull_request" in item
        number = int(item["number"])
        title: str = item.get("title") or ""
        body: str | None = item.get("body")

        created_at = _parse_timestamp(item.get("created_at"))
        closed_at = _parse_timestamp(item.get("closed_at"))
        time_to_resolve: float | None = None
        if created_at and closed_at:
            time_to_resolve = (closed_at - created_at).total_seconds() / 3600.0

        is_revert = bool(is_pr and _REVERT_TITLE_RE.match(title))
        reverts_number: int | None = None
        if is_revert:
            reverts_number = parse_reverted_number(title, body)
            if reverts_number is not None:
                reverts[reverts_number] = number

        cases.append(
            {
                "case_id": f"{'pr' if is_pr else 'issue'}-{number}",
                "kind": "pr" if is_pr else "issue",
                "number": number,
                "title": title,
                "body_snippet": (body or "")[:BODY_SNIPPET_CHARS] or None,
                "url": item.get("html_url"),
                "labels": [label["name"] for label in item.get("labels", [])],
                "created_at": created_at,
                "closed_at": closed_at,
                "time_to_resolve_hours": time_to_resolve,
                "is_revert": is_revert,
                "reverts_number": reverts_number,
                "was_reverted": False,   # resolved in pass 2
                "reverted_by": None,
            }
        )

    for case in cases:
        if case["kind"] == "pr" and case["number"] in reverts:
            case["was_reverted"] = True
            case["reverted_by"] = reverts[case["number"]]

    linked = sum(1 for c in cases if c["was_reverted"])
    log.info("kb_cases_built", total=len(cases), reverts=len(reverts), linked=linked)
    return cases


def case_text(case: dict) -> str:
    """The text that gets embedded for a case: title + body snippet."""
    snippet = case.get("body_snippet") or ""
    return f"{case['title']}\n{snippet}".strip()


# ── Parquet output ────────────────────────────────────────────────────────────

def write_kb(cases: list[dict], kb_dir: Path = KB_DIR) -> Path:
    """Write cases to <kb_dir>/kb.parquet, replacing any previous file."""
    kb_dir.mkdir(parents=True, exist_ok=True)
    path = kb_dir / KB_FILENAME
    table = pa.Table.from_pylist(cases, schema=KB_CASE_SCHEMA)
    pq.write_table(table, path)
    log.info("kb_written", path=str(path), rows=table.num_rows)
    return path


def main() -> None:
    """Fetch → flatten/link → write. The knowledge-base refresh entry point."""
    log.info("kb_ingest_starting", repo=KB_REPO, max_items=KB_MAX_ITEMS)
    items = fetch_closed_items()
    cases = build_cases(items)
    write_kb(cases)


if __name__ == "__main__":
    main()
