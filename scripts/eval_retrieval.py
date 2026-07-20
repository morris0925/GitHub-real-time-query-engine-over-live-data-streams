#!/usr/bin/env python3
"""
scripts/eval_retrieval.py — Retrieval evaluation harness: recall@k / MRR

Turns "retrieval works" into a defended number instead of an assertion.
Builds a small labeled (query -> expected_case_id) set directly from the
real, currently-ingested knowledge base, then measures how often the
retriever's ranked results actually surface the case each query came from.

Labeling technique — self-retrieval on a held-out field
---------------------------------------------------------
Every case's embedded text is `title + "\\n" + body_snippet` (see
knowledge.ingest.case_text). Each eval query is the case's TITLE ALONE,
with that case's own id as ground truth. The query is a genuine substring
of the embedded text but is missing the body — a modest, honest proxy for
"a short query similar to a real incident should surface that incident,"
without fabricating hand-labeled relevance judgments we're not positioned
to author for a kubernetes/kubernetes corpus we don't own.

This measures retrieval MECHANICS (embedding round-trip, cosine ranking,
index correctness) on real data, not a domain-expert relevance benchmark —
say so wherever these numbers get cited.

Usage:
    cd "GitHub real-time query engine over live data streams"
    PYTHONPATH=src python scripts/eval_retrieval.py
    PYTHONPATH=src python scripts/eval_retrieval.py --n 100 --k 1,3,5,10

Requires VOYAGE_API_KEY (same as any real retrieval) and an already-built
KB:
    PYTHONPATH=src python src/knowledge/ingest.py
    PYTHONPATH=src python src/knowledge/embeddings.py

Results are written to results/retrieval_eval.json.
"""

from __future__ import annotations

import argparse
import json
import random
import sys
import time
from pathlib import Path

# ── path bootstrap (run from project root) ────────────────────────────────────
PROJECT_ROOT = Path(__file__).resolve().parent.parent
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

import httpx
import pyarrow.parquet as pq
from rich.console import Console
from rich.table import Table

from knowledge.embeddings import KB_DIR
from knowledge.ingest import KB_FILENAME
from knowledge.retriever import Retriever

RESULTS_DIR = PROJECT_ROOT / "results"

# Titles shorter than this carry almost no retrieval signal on their own
# (e.g. "fix bug") and would just add noise to the eval set.
MIN_TITLE_CHARS = 15


# ── Eval-set construction ──────────────────────────────────────────────────────

def build_eval_set(kb_dir: Path, n: int = 50, seed: int = 42) -> list[dict]:
    """
    Deterministically sample up to n cases from kb.parquet and build a
    (query -> expected_case_id) pair from each, using the case's title as
    the query.

    Same kb_dir + n + seed always produces the same set, so results are
    reproducible across runs (until the KB itself changes).
    """
    kb_path = kb_dir / KB_FILENAME
    if not kb_path.exists():
        raise FileNotFoundError(
            f"No kb.parquet at {kb_path} — run knowledge/ingest.py first."
        )

    cases = pq.read_table(kb_path).to_pylist()
    candidates = [c for c in cases if len(c["title"].strip()) >= MIN_TITLE_CHARS]

    sample = random.Random(seed).sample(candidates, k=min(n, len(candidates)))
    return [
        {"query": case["title"].strip(), "expected_case_id": case["case_id"]}
        for case in sample
    ]


# ── Metrics ───────────────────────────────────────────────────────────────────

def _search_with_retry(
    retriever: Retriever,
    query: str,
    top_k: int,
    max_retries: int,
    retry_delay: float,
    console: Console | None = None,
) -> list[dict]:
    """
    retriever.search() makes one live embedding API call per query. Free-tier
    Voyage accounts (no payment method on file) are capped at 3 RPM — a
    harness that samples more than ~3 queries will hit HTTP 429 well before
    finishing. Retry with a fixed backoff instead of crashing partway through.
    """
    for attempt in range(max_retries + 1):
        try:
            return retriever.search(query, top_k=top_k)
        except httpx.HTTPStatusError as exc:
            if exc.response.status_code != 429 or attempt == max_retries:
                raise
            if console:
                console.print(
                    f"[yellow]429 rate limited, retrying in {retry_delay:.0f}s "
                    f"(attempt {attempt + 1}/{max_retries})[/yellow]"
                )
            time.sleep(retry_delay)
    return []  # unreachable — loop always returns or raises


def evaluate(
    retriever: Retriever,
    eval_set: list[dict],
    k_values: tuple[int, ...] = (1, 3, 5),
    search_top_k: int = 10,
    max_retries: int = 3,
    retry_delay: float = 21.0,
    console: Console | None = None,
) -> dict:
    """
    Run every (query, expected_case_id) pair through retriever.search() and
    report recall@k for each k plus Mean Reciprocal Rank.

    recall@k: fraction of queries where the expected case appears in the
              top k results.
    MRR:      mean of 1/rank across queries (0 contribution if the expected
              case isn't found within search_top_k).
    """
    top_k = max(search_top_k, max(k_values, default=0))

    detail: list[dict] = []
    reciprocal_ranks: list[float] = []

    for pair in eval_set:
        results = _search_with_retry(
            retriever, pair["query"], top_k, max_retries, retry_delay, console
        )
        rank = next(
            (i + 1 for i, r in enumerate(results) if r["case_id"] == pair["expected_case_id"]),
            None,
        )
        reciprocal_ranks.append(1.0 / rank if rank else 0.0)
        detail.append({**pair, "rank": rank})

    n = len(eval_set) or 1
    recall_at_k = {
        k: round(sum(1 for d in detail if d["rank"] is not None and d["rank"] <= k) / n, 4)
        for k in k_values
    }
    mrr = round(sum(reciprocal_ranks) / n, 4)

    return {"n": len(eval_set), "recall_at_k": recall_at_k, "mrr": mrr, "detail": detail}


# ── CLI ───────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--kb-dir", type=Path, default=KB_DIR)
    parser.add_argument("--n", type=int, default=50, help="Number of labeled pairs to sample")
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--k", type=str, default="1,3,5", help="Comma-separated k values for recall@k")
    parser.add_argument("--search-top-k", type=int, default=10)
    parser.add_argument("--out", type=Path, default=RESULTS_DIR / "retrieval_eval.json")
    parser.add_argument(
        "--retry-delay", type=float, default=21.0,
        help="Seconds to wait before retrying after an HTTP 429 (default: 21s, "
             "just over the 3-RPM free-tier Voyage limit)",
    )
    parser.add_argument("--max-retries", type=int, default=3)
    args = parser.parse_args()

    k_values = tuple(int(k) for k in args.k.split(","))

    console = Console()
    console.print(f"[bold cyan]Retrieval eval[/bold cyan] — kb_dir={args.kb_dir}")

    eval_set = build_eval_set(args.kb_dir, n=args.n, seed=args.seed)
    console.print(f"Sampled {len(eval_set)} (query -> expected_case_id) pairs (seed={args.seed})")

    retriever = Retriever(kb_dir=args.kb_dir)
    if not retriever.ready:
        console.print("[bold red]KB not ready[/bold red] — build it first (ingest.py + embeddings.py)")
        raise SystemExit(1)

    report = evaluate(
        retriever,
        eval_set,
        k_values=k_values,
        search_top_k=args.search_top_k,
        max_retries=args.max_retries,
        retry_delay=args.retry_delay,
        console=console,
    )

    table = Table(title=f"Retrieval quality — provider={retriever.provider_name}, n={report['n']}")
    table.add_column("Metric", style="bold")
    table.add_column("Value", justify="right", style="green")
    for k in k_values:
        table.add_row(f"recall@{k}", f"{report['recall_at_k'][k]:.1%}")
    table.add_row("MRR", f"{report['mrr']:.4f}")
    console.print(table)

    misses = [d for d in report["detail"] if d["rank"] is None]
    if misses:
        console.print(
            f"[yellow]{len(misses)} of {report['n']} queries found no match "
            f"within top {args.search_top_k}[/yellow]"
        )

    args.out.parent.mkdir(parents=True, exist_ok=True)
    with open(args.out, "w") as fh:
        json.dump(
            {
                "kb_dir": str(args.kb_dir),
                "provider": retriever.provider_name,
                "seed": args.seed,
                "k_values": list(k_values),
                **report,
            },
            fh,
            indent=2,
        )
    console.print(f"[dim]Results written to {args.out}[/dim]")


if __name__ == "__main__":
    main()
