"""
tests/test_knowledge_ingest.py — Unit tests for knowledge/ingest.py

Tests cover:
  1. Revert-number parsing (body reference, title fallback, none found)
  2. build_cases — flattening, labels, time-to-resolve, revert linkage
  3. write_kb — Parquet round trip, schema conformance, DuckDB readability
  4. fetch_closed_items — pagination and max_items cap (requests mocked)

Run with:
    PYTHONPATH=src pytest tests/test_knowledge_ingest.py -v
"""

from pathlib import Path

import duckdb
import pyarrow.parquet as pq
import pytest

from knowledge import ingest
from knowledge.kb_schema import KB_CASE_SCHEMA, KB_CASE_COLUMNS


# ── Fixtures ──────────────────────────────────────────────────────────────────

def make_item(
    number: int,
    title: str = "Fix flaky scheduler test",
    body: str | None = "Some body text",
    is_pr: bool = True,
    labels: list[str] | None = None,
    created_at: str = "2026-01-10T00:00:00Z",
    closed_at: str | None = "2026-01-12T12:00:00Z",
) -> dict:
    """Minimal raw GitHub issue/PR dict, as /repos/{repo}/issues returns."""
    item: dict = {
        "number": number,
        "title": title,
        "body": body,
        "html_url": f"https://github.com/acme/widgets/pull/{number}",
        "labels": [{"name": name} for name in (labels or [])],
        "created_at": created_at,
        "closed_at": closed_at,
    }
    if is_pr:
        item["pull_request"] = {"url": "..."}
    return item


# ── parse_reverted_number ─────────────────────────────────────────────────────

def test_parse_reverted_number_from_body() -> None:
    number = ingest.parse_reverted_number(
        'Revert "Fix flaky test"', "Reverts acme/widgets#123"
    )
    assert number == 123


def test_parse_reverted_number_title_fallback() -> None:
    assert ingest.parse_reverted_number("Revert #45", None) == 45


def test_parse_reverted_number_not_found() -> None:
    assert ingest.parse_reverted_number('Revert "something"', "no refs here") is None


# ── build_cases ───────────────────────────────────────────────────────────────

def test_build_cases_flattens_fields() -> None:
    cases = ingest.build_cases(
        [make_item(7, labels=["kind/bug", "severity:high"], is_pr=False)]
    )
    case = cases[0]
    assert case["case_id"] == "issue-7"
    assert case["kind"] == "issue"
    assert case["labels"] == ["kind/bug", "severity:high"]
    assert case["time_to_resolve_hours"] == pytest.approx(60.0)  # 2.5 days
    assert case["is_revert"] is False
    assert case["was_reverted"] is False


def test_build_cases_links_reverts() -> None:
    original = make_item(100, title="Add retry logic to consumer")
    revert = make_item(
        105,
        title='Revert "Add retry logic to consumer"',
        body="Reverts acme/widgets#100",
    )
    cases = {c["case_id"]: c for c in ingest.build_cases([original, revert])}

    assert cases["pr-105"]["is_revert"] is True
    assert cases["pr-105"]["reverts_number"] == 100
    assert cases["pr-100"]["was_reverted"] is True
    assert cases["pr-100"]["reverted_by"] == 105


def test_build_cases_issue_titled_revert_is_not_revert_pr() -> None:
    # Only PRs can be revert PRs — an *issue* asking to revert is not one.
    cases = ingest.build_cases([make_item(9, title="Revert the new config?", is_pr=False)])
    assert cases[0]["is_revert"] is False


def test_build_cases_open_item_has_no_resolve_time() -> None:
    cases = ingest.build_cases([make_item(3, closed_at=None)])
    assert cases[0]["time_to_resolve_hours"] is None
    assert cases[0]["closed_at"] is None


def test_case_text_combines_title_and_snippet() -> None:
    cases = ingest.build_cases([make_item(1, title="A title", body="A body")])
    assert ingest.case_text(cases[0]) == "A title\nA body"


# ── write_kb ──────────────────────────────────────────────────────────────────

def test_write_kb_round_trip(tmp_path: Path) -> None:
    cases = ingest.build_cases(
        [
            make_item(1, labels=["severity:high"]),
            make_item(2, title='Revert "x"', body="Reverts acme/widgets#1"),
        ]
    )
    path = ingest.write_kb(cases, kb_dir=tmp_path)

    table = pq.read_table(path)
    assert table.schema.equals(KB_CASE_SCHEMA)
    assert table.num_rows == 2
    assert table.column_names == KB_CASE_COLUMNS


def test_write_kb_duckdb_readable(tmp_path: Path) -> None:
    cases = ingest.build_cases(
        [make_item(1), make_item(2, title='Revert "x"', body="Reverts acme/widgets#1")]
    )
    path = ingest.write_kb(cases, kb_dir=tmp_path)

    rows = duckdb.sql(
        f"SELECT case_id, was_reverted FROM read_parquet('{path}') ORDER BY case_id"
    ).fetchall()
    assert rows == [("pr-1", True), ("pr-2", False)]


# ── fetch_closed_items (mocked HTTP) ──────────────────────────────────────────

class _FakeResponse:
    def __init__(self, payload: list[dict]) -> None:
        self._payload = payload

    def raise_for_status(self) -> None:
        pass

    def json(self) -> list[dict]:
        return self._payload


def test_fetch_closed_items_paginates_and_caps(monkeypatch: pytest.MonkeyPatch) -> None:
    pages = {
        1: [make_item(i) for i in range(1, 101)],   # full page → keep going
        2: [make_item(i) for i in range(101, 151)],  # partial page
        3: [],
    }

    def fake_get(url: str, headers: dict, params: dict, timeout: int) -> _FakeResponse:
        assert params["state"] == "closed"
        return _FakeResponse(pages[params["page"]])

    monkeypatch.setattr(ingest.requests, "get", fake_get)

    items = ingest.fetch_closed_items(repo="acme/widgets", max_items=120)
    assert len(items) == 120  # capped, second page fetched
