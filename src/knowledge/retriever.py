"""
knowledge/retriever.py — DuckDB vector-similarity search over the knowledge base

Loads kb.parquet (cases + metadata) and kb_embeddings.parquet (vectors) into
an in-memory DuckDB table with a fixed-size FLOAT[dim] embedding column, then
answers "which historical cases look like this text?" queries.

Index strategy
--------------
- `array_cosine_similarity` is core DuckDB (≥1.0) — retrieval always works.
- The VSS extension's HNSW index is attempted as an acceleration on top
  (CREATE INDEX ... USING HNSW). If the extension can't be installed (e.g.
  offline), we log and fall back to brute-force scanning, which is easily
  fast enough for a few hundred cases.

Confidence is exposed as a qualitative band ("high" / "medium" / "low"), not
a percentage — design proposal §4: a number would imply false precision for
a similarity search over historical text.
"""

from pathlib import Path

import duckdb
import structlog

from knowledge.embeddings import (
    EMBEDDINGS_FILENAME,
    KB_DIR,
    EmbeddingProvider,
    get_provider,
)
from knowledge.ingest import KB_FILENAME

log = structlog.get_logger(__name__)

# Cosine-similarity thresholds for the qualitative bands.
HIGH_SIMILARITY: float = 0.75
MEDIUM_SIMILARITY: float = 0.50

DEFAULT_TOP_K: int = 5


def similarity_band(score: float) -> str:
    """Map a cosine similarity to the qualitative band shown in the UI."""
    if score >= HIGH_SIMILARITY:
        return "high"
    if score >= MEDIUM_SIMILARITY:
        return "medium"
    return "low"


class Retriever:
    """
    In-memory DuckDB search over the knowledge base.

    Build one per process (the FastAPI service holds a singleton); reload()
    picks up a refreshed kb.parquet without restarting.
    """

    def __init__(
        self,
        kb_dir: Path = KB_DIR,
        provider: EmbeddingProvider | None = None,
    ) -> None:
        self._kb_dir = kb_dir
        # Resolved lazily: get_provider() raises without a key, and we must
        # not force that at construction (create_app runs at import time, and
        # CI / tooling import the app with no keys set). Tests inject a
        # provider explicitly, so they never hit the lazy path.
        self._provider = provider
        self._conn: duckdb.DuckDBPyConnection = duckdb.connect()
        self._dim: int = 0
        self.ready: bool = False
        self.reload()

    def _get_provider(self) -> EmbeddingProvider:
        """Resolve the embedding provider on first use (raises without a key)."""
        if self._provider is None:
            self._provider = get_provider()
        return self._provider

    @property
    def provider_name(self) -> str:
        """Which embedding provider is active — surfaced in API responses."""
        return self._get_provider().name

    @property
    def case_count(self) -> int:
        """Number of cases currently loaded — surfaced in /health for freshness."""
        if not self.ready:
            return 0
        row = self._conn.execute("SELECT count(*) FROM kb").fetchone()
        return int(row[0]) if row else 0

    def reload(self) -> None:
        """(Re)load the Parquet files into the in-memory table + index."""
        kb_path = self._kb_dir / KB_FILENAME
        emb_path = self._kb_dir / EMBEDDINGS_FILENAME
        if not kb_path.exists() or not emb_path.exists():
            log.warning("kb_missing", kb=str(kb_path), embeddings=str(emb_path))
            self.ready = False
            return

        row = self._conn.execute(
            f"SELECT len(embedding) FROM read_parquet('{emb_path}') LIMIT 1"
        ).fetchone()
        if row is None:
            log.warning("kb_embeddings_empty", path=str(emb_path))
            self.ready = False
            return
        self._dim = int(row[0])

        self._conn.execute("DROP TABLE IF EXISTS kb")
        self._conn.execute(
            f"CREATE TABLE kb AS "
            f"SELECT c.*, CAST(e.embedding AS FLOAT[{self._dim}]) AS embedding "
            f"FROM read_parquet('{kb_path}') c "
            f"JOIN read_parquet('{emb_path}') e USING (case_id)"
        )
        self._try_hnsw_index()
        count = self._conn.execute("SELECT count(*) FROM kb").fetchone()
        self.ready = True
        log.info("kb_loaded", cases=count[0] if count else 0, dim=self._dim)

    def _try_hnsw_index(self) -> None:
        """Best-effort VSS HNSW index; brute force is the fallback."""
        try:
            self._conn.execute("INSTALL vss; LOAD vss;")
            self._conn.execute(
                "CREATE INDEX kb_hnsw ON kb USING HNSW (embedding) WITH (metric = 'cosine')"
            )
            log.info("vss_hnsw_index_created")
        except duckdb.Error as exc:
            log.warning("vss_unavailable_brute_force_fallback", error=str(exc))

    def search(self, query_text: str, top_k: int = DEFAULT_TOP_K) -> list[dict]:
        """
        Return the top_k most similar cases for a free-text query.

        Each result carries the full case metadata (labels, revert linkage,
        time-to-resolve) plus `similarity` (float) and `similarity_band`.
        """
        if not self.ready:
            return []

        (vector,) = self._get_provider().embed([query_text])
        cursor = self._conn.execute(
            f"SELECT * EXCLUDE (embedding), "
            f"array_cosine_similarity(embedding, CAST(? AS FLOAT[{self._dim}])) AS similarity "
            f"FROM kb ORDER BY similarity DESC LIMIT ?",
            [vector, top_k],
        )
        columns = [desc[0] for desc in cursor.description]
        results = [dict(zip(columns, row)) for row in cursor.fetchall()]
        for result in results:
            result["similarity_band"] = similarity_band(result["similarity"])
        return results
