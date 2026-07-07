"""
knowledge/embeddings.py — Text → vector providers for the knowledge base

Two providers behind one interface:

1. VoyageEmbeddings — real semantic embeddings via the Voyage AI API
   (Anthropic's recommended embedding partner; Anthropic itself has no
   embeddings endpoint). Used when VOYAGE_API_KEY is set.

2. HashEmbeddings — deterministic, offline, hash-based vectors. Used when no
   key is configured and in tests. Retrieval still *works* mechanically
   (identical text → identical vector), but similarity is meaningless.
   The provider name is carried through to the API response so the UI can
   surface that the demo is running on stub embeddings.

Both produce unit-length vectors so cosine similarity is a plain dot product
and always lands in [-1, 1].

Also here: build_embeddings_file(), which embeds every case in kb.parquet and
writes kb_embeddings.parquet (KB_EMBEDDING_SCHEMA). Kept separate from
ingest.py so the corpus can be re-embedded without re-fetching from GitHub.

Run manually (after ingest.py):
    PYTHONPATH=src python src/knowledge/embeddings.py
"""

import hashlib
import math
import os
import struct
from pathlib import Path
from typing import Protocol

import httpx
import pyarrow as pa
import pyarrow.parquet as pq
import structlog
from dotenv import load_dotenv

from knowledge.kb_schema import KB_EMBEDDING_SCHEMA

load_dotenv()

log = structlog.get_logger(__name__)

# ── Configuration ─────────────────────────────────────────────────────────────

VOYAGE_API_URL:  str = "https://api.voyageai.com/v1/embeddings"
VOYAGE_API_KEY:  str | None = os.getenv("VOYAGE_API_KEY")
EMBEDDING_MODEL: str = os.getenv("EMBEDDING_MODEL", "voyage-3-lite")
KB_DIR:          Path = Path(os.getenv("KB_DIR", "data/knowledge"))

EMBEDDINGS_FILENAME: str = "kb_embeddings.parquet"

# Dimension of the hash-stub vectors. voyage-3-lite is 512-dimensional; the
# stub matches so downstream SQL doesn't care which provider produced a file.
STUB_DIM: int = 512

# Voyage caps batch size at 128 inputs per request.
VOYAGE_BATCH_SIZE: int = 128


# ── Provider interface ────────────────────────────────────────────────────────

class EmbeddingProvider(Protocol):
    """Anything that can turn a list of texts into unit-length vectors."""

    name: str

    def embed(self, texts: list[str]) -> list[list[float]]:
        ...


class VoyageEmbeddings:
    """Voyage AI API client. Requires VOYAGE_API_KEY."""

    name = "voyage"

    def __init__(self, api_key: str, model: str = EMBEDDING_MODEL) -> None:
        self._api_key = api_key
        self._model = model

    def embed(self, texts: list[str]) -> list[list[float]]:
        """Embed texts in batches of VOYAGE_BATCH_SIZE, preserving order."""
        vectors: list[list[float]] = []
        for start in range(0, len(texts), VOYAGE_BATCH_SIZE):
            batch = texts[start : start + VOYAGE_BATCH_SIZE]
            response = httpx.post(
                VOYAGE_API_URL,
                headers={"Authorization": f"Bearer {self._api_key}"},
                json={"model": self._model, "input": batch},
                timeout=60,
            )
            response.raise_for_status()
            data = response.json()["data"]
            # Voyage returns entries with an "index" field; sort defensively.
            vectors.extend(entry["embedding"] for entry in sorted(data, key=lambda e: e["index"]))
        log.info("voyage_embedded", count=len(vectors), model=self._model)
        return vectors


class HashEmbeddings:
    """
    Deterministic offline stub: SHA-256-seeded pseudo-vectors, unit length.

    Not semantic — two similar sentences get unrelated vectors. Exists so the
    pipeline and tests run without network or keys, and so the demo degrades
    loudly (provider name is surfaced) rather than failing quietly.
    """

    name = "hash-stub"

    def __init__(self, dim: int = STUB_DIM) -> None:
        self._dim = dim

    def embed(self, texts: list[str]) -> list[list[float]]:
        return [self._one(text) for text in texts]

    def _one(self, text: str) -> list[float]:
        values: list[float] = []
        counter = 0
        while len(values) < self._dim:
            digest = hashlib.sha256(f"{counter}:{text}".encode("utf-8")).digest()
            # 8 float32-ish values per 32-byte digest, mapped to [-1, 1]
            for i in range(0, 32, 4):
                (raw,) = struct.unpack(">I", digest[i : i + 4])
                values.append(raw / 2**31 - 1.0)
            counter += 1
        values = values[: self._dim]
        norm = math.sqrt(sum(v * v for v in values)) or 1.0
        return [v / norm for v in values]


def get_provider() -> EmbeddingProvider:
    """Voyage when a key is configured, otherwise the loud hash stub."""
    if VOYAGE_API_KEY:
        return VoyageEmbeddings(api_key=VOYAGE_API_KEY)
    log.warning("no_voyage_key", fallback="hash-stub embeddings — retrieval quality is meaningless")
    return HashEmbeddings()


# ── Embedding-file builder ────────────────────────────────────────────────────

def build_embeddings_file(
    kb_dir: Path = KB_DIR,
    provider: EmbeddingProvider | None = None,
) -> Path:
    """
    Embed every case in <kb_dir>/kb.parquet → <kb_dir>/kb_embeddings.parquet.

    Reads title + body snippet (same text contract as ingest.case_text) and
    writes one row per case_id. Replaces any previous embeddings file.
    """
    from knowledge.ingest import KB_FILENAME, case_text  # avoid import cycle at module load

    provider = provider or get_provider()
    kb_path = kb_dir / KB_FILENAME
    cases = pq.read_table(kb_path).to_pylist()
    texts = [case_text(case) for case in cases]

    vectors = provider.embed(texts)

    table = pa.Table.from_pylist(
        [
            {"case_id": case["case_id"], "embedding": vector}
            for case, vector in zip(cases, vectors)
        ],
        schema=KB_EMBEDDING_SCHEMA,
    )
    path = kb_dir / EMBEDDINGS_FILENAME
    pq.write_table(table, path)
    log.info("embeddings_written", path=str(path), rows=table.num_rows, provider=provider.name)
    return path


if __name__ == "__main__":
    build_embeddings_file()
