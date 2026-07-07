# knowledge/ — RAG knowledge base for the AI diagnostic layer.
#
# Pipeline: ingest.py (GitHub REST → kb.parquet with labels + revert linkage)
#           → embeddings.py (text → vectors) → retriever.py (DuckDB VSS search)
#           → outcomes.py (Tier 2 historical-outcome aggregation).
