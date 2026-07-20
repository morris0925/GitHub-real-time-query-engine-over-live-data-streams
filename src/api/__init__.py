# api/ — FastAPI service for the AI diagnostic layer.
#
# The Next.js frontend talks to this service over HTTP; it never touches
# Parquet/DuckDB directly. Endpoints: /anomalies, /diagnose/{id}, /query,
# /signal, /snapshot.
