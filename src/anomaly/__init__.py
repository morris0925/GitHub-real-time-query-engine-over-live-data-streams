# anomaly/ — Tier 1 rule-based anomaly detection for the AI diagnostic layer.
#
# ci_fetch.py  — GitHub Actions workflow runs → data/ci_runs/ (CI signal source;
#                the public Events feed carries no CI/status events)
# detector.py  — CI failure spike / merge-time anomaly / commit drought rules
#                + the Dev Pipeline Signal components
# store.py     — anomaly Parquet store + demo-anomaly seeding
