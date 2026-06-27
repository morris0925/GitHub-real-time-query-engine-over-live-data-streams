# Schema Changelog

StreamLens defines its Parquet schema via PyArrow in `src/storage/schema.py`.

**Key rules:**
- Parquet files are **immutable**: once written, they cannot be modified
- Old partitions keep the old schema; new partitions use the new schema
- When DuckDB glob-scans all partitions, it automatically merges different schema versions (provided changes are backward-compatible)
- **Every schema change must be recorded here**, noting which fields were added, removed, or retyped

---

## v1.0.0 — Initial Schema (Day 4)

**Date:** 2026-06-25

**Fields:**

| Field          | Type                      | Nullable | Description                                    |
|----------------|--------------------------|----------|------------------------------------------------|
| `event_id`     | `string`                  | ❌       | GitHub event unique ID                         |
| `event_type`   | `string`                  | ❌       | PushEvent / WatchEvent / ...                   |
| `actor_id`     | `int64`                   | ✅       | GitHub user ID                                 |
| `actor_login`  | `string`                  | ✅       | GitHub username                                |
| `repo_id`      | `int64`                   | ✅       | GitHub repository ID                           |
| `repo_name`    | `string`                  | ❌       | "owner/repo" format                            |
| `payload_json` | `string`                  | ✅       | Event-specific payload as a JSON string        |
| `public`       | `bool`                    | ✅       | Whether the event is public                    |
| `created_at`   | `timestamp(us, tz=UTC)`   | ❌       | When GitHub recorded the event                 |
| `ingested_at`  | `timestamp(us, tz=UTC)`   | ❌       | When the consumer wrote it to Parquet          |

**Design decisions:**

- `event_id` is `nullable=False`: it's the unique identifier — without it, deduplication is impossible
- `payload` stored as a JSON string rather than a nested struct: GitHub has 30+ event types each with a different payload structure; storing as a string avoids schema explosion and allows `json.loads()` when field-level access is needed
- Added `ingested_at`: subtracting `created_at` gives pipeline lag — a key system health metric
- Timestamp fields use `timestamp(us, tz=UTC)` rather than string: lets DuckDB do native time arithmetic (`created_at >= NOW() - INTERVAL '60' MINUTE`) without `CAST`

**Partition strategy (v1.0.0):** by ingestion date (`ingested_at`)

---

## v1.1.0 — Event-Time Partitioning (Day 7)

**Date:** 2026-06-27

**The schema itself did not change.** Only the partition strategy changed.

**Partition strategy change:**

| Version | Partition by          | Late event handling                  |
|---------|-----------------------|--------------------------------------|
| v1.0.0  | Ingestion date        | Mixed into the same partition as normal events |
| v1.1.0  | Event time (`created_at` date) | Events older than 24h → `date=late/` |

**Why this change?**

With v1.0.0, an event with `created_at = yesterday` was written to `date=today/`. A query for "yesterday's data" would have DuckDB skip yesterday's directory via partition pruning — the event simply disappeared from results.

v1.1.0 switches to partitioning by `created_at` date, with a 24-hour watermark:
- Normal event → `date=<created_at date>/`
- Event older than 24h → `date=late/` (quarantine)

**Backward compatibility:**
This change does not affect existing Parquet files. When DuckDB scans the glob, old `date=2026-06-25/` files (written by ingestion time) and new ones (written by event time) are returned together. The only caveat: files written during the v1.0.0 period may have a mismatch between `created_at` and their partition date — this is a known limitation documented alongside the `date=late/` handling.

**Related code:** `src/storage/writer.py` — `_partition_key()` function

---

## Candidate Future Changes

The following schema changes were considered during development but not yet implemented.

### Candidate v1.2.0: Add `actor_type` field

**Motivation:** Distinguish human users from bots (e.g. `dependabot[bot]`, `github-actions[bot]`). Bot push activity is high-volume but low analytical value and may need to be filtered.

**Proposal:**
```python
pa.field("actor_type", pa.string(), nullable=True)
# Value: "User" or "Bot", sourced from actor.type field
```

**Backward compatibility:** Backward-compatible addition (new field is nullable). Old partitions lack this field; DuckDB fills it with `NULL` on read.

### Candidate v1.3.0: Add `org_login` field

**Motivation:** GitHub events sometimes include an `org` field (organization). Useful for analyzing which organizations are most active.

**Proposal:**
```python
pa.field("org_login", pa.string(), nullable=True)
# Sourced from event.get("org", {}).get("login")
```

---

## How to Run a Schema Migration

Parquet files cannot be modified once written. There are two migration strategies:

**Strategy A: Forward-only (recommended)**
Make only backward-compatible changes (add nullable fields, do not alter existing ones). Old and new partitions can be scanned together by DuckDB; rows from old partitions fill missing new fields with `NULL`.

**Strategy B: Backfill**
Required when rewriting old partitions (e.g. changing a field's type):
1. Update the schema in `schema.py`
2. Write a one-off migration script (read old Parquet → transform → write new Parquet → delete old files)
3. Record the migration date and method here
4. Update `docs/schema_changelog.md`

**When backfill is required:**
- Changing a nullable field to non-nullable
- Changing a field's type (e.g. `string` → `int64`)
- Removing a field

**When forward-only is sufficient:**
- Adding a new nullable field
- Adding a new partition
