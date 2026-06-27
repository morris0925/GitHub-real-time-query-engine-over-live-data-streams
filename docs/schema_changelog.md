# Schema Changelog

StreamLens 使用 PyArrow 定義 Parquet schema（`src/storage/schema.py`）。

**關鍵規則：**
- Parquet 檔案是 **immutable**：一旦寫入就不能修改
- 舊 partition 保留舊 schema，新 partition 使用新 schema
- DuckDB 用 glob 掃所有 partition 時，會自動合併不同 schema 版本（前提是做了 backward-compatible 的變更）
- **每次改 schema 都要在這裡記錄**，說明哪些欄位加了/移了/改型別了

---

## v1.0.0 — 初始 Schema（Day 4）

**日期：** 2026-06-25

**欄位：**

| 欄位名稱      | 型別                      | Nullable | 說明                                        |
|--------------|--------------------------|----------|---------------------------------------------|
| `event_id`   | `string`                  | ❌       | GitHub event 唯一 ID（字串）                 |
| `event_type` | `string`                  | ❌       | PushEvent / WatchEvent / ...                |
| `actor_id`   | `int64`                   | ✅       | GitHub 使用者 ID                            |
| `actor_login`| `string`                  | ✅       | GitHub username                             |
| `repo_id`    | `int64`                   | ✅       | GitHub repository ID                        |
| `repo_name`  | `string`                  | ❌       | "owner/repo" 格式                           |
| `payload_json`| `string`                 | ✅       | event-specific payload，JSON string         |
| `public`     | `bool`                    | ✅       | 是否為公開 event                            |
| `created_at` | `timestamp(us, tz=UTC)`   | ❌       | GitHub 記錄 event 的時間                    |
| `ingested_at`| `timestamp(us, tz=UTC)`   | ❌       | Consumer 寫入 Parquet 的時間                |

**設計決策：**

- `event_id` 設為 `nullable=False`：它是唯一識別符，沒有就沒辦法做 deduplication
- `payload` 存成 JSON string 而非 nested struct：GitHub 有 30+ event types，每種 payload 結構不同，統一存 string 避免 schema 爆炸；需要用到 payload 細節時用 `json.loads()` 即可
- 加 `ingested_at`：和 `created_at` 相減可以算出 pipeline lag，這是衡量系統健康的重要指標
- 時間欄位用 `timestamp(us, tz=UTC)` 而非 string：讓 DuckDB 能直接做時間運算（`created_at >= NOW() - INTERVAL '60' MINUTE`），不需要 `CAST`

**Partition 策略（v1.0.0）：** 按 ingestion date（`ingested_at`）

---

## v1.1.0 — Event-Time Partitioning（Day 7）

**日期：** 2026-06-27

**Schema 本身沒有變動**。改的是 partition 策略。

**Partition 策略變更：**

| 版本   | Partition by          | Late events 處理   |
|--------|----------------------|-------------------|
| v1.0.0 | ingestion date（今天）| 和正常 events 混在一起 |
| v1.1.0 | event time（created_at date）| 超過 24h → `date=late/` |

**為什麼改？**

v1.0.0 的問題：一個 `created_at=昨天` 的 event 被寫進 `date=今天/`。之後查「昨天的資料」時，DuckDB partition pruning 跳過昨天的資料夾，這筆 event 消失。

v1.1.0 改成按 `created_at` 日期 partition，配合 24 小時 watermark：
- 正常 event → `date=<created_at 日期>/`
- 超過 24h 的 late event → `date=late/`（隔離區）

**向下相容性：**
這個改動不影響已存在的 Parquet 檔案。DuckDB 掃 glob 時，舊的 `date=2026-06-25/`（按 ingestion time 寫的）和新的（按 event time 寫的）檔案會一起回傳。唯一影響是：v1.0.0 期間寫的檔案裡，`created_at` 和 partition date 可能不一致 — 這是已知限制，記錄在 `date=late/` 的處理文件裡。

**相關程式碼：** `src/storage/writer.py` — `_partition_key()` 函式

---

## 未來可能的 Schema 變更（待規劃）

以下是在開發過程中考慮過、但還沒實作的 schema 變更。記錄在這裡方便未來評估。

### 候選 v1.2.0：加 `actor_type` 欄位

**動機：** 區分 human user 和 bot（例如 `dependabot[bot]`、`github-actions[bot]`）。Bot 的 push activity 很高但分析價值低，可能需要過濾。

**方案：**
```python
pa.field("actor_type", pa.string(), nullable=True)
# 值為 "User" 或 "Bot"，從 actor.type 欄位取
```

**向下相容性：** Backward-compatible add（新欄位 nullable）。舊 partition 沒有這個欄位，DuckDB 讀取時會填 `NULL`。

### 候選 v1.3.0：加 `org_login` 欄位

**動機：** GitHub event 有時帶 `org` 欄位（organization），可以用來分析哪些 org 最活躍。

**方案：**
```python
pa.field("org_login", pa.string(), nullable=True)
# 從 event.get("org", {}).get("login") 取
```

---

## 如何執行 Schema Migration

Parquet 檔案一旦寫入就不能修改。Schema migration 有兩種策略：

**策略 A：Forward-only（推薦）**
只做 backward-compatible 的變更（加 nullable 欄位、不改已有欄位）。舊 partition 和新 partition 可以被 DuckDB 同時掃描，缺少新欄位的舊資料以 `NULL` 補。

**策略 B：Backfill**
需要重寫舊 partition（例如改欄位型別）：
1. 把新 schema 寫進 `schema.py`
2. 寫一個一次性的 migration script（讀舊 Parquet → 轉換 → 寫新 Parquet → 刪舊檔）
3. 在這裡記錄 migration 的日期和方法
4. 更新 `docs/schema_changelog.md`

**什麼時候需要 Backfill：**
- 把 nullable 欄位改成 non-nullable
- 改欄位型別（e.g., `string` → `int64`）
- 刪除欄位

**什麼時候不需要（Forward-only 就夠）：**
- 加新的 nullable 欄位
- 加新的 partition
