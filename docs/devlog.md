# StreamLens — Dev Log

每天做了什麼、為什麼這樣做、遇到什麼問題。給未來的自己看。

---

## Day 1 — 環境建置 & Docker

**目標：** 讓 Kafka 在本機跑起來。

**做了什麼：**
- 建立整個 `streamlens/` 專案資料夾結構
- 寫 `docker-compose.yml`：兩個服務 — Zookeeper（Kafka 的依賴）和 Kafka broker
- Kafka container 加了 health check，確保 Kafka 真的 ready 之後才算啟動成功
- 用 named volume 讓 Kafka 的資料在 `docker-compose down` 後不會消失
- 驗證：`docker-compose up -d` → 兩個 container 都 healthy → `docker-compose down`

**學到什麼：**
- Kafka 需要 Zookeeper 來管理 broker 的 metadata（誰是 leader、哪些 partition 在哪裡）
- Docker health check 讓 `depends_on` 能等 Kafka 真的好了才啟動其他服務
- Named volume vs bind mount：named volume 由 Docker 管理，比較乾淨

**關鍵設定（docker-compose.yml）：**
- `KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092` — 告訴 producer/consumer 要連哪裡
- `KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1` — 單節點開發用，不需要 replication

---

## Day 2 — 認識 GitHub Events API

**目標：** 用 Python 打 GitHub API，搞清楚資料長什麼樣子。

**做了什麼：**
- 寫一個簡單腳本直接呼叫 `https://api.github.com/events`
- 探索 response 結構：每個 event 有 `id`, `type`, `actor`, `repo`, `payload`, `created_at`
- 發現 GitHub 的 ETag 機制：同樣的資料第二次呼叫會回傳 304 Not Modified，省頻寬
- 實作 ETag 快取：把上次的 ETag 存起來，下次帶在 `If-None-Match` header 裡

**學到什麼：**
- GitHub Events API 是 public 的，不需要 token（但有 rate limit：60 req/hr）
- ETag 是 HTTP 的條件式請求機制 — server 說「這份資料的版本號是 XYZ」，client 下次帶著版本號問「還是 XYZ 嗎？」，沒變就回 304 + 空 body
- GitHub event types：最常見的是 `PushEvent`、`WatchEvent`（star）、`CreateEvent`（新 branch/repo）

**資料結構（簡化版）：**
```json
{
  "id": "12345678901",
  "type": "PushEvent",
  "actor": { "id": 1, "login": "alice" },
  "repo": { "id": 99, "name": "alice/myrepo" },
  "payload": { "commits": [...] },
  "public": true,
  "created_at": "2024-01-15T10:30:00Z"
}
```

---

## Day 3 — GitHub Events → Kafka Producer

**目標：** 把 API 抓到的 events 推進 Kafka topic。

**做了什麼：**
- 寫 `src/producer.py`：每 5 秒 poll GitHub API，把每個 event 送到 `github-events` topic
- 用 `kafka-python` 的 `KafkaProducer`，設定 `value_serializer` 自動把 dict 轉成 JSON bytes
- Message key = `event["id"]`（GitHub 的 event ID），讓 Kafka 把相同 key 的訊息路由到同一個 partition
- 錯誤處理：`NoBrokersAvailable` 時直接印出提示（Docker 沒開）
- 設定：broker address 和 topic name 都 hardcode 在這個版本（TODO: 之後換 env var）

**學到什麼：**
- Kafka 的核心概念：Producer → Topic → Consumer
- Topic 就是一個有名字的 log，可以有多個 partition
- `producer.flush()` 確保 buffer 裡的訊息都真的送出去（不然程式結束時可能丟訊息）
- Serialization：Kafka 只存 bytes，所以 Python dict 要先 `json.dumps()` 再 `.encode("utf-8")`

**為什麼每個 event 是獨立的訊息：**
GitHub API 一次回傳最多 30 個 events 的 array，但我們把每個 event 拆成獨立的 Kafka message。這樣 consumer 可以獨立處理每一個，也讓 partition 更均勻。

---

## Day 4 — Storage 層 + Kafka Consumer

**目標：** 把 Kafka 裡的 events 寫成 Parquet 檔案，並建立 DuckDB 查詢層。

**做了什麼：**

### `src/storage/schema.py`
- 定義 `GITHUB_EVENT_SCHEMA`：一個 PyArrow schema，10 個欄位
- 把原本 nested 的 JSON（`actor.login`, `repo.name`）壓平成獨立欄位
- 複雜的 `payload` 存成 JSON string（不然要為每種 event type 寫不同 schema）
- 多了一個 `ingested_at` 欄位（consumer 寫入的時間），可以和 `created_at` 相減算 pipeline lag

### `src/storage/writer.py`
- `flatten_event()`：把一個 raw GitHub event dict 轉成符合 schema 的 flat dict
- `write_batch()`：一批 events → PyArrow Table → Parquet 檔
- Date partition：`data/events/date=2026-06-25/part-<uuid>.parquet`
- Snappy 壓縮（快、壓縮率中等，適合 analytics workload）
- UUID 檔名確保並行寫入不會互相覆蓋

### `src/storage/reader.py`
- DuckDB 單例連線（module-level singleton）— 一個 connection 用到底，不重複開
- `get_recent_events()` — 最新 N 筆，給 dashboard event feed
- `get_event_counts_by_type()` — 按 type 分組計數，給 dashboard stats panel
- `get_top_repos()` — 最活躍的 repo，給 dashboard top repos panel
- `get_total_event_count()` — 總數，給 status bar
- 查詢用 DuckDB glob：`read_parquet('data/events/**/*.parquet')` 一次掃所有 partition

### `src/storage/queries/` — 三個 SQL 檔
- `event_counts_by_type.sql`
- `top_repos.sql`
- `recent_events.sql`
- 超過 5 行的 SQL 獨立成 .sql 檔，用 `Path.read_text()` 載入

### `src/consumer.py`
- `enable_auto_commit=False` — 手動 commit offset，只在 Parquet 寫成功後才 commit
- Micro-batch：累積 100 筆 or 30 秒，擇一觸發 flush
- 錯誤處理：`StorageWriteError` → 不 commit（讓 Kafka 重送），其他 Exception → raise
- `auto_offset_reset="earliest"` — 第一次啟動從頭讀

### `tests/test_storage.py`
- 23 個 unit test，全部通過
- 涵蓋：schema 驗證、flatten_event 邊界情況（壞 timestamp、缺 actor/payload）、write_batch round-trip、reader 所有函式
- 用 pytest `tmp_path` fixture，測試不碰真實 `data/` 目錄

**關鍵設計決策：**
- 為什麼 date partition？DuckDB 可以做 partition pruning — 只查今天的資料時，它完全不看其他資料夾
- 為什麼 `ingested_at`？可以量測 pipeline lag（= `ingested_at - created_at`）
- 為什麼 payload 存成 JSON string 而不是 nested struct？GitHub 有 30+ 種 event type，每種 payload 結構不同，統一存 string 最簡單，要用時再 `json.loads()`

---

## Day 5 — Rich 終端機 Dashboard

**目標：** 用 Rich 把 DuckDB 查詢結果變成一個會自動重整的終端機畫面。

**做了什麼：**

### `src/dashboard/dashboard.py`
四個面板的 Layout：
```
┌─ ● StreamLens  │  topic: github-events  │  ↻ every 4s ─────────────┐
├──── Live Event Feed (last 20) ───┬─── Event Types (last 60 min) ────┤
│  PushEvent  alice  torvalds/...  │  PushEvent    3  ██████████████  │
│  WatchEvent diana  django/...    │  WatchEvent   2  ████████        │
│  ForkEvent  bob    python/...    │  ForkEvent    1  ████            │
│                                  ├─── Top Repositories ─────────────┤
│                                  │  1  torvalds/linux   3 events    │
│                                  │  2  django/django    2 events    │
╰──────────────────────────────────┴──────────────────────────────────╯
╭─ Total events: 7  │  Updated: 14:17:49 UTC  │  Ctrl+C to exit ──────╮
```

**細節：**
- `rich.live.Live(screen=True)` — 全螢幕接管，退出後還原終端機
- `refresh_per_second=4`，但實際資料更新是 sleep(4) 控制的
- 每個 event type 有自己的顏色（`PushEvent` = 綠、`WatchEvent` = 黃、`PullRequestEvent` = 藍...）
- Stats panel 有 ASCII bar chart（`█` 字元），讓數字大小一眼看出來
- 空資料時顯示 "no data yet"，不會 crash
- Header 的 ● 點：有資料時綠色，沒資料時灰色

**Rich Layout 結構：**
```
root (vertical)
├── header   (size=3, 固定高度)
├── body     (填滿剩餘空間)
│   ├── left   (ratio=55, event feed)
│   └── right  (ratio=45)
│       ├── counts (ratio=45, event type stats)
│       └── repos  (ratio=55, top repos)
└── footer   (size=3, 固定高度)
```

**如何跑完整 pipeline：**
```bash
# Terminal 1
docker-compose up -d

# Terminal 2
PYTHONPATH=src python src/producer.py

# Terminal 3
PYTHONPATH=src python src/consumer.py

# Terminal 4
PYTHONPATH=src python src/dashboard/dashboard.py
```

**同時也建立了：**
- `src/dashboard/__init__.py`
- `src/storage/__init__.py`
- `requirements.txt`（所有依賴版本固定）

---

---

## Day 6 — Refactor + Compaction + Lag 監控

**目標：** 補上三個重要缺口：producer 的 hardcode、生產環境必備的 compaction、以及可以量化 pipeline 健康度的 lag 指標。

---

### 1. Refactor producer.py — env vars + structlog

**改了什麼：**
- 把 `KAFKA_BROKER = "localhost:9092"` 等三個 hardcode 常數全部換成 `os.getenv()` + `python-dotenv`
- 新增支援 `GITHUB_TOKEN` 環境變數（有 token 的話 rate limit 從 60 req/hr 升到 5000 req/hr）
- 把所有 `print()` 換成 `structlog`，輸出格式統一（timestamp + log level + key-value pairs）

**為什麼這樣改：**
CLAUDE.md 本來就標記這是 TODO。更重要的是：如果你把這個 producer 包成 Docker image 部署，hardcode 的 broker address 直接讓它變成「只能在本機跑」的程式。env var 讓同一份 image 可以指向不同環境的 Kafka。

---

### 2. storage/compaction.py — 小檔案合併

**問題背景（Small File Problem）：**

Consumer 每 30 秒 flush 一次，一天下來就是 2,880 個小 Parquet 檔案。每次 DuckDB 執行 `SELECT COUNT(*) FROM read_parquet('data/events/**/*.parquet')` 時，它要打開、讀取 footer metadata、再關閉 2,880 個檔。這些 syscall 的開銷遠超過實際讀資料的時間。

**解法：**

```
Before:
  date=2026-06-25/
    part-a1b2.parquet   (50 rows)
    part-c3d4.parquet   (50 rows)
    part-e5f6.parquet   (50 rows)
    ... × 2880

After compact_partition():
  date=2026-06-25/
    compacted-uuid.parquet  (144,000 rows)
```

**關鍵細節：**
- 先寫新檔案，成功後才刪舊檔案 — 確保資料不會在中途遺失
- `min_files=2`：只有超過一個檔才值得合併（一個檔 compact 沒有意義）
- `compacted-` 前綴：讓合併後的檔和原始 `part-` 檔一眼就分得清楚
- `compact_all()` 掃所有 date partition，通常排 cron job 每天凌晨跑一次

**面試角度：**
這就是 Delta Lake 的 `OPTIMIZE`、Apache Iceberg 的 `rewrite_data_files`、Spark 的 `coalesce()` 在做的事。同樣的問題在 S3 + Athena 架構下每次 query 的費用會隨檔案數線性增加（每個 `GetObject` API call 收費）。

---

### 3. Lag 監控 — reader.py + dashboard

**新增 `get_avg_lag()`：**
- 計算 `AVG(ingested_at - created_at)` — 也就是從 GitHub 記錄這個 event 到我們存進 Parquet 的平均延遲
- SQL 放在 `storage/queries/avg_lag.sql`（符合「超過 5 行放 .sql 檔」的規則）
- 同時回傳 min、max、sample_size

**Dashboard status bar 更新：**
```
Before:  Total events: 1,234  │  Last updated: 14:17:36 UTC  │  Ctrl+C to exit
After:   Total: 1,234 events  │  Lag: 28.4s avg (n=847)  │  Updated: ...
```

Lag 顏色：
- 綠色：< 30s（正常，等於 flush interval）
- 黃色：30–60s（稍慢，可能 Kafka 積壓）
- 紅色：> 60s（有問題）

**為什麼 `ingested_at - created_at` 有意義：**
- `created_at` = GitHub API 記錄的時間
- `ingested_at` = 我們 consumer 寫入 Parquet 的時間
- 兩者差值 = 「資料從來源到我們倉儲的延遲」= 業界說的 end-to-end pipeline latency
- 如果 lag 突然升高，可能是：GitHub API 變慢、Kafka 積壓、consumer crash 過後重啟從頭讀

---

**Tests：**
- `tests/test_compaction.py` — 15 個 test
  - `compact_partition`：output 只剩一個檔、名稱 `compacted-`、row count 不變、原始檔刪除、一個檔跳過
  - `compact_all`：空目錄、多 partition
  - Lag metric：無資料 → None、計算準確度（±5s 容差）、回傳欄位完整
- 全部 38 tests 通過（23 舊 + 15 新）

---

**今天之後的完整檔案結構：**
```
src/
├── producer.py          ← refactored: env vars + structlog
├── consumer.py
├── storage/
│   ├── schema.py
│   ├── writer.py
│   ├── reader.py        ← 新增 get_avg_lag()
│   ├── compaction.py    ← 新增
│   └── queries/
│       ├── event_counts_by_type.sql
│       ├── top_repos.sql
│       ├── recent_events.sql
│       └── avg_lag.sql  ← 新增
└── dashboard/
    └── dashboard.py     ← status bar 加上 lag 顯示
tests/
├── test_storage.py      (23 tests)
└── test_compaction.py   (15 tests)  ← 新增
```

---

---

## Day 7 — Late-Arriving Events + README

**目標：** 解決 writer.py 的 partition 設計缺陷，並把整個 portfolio 包裝成一份讓面試官看得懂的 README。

---

### 1. writer.py 大改 — Event-Time Partitioning + Watermark

**原本的問題：**

Day 4 寫的 writer.py 把所有 event 都寫進「今天」的 partition：

```python
today_str = ingested_at.strftime("%Y-%m-%d")   # ← 以處理時間為準
partition_dir = data_dir / f"date={today_str}"
```

這表示：
- 一個 `created_at=昨天` 的 event 會被寫進 `date=今天/`
- 之後查「昨天的資料」時，DuckDB 直接跳過昨天的 partition，這筆資料就消失了

**修法 — Event-Time Partitioning with Watermark：**

```
事件年齡 ≤ 24 小時  →  寫進 date=<created_at 的日期>/
事件年齡 > 24 小時  →  寫進 date=late/（隔離區）
```

核心函式 `_partition_key(created_at, ingested_at, threshold_hours)` 決定每個 event 該去哪個 partition。

**API 也改了：**
`write_batch()` 現在回傳 `list[Path]` 而不是單一 `Path`，因為同一批 message 可能橫跨多個日期（例如 consumer 重啟後，重播昨天和今天的 event）。

**為什麼叫 Watermark：**
"Watermark" 是 streaming 系統的術語，代表「我們認為這個時間點之前的所有 event 都到齊了」。超過 watermark 的 late event 用 side output 處理（我們叫 `date=late/`）。Apache Flink、Spark Structured Streaming、Apache Beam 都用這個機制。

**Late partition 的用途：**
`date=late/` 不是垃圾桶，是隔離區。未來可以：
- 檢視裡面有什麼（反常現象？API 延遲問題？）
- 用 `compaction.py` 合併後轉移到正確的 date partition（人工修正）

**Tests 新增 `TestLateArrivingEvents`（5 個）：**
- 近期 event → date partition
- 超過 watermark → date=late/
- 混合批次 → 分成兩個檔案
- late partition 的 row count 正確
- 剛好在 watermark 內的 event 不算 late

---

### 2. README.md — Portfolio 門面

寫了一份完整的 README，包含：
- ASCII architecture diagram（面試官打開 GitHub 第一眼看到的東西）
- Quick Start（3 個 terminal 跑起完整 pipeline）
- 所有 env var 的 table
- 完整 project structure
- **Engineering Design Decisions** — 這是核心：解釋為什麼做這些技術選擇，不只是「我用了 Kafka」，而是「為什麼用 Kafka」

Design decisions 涵蓋的四個問題（面試常問）：
1. **為什麼用 Kafka？** → 解耦 producer/consumer、rate limit 保護、replay buffer
2. **為什麼 Parquet + DuckDB 而不是資料庫？** → 類比 S3 + Athena，columnar storage 的 analytics 優勢
3. **為什麼 partition by event time 而不是 ingestion time？** → 正確性 vs 複雜度，watermark 的取捨
4. **為什麼要 compact？** → Small file problem，2,880 files → 1 file

---

**Tests：** 43 passed（+5 新的 late-event tests）

**今天改動的檔案：**
```
src/storage/writer.py    ← 重寫：event-time partitioning + watermark
src/consumer.py          ← 小改：適配 list[Path] return type
tests/test_storage.py    ← 更新 write_batch tests + 新增 TestLateArrivingEvents
README.md                ← 完整重寫
```

---

---

## Day 8 — processors/ 層 + .env.example

**目標：** 建立 CLAUDE.md 計劃中最後一個還沒蓋的架構塊：event-type-specific processor 層。順便補 `.env.example` 讓初次使用的人不用猜。

---

### 1. processors/ 層

**問題背景：**

GitHub Events API 有 30+ 種 event type，每種的 `payload` 結構完全不同：
- `PushEvent` payload 有 `commits` array 和 `ref`
- `WatchEvent` payload 只有 `action: "started"`
- `PullRequestEvent` payload 有完整的 PR 物件

如果把所有 event type 的 validate/enrich 邏輯全塞進 consumer.py，就會變成一大坨 `if event_type == "PushEvent": ... elif event_type == "WatchEvent": ...`，很難維護、很難測試。

**解法：Strategy Pattern**

每個 event type 一個 processor 類別，都繼承自同一個 abstract base class：

```
processors/
├── __init__.py          # Registry + get_processor()
├── base.py              # EventProcessor ABC, ValidationError, ProcessorResult
├── push_event.py        # PushEventProcessor
├── watch_event.py       # WatchEventProcessor
├── pull_request_event.py# PullRequestEventProcessor
└── default.py           # DefaultProcessor (fallback for 未知 type)
```

**每個 Processor 做兩件事：**
1. **Validate** — 確認這個 event type 必要的欄位都在，不在就 `raise ValidationError`
2. **Enrich** — 從 payload 提取有用的 metrics（不存入 Parquet，只用於 logging/monitoring）

**ProcessorResult dataclass：**
```python
@dataclass
class ProcessorResult:
    event:   dict   # original event (可能加了 enrichment)
    metrics: dict   # extracted metrics (commit_count, branch, is_merged...)
    skipped: bool   # True 的話 consumer 跳過這筆不存
```

**Registry + Singleton：**
`get_processor("PushEvent")` 會回傳 cached `PushEventProcessor` instance。未知的 type 回傳 `DefaultProcessor`（pass-through，不丟資料）。新增 processor 只需要：
1. 寫新的 processor class
2. 加進 `REGISTRY` dict
3. 寫測試
→ consumer.py 完全不需要改

**PushEventProcessor 提取的 metrics 範例：**
```python
{
    "commit_count":      3,
    "branch":            "main",         # stripped "refs/heads/"
    "is_default_branch": True,
    "distinct_size":     3,
}
```

**PullRequestEventProcessor 的 is_merged 邏輯：**
GitHub 沒有單獨的 "MergedEvent"。判斷一個 PR 是被 merge 還是被 close 的方式：
```python
is_merged = (action == "closed") and bool(pr.get("merged"))
```

**consumer.py 的改動：**
在 poll loop 裡加了 processor 層：
```python
result = get_processor(event["type"]).process(raw_event)
# ValidationError → log + skip（不加進 batch）
# result.skipped → 同樣略過
# 成功 → batch.append(result.event)
```
offset commit 邏輯不變：write_batch 成功後才 commit。

---

### 2. .env.example

所有 env var 都列出來，有預設值和簡短說明，包含 `GITHUB_TOKEN` 的申請連結。使用者 `cp .env.example .env` 就能直接跑。

---

**Tests：** `tests/test_processors.py` — 32 個新 tests，涵蓋 Registry、每個 processor 的 valid/invalid 路徑、ValidationError 結構。

全套 **75 passed**（43 舊 + 32 新）。

---

**今天之後的完整架構（CLAUDE.md 計劃 100% 完成）：**
```
src/
├── producer.py
├── consumer.py              ← 整合 processors 層
├── processors/              ← 新增（完成 CLAUDE.md 計劃）
│   ├── __init__.py
│   ├── base.py
│   ├── push_event.py
│   ├── watch_event.py
│   ├── pull_request_event.py
│   └── default.py
├── storage/
│   ├── schema.py
│   ├── writer.py
│   ├── reader.py
│   ├── compaction.py
│   └── queries/
└── dashboard/
    └── dashboard.py
tests/
├── test_storage.py     (43 tests)
├── test_compaction.py  (15 tests)
└── test_processors.py  (32 tests)  ← 新增
```

---

---

## Day 9 — Interview Narrative + Schema Changelog + Git

**目標：** 把 portfolio 收尾 — 用第一人稱寫面試準備文件、補上 schema 演進記錄、把全部程式碼 push 上 GitHub。

---

### 1. docs/interview_narrative.md

完整的面試準備文件，每個問題有「一句話版本」（15 秒電梯答案）和「展開版本」（3–5 分鐘深入討論）。

**涵蓋的問題：**
- "Tell me about a project you built." — pipeline 整體介紹
- "Why Kafka?" — 解耦、rate limit 保護、replay、可擴展性
- "Why Parquet + DuckDB?" — analytics vs transactional，columnar 格式，S3+Athena 類比
- "How do you handle late-arriving events?" — event-time partitioning + watermark + side output
- "What's the small file problem?" — syscall overhead + compaction 解法，類比 Delta Lake OPTIMIZE
- "How do you ensure data isn't lost?" — at-least-once，offset commit ordering，deduplication 策略
- "What would you do differently?" — asyncio producer、schema registry、Prometheus metrics
- "What did you learn?" — Kafka partition 原理、Parquet footer/predicate pushdown、watermark 的本質是 tradeoff

---

### 2. docs/schema_changelog.md

記錄了：
- **v1.0.0**（Day 4）：初始 10 欄位 schema，加了 `ingested_at` 用於 lag 計算，`payload` 存 JSON string 的原因
- **v1.1.0**（Day 7）：Schema 本身沒變，但 partition 策略改為 event-time + watermark — 記錄在 changelog 是因為它影響了資料的邏輯組織
- **未來候選變更**：`actor_type`（人 vs bot）、`org_login`（組織）
- **Migration 策略**：forward-only（加 nullable 欄位）vs backfill（改型別）

---

### 3. Git

Repo 已存在（`morris0925/GitHub-real-time-query-engine-over-live-data-streams`），之前有 `.git/index.lock` 衝突（被本機 editor 鎖住），已提供手動解鎖指令。

Commit message 涵蓋 Day 4–9 全部工作，75 tests passing。

---

**Day 9 後的完整 docs/ 結構：**
```
docs/
├── devlog.md               ← 每天工程日誌（本檔）
├── interview_narrative.md  ← 面試問答準備
└── schema_changelog.md     ← Schema 演進記錄
```

---

_（完成）_
