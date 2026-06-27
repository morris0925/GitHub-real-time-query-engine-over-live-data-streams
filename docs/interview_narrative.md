# StreamLens — Interview Narrative

這份文件是用第一人稱寫的，給面試前準備用。
每個問題都有「一句話版本」（適合快速回答）和「展開版本」（適合深入討論）。

---

## "Tell me about a project you built."

**一句話版本：**
我建了一個叫 StreamLens 的即時資料管道，從 GitHub 的公開 event 串流拉資料，透過 Kafka 傳輸、以 Parquet 格式儲存、用 DuckDB 查詢，最後在終端機顯示即時指標。

**展開版本：**
這個 project 的起點是我想親手踩過「資料工程」的完整路徑 — 不只是會用工具，而是真的理解每個元件為什麼存在、解決什麼問題。所以我選了 GitHub 公開 events 作為資料源（不需要帳號、有真實流量、事件類型多元），然後從零開始建：

- Producer 每 5 秒 poll GitHub API，把 events 推進 Kafka topic
- Consumer 讀 Kafka，過 processor 層做 validate/enrich，批次寫成 date-partitioned Parquet
- DuckDB 直接掃 Parquet 做查詢，回傳結果給 Rich 終端機 dashboard

整個 pipeline 在本機跑，用 Docker Compose 起 Kafka。

---

## "Why Kafka? Couldn't you just write directly to Parquet?"

**一句話版本：**
Kafka 解決了三個問題：rate limit 保護、consumer 當機時不丟資料、以及讓 producer 和 consumer 可以獨立擴展。

**展開版本：**
最直接的做法當然是 producer 直接寫 Parquet，但這帶來幾個問題：

第一是 **rate limit**。GitHub API 未驗證的話是 60 requests/hr，如果 storage 層寫很慢或當掉，直接設計會讓 producer 等或丟資料。Kafka 讓 producer 以自己的速度寫，consumer 以自己的速度讀。

第二是 **replay**。Kafka 保留 topic 裡的 message 有一段時間（預設 7 天）。如果 consumer 當機或有 bug，重啟後可以從上次成功的 offset 繼續，不會丟資料。

第三是 **可擴展性**。現在只有一個 consumer，但如果 event volume 變大，我可以啟多個 consumer（同一個 consumer group），Kafka 自動分配 partition。不需要改任何程式。

這個設計跟 AWS 的 Kinesis → Lambda → S3 是同樣的 pattern，只是用開源工具在本機重現。

---

## "Why Parquet + DuckDB instead of a database like PostgreSQL?"

**一句話版本：**
這是個 analytics workload，不是 transactional workload。Parquet 的 columnar 格式讓 DuckDB 只讀需要的欄位，不需要維護 server，跟生產環境的 S3 + Athena 是同一個 pattern。

**展開版本：**
如果用 PostgreSQL，我需要一個一直在跑的 server、connection pool、schema migration 工具。對這個 use case 來說是 overkill。

Analytics workload 的特性是：write once（events 不會被修改）、read many（dashboard 每 4 秒查一次）、query 通常只碰少數幾個欄位（例如 `event_type`、`repo_name`、`created_at`）。

Parquet 是 columnar format，意思是每個欄位的資料是連續存放的。查 `SELECT event_type, COUNT(*) FROM ...` 時，DuckDB 根本不讀其他欄位的資料，IO 大幅減少。

另外 DuckDB 是 in-process 的，不需要另一個 server。`import duckdb` 就有一個完整的 SQL engine。

最重要的是，這就是 AWS Athena 的做法：S3 存 Parquet，Athena（底層是 Presto）直接掃。我在本機重現的是同一個架構。

---

## "How do you handle late-arriving events?"

**一句話版本：**
用 event time partitioning 加 watermark：events 在 created_at 日期的 partition，超過 24 小時的晚到事件去 date=late/ 隔離。

**展開版本：**
這是我在這個 project 裡遇到的一個真實問題。一開始我的 writer 把所有 event 都寫進「今天」的 partition（按 ingestion time）。但如果 consumer 重啟後重播 Kafka，或者 GitHub API 延遲回傳舊 events，這些 event 的 `created_at` 是昨天，卻被寫進今天的 partition。之後查「昨天的資料」時，DuckDB 做 partition pruning 跳過昨天的資料夾，這些 event 就查不到了。

修法是改成 **event-time partitioning**：每個 event 按照自己的 `created_at` 日期決定去哪個 partition。

但這帶來一個新問題：如果永遠接受 late events，partition 就永遠不會「關閉」——今天寫資料、三個月後還有 late event 進來，你的 compaction job 要怎麼判斷什麼時候可以合併？

所以加了 **watermark**：超過 24 小時的 event 不進入它「應該」在的 partition，而是去 `date=late/`。這個 watermark 讓每個 date partition 在超過 24 小時之後就封起來，compaction 可以安全地處理它。

這和 Apache Flink 的 watermark 機制、Spark Structured Streaming 的 `withWatermark()` 是同樣的概念。晚到的 event 用 side output 處理（我的 `date=late/` 就是 side output）。

---

## "What's the small file problem and how did you solve it?"

**一句話版本：**
Consumer 每 30 秒寫一個 Parquet 檔，一天下來 2880 個小檔，DuckDB 每次查詢要打開每個檔的 metadata，syscall overhead 遠超過實際讀資料。我寫了 compaction script 每天把同一個 partition 的所有小檔合成一個。

**展開版本：**
小檔問題在任何需要頻繁寫入的儲存系統都會出現。我的 consumer 每 30 秒 flush 一次，24 小時下來一個 partition 有 2,880 個檔案。

問題在於：DuckDB 執行 `SELECT COUNT(*) FROM read_parquet('data/events/**/*.parquet')` 時，它需要對每個 Parquet 檔讀取 file footer 的 metadata（schema、row group stats 等），然後才知道要不要讀這個檔的實際資料。2,880 個檔就是 2,880 次 `open()` syscall + metadata 讀取。這個 overhead 在大資料集上很明顯。

解法是 compaction：用 PyArrow 讀取同一個 partition 下所有小檔，`pa.concat_tables()` 合在一起，寫成一個大檔，然後刪掉原始小檔。合完之後那個 partition 只有一個檔，一次 open 搞定。

關鍵細節：**先寫新檔，成功後才刪舊檔**。如果反過來（先刪再寫），過程中當機就永久丟資料。這是典型的「write-then-delete」pattern。

這就是 Delta Lake 的 `OPTIMIZE` command 和 Apache Iceberg 的 `rewrite_data_files()` 在做的事，只是規模更大。

---

## "How do you ensure data isn't lost if the consumer crashes?"

**一句話版本：**
Kafka offset 只在 Parquet 寫入成功後才 commit，at-least-once 語意保證不丟資料，可能重複但不會遺漏。

**展開版本：**
我用的是 `enable_auto_commit=False`，手動控制 offset commit 的時機：

```python
paths = write_batch(batch, data_dir=DATA_DIR)  # 1. 先寫 Parquet
consumer.commit()                               # 2. 再 commit offset
```

如果在步驟 1 和 2 之間當機，Kafka 保留舊的 offset，重啟後 consumer 從那個 offset 重新讀，這批 message 會被重新處理。結果是這批 event 可能被寫兩次（at-least-once），但不會消失（不是 at-most-once）。

重複寫入的問題：GitHub event 有唯一的 `event_id`。如果需要 exactly-once，可以在查詢時加 `DISTINCT ON (event_id)`，或在寫入前做 deduplication。這個 project 目前沒做，因為對 analytics dashboard 來說，偶爾一兩筆重複影響不大（計數誤差 < 0.01%）。

---

## "What would you do differently if you rebuilt this?"

**主要三點：**

**1. 用 `asyncio` 重寫 producer**
目前用同步的 `requests` + `time.sleep()`，每次 poll 都阻塞 5 秒。如果改用 `aiohttp` + `asyncio`，可以在等 GitHub API 回應的時候同時處理其他事情，對多個 topic 的 producer 特別有用。

**2. Schema registry**
目前 schema 就是一個 Python 檔案 (`schema.py`)。如果有多個 consumer 或多個語言的 client，就需要一個中央化的 schema registry（例如 Confluent Schema Registry）來確保所有人用同一個版本。Avro 或 Protobuf 格式也有自帶 schema 版本控制。

**3. Metrics / observability**
目前 lag metric 只在 dashboard 顯示，沒有 alerting。真實系統應該把 lag、error rate、事件數量等 metrics 推到 Prometheus，設定 Grafana 的 alert rule，lag 超過閾值時發 PagerDuty。

---

## "What did you learn building this?"

最大的收穫是真實感受到「為什麼」這些分散式系統的設計是這樣的：

- **Kafka 的 consumer group** 讓我明白 partition 和 consumer 之間的關係不是「一對一」，而是「partition 不超過 consumer 數量」才能完整利用並行。
- **Parquet 的 footer** — 我以前以為 Parquet 只是一種「比 CSV 更快的格式」，做了這個 project 才理解 file footer 儲存了 row group stats（min/max per column），讓 query engine 在讀資料之前就知道哪些 row group 要跳過（predicate pushdown）。
- **Watermark 的本質** 是在「正確性」和「延遲」之間的取捨。watermark 越長，late event 被正確放進對的 partition 的機率越高，但每個 partition 要等更久才能「封起來」。沒有絕對正確的答案，是業務需求決定的。
