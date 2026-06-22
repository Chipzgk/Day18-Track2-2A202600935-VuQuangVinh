# Bonus Challenge — LLM Observability Lakehouse at 1B req/day

**Người thực hiện:** Vũ Quang Vinh — 2A202600935  
**Chủ đề:** LLM Observability Platform at 1 Billion Requests/Day  
**Stack tham chiếu:** Delta Lake + Apache Spark + MinIO (y hệt lab nhưng scale production)

---

## 1. Bài toán

Một platform AI phục vụ 1B API requests/day cần hệ thống observability đáp ứng:

- **Latency SLA monitoring:** p50/p95/p99 theo model, region, user tier — cập nhật mỗi 5 phút
- **Cost attribution:** chi phí token theo team, project, model — billing chính xác đến USD
- **Error rate alerting:** phát hiện anomaly trong vòng < 2 phút
- **Audit lineage:** mỗi request phải truy vết được từ Gold về raw log gốc (compliance)
- **Data retention:** raw logs giữ 90 ngày, aggregates giữ 2 năm

**Quy mô số liệu:**
- 1B req/day = ~11,600 req/s peak
- Mỗi log record ~2 KB (JSON) → ~2 TB raw data/ngày
- 30 ngày = ~60 TB Bronze

---

## 2. Kiến trúc Medallion

### Bronze — Raw Ingestion
**Lưu trữ:** Delta Lake on object storage (S3/MinIO)  
**Partitioning:** `date` + `hour` (partition nhỏ để streaming append nhanh)  
**Format:** raw JSON giữ nguyên, không transform  
**Retention:** 90 ngày → auto-expire bằng Delta `VACUUM` + lifecycle policy  
**Ingest:** Kafka → Spark Structured Streaming, micro-batch mỗi 30 giây  

**Quyết định bị loại bỏ:** Parquet thuần thay vì Delta  
→ Lý do loại: không có ACID, concurrent writer từ nhiều Kafka consumer gây corrupt

### Silver — Dedup + Parse + Enrich
**Transform:**
- `dropDuplicates(["request_id"])` — at-least-once Kafka delivery sinh duplicate
- Parse JSON `raw_json` → typed columns (model, latency_ms, token counts, status)
- Enrich: join với bảng `user_tier` để gắn `plan` (free/pro/enterprise)
- Filter: loại bỏ internal health-check requests (`user_id = "system"`)

**Partitioning:** `date` + `model`  
**Z-ORDER:** `user_id, model` — tối ưu filter dashboard theo user hoặc model  
**SLA:** Silver lag sau Bronze < 5 phút

**Quyết định bị loại bỏ:** Làm thẳng từ Bronze lên Gold  
→ Lý do loại: lab NB4 cho thấy 50K duplicates/1M rows (5%). Ở 1B req/day = 50M duplicate rows/ngày nếu không dedup — số liệu billing sẽ sai ~5%

### Gold — Aggregated Metrics
**Hai loại Gold table:**

**Gold_realtime** — cập nhật mỗi 5 phút:
```
GROUP BY window(5min), model, region
METRICS: p50/p95/p99 latency, error_rate, request_count
```
Dùng cho alerting dashboard.

**Gold_billing** — cập nhật mỗi giờ:
```
GROUP BY date, hour, team_id, project_id, model
METRICS: total_prompt_tokens, total_completion_tokens, cost_usd
```
Dùng cho billing và FinOps.

**Z-ORDER Gold_realtime:** `model, region` — query dashboard lọc theo model + region  
**Z-ORDER Gold_billing:** `team_id, date` — query billing lọc theo team trong khoảng ngày

---

## 3. Các quyết định thiết kế quan trọng

### 3.1 Tại sao không dùng Lambda Architecture (batch + speed layer riêng)?
Lambda phổ biến cho real-time nhưng sinh ra **hai code path** — một cho batch, một cho streaming — phải maintain song song. Khi logic tính cost thay đổi (giá token model mới), phải sửa cả hai chỗ. Delta Lake + Spark Structured Streaming cho phép **unified pipeline**: cùng một PySpark code chạy được cả batch lẫn streaming. Giảm technical debt đáng kể.

### 3.2 Tại sao partition Bronze theo `date/hour` thay vì chỉ `date`?
1B req/day = ~42M req/hour. Nếu partition chỉ theo `date`, mỗi partition ~2 TB — quá lớn cho một Spark task đọc. Partition theo `hour` → mỗi partition ~83 GB, phù hợp với executor memory 16–32 GB. Đồng thời Silver job chạy mỗi 5 phút chỉ cần đọc partition của giờ hiện tại.

### 3.3 Tại sao giữ raw JSON ở Bronze thay vì parse ngay?
Schema của LLM API response thay đổi thường xuyên (model mới, field mới). Nếu parse tại Bronze, mỗi lần schema thay đổi phải backfill toàn bộ Bronze. Giữ raw JSON → chỉ cần update logic parse ở Silver → re-run Silver từ Bronze không mất data gốc. Đây là nguyên tắc **schema-on-read** của Lakehouse.

### 3.4 Tại sao dùng Delta MERGE cho Silver thay vì overwrite?
Mỗi 5 phút Silver job chạy, chỉ có ~4M records mới (5 phút × 800K req/phút). Nếu overwrite toàn bộ Silver → scan + rewrite 60 TB mỗi 5 phút = không khả thi. MERGE chỉ touch các files chứa records bị ảnh hưởng → hiệu quả hơn nhiều.

---

## 4. ACID và Time Travel trong bối cảnh production

**Scenario thực tế:** Model pricing thay đổi (GPT-5 ra mắt, giá token giảm 50%). Cần recalculate `cost_usd` cho toàn bộ tháng trước.

**Với Delta Time Travel:**
```python
# Đọc Silver tại thời điểm trước khi pricing thay đổi
silver_old = spark.read.format("delta") \
    .option("versionAsOf", 142) \
    .load(SILVER)
# Recalculate với bảng giá mới
gold_corrected = silver_old.join(new_pricing, "model") \
    .groupBy("date", "model") \
    .agg(...)
# Ghi đè Gold với số liệu đã correct
gold_corrected.write.format("delta").mode("overwrite").save(GOLD_BILLING)
```
Không cần re-ingest từ raw. ACID đảm bảo quá trình recalculate không ảnh hưởng dashboard đang chạy.

---

## 5. FinOps và Cost Control

| Tầng | Storage class | Chi phí ước tính |
|---|---|---|
| Bronze (0–30 ngày) | S3 Standard | ~$46/TB/tháng × 60 TB = $2,760/tháng |
| Bronze (30–90 ngày) | S3 Infrequent Access | ~$12.5/TB/tháng × 120 TB = $1,500/tháng |
| Silver | S3 Standard | ~$46/TB/tháng × 5 TB = $230/tháng |
| Gold | S3 Standard | ~$46/TB/tháng × 0.1 TB = $4.6/tháng |
| **Total** | | **~$4,500/tháng** |

**Tối ưu thêm:**
- `OPTIMIZE` Bronze mỗi đêm → giảm số files → giảm S3 LIST request cost
- `VACUUM RETAIN 90 DAYS` → xóa old versions, tiết kiệm ~30% storage
- Compress Bronze bằng ZSTD thay vì Snappy → giảm ~20% size

---

## 6. Lineage và Compliance

Mỗi Gold row có thể trace về:
```
Gold (billing) → Silver (request_id) → Bronze (raw_json) → Kafka offset
```

Delta transaction log (`_delta_log/`) lưu toàn bộ lịch sử write operations → đủ để audit khi có tranh chấp billing với khách hàng enterprise.

---

## 7. Điều tôi sẽ làm khác nếu làm lại

Trong lab, Bronze generator tạo data flat JSON. Ở production thực tế, tôi sẽ thêm **schema registry** (Confluent Schema Registry hoặc AWS Glue) để enforce schema tại Kafka producer — phát hiện bad schema trước khi vào Bronze, không phải sau. Đây là anti-pattern "schema chaos" mà Day 18 đề cập — fix sớm rẻ hơn fix muộn rất nhiều.