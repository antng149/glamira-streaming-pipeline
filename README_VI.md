# Nền Tảng Dữ Liệu Streaming với Airflow

Pipeline dữ liệu end-to-end xử lý các sự kiện product-view theo thời gian thực từ Kafka, xử lý bằng Spark Structured Streaming trên YARN, tải dữ liệu phân tích vào star schema PostgreSQL và giám sát toàn bộ hệ thống bằng Airflow DAGs và cảnh báo Telegram.

---

## Kiến Trúc

```
Kafka từ xa (SASL_PLAINTEXT)
        │
        ▼
Spark Structured Streaming (YARN cluster mode)
   driver + executors chạy bên trong các Hadoop nodemanager container
        │
        ▼
PostgreSQL Star Schema (fact_product_view + 6 dims)
        │
        ▼
Airflow DAGs (monitoring, quality, archival)
        │
        ▼
Telegram Alerts (thành công + thất bại)
```

---

## Công Nghệ Sử Dụng

| Thành phần | Công nghệ |
|-----------|-----------|
| Điều phối | Apache Airflow 2.10.4 (CeleryExecutor) |
| Xử lý streaming | Apache Spark 3.5.1 + Structured Streaming |
| Cluster manager | Hadoop YARN 3.3.6 |
| Message broker | Apache Kafka (SASL_PLAINTEXT, remote) |
| Kho dữ liệu | PostgreSQL 13 |
| Container hóa | Docker Compose |
| Cảnh báo | Telegram Bot API |
| Python deps cho Spark job | `psycopg2-binary`, `user-agents` (baked vào Hadoop image) |

---

## Cấu Trúc Dự Án

```
airflow-pipeline/
├── dags/
│   ├── kafka_monitoring.py      # Kiểm tra sức khỏe Kafka mỗi 5 phút
│   ├── spark_monitoring.py      # Kiểm tra YARN + Spark mỗi 5 phút
│   ├── spark_control.py         # Tạo schema + submit Spark job (daily)
│   ├── quality_check.py         # 12 kiểm tra chất lượng dữ liệu (hourly)
│   ├── data_transfer.py         # Kafka → raw_product_view (hourly)
│   ├── archival.py              # Archive raw data cũ hơn 30 ngày (daily)
│   └── telegram_alert.py        # Shared alert callbacks
│
├── plugins/operators/
│   ├── kafka_operator.py        # KafkaHealthCheckOperator, KafkaDataFlowCheckOperator
│   ├── spark_operator.py        # SparkJobOperator, SparkHealthCheckOperator
│   ├── quality_operator.py      # DataQualityOperator
│   ├── transfer_operator.py     # KafkaToPostgresOperator
│   └── archival_operator.py     # DataArchivalOperator
│
├── scripts/spark/
│   └── kafka_streaming.py       # Spark Structured Streaming job
│
├── tests/
│   ├── unit/
│   │   ├── test_operators.py    # 6 unit tests cho operators
│   │   └── test_dags.py         # 6 DAG integrity tests
│   └── integration/
│       └── test_pipeline.py     # 4 integration tests
│
├── config/hadoop/               # Hadoop XML config files
├── hadoop/                      # File build Hadoop image (Python deps cho Spark job)
│   ├── Dockerfile               #   build image đầy đủ
│   ├── Dockerfile.python        #   build nhanh (chỉ deps)
│   ├── requirements-spark.txt   #   psycopg2-binary, user-agents
│   └── README.md                #   giải thích vì sao/cách baked deps vào image
├── Dockerfile                   # Custom Airflow image
├── docker-compose.yaml          # Định nghĩa toàn bộ stack
└── requirements.txt             # Python dependencies
```

---

## Các DAG

### `kafka_monitoring` — mỗi 5 phút
Giám sát sức khỏe pipeline Kafka:
- **check_broker**: kết nối broker, topic availability, xác thực consumer group
- **check_data_flow**: throughput (rows/min), ingest lag, processing rate, tỷ lệ bad record

### `spark_monitoring` — mỗi 5 phút
Giám sát ứng dụng Spark trên YARN:
- Xác nhận `glamira-streaming` đang ở trạng thái RUNNING trong YARN
- Kiểm tra nodemanager availability và mức sử dụng CPU/RAM qua ResourceManager REST API
- Xác nhận dữ liệu mới nhất đã được ghi vào `fact_product_view`

### `spark_control` — daily
- Tạo toàn bộ star schema PostgreSQL (`CREATE TABLE IF NOT EXISTS`)
- Submit `kafka_streaming.py` lên YARN ở cluster mode

### `quality_check` — hourly
Chạy 12 kiểm tra trên `fact_product_view`:
- Bảng không rỗng
- Không có null ở `view_id`, `event_ts`, `date_key`, `device_key`, `store_key`, `ip_address`
- Không có `view_id` trùng lặp
- Không có timestamp không hợp lệ (ngoài cửa sổ 90 ngày)
- Tỷ lệ phủ product key trên 40%
- Tồn tại dữ liệu streaming mới (trong 2 giờ qua)
- Tỷ lệ bad JSON record dưới ngưỡng cho phép

### `data_transfer` — hourly
Chuyển tối đa 1,000 messages mỗi lần từ Kafka topic `product_view` vào bảng `raw_product_view` (raw landing zone).

### `archival` — daily
Archive các record trong `raw_product_view` cũ hơn 30 ngày vào `raw_product_view_archive`, sau đó dọn dẹp bảng nguồn.

---

## Schema PostgreSQL

```
dim_device       (device_key, browser, os, device_type)
dim_date         (date_key, year, month, day, hour)
dim_product      (product_key, product_id)
dim_store        (store_key, store_id)
dim_referrer     (referrer_key, referrer_domain)
dim_location     (location_key, ip_address, country_code, domain)

fact_product_view (view_id PK, date_key, product_key, store_key,
                   location_key, device_key, referrer_key,
                   event_ts, event_hour, time_stamp,
                   current_url, referrer_url, ip_address,
                   collection, api_version, ingested_at)

raw_product_view         (id, raw_data JSONB, transferred_at)
raw_product_view_archive (id, raw_data JSONB, transferred_at, archived_at)
bad_product_view_records (id, batch_id, kafka_value, error_reason, ingested_at)
```

---

## Thống Kê Kho Dữ Liệu

| Bảng | Số dòng |
|------|---------|
| fact_product_view | 5,839,576+ |
| dim_location | 558,197 |
| dim_product | 18,194 |
| dim_referrer | 4,983 |
| dim_device | 189 |
| dim_date | 170 |
| dim_store | 84 |
| bad_product_view_records | 0 |

---

## Hướng Dẫn Cài Đặt

### Yêu cầu
- Docker Desktop (host 16GB RAM; cấp **12GB cho Docker** để có headroom thoải mái — toàn bộ stack với stream Spark đang chạy dùng ~8GB)
- Docker Compose

### Build Hadoop image (Python dependencies cho Spark job)

Spark job chạy ở **YARN cluster mode**, nghĩa là driver và executor thực thi bên
trong các Hadoop **nodemanager** container — không phải bên trong Airflow. Do đó
các container này phải có sẵn các Python package mà job import:

- `psycopg2` — ghi batch vào PostgreSQL qua `execute_values`
- `user_agents` — parse chuỗi `user_agent` thành `dim_device`

Các package này được khai báo trong `hadoop/requirements-spark.txt` và được baked
vào Hadoop image để cluster có thể tái lập hoàn toàn (xem `hadoop/README.md` để
biết lý do chi tiết). Build image trước lần chạy đầu:

```bash
cd hadoop/

# Cách nhanh — mở rộng image base đã cache, chỉ cài deps (~30s)
docker build -f Dockerfile.python -t unigap/hadoop:3.3.6 .

# HOẶC build đầy đủ từ đầu (tải Hadoop)
# docker build -t unigap/hadoop:3.3.6 .
```

Xác nhận cả hai package có mặt trong nodemanager sau khi cluster khởi động:

```bash
docker exec -ti hadoop-nodemanager1-1 bash -c \
  "python3 -c 'import psycopg2, user_agents; print(\"both OK\")'"
```

> Các file `Dockerfile`, `Dockerfile.python` và `requirements-spark.txt` trong thư
> mục `hadoop/` giống với các file dùng trong K20 Hadoop setup
> (`hadoop/00-setup/hadoop/`). Copy chúng vào đó để build lại image cluster dùng chung.

### Khởi động stack

```bash
# Khởi động Hadoop trước
cd hadoop/00-setup/hadoop
docker compose up -d

# Kết nối lại Hadoop vào streaming-network (bắt buộc sau mỗi lần restart)
for container in hadoop-namenode-1 hadoop-datanode1-1 hadoop-datanode2-1 \
  hadoop-resourcemanager-1 hadoop-nodemanager1-1 hadoop-nodemanager2-1; do
  docker network connect streaming-network $container
done

# Tạo thư mục HDFS
docker exec -ti hadoop-namenode-1 bash -c "
  hdfs dfs -mkdir -p /user/default &&
  hdfs dfs -chown default:supergroup /user/default
"

# Khởi động Airflow
cd airflow-pipeline
docker compose up -d
```

### Cấu hình Airflow Variables

Vào **Admin → Variables** trong Airflow UI (`http://localhost:18080`) và thêm:

| Key | Value |
|-----|-------|
| `kafka_bootstrap_servers` | `<kafka_bootstrap_servers>` |
| `kafka_security_protocol` | `SASL_PLAINTEXT` |
| `kafka_sasl_mechanism` | `PLAIN` |
| `kafka_sasl_username` | `kafka` |
| `kafka_sasl_password` | `<password>` |
| `pg_host` | `postgres` |
| `pg_db` | `airflow` |
| `pg_user` | `airflow` |
| `pg_password` | `airflow` |
| `telegram_bot_token` | `<bot_token>` |
| `telegram_chat_id` | `<chat_id>` |
| `spark_run_mode` | `yarn` |

### Cấu hình Airflow Connections

Vào **Admin → Connections** và thêm:

| Conn Id | Type | Host | Schema | Login | Password | Port |
|---------|------|------|--------|-------|----------|------|
| `postgres` | Postgres | `postgres` | `airflow` | `airflow` | `airflow` | `5432` |

### Triển khai Spark Streaming Job

1. Trigger DAG `spark_control` thủ công từ Airflow UI
2. DAG sẽ tạo tất cả bảng và submit app `glamira-streaming` lên YARN
3. Pause `spark_control` sau lần chạy đầu để tránh submit trùng lặp
4. Bật các DAG `kafka_monitoring`, `spark_monitoring`, và `quality_check`

---

## Chạy Kiểm Thử

```bash
docker exec -ti airflow-pipeline-airflow-worker-1 bash -c \
  "cd /opt/airflow && python -m pytest tests/ -v"
```

Kết quả mong đợi: **14 passed, 2 skipped**

---

## Spark Streaming Job

`scripts/spark/kafka_streaming.py` triển khai pipeline Spark Structured Streaming:

1. Đọc từ Kafka topic `product_view` (SASL_PLAINTEXT)
2. Parse JSON theo schema Glamira
3. Tách các record lỗi vào `bad_product_view_records`
4. Ghi các dimension table trước (device, date, product, store, referrer, location)
5. Join để lấy foreign key, ghi `fact_product_view`
6. Checkpoint vào HDFS (`hdfs://namenode/checkpoint/glamira-yarn`)
7. Trigger mỗi 30 giây, 10,000 messages mỗi batch

Toàn bộ credentials được truyền qua `spark.yarn.appMasterEnv` và `spark.executorEnv` — không có hardcoded secrets.

---

## Bảo Mật

- Tất cả credentials lưu trong Airflow Variables — không bao giờ trong source code
- Các giá trị nhạy cảm được che trong Airflow task logs qua `_log_command()`
- Không có secrets nào được commit lên Git

---

## Quyết Định Triển Khai

### Tại Sao Spark Chạy ở YARN Cluster Mode

Ban đầu Spark được thực thi ở client mode, trong đó Spark driver gắn liền với tiến trình Airflow worker.

Các vấn đề gặp phải:
- Vòng đời driver phụ thuộc vào sự sẵn sàng của Airflow worker
- Worker restart có thể dừng ứng dụng streaming
- Spark chạy lâu dài xung đột với hành vi thực thi task
- Khó cô lập và monitoring hơn

Quyết định cuối cùng:

```python
deploy_mode='cluster'
```

Lợi ích:
- Spark driver chạy bên trong YARN thay vì Airflow
- Cô lập lỗi tốt hơn
- Kiến trúc gần với production hơn
- Airflow chỉ submit và monitor job
- Ứng dụng streaming tồn tại độc lập, không phụ thuộc vào task execution

---

## Quyết Định Thiết Kế

### Tại Sao Dùng Airflow Variables và Connections

Các giá trị nhạy cảm được lưu trong Airflow Variables và Connections thay vì trong source code.

Lợi ích:
- Ngăn secrets đi vào Git history
- Dễ dàng xoay vòng credentials
- Dễ chuyển giữa các môi trường
- Tuân thủ các best practice về bảo mật

### Tại Sao Dùng YARN REST API Thay Vì CLI

Monitoring sử dụng ResourceManager REST API thay vì gọi lệnh YARN trực tiếp.

Lợi ích:
- Hoạt động tốt trong môi trường container
- Tránh vấn đề quyền truy cập
- Dễ monitor và debug hơn
- Ít phụ thuộc vào binary cục bộ hơn

### Tại Sao Cách Ly Bad Records

Các Kafka message bị lỗi được ghi vào `bad_product_view_records` thay vì bị bỏ đi.

Lợi ích:
- Giữ được khả năng quan sát
- Hỗ trợ phân tích nguyên nhân gốc rễ
- Ngăn streaming bị dừng do lỗi
- Cho phép xử lý lại trong tương lai

### Tại Sao Tách Thành Các DAG Monitoring Riêng Biệt

Kafka, Spark, Data Quality và Data Transfer được tách thành các DAG độc lập.

Lợi ích:
- Phạm vi lỗi độc lập nhau
- Dễ bảo trì hơn
- Khả năng quan sát vận hành tốt hơn
- Troubleshooting đơn giản hơn

---

## Thách Thức và Cách Giải Quyết

### Thách Thức 1: Duplicate Spark Streaming Applications

**Vấn đề**

Trigger DAG Spark nhiều lần tạo ra nhiều Spark application chạy đồng thời.

**Nguyên nhân**

Spark Structured Streaming job là ứng dụng chạy liên tục. Mỗi lần trigger sẽ submit một YARN application mới.

**Giải pháp**
- Xác định các application trùng lặp trong YARN
- Kill các application RUNNING và ACCEPTED thừa
- Thiết lập quy trình vận hành để duy trì đúng một streaming application hoạt động

**Kết quả**

Chỉ có một app `glamira-streaming` chạy tại một thời điểm.

### Thách Thức 2: Lỗi Quyền Truy Cập Khi Monitoring Spark

**Vấn đề**

Monitoring thất bại khi cố gắng thực thi lệnh YARN từ bên trong Airflow container.

**Nguyên nhân**

Airflow worker trong container không có quyền truy cập trực tiếp vào YARN binary cần thiết.

**Giải pháp**

Thay thế shell execution bằng các lời gọi YARN ResourceManager REST API.

**Kết quả**

Monitoring trở nên đáng tin cậy hơn và thân thiện với container hơn.

### Thách Thức 3: Tỷ Lệ Phủ Product Key

**Vấn đề**

Nhiều event hợp lệ không có product identifier.

**Điều tra**

Traffic bao gồm lượt truy cập trang chủ, navigation event, duyệt danh mục và các tương tác không liên quan đến product cụ thể.

**Giải pháp**

Triển khai kiểm tra theo ngưỡng — tỷ lệ phủ product key phải trên 40% — thay vì yêu cầu 100%.

**Kết quả**

Kiểm tra chất lượng dữ liệu phản ánh đúng thực tế nghiệp vụ hơn.

### Thách Thức 4: Malformed Kafka Messages

**Vấn đề**

JSON không hợp lệ có thể làm gián đoạn quá trình xử lý downstream.

**Giải pháp**

Triển khai bảng quarantine cho các record bị lỗi và tiếp tục xử lý các record hợp lệ.

**Kết quả**

Độ tin cậy của pipeline được cải thiện trong khi vẫn giữ được khả năng quan sát các vấn đề dữ liệu.

### Thách Thức 5: Checkpoint Cũ vs Kafka Retention

**Vấn đề**

Sau khi cluster bị dừng và khởi động lại, Spark job thất bại ngay ở batch đầu tiên với lỗi `OffsetOutOfRangeException`.

**Nguyên nhân**

HDFS checkpoint lưu các offset từ một lần chạy trước đó rất lâu. Đến khi job resume, Kafka broker từ xa đã xóa các message đó theo retention policy, nên các offset được yêu cầu không còn tồn tại.

**Giải pháp**

- Xóa checkpoint YARN cũ: `hdfs dfs -rm -r /checkpoint/glamira-yarn`
- Job sau đó resume từ offset *sớm nhất còn tồn tại*
- Để hardening cho production, `.option("failOnDataLoss", "false")` cho phép stream bỏ qua các offset bị mất một cách nhẹ nhàng thay vì crash

**Kết quả**

Streaming job khởi động sạch và tiếp tục ingest dữ liệu.

### Thách Thức 6: Thiếu Python Dependencies trên Container Vừa Tạo Lại

**Vấn đề**

Trên một cluster vừa được tạo lại, Spark job crash ngay khi khởi động với lỗi `ModuleNotFoundError: No module named 'psycopg2'` (và sau đó là `user_agents`).

**Nguyên nhân**

Các package ban đầu được cài thủ công bằng `pip install` bên trong các nodemanager container đang chạy. Trạng thái đó không tồn tại qua `docker compose down` / việc tạo lại container, nên cluster mới không có Python dependencies nào.

**Giải pháp**

- Khai báo dependencies trong `hadoop/requirements-spark.txt`
- Baked chúng vào Hadoop image qua Dockerfile (`pip install -r requirements-spark.txt`)
- Cluster giờ có thể tái lập từ code — không còn bước cài thủ công

**Kết quả**

Mỗi lần build image đều tạo ra một cluster hoạt động được; yêu cầu dependency được lưu trong version control.

---

## Kết Quả Kiểm Tra Cuối Cùng

### Spark Streaming
- Spark application chạy thành công trên YARN
- Hai NodeManager sẵn sàng hoạt động
- Mức sử dụng tài nguyên được theo dõi qua Airflow

### Kho Dữ Liệu

| Bảng | Số dòng |
|------|---------|
| `fact_product_view` | 5,839,576+ |
| `dim_location` | 558,197+ |
| `dim_product` | 18,194+ |
| `raw_product_view` | ~96,000 |

### Chất Lượng Dữ Liệu

Tất cả 12 kiểm tra đều passed:
- Không có `view_id` trùng lặp
- Không có NULL ở các surrogate key quan trọng
- Timestamp hợp lệ
- Tỷ lệ phủ product key trên ngưỡng
- Xác nhận streaming ingest gần đây tồn tại
- Tỷ lệ JSON lỗi dưới ngưỡng cho phép

### Monitoring

Spark monitoring xác nhận thành công:
- Spark application đang chạy
- Tài nguyên worker sẵn sàng
- Dữ liệu mới nhất đã được ghi vào warehouse
- Ingest lag trong giới hạn chấp nhận được

---

## Bài Học Rút Ra

1. **Streaming workload yêu cầu pattern điều phối khác với batch job.** Spark job chạy lâu dài nên chạy liên tục trên YARN; vai trò của Airflow là submit và monitor, không phải quản lý vòng đời thực thi. Xử lý streaming job như batch task dẫn đến restart, duplicate application và nhầm lẫn offset.

2. **Deploy mode của Spark có ảnh hưởng lớn đến vận hành.** Chạy ở client mode gắn Spark driver với Airflow worker — worker restart sẽ kill stream. Chuyển sang cluster mode với `preexec_fn=os.setsid` trong `subprocess.Popen` tách driver khỏi Celery hoàn toàn, cho phép application tồn tại độc lập trên YARN.

3. **Quản lý checkpoint riêng biệt theo môi trường là điều bắt buộc.** Dùng chung HDFS checkpoint path cho cả local và YARN gây ra xung đột offset và schema mismatch. Dự án duy trì ba path riêng biệt: `/checkpoint/glamira` (test ban đầu), `/checkpoint/glamira-local` (local mode) và `/checkpoint/glamira-yarn` (YARN production).

4. **YARN REST API đáng tin cậy hơn shell command trong môi trường container.** Airflow worker trong container không có quyền truy cập trực tiếp vào YARN binary. Gọi ResourceManager REST API (`/ws/v1/cluster/apps`, `/ws/v1/cluster/nodes`) trực tiếp sạch hơn, dễ chuyển đổi môi trường hơn và dễ parse hơn.

5. **Quy tắc data quality phải phản ánh thực tế nghiệp vụ, không phải giả định lý tưởng.** Không phải mọi product view event đều có product identifier — lượt truy cập trang chủ, duyệt danh mục và navigation event đều là traffic hợp lệ. Yêu cầu 100% product key coverage sẽ đánh dấu dữ liệu hợp lệ là lỗi. Ngưỡng 40% được thiết lập dựa trên thực tế traffic.

6. **Bad record nên được cách ly và quan sát được, không bị loại bỏ im lặng.** Các Kafka message bị lỗi được ghi vào `bad_product_view_records` cùng với raw value và error reason, giữ cho pipeline tiếp tục chạy trong khi vẫn bảo toàn đầy đủ thông tin để phân tích nguyên nhân và xử lý lại sau này.

7. **Observability là yêu cầu cốt lõi của data engineering, không phải tính năng bổ sung.** Các DAG riêng biệt cho Kafka health, YARN application status và data quality chạy độc lập theo lịch của chúng. Khi có sự cố, phạm vi lỗi lập tức rõ ràng thay vì bị chôn vùi trong một monolithic pipeline.

8. **Runtime dependencies thuộc về image, không phải container đang chạy.** Cluster ban đầu phụ thuộc vào các Python package được pip-install thủ công bên trong nodemanager container. Trạng thái đó biến mất ngay khi container bị tạo lại, gây ra `ModuleNotFoundError` trên một pipeline vốn hoàn toàn đúng. Chuyển dependencies vào `requirements-spark.txt` và baked vào Hadoop image giúp cluster tái lập được từ code — bất kỳ ai cũng có thể build và chạy mà không cần bước thủ công ẩn. "Chạy được trên máy tôi" chỉ trở thành "chạy được ở mọi nơi" khi mọi dependency được khai báo, không phải ghi nhớ.

---

## Lời Cảm Ơn

Được xây dựng trong khuôn khổ chương trình K20 Data Engineering tại Unigap.
Dataset: Glamira jewelry e-commerce product view events.
