# 💎 Glamira Automated Data Pipeline Project (41.4M Records)

Dự án này xây dựng một hệ thống **Automated Data Pipeline** quy mô lớn để xử lý dữ liệu hành vi người dùng (User Behavioral Data) của Glamira. Hệ thống tự động hóa luồng dữ liệu từ MongoDB (On-premise VM) lên Google BigQuery (Cloud Warehouse), đảm bảo tính toàn vẹn và khả năng truy vấn hiệu suất cao.

## 🏗 System Architecture

Hệ thống được thiết kế theo kiến trúc **Medallion Architecture (Bronze Layer)**:

1.  **Extraction**: Python script (`export_parallel.py`) sử dụng kỹ thuật xử lý song song (Multi-processing) và định dạng **Parquet** (Snappy compression) để tối ưu hóa việc trích xuất 41.4M bản ghi từ MongoDB.
2.  **Staging**: Dữ liệu được đẩy lên **Google Cloud Storage (GCS)** làm vùng đệm.
3.  **Automation**: **Cloud Function (Gen 2)** tự động kích hoạt qua Eventarc để nạp dữ liệu vào BigQuery ngay khi có file mới tải lên bucket.
4.  **Storage**: Tổ chức dữ liệu vào 2 Dataset (`glamira_bronze` và `glamira_project6`) trên **BigQuery**.



---

## 📊 Data Inventory & Schema (Requirement 3)

Hệ thống quản lý 3 nguồn dữ liệu đa định dạng, tổng hợp thành 5 bảng chiến lược:

### 1. Dataset: `glamira_bronze`
* **`summary_raw`**: Dữ liệu thô nạp từ 4 file Parquet (41.4 triệu dòng).
* **`summary_final` (33 Fields)**: Dữ liệu hành vi đã được bóc tách JSON. Chứa các nhóm trường:
    * *Identity*: `ip`, `device_id`, `email`.
    * *Behavior*: `event_type`, `key_search`, `current_url`.
    * *Commerce*: `price`, `currency`, `order_id`.
* **`ip_locations` (5 Fields)**: Bảng tra cứu địa lý (`country`, `region`, `city`) dựa trên IP address.
* **`summary_with_locations`**: Bảng tổng hợp (Join) phục vụ phân tích Reporting.

### 2. Dataset: `glamira_project6`
* **`products_raw` (28 Fields)**: 18,000 bản ghi thông tin sản phẩm từ file **JSONL**.
    * *Jewelry Attributes*: `gold_weight`, `material_design`, `collection`, `price`.

---

## 🔍 Data Profiling Results

Quá trình "khám sức khỏe" dữ liệu (Profiling) xác nhận:
* **Integrity**: Tỷ lệ khớp IP giữa `summary_final` và `ip_locations` đạt mức tối ưu, hỗ trợ phân tích bản đồ nhiệt (Heatmap) chính xác.
* **Optimization**: Bảng lớn được cấu hình **Partitioning** theo thời gian giúp giảm 90% chi phí truy vấn.
* Tổng số dòng Summary,"41,432,460"
* Tổng số dòng IP Locations,"3,239,628"
* Tổng số dòng Products,"35,296"
* Số lượng IP thiếu,0 
* Tỷ lệ khớp IP (Match Rate),100.0% 
* Số lượng Email thiếu,"397"
* Số loại tiền tệ,85

---

## 🛠 Tech Stack & Skills
* **Languages**: Python (Pandas, PyArrow, Pymongo), SQL (BigQuery Standard SQL).
* **GCP Services**: Compute Engine, Cloud Storage, Cloud Functions (Gen 2), BigQuery.
* **Data Formats**: Parquet, JSONL, CSV.
* **Techniques**: Parallel Processing, Data Flattening, Medallion Architecture.

## 📁 Repository Structure
```text
├── scripts/
│   ├── export_parallel.py      # Extracting 41M rows with Parallelism
│   └── main.py                 # Cloud Function for Automated Ingestion
├── sql/
│   ├── bigquery_schema.sql     # Full DDL for 5 tables
│   └── data_profiling.sql      # Quality check & Analysis queries
├── screenshots/                # Evidence of successful GCP deployment
└── Data_Profiling_Report.pdf   # Executive summary of data quality
