import functions_framework
from google.cloud import bigquery
import os

# Khởi tạo BigQuery Client bên ngoài hàm để tối ưu hiệu suất
client = bigquery.Client()


@functions_framework.cloud_event
def hello_gcs(cloud_event):
    # Lấy thông tin về file vừa được upload từ Cloud Event
    data = cloud_event.data
    bucket_name = data["bucket"]
    file_name = data["name"]

    print(f"Phát hiện file mới: {file_name} trong bucket: {bucket_name}")

    # 1. PHÂN LOẠI FILE: Dựa vào tên thư mục để chọn bảng đích
    # Bạn hãy thay 'your-project-id' bằng ID dự án của bạn
    project_id = os.environ.get('GCP_PROJECT')  # Hoặc điền trực tiếp ID vào đây
    dataset_id = "glamira_bronze"

    if "raw_summary/" in file_name:
        table_name = "summary_raw"
    elif "raw_products/" in file_name:
        table_name = "products_raw"
    elif "raw_ip/" in file_name:
        table_name = "ip_locations"
    else:
        print(f"File {file_name} không nằm trong thư mục cần xử lý. Bỏ qua.")
        return

    table_id = f"{dataset_id}.{table_name}"

    # 2. CẤU HÌNH NẠP DỮ LIỆU (LOAD JOB CONFIG)
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        # Thay đổi từ '\0' sang '|' (hoặc một ký tự cực hiếm trong data của bạn)
        field_delimiter='|',
        quote_character='',
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        max_bad_records=100,
    )

    uri = f"gs://{bucket_name}/{file_name}"

    # 3. THỰC THI NẠP VÀO BIGQUERY
    try:
        load_job = client.load_table_from_uri(uri, table_id, job_config=job_config)
        print(f"Đã bắt đầu Job {load_job.job_id} để nạp file {file_name} vào bảng {table_name}.")

        # Chờ job hoàn thành (tùy chọn, nhưng nên có để ghi log)
        load_job.result()
        print(f"Nạp dữ liệu thành công! Bảng {table_name} đã được cập nhật.")
    except Exception as e:
        print(f"Lỗi xảy ra khi nạp dữ liệu: {str(e)}")