import pandas as pd
from pymongo import MongoClient
from google.cloud import storage
import logging
import os

# --- THÔNG TIN HỆ THỐNG CỦA BẠN ---
MONGO_URI = "mongodb://admin:HuyAnh778899@localhost:27017/?authSource=admin"
DB_NAME = "glamira_full"
COLLECTION_NAME = "products"
BUCKET_NAME = "glamira-backup-17dec"
OUTPUT_FILE = "glamira_raw_28fields.jsonl"

# Thiết lập Logging (Yêu cầu của Project 6) [cite: 21]
logging.basicConfig(
    filename='pipeline_export.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def export_to_gcs():
    """Hàm thực thi ETL từ MongoDB sang Google Cloud Storage [cite: 14]"""
    try:
        # 1. Kết nối MongoDB [cite: 15]
        logging.info("Đang kết nối tới MongoDB...")
        client = MongoClient(MONGO_URI)
        db = client[DB_NAME]
        col = db[COLLECTION_NAME]

        # 2. Trích xuất dữ liệu (Extraction) 
        # Lấy tất cả 18.838 bản ghi, bỏ trường _id để tránh lỗi định dạng BigQuery
        logging.info(f"Đang trích xuất dữ liệu từ collection: {COLLECTION_NAME}...")
        cursor = col.find({}, {"_id": 0})
        df = pd.DataFrame(list(cursor))
        
        row_count = len(df)
        logging.info(f"Đã lấy thành công {row_count} dòng dữ liệu.")

        # 3. Chuyển đổi định dạng sang JSONL (Appropriate format) [cite: 17]
        # JSONL là định dạng chuẩn để BigQuery đọc các cấu trúc dữ liệu phức tạp
        logging.info(f"Đang chuyển đổi sang định dạng {OUTPUT_FILE}...")
        df.to_json(OUTPUT_FILE, orient='records', lines=True, force_ascii=False)

        # 4. Upload lên GCS [cite: 18]
        logging.info(f"Đang upload file lên bucket: {BUCKET_NAME}...")
        storage_client = storage.Client()
        bucket = storage_client.bucket(BUCKET_NAME)
        blob = bucket.blob(f"landing_zone/{OUTPUT_FILE}") # Đưa vào folder landing_zone
        
        blob.upload_from_filename(OUTPUT_FILE)
        
        success_msg = f"THÀNH CÔNG: Đã đẩy {row_count} bản ghi lên gs://{BUCKET_NAME}/landing_zone/{OUTPUT_FILE}"
        logging.info(success_msg)
        print(success_msg)

    except Exception as e:
        # Xử lý lỗi (Error Handling) [cite: 20]
        error_msg = f"LỖI PIPELINE: {str(e)}"
        logging.error(error_msg)
        print(error_msg)

if __name__ == "__main__":
    export_to_gcs()
