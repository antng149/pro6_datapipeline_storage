import pymongo
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import sys
import json

# Lấy tham số
part_no = sys.argv[1]
skip_val = int(sys.argv[2])
limit_val = int(sys.argv[3])

# Cấu hình
USERNAME = "admin"
PASSWORD = "HuyAnh778899"
DB_NAME = "countly"
COLLECTION_NAME = "summary"
# Ghi thẳng vào ổ SSD mới
OUTPUT_FILE = f"/mnt/data/summary_part_{part_no}.parquet"
CHUNK_SIZE = 100000 

MONGO_URI = f"mongodb://{USERNAME}:{PASSWORD}@localhost:27017/?authSource=admin"

def serialize_complex(v):
    """Hàm biến mọi thứ phức tạp thành chuỗi JSON an toàn"""
    if isinstance(v, (list, dict)):
        return json.dumps(v, ensure_ascii=False)
    if pd.isna(v):
        return ""
    return str(v)

try:
    client = pymongo.MongoClient(MONGO_URI)
    col = client[DB_NAME][COLLECTION_NAME]

    print(f"🚀 PART {part_no}: Đang quét {limit_val:,} dòng từ mốc {skip_val:,}...")
    cursor = col.find({}, no_cursor_timeout=True).skip(skip_val).limit(limit_val).batch_size(5000)

    writer = None
    chunk = []
    processed = 0

    for doc in cursor:
        doc['_id'] = str(doc['_id'])
        chunk.append(doc)
        
        if len(chunk) >= CHUNK_SIZE:
            df = pd.DataFrame(chunk)
            
            # XỬ LÝ SCHEMA ĐỘNG:
            # Tìm tất cả các cột có kiểu Object (thường là List hoặc Dict hoặc Mixed)
            # Ép hết về String/JSON String
            for col_name in df.columns:
                # Ép kiểu cho cột 'option' và bất kỳ cột nào chứa dữ liệu không đồng nhất
                if col_name == 'option' or df[col_name].dtype == 'object':
                    df[col_name] = df[col_name].apply(serialize_complex)
            
            table = pa.Table.from_pandas(df)
            if writer is None:
                writer = pq.ParquetWriter(OUTPUT_FILE, table.schema, compression='snappy')
            
            writer.write_table(table)
            processed += len(chunk)
            print(f"Part {part_no}: ✅ Đã ghi {processed:,} / {limit_val:,}")
            chunk = []

    if chunk:
        df = pd.DataFrame(chunk)
        for col_name in df.columns:
            if col_name == 'option' or df[col_name].dtype == 'object':
                df[col_name] = df[col_name].apply(serialize_complex)
        table = pa.Table.from_pandas(df)
        if writer: writer.write_table(table)
    
    if writer: writer.close()
    cursor.close()
    print(f"🏁 PART {part_no} HOÀN TẤT!")

except Exception as e:
    print(f"💥 Lỗi Part {part_no}: {e}")
