from google.cloud import storage
from dotenv import load_dotenv
import os
load_dotenv()
GCS_CREDENTIALS_PATH = os.getenv('GCS_CREDENTIALS_PATH')
storage_client = storage.Client.from_service_account_json(GCS_CREDENTIALS_PATH)
# Thông tin bucket và đường dẫn file trên GCS
bucket_name = "real_estate_data_datn_20242"  # Thay bằng tên bucket của bạn
source_blob_name = "chungcu_batch/chungcu_batch.jsonl"
destination_file_name = "data/chungcu_batch.jsonl"

# Lấy bucket và blob (tệp tin)
bucket = storage_client.bucket(bucket_name)
blob = bucket.blob(source_blob_name)

# Tải file về thư mục cục bộ
blob.download_to_filename(destination_file_name)

print(f"Tải file {source_blob_name} về {destination_file_name} thành công!")
