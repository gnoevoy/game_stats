from google.cloud import storage
import json
import os


bucket_name = os.getenv("BUCKET_NAME")
storage_client = storage.Client()
bucket = storage_client.bucket(bucket_name)


def write_to_bucket(blob_name, data, file_type="json"):
    blob = bucket.blob(blob_name)

    # Different logic for CSV and JSON
    if file_type == "csv":
        blob.upload_from_string(data.to_csv(index=False), content_type="text/csv")
    else:
        content = json.dumps(data, indent=4, ensure_ascii=False)
        blob.upload_from_string(content, content_type="application/json")


def read_from_bucket(blob_name):
    blob = bucket.blob(blob_name)
    content = json.loads(blob.download_as_string())
    return content
