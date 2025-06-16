from google.cloud import storage
from dotenv import load_dotenv
import json
import os 

load_dotenv(".env")

bucket_name = os.getenv("BUCKET_NAME")
storage_client = storage.Client()
bucket = storage_client.bucket(bucket_name)

def write_to_bucket(blob_name, data):
    blob = bucket.blob(blob_name)
    content = json.dumps(data, indent=4)
    blob.upload_from_string(content, content_type="application/json")