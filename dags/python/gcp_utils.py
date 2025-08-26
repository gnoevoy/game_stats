from airflow.providers.google.cloud.hooks.gcs import GCSHook
from io import BytesIO
import pandas as pd
import logging
import json
import os


logger = logging.getLogger(__name__)


# Helper functions to read / write data to GCS


def read_from_bucket(blob_name, file_type="json"):
    bucket_name = os.getenv("BUCKET_NAME")
    hook = GCSHook(gcp_conn_id="google_cloud")
    data = hook.download(bucket_name=bucket_name, object_name=blob_name)

    # Handle CSV and JSON files differently
    if file_type == "csv":
        csv_buffer = BytesIO(data)
        result = pd.read_csv(csv_buffer)
    else:
        result = json.loads(data)

    logger.info(f">>> File {blob_name} - successfully retrieved from GCS bucket")
    return result


def write_to_bucket(blob_name, data, file_type="json"):
    bucket_name = os.getenv("BUCKET_NAME")
    hook = GCSHook(gcp_conn_id="google_cloud")

    if file_type == "csv":
        upload_data = data.to_csv(index=False)
        hook.upload(bucket_name=bucket_name, object_name=blob_name, data=upload_data, mime_type="text/csv")
    else:
        json_data = json.dumps(data, indent=4, ensure_ascii=False)
        hook.upload(bucket_name=bucket_name, object_name=blob_name, data=json_data, mime_type="application/json")

    logger.info(f">>> File {blob_name} - successfully uploaded to GCS bucket")
