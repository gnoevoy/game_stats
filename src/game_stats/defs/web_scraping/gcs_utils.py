import pandas as pd
import json
import os


# Env variables
bucket_name = os.getenv("BUCKET_NAME")


# Helper functions for Google Cloud Services


def get_bucket_conn(gcs):
    gcs_client = gcs.get_client()
    bucket = gcs_client.bucket(bucket_name)
    return bucket


def write_to_bucket(context, gcs, blob_name, data, file_type="json"):
    bucket = get_bucket_conn(gcs)
    blob = bucket.blob(blob_name)

    # Handle csv and json formats
    if file_type == "csv":
        blob.upload_from_string(data.to_csv(index=False, encoding="utf-8"), content_type="text/csv")
    else:
        content = json.dumps(data, indent=4, ensure_ascii=False)
        blob.upload_from_string(content, content_type="application/json")

    context.log.info(f"Data written to bucket as {blob_name}")


def read_from_bucket(context, gcs, blob_name, file_type="json"):
    bucket = get_bucket_conn(gcs)
    blob = bucket.blob(blob_name)

    if file_type == "csv":
        df = pd.read_csv(f"gs://{bucket_name}/{blob_name}")
        context.log.info(f"File {blob_name} extracted from bucket")
        return df
    else:
        content = json.loads(blob.download_as_string())
        context.log.info(f"File {blob_name} extracted from bucket")
        return content
