from google.cloud import bigquery, storage
import json
import os


# Bucket settings
bucket_name = os.getenv("BUCKET_NAME")
storage_client = storage.Client()
bucket = storage_client.bucket(bucket_name)

# BigQuery settings
bigquery_project_id = os.getenv("BIGQUERY_PROJECT_ID")
bigquery_dataset = os.getenv("DATASET_NAME")
bigquery_client = bigquery.Client()


# Read and write functions for Google Cloud Services


def write_to_bucket(blob_name, data, file_type="json"):
    blob = bucket.blob(blob_name)

    # Handle csv and json formats
    if file_type == "csv":
        blob.upload_from_string(data.to_csv(index=False, encoding="utf-8"), content_type="text/csv")
    else:
        content = json.dumps(data, indent=4, ensure_ascii=False)
        blob.upload_from_string(content, content_type="application/json")


def read_from_bucket(blob_name):
    blob = bucket.blob(blob_name)
    content = json.loads(blob.download_as_string())
    return content


def write_to_bigquery(bucket_path, table_name):
    gcs_uri = f"gs://{bucket_name}/{bucket_path}"
    table_id = f"{bigquery_project_id}.{bigquery_dataset}.{table_name}"

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        # Replace existing data with new one if table already exists
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        skip_leading_rows=1,
        autodetect=True,
    )

    load_job = bigquery_client.load_table_from_uri(gcs_uri, table_id, job_config=job_config)
    load_job.result()
