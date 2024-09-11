#!/usr/bin/env python

import sys
import getpass
from datetime import datetime
import subprocess
import snowflake.connector
from google.cloud import bigquery, storage
import json
import gzip

if len(sys.argv) != 5:
    print("Usage: python sf2cdp_1.py <database> <schema> <table_name> <env>")
    sys.exit(1)

# Snowflake connection details
snowflake_account = 'vc21971.us-central1.gcp'
system_username = getpass.getuser()
snowflake_user = system_username + '@atd-us.com'
snowflake_warehouse = 'PRD_QUERY_WH'
snowflake_database = sys.argv[1].lower()
snowflake_schema = sys.argv[2].lower()
snowflake_tablename = sys.argv[3].lower()
env = sys.argv[4].lower()
today_date = datetime.today().strftime('%Y-%m-%d')

# Snowflake connection
conn = snowflake.connector.connect(
    user=snowflake_user,
    account=snowflake_account,
    warehouse=snowflake_warehouse,
    database=snowflake_database,
    schema=snowflake_schema,
    authenticator='externalbrowser'
)
print(f'Connected to Snowflake')

cur = conn.cursor()

# Define bucket names based on environment
if env == 'dev':
    source_bucket_name = f'atd_dlk_source_{env}_new'
else:
    source_bucket_name = f'atd_dlk_source_{env}'

target_bucket_name = f'atd_cdp_source_{env}'

# Define stage name and bucket path
stage_name = f"{env}_inbound.inbound_file.{snowflake_tablename}"
bucket_path = f'gcs://{source_bucket_name}/{snowflake_schema.lower()}/{snowflake_tablename.lower()}/loadtype=replace/loadset={today_date}/'

# Ensure the Snowflake stage exists
def ensure_stage_exists(cur, stage_name, bucket_path):
    try:
        cur.execute(f"""
            CREATE OR REPLACE STAGE {stage_name}
            URL='{bucket_path}'
            STORAGE_INTEGRATION = {env}_sf_source_gcs;
        """)
        print(f"Stage {stage_name} created or replaced at {bucket_path}.")
    except Exception as e:
        print(f"Failed to create or replace stage: {e}")

ensure_stage_exists(cur, stage_name, bucket_path)

# Unload data from Snowflake to GCS
unload_query = f"""
COPY INTO @{stage_name}
FROM (
    SELECT OBJECT_CONSTRUCT(*) AS json_output
    FROM {snowflake_database}.{snowflake_schema}.{snowflake_tablename}
)
FILE_FORMAT = (TYPE = 'JSON', COMPRESSION = 'AUTO')
OVERWRITE = TRUE
SINGLE = FALSE
MAX_FILE_SIZE = 4900000000
HEADER = FALSE
"""

try:
    cur.execute(unload_query)
    print(f"Data unloaded successfully into GCS {bucket_path}, with files overwritten in stage {stage_name}.")
except Exception as e:
    print(f"Failed to unload data: {e}")

# Move files from one GCS bucket to another using gsutil mv
source_blob_name = f'{snowflake_schema.lower()}/{snowflake_tablename.lower()}/loadtype=replace/loadset={today_date}/*'
destination_blob_name = source_blob_name

def move_files_via_gsutil(source_bucket_name, source_blob_name, destination_bucket_name, destination_blob_name):
    source_path = f"gs://{source_bucket_name}/{source_blob_name}"
    destination_path = f"gs://{destination_bucket_name}/{destination_blob_name}".rstrip('/*')

    # Construct the gsutil mv command
    command = ['gsutil', '-m', 'mv', source_path, destination_path]

    try:
        # Run the gsutil mv command
        result = subprocess.run(command, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        print(f"Files moved successfully from {source_path} to {destination_path}")
        return destination_blob_name  # Return the destination path for further processing
    except subprocess.CalledProcessError as e:
        print(f"Error moving files: {e.stderr.decode('utf-8')}")
        return None

# Move files and get the moved path
moved_blob_path = move_files_via_gsutil(source_bucket_name, source_blob_name, target_bucket_name, destination_blob_name)

def infer_schema_from_json_sample(client, gcs_uri):
    """Fetch sample JSON objects from multiple GCS files and infer the combined BigQuery schema."""
    storage_client = storage.Client()
    # Parse the bucket name and prefix from the GCS URI
    bucket_name = gcs_uri.split('/')[2]
    prefix = '/'.join(gcs_uri.split('/')[3:]).rstrip('/*') + '/'
    # List blobs (files) in the GCS path
    blobs = storage_client.list_blobs(bucket_name, prefix=prefix)

    combined_schema = {}
    file_count = 0  # Count how many files are found

    for blob in blobs:
        file_count += 1
        if blob.name.endswith('.json.gz'):
            with blob.open("rb") as f:
                with gzip.GzipFile(fileobj=f) as gz:
                    for line in gz:
                        sample_data = json.loads(line.decode('utf-8'))
                        # Update the combined schema based on the current file
                        for key, value in sample_data.items():
                            if key not in combined_schema:
                                if isinstance(value, str):
                                    combined_schema[key] = 'STRING'
                                elif isinstance(value, int):
                                    combined_schema[key] = 'INTEGER'
                                elif isinstance(value, float):
                                    combined_schema[key] = 'FLOAT64'
                                elif isinstance(value, bool):
                                    combined_schema[key] = 'BOOLEAN'
                                elif isinstance(value, dict):
                                    combined_schema[key] = 'STRING'  # Assume nested objects are serialized as strings
                                else:
                                    combined_schema[key] = 'STRING'
                        # Stop after the first 10 lines for schema inference if files are large
                        if file_count >= 10:
                            break
    print(file_count)
    if file_count == 0:
        raise ValueError("No files found in the specified GCS path.")
    
    if not combined_schema:
        raise ValueError("No JSON data found in the specified GCS path.")

    # Convert the combined schema dictionary to BigQuery SchemaField list
    schema = [bigquery.SchemaField(name=key, field_type=field_type) for key, field_type in combined_schema.items()]

    # Print the final schema (optional)
    print("Inferred Schema:")
    for field in schema:
        print(f"Column: {field.name}, Type: {field.field_type}")

    return schema





if moved_blob_path:
    # Load data from GCS into BigQuery
    client = bigquery.Client(project=f"atd-cdp-{env}")
    dataset_id = "eif"
    table_id = f"{snowflake_schema}_{snowflake_tablename}"
    table_ref = client.dataset(dataset_id).table(table_id)

    # Define the job configuration for loading data into BigQuery
    # job_config = bigquery.LoadJobConfig(
    #     source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
    #     autodetect=True,
    #     write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE  # Replace table data
    # )
    gcs_uri = f"gs://{target_bucket_name}/{moved_blob_path}"

    try:
        schema = infer_schema_from_json_sample(client, gcs_uri)
    except ValueError as e:
        print(f"Error: {e}")
        sys.exit(1)

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        schema=schema,
        autodetect=False,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE  # Replace table data
    )

    # Load data into BigQuery
    
    load_job = client.load_table_from_uri(
        gcs_uri,
        table_ref,
        job_config=job_config
    )
    load_job.result()  # Wait for the job to complete
    print(f"Loaded {load_job.output_rows} rows from {gcs_uri} into {dataset_id}:{table_id}.")

# Close the Snowflake cursor and connection
cur.close()
conn.close()
