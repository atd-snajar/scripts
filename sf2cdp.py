#!/usr/bin/env python

import sys
import getpass
from datetime import datetime
import subprocess
import snowflake.connector
from google.cloud import bigquery

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

if moved_blob_path:
    # Load data from GCS into BigQuery
    client = bigquery.Client(project=f"atd-cdp-{env}")
    dataset_id = "eif"
    table_id = f"{snowflake_schema}_{snowflake_tablename}"
    table_ref = client.dataset(dataset_id).table(table_id)

    # Define the job configuration for loading data into BigQuery
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        autodetect=True,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE  # Replace table data
    )

    # Load data into BigQuery
    gcs_uri = f"gs://{target_bucket_name}/{moved_blob_path}"
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