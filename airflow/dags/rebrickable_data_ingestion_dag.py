# 1. Download the CSV data from each dataset URL and save it to a local file in the AIRFLOW_HOME directory
# 2. Unzip the CSV files
# 3. Format it to Parquet format
# 4. Upload the Parquet file to GCS
# 5. Delete the local files
# 6. Create an external table in BigQuery for each

import os
from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateExternalTableOperator,
)
from airflow.utils.task_group import TaskGroup

from include.download_rebrickable_files import download_files_callable
from include.format_to_parquet import format_to_parquet_callable
from include.upload_to_gcs import upload_to_gcs_callable

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
DATASET_ID = os.environ.get("GCP_DATASET_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")
AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")

# The tables from Rebrickable that we can use
SOURCE_FILE_NAMES = [
    "themes",
    "colors",
    "part_categories",
    "parts",
    "part_relationships",
    "elements",
    "sets",
    "minifigs",
    "inventories",
    "inventory_parts",
    "inventory_sets",
    "inventory_minifigs",
]

# The output CSV file names
OUTPUT_CSV_FILE_NAMES = [source_file + ".csv" for source_file in SOURCE_FILE_NAMES]

# The output Parquet file names
OUTPUT_PARQUET_FILE_NAMES = [
    source_file + ".parquet" for source_file in SOURCE_FILE_NAMES
]

local_workflow = DAG(
    "REBRICKABLE_DATA_INGESTION",
    schedule_interval="0 7 * * 1",  # Run the DAG every Monday at 7:00 AM
    start_date=datetime(2024, 3, 1),  # Change to anything you like
    end_date=datetime(2024, 3, 27),  # Change to anything you like
    catchup=False,  # Do not backfill missed runs
    max_active_runs=1,  # Limits concurrent runs to 3
    default_args={"retries": 3},  # Set the number of retries to 3
    tags=["Lego Data"],
)

with local_workflow:

    # Downloads the CSV files from Rebrickable
    download_files_task = PythonOperator(
        task_id="download_files_task",
        python_callable=download_files_callable,
        op_kwargs={
            "url": "https://rebrickable.com/downloads/",
            "file_names": SOURCE_FILE_NAMES,
            "airflow_home_directory": AIRFLOW_HOME,
        },
    )

    # Simply shows what files got dowloaded to AIRFLOW_HOME
    show_files_task = BashOperator(
        task_id="show_files_task",
        bash_command=f"ls -l {AIRFLOW_HOME}",
    )

    # Decompresses the CSV files
    decompress_task = BashOperator(
        task_id="decompress_task",
        # Unzip all of the csv.gz files
        bash_command=f"find {AIRFLOW_HOME} -type f -name '*.gz' -exec gunzip -f {{}} +",
    )

    # Formats the CSV files to Parquet
    format_to_parquet_task = PythonOperator(
        task_id="format_to_parquet_task",
        python_callable=format_to_parquet_callable,
        op_kwargs={
            # "src_files_path": [AIRFLOW_HOME + "/" + file_name for file_name in OUTPUT_CSV_FILE_NAMES],
            "src_file_names": OUTPUT_CSV_FILE_NAMES,
        },
    )

    # Uploads the Parquet files to GCS
    local_to_gcs_task = PythonOperator(
        task_id="local_to_gcs_task",
        python_callable=upload_to_gcs_callable,
        op_kwargs={
            "bucket": BUCKET,
            "src_files_path": [
                AIRFLOW_HOME + "/" + file_name
                for file_name in OUTPUT_PARQUET_FILE_NAMES
            ],
        },
    )

    def create_external_table(parquet_file):
        """
        Creates an external table in BigQuery for the given Parquet file.

        Args:
        - parquet_file: The name of the Parquet file to create the external table for
        """
        return BigQueryCreateExternalTableOperator(
            task_id=f"create_external_table_{parquet_file.replace('.parquet', '')}",
            table_resource={
                "tableReference": {
                    "projectId": PROJECT_ID,
                    "datasetId": DATASET_ID,
                    "tableId": parquet_file.replace(".parquet", ""),
                },
                "externalDataConfiguration": {
                    "sourceFormat": "PARQUET",
                    "sourceUris": [f"gs://{BUCKET}/raw/{parquet_file}"],
                },
            },
            dag=local_workflow,
        )

    # Create a task group for creating external tables
    with TaskGroup("create_external_tables") as create_external_tables_group:
        # Loop over each Parquet file and define a task to create external table
        for parquet_file in OUTPUT_PARQUET_FILE_NAMES:
            create_external_table_task = create_external_table(parquet_file)
            create_external_table_task.dag = local_workflow

    # Cleans up the local files
    cleanup_task = BashOperator(
        task_id="cleanup_task",
        bash_command=f"rm -f {AIRFLOW_HOME}/*.csv.gz \
            {AIRFLOW_HOME}/*.csv \
                {AIRFLOW_HOME}/*.parquet",
    )

    # A pre-cleanup task in case the files already exist
    pre_cleanup_task = BashOperator(
        task_id="pre_cleanup_task",
        bash_command=f"rm -f {AIRFLOW_HOME}/*.csv.gz \
            {AIRFLOW_HOME}/*.csv \
                {AIRFLOW_HOME}/*.parquet",
    )

    (
        pre_cleanup_task
        >> download_files_task
        >> show_files_task
        >> decompress_task
        >> format_to_parquet_task
        >> local_to_gcs_task
        >> create_external_tables_group
        >> cleanup_task
    )
