import os
from datetime import datetime
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateExternalTableOperator,
)
from include.upload_to_gcs import upload_to_gcs_callable


PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")
AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")

local_workflow = DAG(
    "AGGREGATED_LEGO_DATA_INGESTION",
    schedule_interval="0 8 * * 1",  # Run the DAG every Monday at 8:00 AM
    start_date=datetime(2024, 3, 1),
    end_date=datetime(2024, 3, 27),
    catchup=False,
    max_active_runs=1,  # Limits concurrent runs to 3
    default_args={"retries": 3},  # Set the number of retries to 3
    tags=["Lego Data"],
)


def xlsx_to_parquet():
    # Read the Excel file into a pandas DataFrame
    df = pd.read_excel(f"{AIRFLOW_HOME}/lego_final_data.xlsx")
    # Write the DataFrame to a Parquet file
    df.to_parquet(f"{AIRFLOW_HOME}/lego_final_data.parquet")
    return df


with local_workflow:

    download_file_task = BashOperator(
        task_id="download_file_task",
        bash_command=f"curl -L -o {AIRFLOW_HOME}/lego_final_data.xlsx https://mostwiedzy.pl/en/open-research-data/data-on-lego-sets-release-dates-and-retail-prices-combined-with-aftermarket-transaction-prices-betwe,10210741381038465-0/download",
    )

    # convert xlsx to parquet
    convert_to_parquet_task = PythonOperator(
        task_id="convert_to_parquet_task",
        python_callable=xlsx_to_parquet,
    )

    local_to_gcs_task = PythonOperator(
        task_id="local_to_gcs_task",
        python_callable=upload_to_gcs_callable,
        op_kwargs={
            "bucket": BUCKET,
            "src_files_path": [os.path.join(AIRFLOW_HOME, "lego_final_data.parquet")],
        },
        dag=local_workflow,
    )

    external_table_task = BigQueryCreateExternalTableOperator(
        task_id="external_table_task",
        table_resource={
            "tableReference": {
                "projectId": PROJECT_ID,
                "datasetId": "lego_raw",
                "tableId": "lego_final_data",
            },
            "externalDataConfiguration": {
                "sourceFormat": "PARQUET",
                "autodetect": True,
                "skipLeadingRows": 1,  # In case the file has a header
                "sourceUris": [f"gs://{BUCKET}/raw/lego_final_data.parquet"],
            },
        },
        dag=local_workflow,
    )

    # Define the task dependencies
    (
        download_file_task
        >> convert_to_parquet_task
        >> local_to_gcs_task
        >> external_table_task
    )
