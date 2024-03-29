import os
from datetime import datetime
import numpy as np
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateExternalTableOperator,
)
from include.brick_insights_rating_request import brick_insights_ingestion_callable
from include.retrieve_sets_by_year import query_bigquery_table
from include.upload_to_gcs import upload_to_gcs_callable

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")
AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")

local_workflow = DAG(
    "BRICK_INSIGHTS_LEGO_SET_RATINGS",
    schedule_interval="0 8 * * 1",  # Run the DAG every Monday at 8:00 AM
    start_date=datetime(2024, 3, 1),  # Change to anything you like
    end_date=datetime(2024, 3, 27),  # Change to anything you like
    catchup=False,  # Do not backfill missed runs
    max_active_runs=1,  # Limits concurrent runs to 3
    default_args={"retries": 3},  # Set the number of retries to 3
    tags=["Lego Data"],
)


def fetch_lego_ratings_task(**kwargs):
    task_instance = kwargs["task_instance"]
    set_numbers = task_instance.xcom_pull(task_ids="get_sets_by_year_task")
    brick_insights_ingestion_callable(set_numbers)


with local_workflow:

    # Gets the LEGO sets for each year you choose
    get_sets_by_year_task = PythonOperator(
        task_id="get_sets_by_year_task",
        python_callable=query_bigquery_table,
        op_kwargs={
            "years": np.arange(1949, 2025).tolist()
        },  # Provide a list of years you want to get sets for
        dag=local_workflow,
    )

    # Fetches the ratings and reviews data for the LEGO sets
    fetch_lego_ratings_task = PythonOperator(
        task_id="fetch_lego_ratings_task",
        python_callable=fetch_lego_ratings_task,
        provide_context=True,
        dag=local_workflow,
    )

    # Uploads the Parquet file to the GCS bucket
    local_to_gcs_task = PythonOperator(
        task_id="local_to_gcs_task",
        python_callable=upload_to_gcs_callable,
        op_kwargs={
            "bucket": BUCKET,
            "src_files_path": [
                os.path.join(AIRFLOW_HOME, "brick_insights_ratings_and_reviews.parquet")
            ],
        },
        dag=local_workflow,
    )

    # Creates an external table in BigQuery
    external_table_task = BigQueryCreateExternalTableOperator(
        task_id="external_table_task",
        table_resource={
            "tableReference": {
                "projectId": PROJECT_ID,
                "datasetId": "lego_raw",
                "tableId": "brick_insights_ratings_and_reviews",
            },
            "externalDataConfiguration": {
                "sourceFormat": "PARQUET",
                "autodetect": True,
                "skipLeadingRows": 1,  # In case the file has a header
                "sourceUris": [
                    f"gs://{BUCKET}/raw/brick_insights_ratings_and_reviews.parquet"
                ],
            },
        },
        dag=local_workflow,
    )

    # Cleans up the local files
    cleanup_task = BashOperator(
        task_id="cleanup_task",
        bash_command=f"rm -f {AIRFLOW_HOME}/brick_insights_ratings_and_reviews.parquet",
    )

    (
        get_sets_by_year_task
        >> fetch_lego_ratings_task
        >> local_to_gcs_task
        >> external_table_task
        >> cleanup_task
    )
