import os
from datetime import datetime
import numpy as np
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateExternalTableOperator,
)
from include.retrieve_sets_by_year import query_bigquery_table
from include.upload_to_gcs import upload_to_gcs_callable
from include.lego_website_set_rating_price_scraping import get_price_and_rating_callable
import asyncio

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")
AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")

local_workflow = DAG(
    "LEGO_WEBSITE_SET_PRICES_RATINGS",
    schedule_interval="0 8 * * 1",  # Run the DAG every Monday at 8:00 AM
    start_date=datetime(2024, 3, 1),  # Change to anything you like
    end_date=datetime(2024, 3, 27),  # Change to anything you like
    catchup=False,  # Do not backfill missed runs
    max_active_runs=1,  # Limits concurrent runs to 1
    default_args={"retries": 3},  # Set the number of retries to 3
    tags=["Lego Data"],
)


def scrape_lego_website_task(**kwargs):
    task_instance = kwargs["task_instance"]
    lego_sets = task_instance.xcom_pull(task_ids="get_sets_by_year_task")
    get_price_and_rating_callable_sync(lego_sets)


def get_price_and_rating_callable_sync(lego_sets):
    loop = asyncio.get_event_loop()
    loop.run_until_complete(get_price_and_rating_callable(lego_sets))


with local_workflow:

    # Get the LEGO sets for each year you choose
    get_sets_by_year_task = PythonOperator(
        task_id="get_sets_by_year_task",
        python_callable=query_bigquery_table,
        op_kwargs={"years": np.arange(2023, 2024).tolist()},  # Provide a list of years
        dag=local_workflow,
    )

    # Scrape the LEGO website for ratings and prices
    scrape_lego_website = PythonOperator(
        task_id="scrape_lego_website",
        python_callable=scrape_lego_website_task,
        provide_context=True,
        dag=local_workflow,
    )

    # Upload the Parquet file to the GCS bucket
    local_to_gcs_task = PythonOperator(
        task_id="local_to_gcs_task",
        python_callable=upload_to_gcs_callable,
        op_kwargs={
            "bucket": BUCKET,
            "src_files_path": [
                os.path.join(
                    AIRFLOW_HOME, "lego_website_set_prices_and_ratings.parquet"
                )
            ],
        },
        dag=local_workflow,
    )

    # Create an external table in BigQuery
    external_table_task = BigQueryCreateExternalTableOperator(
        task_id="external_table_task",
        table_resource={
            "tableReference": {
                "projectId": PROJECT_ID,
                "datasetId": "lego_raw",
                "tableId": "lego_website_set_prices_and_ratings",
            },
            "externalDataConfiguration": {
                "sourceFormat": "PARQUET",
                "sourceUris": [
                    f"gs://{BUCKET}/raw/lego_website_set_prices_and_ratings.parquet"
                ],
            },
        },
        dag=local_workflow,
    )

    # Cleanup the local files
    cleanup_task = BashOperator(
        task_id="cleanup_task",
        bash_command=f"rm -f {AIRFLOW_HOME}/lego_website_set_prices_and_ratings.parquet",
    )

    (
        get_sets_by_year_task
        >> scrape_lego_website
        >> local_to_gcs_task
        >> external_table_task
        >> cleanup_task
    )
