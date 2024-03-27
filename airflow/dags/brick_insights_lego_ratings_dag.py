import os
from datetime import datetime
import numpy as np
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateExternalTableOperator,
)
from include.brick_insights_rating_request import main
from include.retrieve_sets_by_year import query_bigquery_table
from include.upload_to_gcs import upload_to_gcs_callable

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")
AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")

local_workflow = DAG(
    "BRICK_INSIGHTS_LEGO_SET_RATINGS",
    schedule_interval="0 8 * * 1",  # Run the DAG every Monday at 8:00 AM
    start_date=datetime(2024, 3, 1),
    end_date=datetime(2024, 3, 27),
    catchup=False,
    max_active_runs=1,  # Limits concurrent runs to 3
    default_args={"retries": 3},  # Set the number of retries to 3
    tags=["Lego Data"],
)


# Define the task to scrape LEGO ratings
def scrape_lego_ratings_task(**kwargs):
    task_instance = kwargs["task_instance"]
    set_numbers = task_instance.xcom_pull(task_ids="get_sets_by_year_task")
    main(set_numbers)  # Call the new scraping script


with local_workflow:

    get_sets_by_year_task = PythonOperator(
        task_id="get_sets_by_year_task",
        python_callable=query_bigquery_table,
        op_kwargs={
            "years": np.arange(1949, 2025).tolist()
        },  # Provide a list of years you want to get sets for
        dag=local_workflow,
    )

    scrape_lego_ratings_task = PythonOperator(
        task_id="scrape_lego_ratings_task",
        python_callable=scrape_lego_ratings_task,
        provide_context=True,
        dag=local_workflow,
    )

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

    cleanup_task = BashOperator(
        task_id="cleanup_task",
        bash_command=f"rm -f {AIRFLOW_HOME}/brick_insights_ratings_and_reviews.parquet",
    )

    # Define the task dependencies
    (
        get_sets_by_year_task
        >> scrape_lego_ratings_task
        >> local_to_gcs_task
        >> external_table_task
        >> cleanup_task
    )
