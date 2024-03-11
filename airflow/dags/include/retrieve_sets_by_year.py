from google.cloud import bigquery
import pandas as pd


def query_bigquery_table(years):
    # Initialize a BigQuery client
    client = bigquery.Client(project="dtc-de-kilian")

    set_numbers = []

    # Iterate over each year
    for year in years:
        # Execute the query
        query = f"SELECT set_num AS set_number FROM `dtc-de-kilian.lego_raw.sets` WHERE year = {year}"
        query_job = client.query(query)

        # Fetch the results
        results = query_job.result()

        # Convert the results to a DataFrame
        df = pd.DataFrame(
            data=[row.values() for row in results],
            columns=[field.name for field in results.schema],
        )

        # Append the set numbers for the current year to the list
        set_numbers.extend(df["set_number"].tolist())

        # If set_numbers is not empty, then show a success message, or else say it failed
        if set_numbers:
            print(f"Sets retrieved successfully for year {year}.")
        else:
            print(f"Failed to retrieve sets for year {year}.")

    return set_numbers
