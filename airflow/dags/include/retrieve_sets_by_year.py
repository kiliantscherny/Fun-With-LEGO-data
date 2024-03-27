from google.cloud import bigquery
import pandas as pd


def query_bigquery_table(years):
    # Initialize a BigQuery client
    client = bigquery.Client(project="dtc-de-kilian")

    # Convert list of years to a string with comma-separated values
    years_to_query = ", ".join([str(year) for year in years])

    # Execute the query for all years at once
    query = f"SELECT set_num AS set_number FROM `dtc-de-kilian.lego_raw.sets` WHERE year IN ({years_to_query})"
    query_job = client.query(query)

    # Fetch the results
    results = query_job.result()

    # Convert the results to a DataFrame
    df = pd.DataFrame(
        data=[row.values() for row in results],
        columns=[field.name for field in results.schema],
    )

    # If DataFrame is not empty, then show a success message, or else say it failed
    if not df.empty:
        print(f"Sets retrieved successfully for years: {years_to_query}.")
    else:
        print(f"Failed to retrieve sets for years: {years_to_query}.")

    return df["set_number"].tolist()


# Example usage
years = list(range(2000, 2025))  # Define years range
set_numbers = query_bigquery_table(years)
print(set_numbers)
