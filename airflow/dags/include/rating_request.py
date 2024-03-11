import requests
import pandas as pd
from datetime import datetime
import os

# Define the BrickInsights API endpoint
API_ENDPOINT = "https://brickinsights.com/api/sets/{}"

AIRFLOW_HOME = os.getenv("AIRFLOW_HOME")
print(f"AIRFLOW_HOME: {AIRFLOW_HOME}")


def fetch_rating(set_id, timeout=10):  # Set the timeout to 10 seconds by default
    url = API_ENDPOINT.format(set_id)
    print(f"Fetching rating for set {set_id}...")
    try:
        response = requests.get(url, timeout=timeout)  # Set the timeout here
        if response.status_code == 200:
            print(f"Successfully retrieved the rating for set {set_id}.")
            rating_data = response.json()
            average_rating = rating_data.get("average_rating", None)
            review_count = rating_data.get("review_count", None)
            fetched_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            print(f"Average rating: {average_rating}, across {review_count} reviews.")
            return {
                "set_num": set_id,
                "rating": average_rating,
                "review_count": review_count,
                "fetched_at": fetched_at,
            }
        else:
            print(
                f"Failed to retrieve the rating for set {set_id}. Status code: {response.status_code}"
            )
            fetched_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            return {
                "set_num": set_id,
                "rating": None,
                "review_count": None,
                "fetched_at": fetched_at,
            }
    except requests.exceptions.Timeout:
        print(f"Request timed out for set {set_id}. Skipping...")
        fetched_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        return {
            "set_num": set_id,
            "rating": None,
            "review_count": None,
            "fetched_at": fetched_at,
        }
    except requests.exceptions.RequestException as e:
        print(f"An error occurred: {str(e)}")
        fetched_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        return {
            "set_num": set_id,
            "rating": None,
            "review_count": None,
            "fetched_at": fetched_at,
        }


def main(lego_sets):
    results = []
    total_sets = len(lego_sets)
    for idx, set_id in enumerate(lego_sets, start=1):
        print(f"Processing set {idx} of {total_sets}")
        result = fetch_rating(set_id)
        results.append(result)

    # Create dataframe from results
    df = pd.DataFrame(results)
    print("\nScraping results:")
    print(df)

    parquet_file_path = os.path.join(AIRFLOW_HOME, "brick_insights_set_data.parquet")
    df.to_parquet(parquet_file_path, index=False)
    print(f"DataFrame saved to {parquet_file_path}")


if __name__ == "__main__":
    lego_sets = [
        "71741-1",
        "40450-1",
        "42116-1",
        "77048",
        "77047",
        "77046",
    ]  # Define your LEGO sets here
    print("Starting web scraping...")
    main(lego_sets)
