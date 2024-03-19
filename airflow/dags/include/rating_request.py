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
    print(f"🔍 Looking for ratings for set {set_id}.")
    try:
        response = requests.get(url, timeout=timeout)  # Set the timeout here
        if response.status_code == 200:
            rating_data = response.json()
            if "error" in rating_data and rating_data["error"] == "Set not found":
                print(f"❌ Ratings for set {set_id} not found.\n")
                return []
            print(f"✅ Successfully retrieved the rating for set {set_id}.\n")
            reviews = rating_data.get("reviews", [])
            review_info = []
            for review in reviews:
                review_url = review.get("review_url", None)
                snippet = review.get("snippet", None)
                review_amount = review.get("review_amount", None)
                rating_original = review.get("rating_original", None)
                rating_converted = review.get("rating_converted", None)
                author_name = review.get("author_name", None)
                review_info.append(
                    {
                        "set_num": set_id,
                        "review_url": review_url,
                        "snippet": snippet,
                        "review_amount": review_amount,
                        "rating_original": rating_original,
                        "rating_converted": rating_converted,
                        "author_name": author_name,
                    }
                )
            return review_info
        else:
            print(
                f"Failed to retrieve the rating for set {set_id}. Status code: {response.status_code}"
            )
            return []
    except requests.exceptions.Timeout:
        print(f"Request timed out for set {set_id}. Skipping...")
        return []
    except requests.exceptions.RequestException as e:
        print(f"An error occurred: {str(e)}")
        return []


def main(lego_sets):
    results = []
    total_sets = len(lego_sets)
    for idx, set_id in enumerate(lego_sets, start=1):
        print(f"Processing set {idx} of {total_sets}")
        review_info = fetch_rating(set_id)
        results.extend(review_info)

    # Create dataframe from results
    df = pd.DataFrame(results)
    print("\nScraping results:")
    print(df)

    parquet_file_path = os.path.join(".", "brick_insights_reviews_data.parquet")
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
