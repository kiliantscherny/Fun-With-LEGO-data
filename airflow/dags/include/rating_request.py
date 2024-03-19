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
    print("\033[36m" + f"🔍 Looking for ratings from set {set_id}." + "\033[0m")
    try:
        response = requests.get(url, timeout=timeout)  # Set the timeout here
        if response.status_code == 200:
            rating_data = response.json()
            if "error" in rating_data and rating_data["error"] == "Set not found":
                print(
                    "\033[91m" + f"❌ Ratings for set {set_id} not found.\n" + "\033[0m"
                )
                return []
            print(
                "\033[92m"
                + f"✅ Successfully retrieved the rating for set {set_id}.\n"
                + "\033[0m"
            )
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
                "\033[91m"
                + f"❗️ Failed to retrieve the rating for set {set_id}. Status code: {response.status_code}"
                + "\033[0m"
            )
            return []
    except requests.exceptions.Timeout:
        print(
            "\033[91m"
            + f"⏳ Request timed out for set {set_id}. Skipping..."
            + "\033[0m"
        )
        return []
    except requests.exceptions.RequestException as e:
        print("\033[91m" + f"⛔️ An error occurred: {str(e)}" + "\033[0m")
        return []


def main(lego_sets):
    results = []
    total_sets = len(lego_sets)
    for idx, set_id in enumerate(lego_sets, start=1):
        print("\033[1m" + f"Processing set {idx} of {total_sets}" + "\033[0m")
        review_info = fetch_rating(set_id)
        results.extend(review_info)

    # Create dataframe from results
    df = pd.DataFrame(results)
    print("\nScraping results:")
    print(df)

    parquet_file_path = os.path.join(".", "brick_insights_reviews_data.parquet")
    df.to_csv(parquet_file_path, index=False)
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
    print("\033[95m" + "Starting web scraping..." + "\033[0m")
    main(lego_sets)
