import os
import requests
import pandas as pd
from datetime import datetime
from tenacity import retry, wait_fixed, stop_after_attempt
import time

# Define the BrickInsights API endpoint
API_ENDPOINT = "https://brickinsights.com/api/sets/{}"

# Default value for AIRFLOW_HOME if not set
AIRFLOW_HOME = os.getenv("AIRFLOW_HOME", ".")

# Global variable to store sets that encountered 429 error
retry_sets = []


@retry(wait=wait_fixed(60), stop=stop_after_attempt(3))
def fetch_rating(set_id, timeout=10):  # Set the timeout to 10 seconds by default
    url = API_ENDPOINT.format(set_id)
    print("\033[36m" + f"🔍 Looking for ratings from set {set_id}." + "\033[0m")
    try:
        response = requests.get(url, timeout=timeout)  # Set the timeout here

        # Introduce a 3-second pause between requests
        time.sleep(1)

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
                        "fetched_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"),
                    }
                )
            return review_info
        elif response.status_code == 429:
            print(
                "\033[93m"
                + f"❗️ Rate limit exceeded for set {set_id}. Retrying after delay...\n"
                + "\033[0m"
            )
            retry_sets.append(set_id)
            return []
        else:
            print(
                "\033[91m"
                + f"❗️ Failed to retrieve the rating for set {set_id}. Status code: {response.status_code} \n"
                + "\033[0m"
            )
            return []
    except requests.exceptions.Timeout:
        print(
            "\033[91m"
            + f"⏳ Request timed out for set {set_id}. Skipping...\n"
            + "\033[0m"
        )
        return []
    except requests.exceptions.RequestException as e:
        print("\033[91m" + f"⛔️ An error occurred: {str(e)}\n" + "\033[0m")
        return []


def main(lego_sets):
    results = []
    total_sets = len(lego_sets)
    for idx, set_id in enumerate(lego_sets, start=1):
        print("\033[1m" + f"Processing set {idx} of {total_sets}" + "\033[0m")
        review_info = fetch_rating(set_id)
        results.extend(review_info)

    # Retry failed sets if any
    retry_count = 1
    while retry_sets and retry_count <= 3:
        print(f"\n\033[1mRetry attempt {retry_count}\033[0m")
        failed_sets = retry_sets.copy()
        retry_sets.clear()
        for set_id in failed_sets:
            review_info = fetch_rating(set_id)
            results.extend(review_info)
        retry_count += 1

    # Create dataframe from results
    df = pd.DataFrame(results)
    print("\nAPI request results:")
    print(df)

    parquet_file_path = os.path.join(
        AIRFLOW_HOME, "brick_insights_ratings_reviews.parquet"
    )
    df.to_parquet(parquet_file_path, index=False)
    print(f"DataFrame saved to {parquet_file_path}")


if __name__ == "__main__":
    lego_sets = [
        "0003977811-1",
        "010423-1",
        "100603-1",
        "100871-1",
        "1010206-1",
    ]  # Define your LEGO sets here
    print("\033[95m" + "Starting to fetch set data..." + "\033[0m")
    main(lego_sets)
