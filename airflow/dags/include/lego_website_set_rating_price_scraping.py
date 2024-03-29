import os
import aiohttp
import asyncio
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
import random
from tenacity import retry, wait_exponential

# Default value for AIRFLOW_HOME if not set
AIRFLOW_HOME = os.getenv("AIRFLOW_HOME", ".")

# List of user agents to rotate
USER_AGENTS = [
    # Mozilla Firefox
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:96.0) Gecko/20100101 Firefox/96.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:96.0) Gecko/20100101 Firefox/96.0",
    "Mozilla/5.0 (X11; Linux x86_64; rv:96.0) Gecko/20100101 Firefox/96.0",
    # Google Chrome
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.110 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.110 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.110 Safari/537.36",
    # Safari
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.2 Safari/605.1.15",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 15_2 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.2 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (iPad; CPU OS 15_2 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.2 Mobile/15E148 Safari/604.1",
    # Edge
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.110 Safari/537.36 Edg/96.0.1054.57",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.110 Safari/537.36 Edg/96.0.1054.57",
    # Opera
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.110 Safari/537.36 OPR/85.0.1941.60",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.110 Safari/537.36 OPR/85.0.1941.60",
]

# Semaphore to limit concurrent requests. May help alleviate 429 errors.
semaphore = asyncio.Semaphore(1)  # Adjust the number as needed


@retry(wait=wait_exponential(multiplier=1, min=5, max=15))
async def fetch_rating(session, lego_set, counter):
    """
    Fetch the rating and price for a given LEGO set from the LEGO website.

    Args:
    - session (aiohttp.ClientSession): The aiohttp session object.
    - lego_set (str): The set number of the LEGO set.
    - counter (int): The counter of times the current set has been processed.
    """
    # Extracting the "clean" set number by splitting at the hyphen. Necessary to avoid 404 errors.
    set_num = lego_set.split("-")[0]

    url = f"https://www.lego.com/en-us/product/{set_num}"
    try:
        headers = {"User-Agent": random.choice(USER_AGENTS)}
        async with semaphore:
            await asyncio.sleep(2)  # Pause for 2 seconds
            async with session.get(
                url, headers=headers, timeout=10
            ) as response:
                print(f"Processing {counter} of {len(lego_sets)}")
                if response.status == 200:
                    print(f"Successfully retrieved the page for set {set_num}.")
                    html = await response.text()
                    soup = BeautifulSoup(html, "html.parser")
                    script_tag = soup.find(
                        "script", string=lambda x: "rating" in str(x)
                    )
                    rating = None
                    if script_tag:
                        json_data = str(script_tag)
                        start_index = json_data.find('"rating":') + len('"rating":')
                        end_index = json_data.find(",", start_index)
                        rating = json_data[start_index:end_index]
                        print(f"Rating for set {set_num}: {rating}")

                    price_tag = soup.find("meta", {"property": "product:price:amount"})
                    currency_tag = soup.find(
                        "meta", {"property": "product:price:currency"}
                    )
                    price = price_tag["content"] if price_tag else None
                    currency = currency_tag["content"] if currency_tag else None
                    print(f"Price for set {set_num}: {price} {currency}")

                    return {
                        "original_set_num": lego_set,
                        "set_num": set_num,
                        "rating": rating,
                        "price": price,
                        "currency": currency,
                        "fetched_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        "error_reason": None,  # No error reason
                    }
                elif response.status == 404:
                    print(f"Page not found for set {set_num}.")
                    return {
                        "original_set_num": lego_set,
                        "set_num": set_num,
                        "rating": None,
                        "price": None,
                        "currency": None,
                        "fetched_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        "error_reason": "Page not found",
                    }
                else:
                    print(
                        f"Failed to retrieve the page for set {set_num}. Status code: {response.status}"
                    )
                    return {
                        "original_set_num": lego_set,
                        "set_num": set_num,
                        "rating": None,
                        "price": None,
                        "currency": None,
                        "fetched_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        "error_reason": "Unknown error",
                    }
    except asyncio.TimeoutError:
        print(f"Timeout occurred for set {set_num}. Skipping...")
        return {
            "original_set_num": lego_set,
            "set_num": set_num,
            "rating": None,
            "price": None,
            "currency": None,
            "fetched_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "error_reason": "Page took too long to load",
        }
    except aiohttp.ClientError as e:
        print(f"Client error occurred for set {set_num}: {e}")
        return {
            "original_set_num": lego_set,
            "set_num": set_num,
            "rating": None,
            "price": None,
            "currency": None,
            "fetched_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "error_reason": "Client error",
        }


async def get_price_and_rating_callable(lego_sets, append_to_existing=False):
    """
    Asynchronously fetches the price and rating for a list of LEGO sets.
    
    Args:
    - lego_sets (list of str): List of LEGO set numbers.
    - append_to_existing (bool): Whether to append the new data to an existing Parquet file.
    """
    async with aiohttp.ClientSession() as session:
        counter = 0
        retry_counter = {lego_set: 0 for lego_set in lego_sets}
        while True:
            tasks = []
            for lego_set in lego_sets:
                counter += 1
                tasks.append(fetch_rating(session, lego_set, counter))
            results = await asyncio.gather(
                *tasks, return_exceptions=True
            )

            valid_results = []
            retry_sets = []
            for lego_set, result in zip(lego_sets, results):
                if isinstance(result, Exception):
                    retry_counter[lego_set] += 1
                    if retry_counter[lego_set] <= 3:
                        retry_sets.append(lego_set)
                elif (
                    result.get("price") is None
                    or result.get("currency") is None
                    or result.get("rating") is None
                ):
                    retry_counter[lego_set] += 1
                    if retry_counter[lego_set] <= 3:
                        retry_sets.append(lego_set)
                else:
                    valid_results.append(result)

            if valid_results:
                # Create dataframe from valid results
                df = pd.DataFrame(valid_results)
                if append_to_existing:
                    existing_file = os.path.join(
                        AIRFLOW_HOME, "lego_website_set_prices_and_ratings.parquet"
                    )
                    if os.path.exists(existing_file):
                        existing_df = pd.read_parquet(existing_file)
                        df = pd.concat([existing_df, df], ignore_index=True)

                print("\nScraping results:")
                print(df)

                # Write the dataframe to a Parquet file
                parquet_file_path = os.path.join(
                    AIRFLOW_HOME, "lego_website_set_prices_and_ratings.parquet"
                )
                df.to_parquet(parquet_file_path, index=False)
                print(f"DataFrame saved to {parquet_file_path}")
            else:
                print("No ratings were fetched due to timeouts or errors.")

            if not retry_sets:
                break

            print(f"Retrying failed sets after delay...")
            await asyncio.sleep(20)  # Wait for 20 seconds before retrying
            lego_sets = retry_sets  # Retry only the failed sets

    print("All sets processed.")

# For testing purposes
if __name__ == "__main__":
    lego_sets = [
        "792009-1",
        "80006-1",
        "80007-1",
        "912404-1",
        "92176-1",
        "9789030508595-1",
        "9789030509097-1",
    ]  # Define your LEGO sets here
    print("Starting web scraping...")
    asyncio.run(get_price_and_rating_callable(lego_sets, append_to_existing=True))
