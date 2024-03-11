import aiohttp
import asyncio
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
import random
from tenacity import retry, stop_after_attempt, wait_fixed

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

# Semaphore to limit concurrent requests
semaphore = asyncio.Semaphore(3)  # Adjust the number as needed


@retry(stop=stop_after_attempt(3), wait=wait_fixed(30))
async def fetch_rating(session, lego_set):
    # Extracting the set number by splitting at the hyphen
    set_num = lego_set.split("-")[0]

    url = f"https://www.lego.com/en-us/product/{set_num}"
    print(f"Fetching rating and price for set {set_num}...")
    try:
        headers = {"User-Agent": random.choice(USER_AGENTS)}
        async with semaphore:  # Acquire the semaphore
            async with session.get(
                url, headers=headers, timeout=30
            ) as response:  # Set a timeout
                if response.status == 200:
                    print(f"Successfully retrieved the page for set {set_num}.")
                    html = await response.text()
                    soup = BeautifulSoup(html, "html.parser")
                    # Parsing rating...
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

                    # Parsing price and currency...
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


async def get_price_and_rating_callable(lego_sets):
    async with aiohttp.ClientSession() as session:
        tasks = [fetch_rating(session, lego_set) for lego_set in lego_sets]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Filter out exceptions from the results
        valid_results = [r for r in results if not isinstance(r, Exception)]

        if valid_results:
            # Create dataframe from valid results
            df = pd.DataFrame(valid_results)
            print("\nScraping results:")
            print(df)

            # Write the dataframe to a Parquet file
            parquet_file_path = "lego_website_set_data.parquet"
            df.to_parquet(parquet_file_path, index=False)
            print(f"DataFrame saved to {parquet_file_path}")
        else:
            print("No valid ratings were fetched due to timeouts or errors.")


if __name__ == "__main__":
    lego_sets = [
        "31155-1",
        "9780241657621-1",
        "40708-1",
        "12345",
        "67890",
        "45678",
        "2435678",
        "34567879786",
        "8793864-1",
    ]  # Define your LEGO sets here
    print("Starting web scraping...")
    asyncio.run(get_price_and_rating_callable(lego_sets))
