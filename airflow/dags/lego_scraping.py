import aiohttp
import asyncio
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
from tenacity import retry, stop_after_attempt, wait_fixed
import os

AIRFLOW_HOME = os.getenv("AIRFLOW_HOME")
print(f"AIRFLOW_HOME: {AIRFLOW_HOME}")


@retry(stop=stop_after_attempt(3), wait=wait_fixed(10))
async def fetch_rating(session, lego_set):
    url = f"https://www.lego.com/en-us/product/{lego_set}"
    print(f"Fetching rating for set {lego_set}...")
    try:
        async with session.get(url) as response:
            if response.status == 200:
                print(f"Successfully retrieved the page for set {lego_set}.")
                html = await response.text()
                soup = BeautifulSoup(html, "html.parser")
                script_tag = soup.find("script", string=lambda x: "rating" in str(x))
                if script_tag:
                    json_data = str(script_tag)
                    start_index = json_data.find('"rating":') + len('"rating":')
                    end_index = json_data.find(",", start_index)
                    rating = json_data[start_index:end_index]
                    print(f"Rating for set {lego_set}: {rating}")
                    return {
                        "set_num": lego_set,
                        "rating": rating,
                        "fetched_at": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    }
                else:
                    print(f"Rating not found for set {lego_set}.")
                    return {
                        "set_num": lego_set,
                        "rating": None,
                        "fetched_at": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    }
            else:
                print(
                    f"Failed to retrieve the page for set {lego_set}. Status code: {response.status}"
                )
                return {
                    "set_num": lego_set,
                    "rating": None,
                    "fetched_at": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                }
    except asyncio.TimeoutError:
        print(f"Timeout occurred for set {lego_set}. Skipping...")
        return {"set_num": lego_set, "rating": None, "fetched_at": datetime.now().strftime('%Y-%m-%d %H:%M:%S')}


async def main(lego_sets):
    async with aiohttp.ClientSession() as session:
        tasks = [fetch_rating(session, lego_set) for lego_set in lego_sets]
        results = await asyncio.gather(*tasks)
        # Filter out results where rating is None (indicating timeout occurred)
        results = [result for result in results if result["rating"] is not None]
        if results:
            # Create dataframe from results
            df = pd.DataFrame(results)
            print("\nScraping results:")
            print(df)

            # Write the dataframe to a Parquet file in the AIRFLOW_HOME directory
            parquet_file_path = os.path.join(AIRFLOW_HOME, "lego_ratings.parquet")
            df.to_parquet(parquet_file_path, index=False)
            print(f"DataFrame saved to {parquet_file_path}")
        else:
            print("No ratings were fetched due to timeouts.")


if __name__ == "__main__":
    lego_sets = ["10327", "10294", "10330"]  # Define your LEGO sets here
    print("Starting web scraping...")
    asyncio.run(main(lego_sets))
