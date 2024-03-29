import os
import requests
from bs4 import BeautifulSoup
import re
from datetime import datetime


def download_files_callable(url, file_names, airflow_home_directory):
    """
    Download files from a webpage based on the anchor text.

    Args:
    - url (str): URL of the webpage containing the files.
    - file_names (list of str): List of file names to be downloaded.
    """
    response = requests.get(url)

    if response.status_code == 200:
        soup = BeautifulSoup(response.content, "html.parser")

        # Iterate over each file name and download the corresponding file
        for file_name in file_names:
            # Find all anchor elements containing the given file name
            anchor_tags = soup.find_all(
                "a", text=re.compile(rf"{file_name}\.csv\.gz", re.IGNORECASE)
            )

            if anchor_tags:
                # Get the link (href) behind the anchor text
                file_link = anchor_tags[0]["href"]

                # Download the file
                file_response = requests.get(file_link)

                # Check if the file download was successful (status code 200)
                if file_response.status_code == 200:
                    # Save the file with timestamp appended to the file name
                    file_name_with_timestamp = f"{file_name}.csv.gz"
                    file_path = os.path.join(
                        airflow_home_directory, file_name_with_timestamp
                    )
                    print(airflow_home_directory)
                    print(file_name_with_timestamp)
                    print(file_path)
                    with open(file_path, "wb") as f:
                        f.write(file_response.content)
                    print(f"File '{file_name_with_timestamp}' downloaded successfully.")
                else:
                    print(f"Failed to download the file '{file_name}'.")
            else:
                print(f"Anchor text '{file_name}.csv.gz' not found on the webpage.")
    else:
        print("Failed to retrieve webpage.")
