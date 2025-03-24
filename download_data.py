import requests
from bs4 import BeautifulSoup
import os
import json
from datetime import datetime


def load_config(config_path):
    with open(config_path, "r") as file:
        return json.load(file)


def load_log(log_path):
    if os.path.exists(log_path):
        with open(log_path, "r") as file:
            return json.load(file)
    else:
        with open(log_path, "w") as file:
            json.dump({}, file)
        return {}

def download_files(config_file_path: str, log_file_path: str):
    config = load_config(config_file_path)
    log = load_log(log_file_path)

    for key, data in config.items():
        url = config.get(key, {}).get("download_url", "")
        download_folder = config.get(key, {}).get("download_folder", "")
        last_downloaded_file = log.get(key, {}).get("last_downloaded_file", "")

        # Fetch the HTML content of the page
        response = requests.get(url)
        response.raise_for_status()

        # Parse the HTML content using BeautifulSoup
        soup = BeautifulSoup(response.text, "html.parser")

        # Find all links that end with ".tar.gz"
        links = soup.find_all("a", href=True)
        tar_gz_files = [
            link["href"] for link in links if link["href"].endswith(".tar.gz")
         ]
        print(tar_gz_files[0])

        # Ensure the download folder exists
        os.makedirs(download_folder, exist_ok=True)

        # Download each file and save it locally
        if last_downloaded_file in tar_gz_files:
            last_file_index = tar_gz_files.index(last_downloaded_file)
            print(f"Last downloaded file is {last_file_index} and is found at index {last_file_index}")
        else:
            last_file_index = -1

        if (
            last_file_index == len(tar_gz_files) - 1
        ):  # If the last updated file was already downloaded
            print("No new files to download")
            continue
        else:
            for file_name in tar_gz_files[
                last_file_index + 1 :
            ]:  # As we already downloaded the last file, we start from the next file
                file_url = url + file_name
                download_path = os.path.join(download_folder, file_name)
                print(f"Downloading {file_url} to {download_folder}")

                file_response = requests.get(file_url)
                file_response.raise_for_status()
                with open(download_path, "wb") as file:
                    file.write(file_response.content)
                print(f"Successfully downloaded {file_name} to {download_folder}")

            # Update the last download file and date in the log
            log[key] = {
                "last_downloaded_file": file_name,
                "last_download_date": datetime.now().strftime("%d-%m-%Y %H:%M:%S.%f"),
            }

            with open(log_file_path, "w") as file:
                json.dump(log, file, indent=4)


if __name__ == "__main__":

    download_files(config_file_path="config.json", log_file_path="log.json")
