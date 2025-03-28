import requests
from bs4 import BeautifulSoup
import os
import json
from datetime import datetime
import tarfile
import logging

logging.basicConfig(
    filename="logs/data.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)


def load_config(config_file_path: str):
    with open(config_file_path, "r") as file:
        return json.load(file)


def load_data_history(data_history_path: str):
    if os.path.exists(data_history_path):
        with open(data_history_path, "r") as file:
            return json.load(file)
    else:
        with open(data_history_path, "w") as file:
            json.dump({}, file)
        return {}


def extract_and_remove_tar_files(download_folder: str):
    if not os.path.exists(download_folder):
        logging.warning(f"Folder {download_folder} does not exist")
        return
    if not os.listdir(download_folder):
        logging.warning(f"Folder {download_folder} is empty")
        return
    for file_name in os.listdir(download_folder):
        if file_name.endswith(".tar.gz"):
            logging.info(f"Found {file_name}")
            file_path = os.path.join(download_folder, file_name)
            logging.info(f"Extracting {file_path}")
            try:
                with tarfile.open(file_path, "r:gz") as tar:
                    tar.extractall(path=download_folder)
                os.remove(file_path)
                logging.info(f"Removed {file_path}")
            except Exception as e:
                logging.error(f"Error extracting or removing {file_path}: {e}")


def download_files(config_file_path: str, data_history_path: str):
    config = load_config(config_file_path=config_file_path)
    log = load_data_history(data_history_path=data_history_path)

    for key, data in config.items():
        url = config.get(key, {}).get("download_url", "")
        download_folder = config.get(key, {}).get("download_folder", "")
        last_downloaded_file = log.get(key, {}).get("last_downloaded_file", "")

        try:
            response = requests.get(url)
            response.raise_for_status()

            # Parse the HTML content using BeautifulSoup
            soup = BeautifulSoup(response.text, "html.parser")

            # Find all links that end with ".tar.gz"
            links = soup.find_all("a", href=True)
            tar_gz_files = [
                link["href"] for link in links if link["href"].endswith(".tar.gz")
            ]
            logging.info(
                f"{len(tar_gz_files)} tar.gz files found in {url}: {tar_gz_files}"
            )

            # Ensure the download folder exists
            os.makedirs(download_folder, exist_ok=True)

            if last_downloaded_file in tar_gz_files:
                last_file_index = tar_gz_files.index(last_downloaded_file)
                logging.info(
                    f"Last downloaded file is {last_downloaded_file} according to the data history"
                )
            else:
                last_file_index = -1

            if last_file_index == len(tar_gz_files) - 1:
                logging.info("No new files to download")
            else:
                for file_name in tar_gz_files[
                    last_file_index + 1 :
                ]:  # As we already downloaded the last file, we start from the next file
                    file_url = url + file_name
                    download_path = os.path.join(download_folder, file_name)
                    logging.info(f"Downloading {file_url} to {download_folder}")

                    file_response = requests.get(file_url)
                    file_response.raise_for_status()
                    with open(download_path, "wb") as file:
                        file.write(file_response.content)
                    logging.info(
                        f"Successfully downloaded {file_name} to {download_folder}"
                    )

                # Update the last download file and date in the log
                log[key] = {
                    "last_downloaded_file": file_name,
                    "last_download_date": datetime.now().strftime(
                        "%d-%m-%Y %H:%M:%S.%f"
                    ),
                }

                with open(data_history_path, "w") as file:
                    json.dump(log, file, indent=4)
                logging.info(
                    f"Log config file successfully updated to {data_history_path}"
                )

            extract_and_remove_tar_files(download_folder)
        except Exception as e:
            logging.error(f"Error downloading files: {e}")
