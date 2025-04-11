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
    """
    Load and parse a JSON configuration file.

    Args:
        config_file_path (str): Path to the JSON configuration file to be loaded.

    Returns:
        dict: The parsed JSON content as a Python dictionary.

    Raises:
        FileNotFoundError: If the specified configuration file doesn't exist.
        json.JSONDecodeError: If the configuration file contains invalid JSON.
    """
    with open(config_file_path, "r") as file:
        return json.load(file)


def load_data_history(data_history_path: str):
    """
    Load the downloaded data history from a JSON file.
    This function attempts to read and parse a JSON file at the specified path.

    Parameters:
        data_history_path (str): Path to the JSON file containing the data history.

    Returns:
        dict: The data history as a dictionary. Returns an empty dictionary if the file
              does not exist, is empty, or contains invalid JSON.
    """
    if os.path.exists(data_history_path):
        with open(data_history_path, "r") as file:
            try:
                data = json.load(file)
                return data if data else {}
            except json.JSONDecodeError:
                logging.warning(f"File {data_history_path} is empty or invalid.")
                return {}
    else:
        with open(data_history_path, "w") as file:
            json.dump({}, file)
        return {}


def extract_and_remove_tar_files(download_folder: str):
    """
    Extracts and removes all `.tar.gz` files in the specified folder.

    This function checks if the given folder exists and is not empty. It then iterates
    through all files in the folder, identifies files with a `.tar.gz` extension, 
    extracts their contents into the same folder, and removes the original `.tar.gz` files.

    Args:
        download_folder (str): The path to the folder containing `.tar.gz` files.

    Logs:
        - A warning if the folder does not exist or is empty.
        - Information about each `.tar.gz` file found, extracted, and removed.
        - An error if there is an issue during extraction or removal.

    Raises:
        None: Any exceptions during extraction or removal are logged but not raised.
    """
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
    """
    Downloads .tar.gz files from URLs specified in the configuration file, tracks download history, 
    and extracts the downloaded archives.
    
    The function performs the following steps:
    1. Loads configuration and download history from the provided file paths
    2. For each entry in the configuration:
       - Retrieves the download URL and target folder
       - Fetches the webpage and parses it to find .tar.gz files
       - Prioritizes files with 'freemium' in the name
       - Checks which files have already been downloaded
       - Downloads only new files that are uploaded after the last one downloaded, according to the data history
       - Updates the data history after successful downloads
       - Extracts the downloaded .tar.gz files and removes the archives
    
    Args:
        config_file_path (str): Path to the configuration file containing download URLs and folders
        data_history_path (str): Path to the file tracking download history
        
    Raises:
        Various exceptions during web requests, file operations, or parsing
        All exceptions are caught, logged, and don't halt execution of subsequent downloads
    """
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

            tar_gz_files = sorted(
                [link["href"] for link in links if link["href"].endswith(".tar.gz")]
            )
            # Placing the freemium file at the beginning
            try:
                freemium_file = next(
                    (
                        file
                        for file in tar_gz_files
                        if file.lower().startswith("freemium")
                    ),
                    None,
                )
                tar_gz_files.remove(freemium_file)
                tar_gz_files.insert(0, freemium_file)
            except ValueError:
                logging.info(f"There is no freemium file in {url}")

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
