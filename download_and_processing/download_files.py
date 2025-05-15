from datetime import datetime
from urllib.request import urlopen
from bs4 import BeautifulSoup
from urllib.error import HTTPError
from config import get_logger
import os
import requests
import json
import tarfile
import logging
import shutil
import wget


# logger.basicConfig(
#     filename="logs/data.log",
#     level=logger.INFO,
#     format="%(asctime)s - %(levelname)s - %(message)s",
# )
logger = get_logger(__name__)

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
                logger.warning(f"File {data_history_path} is empty or invalid.")
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
        logger.warning(f"Folder {download_folder} does not exist")
        return
    if not os.listdir(download_folder):
        logger.warning(f"Folder {download_folder} is empty")
        return
    for file_name in os.listdir(download_folder):
        if file_name.endswith(".tar.gz"):
            logger.info(f"Found {file_name}")
            file_path = os.path.join(download_folder, file_name)
            logger.info(f"Extracting {file_path}")
            try:
                with tarfile.open(file_path, "r:gz") as tar:
                    tar.extractall(path=download_folder)
                os.remove(file_path)
                logger.info(f"Removed {file_path}")
            except Exception as e:
                logger.error(f"Error extracting or removing {file_path}: {e}")


def download_files(config_file_path: str, data_history_path: str):
    """
    Downloads and processes files according to a configuration file and maintains a history in a history file.

    This function reads a configuration file listing files or folders to download.
    It handles different types of downloads and updates a history log after each operation.

    Args:
        config_file_path (str): Path to the JSON configuration file.
        data_history_path (str): Path to the JSON history file.

    Raises:
        Exception: Errors are logged.
    """

    config = load_config(config_file_path=config_file_path)
    log = load_data_history(data_history_path=data_history_path)

    for file_name, attributes in config.items():
        if attributes.get("type") == "dila_folder":
            url = attributes.get("download_url", "")
            download_folder = attributes.get("download_folder", "")
            last_downloaded_file = log.get(file_name).get("last_downloaded_file", "")

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
                    logger.info(f"There is no freemium file in {url}")

                logger.info(
                    f"{len(tar_gz_files)} tar.gz files found in {url}: {tar_gz_files}"
                )

                # Ensure the download folder exists
                os.makedirs(download_folder, exist_ok=True)

                if last_downloaded_file in tar_gz_files:
                    last_file_index = tar_gz_files.index(last_downloaded_file)
                    logger.info(
                        f"Last downloaded file is {last_downloaded_file} according to the data history"
                    )
                else:
                    last_file_index = -1

                if last_file_index == len(tar_gz_files) - 1:
                    logger.info("No new files to download")
                else:
                    for filename in tar_gz_files[
                        last_file_index + 1 :
                    ]:  # As we already downloaded the last file, we start from the next file
                        file_url = url + filename
                        download_path = os.path.join(download_folder, filename)
                        logger.info(f"Downloading {file_url} to {download_folder}")

                        file_response = requests.get(file_url)
                        file_response.raise_for_status()
                        with open(download_path, "wb") as file:
                            file.write(file_response.content)
                        logger.info(
                            f"Successfully downloaded {filename} to {download_folder}"
                        )

                    # Update the last download file and date in the log
                    log[file_name] = {
                        "last_downloaded_file": filename,
                        "last_download_date": datetime.now().strftime(
                            "%d-%m-%Y %H:%M:%S.%f"
                        ),
                    }

                    with open(data_history_path, "w") as file:
                        json.dump(log, file, indent=4)
                    logger.info(
                        f"Log config file successfully updated to {data_history_path}"
                    )

                extract_and_remove_tar_files(download_folder)
            except Exception as e:
                logger.error(f"Error downloading files: {e}")

        elif file_name.lower().endswith("directory"):
            storage_dir = attributes.get("download_folder", "")
            os.makedirs(storage_dir, exist_ok=True)
            old_files = os.listdir(storage_dir)

            logger.info(f"downloading {file_name} archive...")
            url = requests.head(attributes["download_url"], allow_redirects=True).url
            info = urlopen(url).info()
            file = info.get_filename() if info.get_filename() else os.path.basename(url)

            try:
                wget.download(
                    attributes["download_url"], os.path.join(storage_dir, file)
                )
            except Exception as e:
                logger.error(f"Error downloading files: {e}")

            logger.info(f"unpacking {file_name} archive...")
            shutil.unpack_archive(os.path.join(storage_dir, file), storage_dir)

            logger.info(f"deleting {file_name} archive...")
            os.remove((os.path.join(storage_dir, file)))

            new_files = [x for x in os.listdir(storage_dir) if x not in old_files]
            logger.debug(f"new files: {new_files}")

            for new_file in new_files:
                if not file.endswith(".json"):
                    logger.debug(f"deleting {new_file}...")
                    os.remove(os.path.join(storage_dir, new_file))

                else:
                    logger.debug(f"renaming {new_file} to {file_name}...")
                    os.rename(
                        os.path.join(storage_dir, new_file),
                        os.path.join(storage_dir, f"{file_name}.json"),
                    )

                    # Update the last download file and date in the log
                    log[file_name] = {
                        "last_downloaded_file": new_file,
                        "last_download_date": datetime.now().strftime(
                            "%d-%m-%Y %H:%M:%S.%f"
                        ),
                    }

                    with open(data_history_path, "w") as file:
                        json.dump(log, file, indent=4)
                    logger.info(
                        f"Log config file successfully updated to {data_history_path}"
                    )

            logger.info("Directories successfully downloaded")

        else: # Mainly for sheets files 
            # Downloading the rest of the files (fork of the pyalbert.corpus.download_rag_sources function)
            storage_dir = attributes.get("download_folder", "")

            # create the storage path if it does not exist
            os.makedirs(storage_dir, exist_ok=True)
            target = f"{storage_dir}/{file_name}"
            filename_tmp = f"{storage_dir}/temp_{file_name}"

            logger.info(f"Downloading '{file_name}'...")
            if os.path.exists(filename_tmp):
                os.remove(filename_tmp)
            try:
                old_name = wget.download(
                    attributes.get("download_url"), out=storage_dir
                )
                shutil.move(old_name, filename_tmp)
            except HTTPError as err:
                logger.error(f"Error: {err}")
                logger.error(
                    f"Failed to fetch source {file_name} from {file_name['url']}"
                )
                continue

            url = requests.head(attributes["download_url"], allow_redirects=True).url
            info = urlopen(url).info()
            content_type = info.get_content_type().split("/")[-1]
            if content_type in ["zip"]:  # List can be extended with other formats
                if os.path.exists(target):
                    shutil.rmtree(target)
                shutil.unpack_archive(
                    filename_tmp, extract_dir=target, format=content_type
                )
            else:
                target = f"{target}.{old_name.split('.')[-1]}"
                shutil.move(filename_tmp, target) # Renaming the file with the correct extension

            # Update the last download file and date in the log
            log[file_name] = {
                "last_downloaded_file": f"temp_{file_name}",
                "last_download_date": datetime.now().strftime("%d-%m-%Y %H:%M:%S.%f"),
            }

            with open(data_history_path, "w") as file:
                json.dump(log, file, indent=4)
            logger.info(f"Log config file successfully updated to {data_history_path}")

            logger.info(f"Corpus files {file_name} successfuly downloaded")


# def download_files(config_file_path: str, data_history_path: str):
#     """
#     Downloads, unpacks, and processes data files as specified in a configuration file.

#     This function performs the following steps:
#     1. Loads configuration and data history from the specified file paths.
#     2. Cleans and prepares download directories as defined in the configuration.
#     3. Downloads and unpacks archives for each 'directory' type data file, retaining only JSON files and renaming them appropriately.
#     4. For each entry in the "DILA" section of the configuration:
#         - Scrapes the download page for available ".tar.gz" files.
#         - Prioritizes downloading "freemium" files if present.
#         - Downloads new archives not previously downloaded, updates the data history log, and extracts the archives.
#     5. Handles errors and logs progress throughout the process.P

#     Args:
#         config_file_path (str): Path to the configuration JSON file specifying download URLs and directories.
#         data_history_path (str): Path to the JSON file maintaining download history for incremental updates.

#     Raises:
#         Exception: Logs and handles any exceptions that occur during the download or extraction process.
#     """

#     config = load_config(config_file_path=config_file_path)
#     log = load_data_history(data_history_path=data_history_path)

#     # Cleaning directories folder once before processing
#     for file_name, attributes in config["directories"].items():
#         storage_dir = attributes.get("download_folder", "")
#         if os.path.exists(storage_dir):
#             shutil.rmtree(storage_dir)
#         os.makedirs(storage_dir, exist_ok=True)
#         logger.info(f"Directory {storage_dir} cleaned...")

#     for file_name, attributes in config["directories"].items():
#         old_files = os.listdir(storage_dir)

#         logger.info(f"downloading {file_name} archive...")
#         url = requests.head(attributes["download_url"], allow_redirects=True).url
#         info = urlopen(url).info()
#         file = info.get_filename() if info.get_filename() else os.path.basename(url)

#         try:
#             wget.download(attributes["download_url"], os.path.join(storage_dir, file))
#         except Exception as e:
#             logger.error(f"Error downloading files: {e}")

#         logger.info(f"unpacking {file_name} archive...")
#         shutil.unpack_archive(os.path.join(storage_dir, file), storage_dir)

#         logger.info(f"deleting {file_name} archive...")
#         os.remove((os.path.join(storage_dir, file)))

#         new_files = [x for x in os.listdir(storage_dir) if x not in old_files]
#         logger.debug(f"new files: {new_files}")

#         for file in new_files:
#             if not file.endswith(".json"):
#                 logger.debug(f"deleting {file}...")
#                 os.remove(os.path.join(storage_dir, file))

#             else:
#                 logger.debug(f"renaming {file} to {file_name}...")
#                 os.rename(
#                     os.path.join(storage_dir, file),
#                     os.path.join(storage_dir, f"{file_name}.json"),
#                 )

#         logger.info("Directories successfully downloaded")

#     for key, data in config["DILA"].items():
#         url = config["DILA"].get(key, {}).get("download_url", "")
#         download_folder = config["DILA"].get(key, {}).get("download_folder", "")
#         last_downloaded_file = log.get(key, {}).get("last_downloaded_file", "")

#         try:
#             response = requests.get(url)
#             response.raise_for_status()

#             # Parse the HTML content using BeautifulSoup
#             soup = BeautifulSoup(response.text, "html.parser")

#             # Find all links that end with ".tar.gz"
#             links = soup.find_all("a", href=True)

#             tar_gz_files = sorted(
#                 [link["href"] for link in links if link["href"].endswith(".tar.gz")]
#             )
#             # Placing the freemium file at the beginning
#             try:
#                 freemium_file = next(
#                     (
#                         file
#                         for file in tar_gz_files
#                         if file.lower().startswith("freemium")
#                     ),
#                     None,
#                 )
#                 tar_gz_files.remove(freemium_file)
#                 tar_gz_files.insert(0, freemium_file)
#             except ValueError:
#                 logger.info(f"There is no freemium file in {url}")

#             logger.info(
#                 f"{len(tar_gz_files)} tar.gz files found in {url}: {tar_gz_files}"
#             )

#             # Ensure the download folder exists
#             os.makedirs(download_folder, exist_ok=True)

#             if last_downloaded_file in tar_gz_files:
#                 last_file_index = tar_gz_files.index(last_downloaded_file)
#                 logger.info(
#                     f"Last downloaded file is {last_downloaded_file} according to the data history"
#                 )
#             else:
#                 last_file_index = -1

#             if last_file_index == len(tar_gz_files) - 1:
#                 logger.info("No new files to download")
#             else:
#                 for file_name in tar_gz_files[
#                     last_file_index + 1 :
#                 ]:  # As we already downloaded the last file, we start from the next file
#                     file_url = url + file_name
#                     download_path = os.path.join(download_folder, file_name)
#                     logger.info(f"Downloading {file_url} to {download_folder}")

#                     file_response = requests.get(file_url)
#                     file_response.raise_for_status()
#                     with open(download_path, "wb") as file:
#                         file.write(file_response.content)
#                     logger.info(
#                         f"Successfully downloaded {file_name} to {download_folder}"
#                     )

#                 # Update the last download file and date in the log
#                 log[key] = {
#                     "last_downloaded_file": file_name,
#                     "last_download_date": datetime.now().strftime(
#                         "%d-%m-%Y %H:%M:%S.%f"
#                     ),
#                 }

#                 with open(data_history_path, "w") as file:
#                     json.dump(log, file, indent=4)
#                 logger.info(
#                     f"Log config file successfully updated to {data_history_path}"
#                 )

#             extract_and_remove_tar_files(download_folder)
#         except Exception as e:
#             logger.error(f"Error downloading files: {e}")
