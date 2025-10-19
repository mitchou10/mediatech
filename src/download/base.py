from abc import ABC, abstractmethod
import os
from src.utils.data_helpers import download_file
import tqdm
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class BaseDownloader(ABC):
    def __init__(self, config_loader: dict, folder_download: str):
        self.config_loader = config_loader
        self.folder_download = folder_download
        os.makedirs(folder_download, exist_ok=True)

    @abstractmethod
    def get_urls(self) -> list[str]: ...

    def download(self, url: str, destination_path: str):
        download_file(url, destination_path)

    def download_all(self):
        urls = self.get_urls()
        for url in tqdm.tqdm(urls):
            filename = url.split("/")[-1]

            destination_path = f"{self.folder_download}/{filename}"
            if os.path.exists(destination_path):
                logger.info(
                    f"File {destination_path} already exists. Skipping download."
                )
                continue
            logger.info(f"Downloading {filename} to {destination_path}")
            self.download(url, destination_path)
            logger.info(f"Downloaded {filename} successfully.")
