from abc import ABC, abstractmethod
import os
from src.utils.data_helpers import download_file
from src.schemas.download.table import download_record_table
from src.schemas.download.models import DownloadRecordCreate
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
        record = download_record_table.get_record_by_url(url=url)
        if record:
            logger.info(f"Record for URL {url} already exists in the database.")
            return
        download_file(url, destination_path)
        record_create = DownloadRecordCreate(
            file_name=destination_path,
            url=url,
        )
        download_record_table.create_record(
            file_name=record_create.file_name,
            url=record_create.url,
        )

    def download_all(self, max_download: int = -1):
        urls = self.get_urls()
        if max_download > 0:
            urls = urls[:max_download]
        for url in tqdm.tqdm(urls):
            filename = url.split("/")[-1]

            destination_path = f"{self.folder_download}/{filename}"
            logger.info(f"Downloading {filename} to {destination_path}")
            self.download(url, destination_path)
            logger.info(f"Downloaded {filename} successfully.")
