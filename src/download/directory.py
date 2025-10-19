from src.download.base import BaseDownloader
import logging
import os
import requests
import shutil
from urllib.request import urlopen

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class DirectoryDownloader(BaseDownloader):

    def __init__(self, config_loader: dict, folder_download: str):
        super().__init__(config_loader, folder_download)

    def get_urls(self) -> list[str]:

        url = requests.head(
            self.config_loader["download_url"], allow_redirects=True
        ).url
        info = urlopen(url).info()
        last_modified = info.get("Last-Modified")
        self.config_loader["last_modified"] = last_modified

        return [url]

    def get_latest_dir(self) -> str | None:
        filtered_dirs = [
            d
            for d in os.listdir(self.folder_download)
            if os.path.isdir(os.path.join(self.folder_download, d))
        ]
        if not filtered_dirs:
            return None
        latest_dir = sorted(filtered_dirs)[-1]
        return os.path.join(self.folder_download, latest_dir)

    def download_all(self):
        urls = self.get_urls()
        download_folder = os.path.join(
            self.folder_download, self.config_loader["last_modified"]
        )
        os.makedirs(download_folder, exist_ok=True)
        for url in urls:
            info = urlopen(url).info()
            file = info.get_filename() if info.get_filename() else os.path.basename(url)
            downloaded_file_path = os.path.join(download_folder, file)
            self.download(
                url=url,
                destination_path=downloaded_file_path,
            )

            shutil.unpack_archive(downloaded_file_path, self.folder_download)
            os.remove(downloaded_file_path)
            os.rmdir(download_folder)

            latest_dir = self.get_latest_dir()
            if latest_dir is None:
                return
            old_files = os.listdir(latest_dir)
            logger.debug(f"old files: {old_files}")

            new_files = [x for x in os.listdir(download_folder) if x not in old_files]
            logger.debug(f"new files: {new_files}")

            for downloaded_file in new_files:
                if not downloaded_file.endswith(".json"):
                    logger.debug(f"deleting {downloaded_file}...")
                    os.remove(os.path.join(download_folder, downloaded_file))


class StaticDirectoryDownloader(DirectoryDownloader):

    def __init__(self, config_loader: dict, folder_download: str):
        super().__init__(config_loader, folder_download)
