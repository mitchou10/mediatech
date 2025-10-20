import re
from src.download.base import BaseDownloader
from src.utils.dila import get_dila_url
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


class DilaDownloader(BaseDownloader):

    def __init__(self, config_loader: dict, folder_download: str, pattern=re.Pattern):
        super().__init__(config_loader, folder_download)
        self.pattern = pattern

    def get_urls(self) -> list[str]:

        return get_dila_url(self.config_loader, self.pattern)
    
    def filter_urls(self, patterns: list[re.Pattern]) -> list[str]:
        urls = self.get_urls()
        filtered_urls = []
        for url in urls:
            for pattern in patterns:
                file_name = url.split("/")[-1]
                if re.search(pattern, file_name):
                    filtered_urls.append(url)
                    break  
        return filtered_urls
        
    


class CNILDownloader(DilaDownloader):

    def __init__(
        self,
        config_loader: dict,
        folder_download: str = "data/unprocessed/cnil",
    ):
        super().__init__(
            config_loader,
            folder_download,
            pattern=re.compile(r"CNIL_\d{8}-\d{6}\.tar\.gz"),
        )


class ConstitDownloader(DilaDownloader):

    def __init__(
        self,
        config_loader: dict,
        folder_download: str = "data/unprocessed/constit",
    ):
        super().__init__(config_loader, folder_download)


class DoleDownloader(DilaDownloader):

    def __init__(
        self,
        config_loader: dict,
        folder_download: str = "data/unprocessed/dole",
    ):
        super().__init__(config_loader, folder_download)


class LegiDownloader(DilaDownloader):

    def __init__(
        self,
        config_loader: dict,
        folder_download: str = "data/unprocessed/legi",
    ):
        super().__init__(
            config_loader,
            folder_download,
            pattern=re.compile(r"(LEGI|Freemium_legi_global)_\d{8}-\d{6}\.tar\.gz"),
        )
