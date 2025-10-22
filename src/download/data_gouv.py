from src.download.base import BaseDownloader
import requests
import re


class DataGouvDownloader(BaseDownloader):
    def __init__(self, config_loader: dict, folder_download: str):
        super().__init__(config_loader, folder_download)

    def get_urls(self) -> list[str]:
        response = requests.get(self.config_loader["download_url"])
        resources = response.json().get("resources")
        datasets = []
        for resource in resources:
            if resource.get("title").startswith("export-dataset") and resource.get(
                "title"
            ).endswith(".csv"):
                # Filter out datasets that are not CSV files
                datasets.append(
                    {
                        "title": resource.get("title"),
                        "url": resource.get("url"),
                    }
                )
            else:
                continue

        datasets = sorted(datasets, key=lambda x: x["title"].lower())
        download_url = datasets[0].get("url")

        return [download_url]

    def filter_urls(self, patterns: list[re.Pattern]) -> list[str]:
        return self.get_urls()
