from src.download.dila import LegiDownloader
import re


def test_legi_downloader_get_urls():
    config = {
        "download_url": "https://echanges.dila.gouv.fr/OPENDATA/LEGI/",
        "download_folder": "data/unprocessed/legi",
        "type": "dila_folder",
    }
    downloader = LegiDownloader(
        config_loader=config, folder_download="data/unprocessed/legi_test"
    )
    urls = downloader.get_urls()
    assert len(urls) > 0
    for url in urls:
        assert url.endswith(".tar.gz")


def test_legi_downloader_download_all():
    config = {
        "download_url": "https://echanges.dila.gouv.fr/OPENDATA/LEGI/",
        "download_folder": "data/unprocessed/legi",
        "type": "dila_folder",
    }
    downloader = LegiDownloader(
        config_loader=config, folder_download="data/unprocessed/legi_test"
    )
    downloader.download_all(max_download=2, patterns=[re.compile(r"LEGI_2025[0-9]{4}-[0-9]{6}\.tar\.gz")])
    
