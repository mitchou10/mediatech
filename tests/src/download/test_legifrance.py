from src.download.dila import LegiDownloader
import os


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
    downloader.download_all(max_download=2)
    urls = downloader.get_urls()
    for url in urls[:2]:
        filename = url.split("/")[-1]
        destination_path = f"{downloader.folder_download}/{filename}"
        assert os.path.exists(destination_path)
