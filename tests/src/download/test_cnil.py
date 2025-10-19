from src.download.dila import CNILDownloader
import os


def test_cnil_downloader_get_urls():
    config = {
        "download_url": "https://echanges.dila.gouv.fr/OPENDATA/CNIL/",
        "download_folder": "data/unprocessed/cnil",
        "type": "dila_folder",
    }
    downloader = CNILDownloader(
        config_loader=config, folder_download="data/unprocessed/cnil_test"
    )
    urls = downloader.get_urls()
    assert len(urls) > 0
    for url in urls:
        assert url.endswith(".tar.gz")


def test_cnil_downloader_download_all():
    config = {
        "download_url": "https://echanges.dila.gouv.fr/OPENDATA/CNIL/",
        "download_folder": "data/unprocessed/cnil",
        "type": "dila_folder",
    }
    downloader = CNILDownloader(
        config_loader=config, folder_download="data/unprocessed/cnil_test"
    )
    downloader.download_all()
    urls = downloader.get_urls()
    for url in urls:
        filename = url.split("/")[-1]
        destination_path = f"{downloader.folder_download}/{filename}"
        assert os.path.exists(destination_path)
