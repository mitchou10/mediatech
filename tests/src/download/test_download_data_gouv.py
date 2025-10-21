from src.download.data_gouv import DataGouvDownloader


def test_data_gouv_downloader_get_urls():
    config_loader = {
        "download_url": "https://www.data.gouv.fr/api/1/datasets/catalogue-des-donnees-de-data-gouv-fr/",
        "download_folder": "data/unprocessed/data_gouv_datasets_catalog",
        "type": "data_gouv",
    }
    downloader = DataGouvDownloader(
        config_loader=config_loader, folder_download="data/unprocessed/data_gouv_test"
    )
    urls = downloader.get_urls()
    assert len(urls) == 1
    assert urls[0].endswith(".csv")


def test_data_gouv_downloader_download_all():
    config_loader = {
        "download_url": "https://www.data.gouv.fr/api/1/datasets/catalogue-des-donnees-de-data-gouv-fr/",
        "download_folder": "data/unprocessed/data_gouv_datasets_catalog",
        "type": "data_gouv",
    }
    downloader = DataGouvDownloader(
        config_loader=config_loader, folder_download="data/unprocessed/data_gouv_test"
    )
    downloader.download_all(max_download=1)
