from src.download.sheets import SheetsDownloader, TravailDownloader


def test_download_sheets_service_public_part():
    config_loader = {
        "download_url": "https://lecomarquage.service-public.fr/vdd/3.4/part/zip/vosdroits-latest.zip",
        "download_folder": "data/unprocessed/service_public_part",
        "type": "sheets",
    }
    downloader = SheetsDownloader(config_loader, config_loader["download_folder"])
    downloader.download_all(max_download=1)


def test_download_sheets_service_public_pro():
    config_loader = {
        "download_url": "https://lecomarquage.service-public.fr/vdd/3.4/pro/zip/vosdroits-latest.zip",
        "download_folder": "data/unprocessed/service_public_pro",
        "type": "sheets",
    }
    downloader = SheetsDownloader(config_loader, config_loader["download_folder"])
    downloader.download_all(max_download=1)


def test_download_sheets_travail_emploi():
    config_loader = {
        "download_url": "https://github.com/SocialGouv/fiches-travail-data/raw/master/data/fiches-travail.json",
        "download_folder": "data/unprocessed/travail_emploi",
        "type": "sheets",
    }
    downloader = TravailDownloader(config_loader, config_loader["download_folder"])
    downloader.download_all(max_download=1)
