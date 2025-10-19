from src.download.directory import DirectoryDownloader


def test_directory_downloader_get_urls():
    config = {
        "download_url": "https://echanges.dila.gouv.fr/OPENDATA/RefOrgaAdminEtat/FluxAnneeCourante/dila_refOrga_admin_Etat_fr_latest.zip",
        "download_folder": "data/unprocessed/state_administrations_directory",
        "type": "directory",
    }
    downloader = DirectoryDownloader(
        config_loader=config,
        folder_download="data/unprocessed/state_administrations_directory",
    )
    urls = downloader.get_urls()
    assert len(urls) == 1


def test_directory_downloader_download_all():
    config = {
        "download_url": "https://echanges.dila.gouv.fr/OPENDATA/RefOrgaAdminEtat/FluxAnneeCourante/dila_refOrga_admin_Etat_fr_latest.zip",
        "download_folder": "data/unprocessed/state_administrations_directory",
        "type": "directory",
    }
    downloader = DirectoryDownloader(
        config_loader=config,
        folder_download="data/unprocessed/state_administrations_directory",
    )
    downloader.download_all(max_download=-1)
