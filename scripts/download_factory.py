import re


def factory_download(config: dict, folder_download: str):
    if config.get("type") == "dila_folder":
        from src.download.dila import DilaDownloader

        return DilaDownloader(
            config,
            folder_download,
            pattern=re.compile(r".*\.tar\.gz$"),
        )
    elif config.get("type") == "directory":
        from src.download.directory import DirectoryDownloader

        return DirectoryDownloader(config, folder_download)
    elif config.get("type") == "sheets":
        if config.get("download_name") == "travail_emploi":
            from src.download.sheets import TravailDownloader

            return TravailDownloader(config, folder_download)
        else:
            from src.download.sheets import SheetsDownloader

            return SheetsDownloader(config, folder_download)
    elif config.get("type") == "data_gouv":
        from src.download.data_gouv import DataGouvDownloader

        return DataGouvDownloader(config, folder_download)
    else:
        raise ValueError(
            f"Download name '{config.get('download_name')}' is not supported for downloading."
        )
