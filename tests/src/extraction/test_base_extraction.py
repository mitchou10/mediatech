from src.extraction.base import DilaBaseExtractor
from src.download.dila import DilaDownloader
import re


def test_dila_base_extractor_extract():
    config = {
        "download_url": "https://echanges.dila.gouv.fr/OPENDATA/CNIL/",
        "download_folder": "data/unprocessed/cnil_test",
        "type": "dila_folder",
    }
    downloader = DilaDownloader(
        config_loader=config,
        folder_download="data/unprocessed/cnil_test",
        pattern=re.compile(r"CNIL_.*\.tar\.gz"),
    )
    downloader.download_all(max_download=2)
    extractor = DilaBaseExtractor(
        config_loader=config, output_dir="data/extracted/cnil_test"
    )
    extractor.extract("data/unprocessed/cnil_test/CNIL_20250711-212007.tar.gz")

    extractor.extract_all(max_extract=2, patterns=[re.compile(r"CNIL_202510[0-9]{2}-[0-9]{6}\.tar\.gz")])
    assert len(extractor.get_all_input_paths("data/unprocessed/cnil_test")) > 0


def test_legi_base_extractor_extract():
    config = {
        "download_url": "https://echanges.dila.gouv.fr/OPENDATA/LEGI/",
        "download_folder": "data/unprocessed/legi_test",
        "type": "dila_folder",
    }
    downloader = DilaDownloader(
        config_loader=config,
        folder_download="data/unprocessed/legi_test",
        pattern=re.compile(r"LEGI_.*\.tar\.gz"),
    )
    downloader.download_all(max_download=2, patterns=[re.compile(r"LEGI_202510[0-9]{2}-[0-9]{6}\.tar\.gz")])
    extractor = DilaBaseExtractor(
        config_loader=config, output_dir="data/extracted/legi_test"
    )
    extractor.extract("data/unprocessed/legi_test/LEGI_20250711-212007.tar.gz")

    extractor.extract_all(max_extract=2, patterns=[re.compile(r"LEGI_202510[0-9]{2}-[0-9]{6}\.tar\.gz")])
    assert len(extractor.get_all_input_paths("data/unprocessed/legi_test")) > 0
