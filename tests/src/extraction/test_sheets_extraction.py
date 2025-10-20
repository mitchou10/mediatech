from src.extraction.sheets import SheetsBaseExtractor
import os


def test_extraction_service_part():
    config_loader = {
        "download_url": "https://lecomarquage.service-public.fr/vdd/3.4/part/zip/vosdroits-latest.zip",
        "download_folder": "data/unprocessed/service_public_part",
        "type": "sheets",
    }
    extractor = SheetsBaseExtractor(
        config_loader, output_dir="data/extracted/service_public_part", ext=".xml"
    )
    extracted_files = extractor.get_all_input_paths(
        folder=config_loader["download_folder"]
    )

    assert len(extracted_files) > 0
    for file in extracted_files:
        extracted = extractor.extract(file)
        for path in extracted:
            assert os.path.exists(path)
            break  # Just test one file
        break

    files = extractor.extract_all(max_extract=1)  # Test extract_all method
    assert len(files) == 1


def test_extraction_service_public_pro():
    config_loader = {
        "download_url": "https://lecomarquage.service-public.fr/vdd/3.4/pro/zip/vosdroits-latest.zip",
        "download_folder": "data/unprocessed/service_public_pro",
        "type": "sheets",
    }
    extractor = SheetsBaseExtractor(
        config_loader, output_dir="data/extracted/service_public_pro", ext=".xml"
    )
    extracted_files = extractor.get_all_input_paths(
        folder=config_loader["download_folder"]
    )

    assert len(extracted_files) > 0
    for file in extracted_files:
        extracted = extractor.extract(file)
        for path in extracted:
            assert os.path.exists(path)
            break  # Just test one file
        break

    files = extractor.extract_all(max_extract=1)  # Test extract_all method
    assert len(files) == 1


def test_extraction_travail_emploi():
    config_loader = {
        "download_url": "https://github.com/SocialGouv/fiches-travail-data/raw/master/data/fiches-travail.json",
        "download_folder": "data/unprocessed/travail_emploi",
        "type": "sheets",
    }
    extractor = SheetsBaseExtractor(
        config_loader, output_dir="data/extracted/travail_emploi", ext=".json"
    )
    extracted_files = extractor.get_all_input_paths(
        folder=config_loader["download_folder"]
    )

    assert len(extracted_files) > 0
    for file in extracted_files:
        extracted = extractor.extract(file)
        for path in extracted:
            assert os.path.exists(path)
            break  # Just test one file
        break

    files = extractor.extract_all(max_extract=1)  # Test extract_all method
    assert len(files) == 1
