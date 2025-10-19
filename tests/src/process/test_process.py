from src.process.base import LegiartiProcessor, CNILProcessor
from src.download.dila import LegiDownloader
from src.extraction.base import DilaBaseExtractor


def test_cnil_processor_process():
    processor = CNILProcessor()
    result = processor.process(
        "data/extracted/cnil_test/20250711-212007/cnil/global/CNIL/TEXT/00/00/51/88/07/CNILTEXT000051880710.xml"
    )
    assert result["cid"] == "CNILTEXT000051880710"
    assert "text_content" in result
    assert isinstance(result["text_content"], str)


def test_legiarti_processor_process():
    obj_downloader = LegiDownloader(
        config_loader={
            "download_url": "https://echanges.dila.gouv.fr/OPENDATA/LEGI/",
            "download_folder": "data/unprocessed/legi",
            "type": "dila_folder",
        },
    )
    obj_downloader.download_all(max_download=2)
    obj_extractor = DilaBaseExtractor(
        config_loader={
            "download_url": "https://echanges.dila.gouv.fr/OPENDATA/LEGI/",
            "download_folder": "data/unprocessed/legi",
            "type": "dila_folder",
        },
        output_dir="data/extracted/legi_test",
    )
    obj_extractor.get_all_input_paths("data/unprocessed/legi_test", recursive=True)
    file = "data/extracted/legi_test/20250729-212953/legi/global/code_et_TNC_en_vigueur/code_en_vigueur/LEGI/TEXT/00/00/23/98/32/LEGITEXT000023983208/article/LEGI/ARTI/00/00/23/98/64/LEGIARTI000023986477.xml"
    processor = LegiartiProcessor(
        input_folder="data/extracted/legi_test",
    )
    result = processor.process(file)
    assert result["cid"] == "LEGIARTI000023986477"
    assert "text_content" in result
    assert isinstance(result["text_content"], str)

    processor.process_all(max_files=2)
