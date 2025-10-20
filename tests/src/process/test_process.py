from src.process.base import CNILProcessor
from src.download.dila import LegiDownloader
from src.extraction.base import DilaBaseExtractor
import re


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
    obj_downloader.download_all(max_download=2, patterns=[re.compile(r"LEGI_202510[0-9]{2}-[0-9]{6}\.tar\.gz")])
    
    obj_extractor = DilaBaseExtractor(
        config_loader={
            "download_url": "https://echanges.dila.gouv.fr/OPENDATA/LEGI/",
            "download_folder": "data/unprocessed/legi",
            "type": "dila_folder",
        },
        output_dir="data/extracted/legi_test",
    )
    obj_extractor.get_all_input_paths("data/unprocessed/legi_test", recursive=True)
    # filter_xml = [ file for file in files if file.endswith(".xml") ]
    # processor = LegiartiProcessor(
    #     input_folder="data/extracted/legi_test",
    # )
    # result = processor.process(filter_xml[0])
    # assert result["cid"] == "LEGIARTI000023986477"
    # assert "text_content" in result
    # assert isinstance(result["text_content"], str)

    # processor.process_all(max_files=2)
