from abc import ABC
from src.utils.data_helpers import extract_tar_file
from src.schemas.extractions.table import extraction_table
from src.schemas.extractions.models import (
    ExtractionModel,
    ExtractionCreateModel,
    ExtractionUpdateModel,
)
import os
from glob import glob
import tqdm
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class BaseExtractor(ABC):
    def __init__(
        self,
        config_loader: dict,
        output_dir: str = "data/extracted/",
    ):
        self.config_loader = config_loader
        self.output_dir = output_dir
        os.makedirs(self.output_dir, exist_ok=True)

    def extract(self, input_path: str):
        record_extraction = extraction_table.get_record_by_uri(uri=input_path)
        if not record_extraction:
            logger.warning(f"No extraction record found for {input_path}")
            return None
        create_record = ExtractionCreateModel(
            uri=input_path,
            status="in_progress",
            output_path=self.output_dir,
        )
        extraction_table.create_record(create_record)
        extract_tar_file(input_path, self.output_dir)
        updated_record = ExtractionUpdateModel(
            output_path=self.output_dir,
            status="completed",
        )
        extraction_table.update_record(record_extraction.id, updated_record)

    def get_all_input_paths(self, folder: str, recursive: bool = False) -> list[str]:
        if recursive:
            return glob(os.path.join(folder, "**", "*.tar.gz"), recursive=True)
        return glob(os.path.join(folder, "*.tar.gz"))

    def extract_all(self, max_extract: int = -1):
        logger.info("===================================")
        logger.info("Starting extraction of all files.")
        logger.info(
            f"Looking for files in {self.config_loader['download_folder']} to extract."
        )
        files = self.get_all_input_paths(self.config_loader["download_folder"])
        logger.info(f"Found {len(files)} files to extract.")
        if max_extract > 0:
            files = files[:max_extract]
        for input_path in tqdm.tqdm(files):
            logger.info(f"Extracting {input_path}")

            self.extract(input_path)
        logger.info("===================================")


class DilaBaseExtractor(BaseExtractor):

    def __init__(self, config_loader: dict, output_dir: str = "data/extracted/"):
        super().__init__(config_loader, output_dir)


class CNILBaseExtractor(DilaBaseExtractor):

    def __init__(self, config_loader: dict, output_dir: str = "data/extracted/cnil/"):
        super().__init__(config_loader, output_dir)


class ConstitBaseExtractor(DilaBaseExtractor):

    def __init__(
        self, config_loader: dict, output_dir: str = "data/extracted/constit/"
    ):
        super().__init__(config_loader, output_dir)


class DoleBaseExtractor(DilaBaseExtractor):

    def __init__(self, config_loader: dict, output_dir: str = "data/extracted/dole/"):
        super().__init__(config_loader, output_dir)


class LegiBaseExtractor(DilaBaseExtractor):

    def __init__(self, config_loader: dict, output_dir: str = "data/extracted/legi/"):
        super().__init__(config_loader, output_dir)
