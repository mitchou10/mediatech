from abc import ABC, abstractmethod
from glob import glob
import os
from tqdm import tqdm
import xml.etree.ElementTree as ET
from src.utils.process import (
    process_legiarti,
    process_cnil_text,
    process_directories,
    process_dole_text,
    process_constit_text,
)
from src.utils.process_sheets import process_sheets

import logging


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class BaseProcessor(ABC):
    def __init__(
        self,
        input_folder: str = "data/extracted/",
    ):
        self.input_folder = input_folder

    @abstractmethod
    def process(self, file_path: str) -> dict:
        pass

    def process_all(self, max_files: int = -1) -> list[dict]:
        files = glob(os.path.join(self.input_folder, "**", "*.xml"), recursive=True)
        if max_files > 0:
            files = files[:max_files]
        for file_path in tqdm(files, desc="Processing files"):
            logger.debug(f"Processing file: {file_path}")
            self.process(file_path)


class LegiartiProcessor(BaseProcessor):
    def process(self, file_path: str) -> dict:
        # Check if file is gzipped
        with open(file_path, "r") as f:
            file_content = f.read()
            try:
                root = ET.fromstring(file_content)
                result = process_legiarti(root, os.path.basename(file_path))
                if not result:
                    logger.debug(f"No data extracted from file: {file_path}")

                return result
            except Exception as e:
                return {}


class CNILProcessor(BaseProcessor):
    def process(self, file_path: str) -> dict:
        with open(file_path, "r", encoding="utf-8") as f:
            file_content = f.read()
        root = ET.fromstring(file_content)
        result = process_cnil_text(root, os.path.basename(file_path))

        if not result:
            logger.debug(f"No data extracted from file: {file_path}")
        else:
            if "doc_id" in result:
                logger.debug(f"No Empty doc_id in file: {file_path}")
                logger.debug(f"doc_id: {result['doc_id']}")

        return result


class DirectoryProcessor(BaseProcessor):
    def process(self, file_path: str) -> dict:
        result = process_directories(file_path)
        if not result:
            logger.warning(f"No data extracted from file: {file_path}")

        return result


class DOLEProcessor(BaseProcessor):
    def process(self, file_path: str) -> dict:
        with open(file_path, "r", encoding="utf-8") as f:
            file_content = f.read()
        root = ET.fromstring(file_content)
        result = process_dole_text(root, os.path.basename(file_path))

        if not result:
            logger.debug(f"No data extracted from file: {file_path}")
        else:
            if "doc_id" in result:
                logger.debug(f"No Empty doc_id in file: {file_path}")
                logger.debug(f"doc_id: {result['doc_id']}")

        return result


class ConstitProcessor(BaseProcessor):
    def process(self, file_path: str) -> dict:
        with open(file_path, "r", encoding="utf-8") as f:
            file_content = f.read()
        root = ET.fromstring(file_content)
        result = process_constit_text(root, os.path.basename(file_path))

        if not result:
            logger.debug(f"No data extracted from file: {file_path}")

        return result


class SheetsProcessor(BaseProcessor):
    def process(self, file_path: str) -> dict:
        result = process_sheets(file_path, "service_public")
        return result
