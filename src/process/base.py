from abc import ABC, abstractmethod
from glob import glob
import os
from tqdm import tqdm
import xml.etree.ElementTree as ET
from src.utils.process import process_legiarti, process_cnil_text, process_directories

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
            logger.info(f"Processing file: {file_path}")
            self.process(file_path)
            


class LegiartiProcessor(BaseProcessor):
    def process(self, file_path: str) -> dict:
        # Check if file is gzipped
        with open(file_path, "r") as f:
            file_content = f.read()
            root = ET.fromstring(file_content)
            result = process_legiarti(root, os.path.basename(file_path))
            if not result:
                logger.warning(f"No data extracted from file: {file_path}")
            
            return result


class CNILProcessor(BaseProcessor):
    def process(self, file_path: str) -> dict:
        with open(file_path, "r", encoding="utf-8") as f:
            file_content = f.read()
        root = ET.fromstring(file_content)
        result = process_cnil_text(root, os.path.basename(file_path))
        if not result:
            logger.warning(f"No data extracted from file: {file_path}")
        
        return result


class DirectoryProcessor(BaseProcessor):
    def process(self, file_path: str) -> dict:
        result = process_directories(file_path)
        if not result:
            logger.warning(f"No data extracted from file: {file_path}")
        
        return result