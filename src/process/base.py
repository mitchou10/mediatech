from abc import ABC, abstractmethod
import json
from hashlib import md5
from glob import glob
import os
from tqdm import tqdm
import xml.etree.ElementTree as ET
from src.utils.process import process_legiarti, process_cnil_text
import logging


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class BaseProcessor(ABC):
    def __init__(
        self,
        input_folder: str = "data/extracted/",
        output_folder: str = "data/processed/",
    ):
        self.input_folder = input_folder
        self.output_folder = output_folder
        os.makedirs(self.output_folder, exist_ok=True)

    @abstractmethod
    def process(self, file_path: str) -> dict:
        pass

    def process_all(self) -> list[dict]:
        files = glob(os.path.join(self.input_folder, "**", "*.xml"), recursive=True)
        for file_path in tqdm(files, desc="Processing files"):
            file_hash = md5(open(file_path, "rb").read()).hexdigest()
            output_path = os.path.join(self.output_folder, f"{file_hash}.json")
            if os.path.exists(output_path):
                logger.info(
                    f"Processed file {output_path} already exists. Skipping processing."
                )
                continue

            logger.info(f"Processing file: {file_path} | Hash: {file_hash}")

            with open(output_path, "w") as f:
                result = self.process(file_path)
                json.dump(result, f)


class LegiartiProcessor(BaseProcessor):
    def process(self, file_path: str) -> dict:
        # Check if file is gzipped
        with open(file_path, "r") as f:
            file_content = f.read()
            root = ET.fromstring(file_content)
            return process_legiarti(root, os.path.basename(file_path))


class CNILProcessor(BaseProcessor):
    def process(self, file_path: str) -> dict:
        with open(file_path, "r", encoding="utf-8") as f:
            file_content = f.read()
        root = ET.fromstring(file_content)
        result = process_cnil_text(root, os.path.basename(file_path))
        logger.info(f"Processed CNIL file {file_path}: {result}")
        return result
