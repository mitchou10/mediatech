from abc import ABC, abstractmethod
from glob import glob
import os
from tqdm import tqdm
import xml.etree.ElementTree as ET
from src.utils.process import process_legiarti, process_cnil_text
from src.schemas.process.table import process_table
from src.schemas.legifrance.table import legifrance_table
from src.schemas.legifrance.models import (
    LegiFranceCreateModel,
)
from src.schemas.process.models import (
    ProcessRecordUpdateModel,
)

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
            process_record = process_table.get_record_by_uri(uri=file_path)
            if process_record:
                if process_record.status == "completed":
                    logger.info(f"Skipping already processed file: {file_path}")
                    continue
            else:
                process_record = process_table.create_record(
                    uri=file_path,
                    status="processing",
                )

            record_id = process_record.id
            logger.info(f"Processing file: {file_path}")
            result = self.process(file_path)
            process_table.update_record(
                record_id=record_id,
                updated_record=ProcessRecordUpdateModel(
                    status="completed" if result else "empty"
                ),
            )


class LegiartiProcessor(BaseProcessor):
    def process(self, file_path: str) -> dict:
        # Check if file is gzipped
        with open(file_path, "r") as f:
            file_content = f.read()
            root = ET.fromstring(file_content)
            result = process_legiarti(root, os.path.basename(file_path))
            if not result:
                logger.warning(f"No data extracted from file: {file_path}")
            else:
                record = legifrance_table.get_record_by_cid(cid=result["cid"])
                if not record or record.status != "completed":
                    logger.info(
                        f"Creating new Legifrance record with CID {result['cid']}"
                    )
                else:
                    logger.info(
                        f"Legifrance record with CID {result['cid']} already exists and is completed. Skipping creation."
                    )

                    result["status"] = "completed"
                    legifrance_table.create_record(
                        legifrance_data=LegiFranceCreateModel(**result)
                    )
            return result


class CNILProcessor(BaseProcessor):
    def process(self, file_path: str) -> dict:
        with open(file_path, "r", encoding="utf-8") as f:
            file_content = f.read()
        root = ET.fromstring(file_content)
        result = process_cnil_text(root, os.path.basename(file_path))
        logger.info(f"Processed CNIL file {file_path}: {result}")
        return result
