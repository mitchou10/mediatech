from src.utils.data_helpers import extract_tar_file
import re
import os
from glob import glob
import tqdm
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class BaseExtractor:
    def __init__(
        self,
        config_loader: dict,
        output_dir: str = "data/extracted/",
    ):
        self.config_loader = config_loader
        self.output_dir = output_dir
        os.makedirs(self.output_dir, exist_ok=True)

    def extract(self, input_path: str) -> list[str]:
        return extract_tar_file(input_path, self.output_dir)

    def get_all_input_paths(self, folder: str, recursive: bool = False) -> list[str]:
        if recursive:
            return glob(os.path.join(folder, "**", "*.tar.gz"), recursive=True)
        return glob(os.path.join(folder, "*.tar.gz"))

    def filter_input_paths(
        self, patterns: list[re.Pattern], recursive: bool = False
    ) -> list[str]:
        all_paths = self.get_all_input_paths(
            self.config_loader["download_folder"], recursive
        )
        filtered_paths = []
        for path in all_paths:
            file_name = os.path.basename(path)
            for pattern in patterns:
                if re.search(pattern, file_name):
                    filtered_paths.append(path)
                    break
        return filtered_paths

    def extract_all(
        self,
        max_extract: int = -1,
        patterns: list[re.Pattern] = [],
        recursive: bool = False,
    ) -> list[str]:
        logger.info("===================================")
        logger.info("Starting extraction of all files.")
        logger.info(
            f"Looking for files in {self.config_loader['download_folder']} to extract."
        )
        files = self.get_all_input_paths(self.config_loader["download_folder"])
        logger.info(f"Found {len(files)} files to extract.")
        files_to_process = []
        if patterns:
            files = self.filter_input_paths(patterns, recursive)
            logger.debug(f"{len(files)} files matched the provided patterns.")
        if max_extract > 0:
            files = files[:max_extract]
        for input_path in tqdm.tqdm(files):
            logger.debug(f"Extracting {input_path}")

            files_to_process.extend(self.extract(input_path))
        logger.info("===================================")
        return files_to_process


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


class DirectoryBaseExtractor(BaseExtractor):

    def __init__(
        self, config_loader: dict, output_dir: str = "data/extracted/directory/"
    ):
        super().__init__(config_loader, output_dir)

    def extract(self, input_path: str) -> list[str]:
        return [input_path]

    def get_all_input_paths(self, folder: str, recursive: bool = False) -> list[str]:
        if recursive:
            return glob(os.path.join(folder, "**", "*.json"), recursive=True)
        return glob(os.path.join(folder, "*.json"))

    def extract_all(
        self,
        max_extract: int = -1,
        patterns: list[re.Pattern] = [],
        recursive: bool = False,
    ) -> list[str]:
        return self.get_all_input_paths(
            self.config_loader["download_folder"], recursive
        )
