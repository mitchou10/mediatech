import shutil
from src.extraction.base import BaseExtractor
import re
import os
from glob import glob


class DataGouvBaseExtractor(BaseExtractor):

    def __init__(
        self,
        config_loader: dict,
        output_dir: str = "data/extracted/sheets/",
        ext: str = ".csv",
    ):
        super().__init__(config_loader, output_dir)
        self.ext = ext

    def extract(self, input_path: str) -> list[str]:
        dest_path = os.path.join(self.output_dir, os.path.basename(input_path))
        if os.path.exists(dest_path):
            print(f"Removing existing file {dest_path}")
            os.remove(dest_path)

        shutil.copy(input_path, dest_path, follow_symlinks=True)
        print(f"Copied {input_path} to {dest_path}")
        return [os.path.basename(dest_path)]

    def get_all_input_paths(self, folder: str, recursive: bool = False) -> list[str]:
        if recursive:
            return glob(os.path.join(folder, "**", f"*{self.ext}"), recursive=True)
        return glob(os.path.join(folder, f"*{self.ext}"))

    def filter_input_paths(
        self, patterns: list[re.Pattern], recursive: bool = False
    ) -> list[str]:
        files_to_process = []
        files = self.get_all_input_paths(
            self.config_loader["download_folder"], recursive
        )
        for input_path in files:
            files_to_process.append(input_path)
        return files_to_process

    def extract_all(
        self,
        max_extract: int = -1,
        patterns: list[re.Pattern] = [],
        recursive: bool = False,
    ) -> list[str]:
        files_to_process = []
        files = self.get_all_input_paths(
            self.config_loader["download_folder"], recursive
        )
        if max_extract > 0:
            files = files[:max_extract]

        for input_path in files:
            files_to_process.extend(self.extract(input_path))
        return files_to_process
