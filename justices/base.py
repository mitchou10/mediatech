import argparse
import hashlib
import logging
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from zipfile import ZipFile

import pandas as pd
from datasets import Dataset, load_dataset
from huggingface_hub import HfApi

from justices.utils import download_file

logger = logging.getLogger(__name__)


def get_content_file(path: str) -> str:
    with open(path, "r") as f:
        return f.read()


def compute_hash(content: str) -> str:
    return hashlib.sha256(content.encode("utf-8", errors="replace")).hexdigest()


def get_existing_filenames(repo_id: str, filename_col: str) -> set[str]:
    try:
        ds = load_dataset(repo_id, split="train", columns=[filename_col])
        return set(ds[filename_col])
    except Exception:
        return set()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Extract and export data based on configuration."
    )
    parser.add_argument(
        "--user-id",
        type=str,
        required=True,
        help="Hugging Face user ID for dataset upload.",
    )
    return parser.parse_args()


class BaseJustice:
    filename_xml_col: str
    filename_zip_col: str
    date_col: str
    on_bad_lines: str = "error"

    def __init__(self, config_loader: dict, folder_download: str, base_url: str):
        self.config_loader = config_loader
        self.folder_download = folder_download
        self.base_url = base_url

    def load_data(self) -> pd.DataFrame:
        df = pd.read_csv(
            self.config_loader["download_url"],
            sep=";",
            encoding="cp1252",
            on_bad_lines=self.on_bad_lines,
        )
        df[self.date_col] = pd.to_datetime(df[self.date_col], format="%d/%m/%Y")
        return df

    def _download_and_extract_zip(self, zip_url: str, zip_path: str) -> None:
        if not Path(zip_path).exists():
            download_file(url=zip_url, destination_path=zip_path)
        with ZipFile(zip_path, "r") as zip_ref:
            zip_ref.extractall(self.folder_download)

    def _build_xml_path(self, name: str) -> str:
        return f"{self.folder_download}/{name}"

    def download_zip_and_extract(self, df: pd.DataFrame, max_workers: int = 4) -> pd.DataFrame:
        # Dédoublonne les zips à télécharger (plusieurs XML peuvent être dans le même zip)
        zip_tasks: dict[str, str] = {}
        for _, row in df.iterrows():
            zip_url = self.base_url.replace(
                "YEAR", str(row[self.date_col].year)
            ).replace("MONTH", f"{row[self.date_col].month:02d}")
            zip_path = f"{self.folder_download}/{row[self.filename_zip_col]}"
            zip_tasks[zip_path] = zip_url

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(self._download_and_extract_zip, url, path): path
                for path, url in zip_tasks.items()
            }
            for future in as_completed(futures):
                future.result()  # propage les exceptions

        df = df.copy()
        df["path_xml"] = df[self.filename_xml_col].apply(
            lambda name: self._build_xml_path(name)
        )
        df["content"] = df["path_xml"].apply(get_content_file)
        df["content_hash"] = df["content"].apply(compute_hash)
        return df

    def run(self, repo_id: str) -> None:
        api = HfApi()
        repo_url = api.create_repo(repo_id=repo_id, repo_type="dataset", exist_ok=True)
        print(f"Repository URL: {repo_url}")

        existing_filenames = get_existing_filenames(repo_id, self.filename_xml_col)
        print(f"Already in HuggingFace: {len(existing_filenames)} documents")

        all_data = self.load_data()
        new_rows = all_data[~all_data[self.filename_xml_col].isin(existing_filenames)]
        new_rows = new_rows.drop_duplicates(subset=[self.filename_xml_col])
        print(f"New documents to process: {len(new_rows)}")
        print(79 * "*")

        if len(new_rows) == 0:
            print("Nothing new to upload.")
            sys.exit(0)

        new_df = self.download_zip_and_extract(new_rows)

        try:
            existing_ds = load_dataset(repo_id, split="train")
            existing_df = pd.DataFrame(existing_ds.to_pandas())

            # Ajoute la colonne hash aux anciennes données si absente
            if "content_hash" not in existing_df.columns:
                existing_df["content_hash"] = existing_df["content"].apply(compute_hash)

            full_df = pd.concat([existing_df, new_df], ignore_index=True)

            if full_df.duplicated(subset=["content_hash"]).any():
                n_before = len(full_df)
                full_df = full_df.drop_duplicates(subset=["content_hash"]).reset_index(drop=True)
                print(f"Removed {n_before - len(full_df)} duplicate(s) by content hash.")

            dataset = Dataset.from_pandas(full_df)
        except Exception:
            dataset = Dataset.from_pandas(new_df)

        commit_message = f"Data update on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} (+{len(new_df)} documents)"
        dataset.push_to_hub(
            repo_id=repo_id,
            split="train",
            create_pr=False,
            num_proc=8,
            revision="main",
            commit_message=commit_message,
            max_shard_size="500MB",
        )
