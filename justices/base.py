import argparse
import logging
import sys
from datetime import datetime
from pathlib import Path
from zipfile import ZipFile

import pandas as pd
from datasets import Dataset, concatenate_datasets, load_dataset
from huggingface_hub import HfApi

from justices.utils import download_file

logger = logging.getLogger(__name__)


def get_content_file(path: str) -> str:
    with open(path, "r") as f:
        return f.read()


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
        "--start_date",
        type=str,
        default="2025-10-16",
        help="Start date for the extraction in YYYY-MM-DD format.",
    )
    parser.add_argument(
        "--end_date",
        type=str,
        default=datetime.now().strftime("%Y-%m-%d"),
        help="End date for the extraction in YYYY-MM-DD format.",
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

    def filter_data(self, start_date: datetime, end_date: datetime) -> pd.DataFrame:
        df = pd.read_csv(
            self.config_loader["download_url"],
            sep=";",
            encoding="cp1252",
            on_bad_lines=self.on_bad_lines,
        )
        df[self.date_col] = pd.to_datetime(df[self.date_col], format="%d/%m/%Y")
        return df.loc[
            (df[self.date_col] >= start_date) & (df[self.date_col] < end_date)
        ]

    def download_zip_and_extract(self, df: pd.DataFrame) -> pd.DataFrame:
        extracted_files = []
        for _, row in df.iterrows():
            zip_file_url = self.base_url.replace(
                "YEAR", str(row[self.date_col].year)
            ).replace("MONTH", f"{row[self.date_col].month:02d}")
            zip_file_path = f"{self.folder_download}/{row[self.filename_zip_col]}"
            if not Path(zip_file_path).exists():
                download_file(url=zip_file_url, destination_path=zip_file_path)
            with ZipFile(zip_file_path, "r") as zip_ref:
                zip_ref.extractall(self.folder_download)
            extracted_files.append(f"{self.folder_download}/{row[self.filename_xml_col]}")

        df = df.copy()
        df["path_xml"] = extracted_files
        df["content"] = df["path_xml"].apply(get_content_file)
        return df

    def run(self, start_date: datetime, end_date: datetime, repo_id: str) -> None:
        api = HfApi()
        repo_url = api.create_repo(repo_id=repo_id, repo_type="dataset", exist_ok=True)
        print(f"Repository URL: {repo_url}")

        existing_filenames = get_existing_filenames(repo_id, self.filename_xml_col)
        print(f"Already in HuggingFace: {len(existing_filenames)} documents")

        filtered = self.filter_data(start_date=start_date, end_date=end_date)
        new_rows = filtered[~filtered[self.filename_xml_col].isin(existing_filenames)]
        print(f"New documents to process: {len(new_rows)}")
        print(79 * "*")

        if len(new_rows) == 0:
            print("Nothing new to upload.")
            sys.exit(0)

        new_df = self.download_zip_and_extract(new_rows)

        try:
            existing_ds = load_dataset(repo_id, split="train")
            dataset = concatenate_datasets([existing_ds, Dataset.from_pandas(new_df)])
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
