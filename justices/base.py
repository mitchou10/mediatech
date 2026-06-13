import argparse
import hashlib
import logging
import sys
import tempfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from zipfile import ZipFile

import pandas as pd
from datasets import Dataset, load_dataset
from huggingface_hub import HfApi
from tqdm import tqdm

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


def get_existing_hashes(repo_id: str) -> set[str]:
    try:
        ds = load_dataset(repo_id, split="train", columns=["content_hash"])
        return set(ds["content_hash"])
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

    def _read_rows(self, df: pd.DataFrame, max_workers: int = 8) -> pd.DataFrame:
        """Lit les fichiers XML et calcule les hashes en parallèle."""
        def _process(name: str) -> tuple[str, str, str]:
            path = self._build_xml_path(name)
            content = get_content_file(path)
            return path, content, compute_hash(content)

        names = df[self.filename_xml_col].tolist()
        name_to_result: dict[str, tuple[str, str, str]] = {}
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(_process, name): name for name in names}
            for future in tqdm(as_completed(futures), total=len(futures), desc="Reading & hashing XML"):
                name, (path, content, h) = futures[future], future.result()
                name_to_result[name] = (path, content, h)

        df = df.copy()
        df["path_xml"] = df[self.filename_xml_col].map(lambda n: name_to_result[n][0])
        df["content"] = df[self.filename_xml_col].map(lambda n: name_to_result[n][1])
        df["content_hash"] = df[self.filename_xml_col].map(lambda n: name_to_result[n][2])
        return df

    def _build_zip_groups(self, df: pd.DataFrame) -> dict[str, tuple[str, pd.DataFrame]]:
        """Regroupe les lignes par zip et retourne {zip_path: (zip_url, rows)}."""
        groups: dict[str, tuple[str, list]] = {}
        for _, row in df.iterrows():
            zip_url = self.base_url.replace(
                "YEAR", str(row[self.date_col].year)
            ).replace("MONTH", f"{row[self.date_col].month:02d}")
            zip_path = f"{self.folder_download}/{row[self.filename_zip_col]}"
            if zip_path not in groups:
                groups[zip_path] = (zip_url, [])
            groups[zip_path][1].append(row)
        return {k: (v[0], pd.DataFrame(v[1])) for k, v in groups.items()}

    def run(self, repo_id: str) -> None:
        api = HfApi()
        repo_url = api.create_repo(repo_id=repo_id, repo_type="dataset", exist_ok=True)
        print(f"Repository URL: {repo_url}")

        existing_filenames = get_existing_filenames(repo_id, self.filename_xml_col)
        existing_hashes = get_existing_hashes(repo_id)
        print(f"Already in HuggingFace: {len(existing_filenames)} documents")

        all_data = self.load_data()
        new_rows = all_data[~all_data[self.filename_xml_col].isin(existing_filenames)]
        new_rows = new_rows.drop_duplicates(subset=[self.filename_xml_col])
        print(f"New documents to process: {len(new_rows)}")
        print(79 * "*")

        if len(new_rows) == 0:
            print("Nothing new to upload.")
            sys.exit(0)

        # Hashes vus dans cette session (pour dédup inter-zips)
        seen_hashes: set[str] = set(existing_hashes)
        zip_groups = self._build_zip_groups(new_rows)
        total_uploaded = 0

        for zip_path, (zip_url, rows) in zip_groups.items():
            zip_name = Path(zip_path).name
            print(f"\n--- Processing {zip_name} ({len(rows)} documents) ---")

            self._download_and_extract_zip(zip_url, zip_path)
            batch_df = self._read_rows(rows)

            # Dédup contre HF + zips déjà uploadés dans cette session
            n_before = len(batch_df)
            batch_df = batch_df[~batch_df["content_hash"].isin(seen_hashes)].reset_index(drop=True)
            if len(batch_df) < n_before:
                print(f"  Skipped {n_before - len(batch_df)} duplicate(s).")

            if batch_df.empty:
                print("  Nothing new to upload for this zip.")
                continue

            seen_hashes.update(batch_df["content_hash"].tolist())

            dataset = Dataset.from_pandas(batch_df, preserve_index=False)
            shard_name = f"data/train-{datetime.now().strftime('%Y%m%d%H%M%S')}.parquet"
            commit_message = f"Add {zip_name} (+{len(batch_df)} documents)"
            with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
                dataset.to_parquet(tmp.name)
                api.upload_file(
                    path_or_fileobj=tmp.name,
                    path_in_repo=shard_name,
                    repo_id=repo_id,
                    repo_type="dataset",
                    commit_message=commit_message,
                )
            total_uploaded += len(batch_df)
            print(f"  Uploaded {len(batch_df)} documents.")

        print(f"\nDone. Total uploaded: {total_uploaded} documents.")
