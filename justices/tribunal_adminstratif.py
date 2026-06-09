import pandas as pd
from huggingface_hub import HfApi
from datetime import datetime
from zipfile import ZipFile
from justices.utils import download_file
from pathlib import Path
from datasets import Dataset
import argparse


def get_content_file(path: str) -> str:
    with open(path, "r") as f:
        return f.read()


class ConseilDadministratif:
    def __init__(
        self,
        config_loader: dict,
        folder_download: str = "data/unprocessed/tribunal_administratif",
    ):

        self.config_loader = config_loader
        self.folder_download = folder_download
        self.base_url = (
            "https://opendata.justice-administrative.fr/DTA/YEAR/MONTH/TA_YEARMONTH.zip"
        )

    def get_urls(self) -> list[str]:
        return []

    def filter_data(
        self,
        start_date: datetime,
        end_date: datetime,
    ) -> pd.DataFrame:
        url = self.config_loader["download_url"]
        df = pd.read_csv(url, sep=";", encoding="cp1252", on_bad_lines="skip")
        # Nom du fichier .xml	N° de la décision	Date de lecture	Date de reversement	Nom du fichier .zip	Juridiction

        column_to_filter = "Date de lecture"
        df[column_to_filter] = pd.to_datetime(df[column_to_filter], format="%d/%m/%Y")
        filtered_df = df.loc[
            (df[column_to_filter] >= start_date) & (df[column_to_filter] < end_date)
        ]

        return filtered_df

    def download_zip_and_extract(self, df: pd.DataFrame) -> pd.DataFrame:
        extracted_files = []

        for _, row in df.iterrows():
            zip_file_name = row["Nom du fichier .zip"]
            column_to_filter = "Date de lecture"
            zip_file_url = self.base_url.replace(
                "YEAR", f"{row[column_to_filter].year}"
            ).replace("MONTH", f"{row[column_to_filter].month:02d}")
            zip_file_path = f"{self.folder_download}/{zip_file_name}"
            # Télécharger le fichier ZIP
            if not Path(zip_file_path).exists():
                download_file(url=zip_file_url, destination_path=zip_file_path)

            with ZipFile(zip_file_path, "r") as zip_ref:
                zip_ref.extractall(self.folder_download)
                xml_file = f"{self.folder_download}/{row['Nom du fichier .xml']}"
                extracted_files.append(xml_file)

        df["path_xml"] = extracted_files

        df["content"] = df["path_xml"].apply(get_content_file)

        return df


def parse_args():
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


if __name__ == "__main__":
    obj = ConseilDadministratif(
        config_loader={
            "download_url": "https://opendata.justice-administrative.fr/DTA/TA_documents_reverses.csv"
        }
    )
    print(79 * "*")
    START_DATE = datetime.strptime(parse_args().start_date, "%Y-%m-%d")
    END_DATE = datetime.strptime(parse_args().end_date, "%Y-%m-%d")
    print(f"Start : {START_DATE} end: {END_DATE}")

    filter = obj.filter_data(start_date=START_DATE, end_date=END_DATE)
    df = obj.download_zip_and_extract(filter)
    user_id = parse_args().user_id
    api = HfApi()
    repo_id = f"{user_id}/conseil-administratives-appel-full-documents"
    repo_url = api.create_repo(
        repo_id=repo_id,
        repo_type="dataset",
        exist_ok=True,
    )

    print(f"Repository URL: {repo_url}")
    print("Size after filtering and downloading:", len(df))
    print(79 * "*")

    dataset = Dataset.from_pandas(df)
    commit_message = f"Data update on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
    dataset.push_to_hub(
        repo_id=repo_id,
        split="train",
        create_pr=False,
        num_proc=8,
        revision="main",
        commit_message=commit_message,
        max_shard_size="500MB",
    )
