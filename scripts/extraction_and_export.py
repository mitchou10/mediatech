from src.extraction.base import (
    BaseExtractor,
    DirectoryBaseExtractor,
)
from src.extraction.sheets import SheetsBaseExtractor
from src.process.base import (
    LegiartiProcessor,
    CNILProcessor,
    DirectoryProcessor,
    DOLEProcessor,
    ConstitProcessor,
    SheetsProcessor,
)
import os
import re
import json
from tqdm import tqdm
from datetime import datetime, timedelta
import pandas as pd
import argparse
from huggingface_hub import HfApi

with open("config/data_config.json", "r") as f:
    CONFIG_LOADER = json.load(f)


def parse_args():
    parser = argparse.ArgumentParser(
        description="Extract and export data based on configuration."
    )
    parser.add_argument(
        "--download_name",
        type=str,
        required=True,
        choices=list(CONFIG_LOADER.keys()),
        help="Name of the download configuration to use.",
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
    args = parse_args()
    download_name = args.download_name
    START_DATE = datetime.strptime(args.start_date, "%Y-%m-%d")
    END_DATE = datetime.strptime(args.end_date, "%Y-%m-%d")
    user_id = args.user_id
    days = (END_DATE - START_DATE).days + 1
    dates = [(START_DATE + timedelta(days=i)).strftime("%Y%m%d") for i in range(days)]
    api = HfApi()

    if download_name not in CONFIG_LOADER:
        raise ValueError(f"Download name '{download_name}' not found in configuration.")

    config = CONFIG_LOADER[download_name]
    output_dir = f"data/extracted/{download_name}/"
    print(79 * "=")
    if config.get("type") == "dila_folder":
        if download_name == "legi":
            pattern_dates = [f"LEGI_{date}" for date in dates] + [
                f"Freemium_legi_global_{date}" for date in dates
            ]
            patterns = [
                re.compile(rf"{pattern}-[0-9]{{6}}\.tar\.gz")
                for pattern in pattern_dates
            ]
            regular_pattern = re.compile(
                r"(LEGI|Freemium_legi_global)_\d{8}-\d{6}\.tar\.gz"
            )
            processor = LegiartiProcessor(input_folder=output_dir)
            ID_FIELD = "cid"

        elif download_name == "cnil":
            pattern_dates = [f"CNIL_{date}" for date in dates] + [
                f"Freemium_cnil_global_{date}" for date in dates
            ]
            patterns = [
                re.compile(rf"{pattern}-[0-9]{{6}}\.tar\.gz")
                for pattern in pattern_dates
            ]
            regular_pattern = re.compile(
                r"(CNIL|Freemium_cnil_global)_\d{8}-\d{6}\.tar\.gz"
            )
            processor = CNILProcessor(input_folder=output_dir)
            ID_FIELD = "doc_id"

        elif download_name == "constit":
            pattern_dates = [f"CONSTIT_{date}" for date in dates] + [
                f"Freemium_constit_global_{date}" for date in dates
            ]
            patterns = [
                re.compile(rf"{pattern}-[0-9]{{6}}\.tar\.gz")
                for pattern in pattern_dates
            ]
            regular_pattern = re.compile(
                r"(CONSTIT|Freemium_constit_global)_\d{8}-\d{6}\.tar\.gz"
            )
            processor = ConstitProcessor(input_folder=output_dir)
            ID_FIELD = "cid"

        elif download_name == "dole":
            pattern_dates = [f"DOLE_{date}" for date in dates] + [
                f"Freemium_dole_global_{date}" for date in dates
            ]
            patterns = [
                re.compile(rf"{pattern}-[0-9]{{6}}\.tar\.gz")
                for pattern in pattern_dates
            ]
            regular_pattern = re.compile(
                r"(DOLE|Freemium_dole_global)_\d{8}-\d{6}\.tar\.gz"
            )
            processor = DOLEProcessor(input_folder=output_dir)
            ID_FIELD = "doc_id"

        obj = BaseExtractor(config, output_dir=output_dir)
        ext = ".xml"
    elif config.get("type") == "directory":
        obj = DirectoryBaseExtractor(config, output_dir=output_dir)
        ext = ".json"
        patterns = []
        processor = DirectoryProcessor(input_folder=output_dir)
        ID_FIELD = "doc_id"
        output_dir = "./"
    elif config.get("type") == "sheets":
        ext = ".json"
        ID_FIELD = "pubId"
        if download_name in ["service_public_pro", "service_public_part"]:
            ext = ".xml"
            ID_FIELD = "sid"
        obj = SheetsBaseExtractor(config, output_dir=output_dir, ext=ext)
        patterns = []
        processor = SheetsProcessor(input_folder=output_dir)

    elif config.get("type") == "data_gouv":
        from src.extraction.data_gouv import DataGouvBaseExtractor
        from src.process.base import DataGouvProcessor

        ext = ".csv"
        ID_FIELD = "id"
        obj = DataGouvBaseExtractor(config, output_dir=output_dir, ext=ext)
        patterns = []
        processor = DataGouvProcessor(input_folder=output_dir)

    else:
        raise ValueError(
            f"Download name '{download_name}' is not supported for extraction."
        )

    print(
        f"Extraction and processing for '{download_name}' from {START_DATE.strftime('%Y-%m-%d')} to {END_DATE.strftime('%Y-%m-%d')}"
    )
    print(f"Output directory: {output_dir}")
    print(f"User ID for dataset upload: {user_id}")
    print(f"Number of days to process: {days}")
    print(f"Processor: {processor.__class__.__name__}")
    print(f"File extension to process: {ext}")
    print(f"{ID_FIELD} will be used as the unique identifier field.")

    print(79 * "=")
    api.create_repo(
        repo_id=f"{user_id}/{download_name}-full-documents",
        repo_type="dataset",
        exist_ok=True,
    )

    output_df_path = f"data/{download_name}/data/{download_name}_full_documents.parquet"
    file_to_extracts = obj.filter_input_paths(patterns=patterns)
    print(file_to_extracts)

    for file_to_extract in file_to_extracts:
        print(f"Extracting from {file_to_extract}")
        base_name = os.path.basename(file_to_extract).replace(".tar.gz", "")
        parquer_file_path = os.path.join(
            output_df_path,
            f"source_file={base_name}",
        )
        print(f"Checking if {parquer_file_path} exists...")
        if not os.path.exists(parquer_file_path):
            file_process = obj.extract(file_to_extract)
            data = []
            if file_process:
                for file in tqdm(file_process, desc="Processing files"):
                    if file.endswith(ext):

                        result = processor.process(
                            file_path=os.path.join(output_dir, file)
                        )
                        if result:
                            if isinstance(result, list):
                                for item in result:
                                    item["source_file"] = base_name
                                data.extend(result)

                            else:
                                data.append(result)
                                result["source_file"] = base_name
            if data:
                pd.DataFrame(data).to_parquet(
                    output_df_path,
                    partition_cols=["source_file"],
                    index=False,
                )
                data = []
    if os.path.exists(output_df_path):
        api.upload_large_folder(
            folder_path=f"data/{download_name}/",
            # path_in_repo=f"/data/{download_name}-full-documents.parquet/",
            repo_id=f"{user_id}/{download_name}-full-documents",
            repo_type="dataset",
        )
        print(f"Uploaded partition for {base_name} to Hugging Face Hub.")
