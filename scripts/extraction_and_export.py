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
from datasets import load_dataset, Dataset
import pandas as pd
import argparse

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

    file_to_process = obj.extract_all(max_extract=-1, patterns=patterns)
    data = []
    for file in tqdm(file_to_process, desc="Processing files"):
        if file.endswith(ext):
            result = processor.process(file_path=os.path.join(output_dir, file))
            if result:
                if isinstance(result, list):
                    data.extend(result)
                else:
                    data.append(result)
                    result["source_file"] = file

    df = pd.DataFrame(data)

    try:
        dataset = load_dataset(
            f"{user_id}/{download_name}-full-documents", split="train"
        )
        df_dataset = dataset.to_pandas()

        # Combiner les dataframes et supprimer les doublons
        df_combined = pd.concat([df_dataset, df], ignore_index=True)
        df_combined: pd.DataFrame = df_combined.drop_duplicates(
            subset=[ID_FIELD], keep="last"
        )
        df_combined.reset_index(drop=True, inplace=True)
        new_dataset = Dataset.from_pandas(df_combined)

    except Exception as e:
        print(f"Dataset not found on hub. Creating new dataset. Error: {e}")
        new_dataset = Dataset.from_pandas(df)

    print(
        f"Final dataset contains {len(new_dataset)} records after processing and deduplication."
    )
    print(new_dataset.column_names)
    new_dataset.push_to_hub(f"{user_id}/{download_name}-full-documents")
