#!/usr/bin/env python3

"""Albert Bibliothèque CLI.

Usage:
    main.py download_files [--config-file=<path>] [--history-file=<path>]
    main.py create_tables [--model=<model_name>] [--delete-existing]
    main.py process_files (--all | --source=<source>) [--folder=<path>] [--model=<model_name>]
    main.py split_table [--source=<source>]
    main.py export_tables [--output=<path>]
    main.py upload_dataset [--input=<path>] [--dataset-name=<name>] [--private]
    main.py -h | --help

Commands:
    download_files          Download files from sources based on configuration file
    create_tables           Create database tables (with option to delete existing ones)
    process_files           Process data from specific source or all sources and insert into database
    split_table             Split a table into multiple smaller tables based on source and criteria
    export_tables           Export tables to Parquet files
    upload_dataset          Upload dataset to Hugging Face

Options:
    --config-file=<path>    Path to the config file
    --history-file=<path>   Path to the data history file
    --delete-existing       Delete existing tables before creating new ones
    --all                   Process all unprocessed data
    --model=<model_name>    Embedding model name [default: BAAI/bge-m3]. It is mandatory to specify the same model for all commands.
    --source=<source>       Source to process (service_public, travail_emploi, legi, cnil,
                            state_administrations_directory, local_administrations_directory, constit, dole)
    --folder=<path>         Folder containing unprocessed data [default: data/unprocessed]
    --input=<path>          Input path of the dataset to upload
    --dataset-name=<name>   Name of the dataset to upload to Hugging Face
    --output=<path>         Output folder for Parquet files [default: data/parquet]
    -h --help               Show this help message

Examples:
    main.py download_files
    main.py create_tables --model BAAI/bge-m3 --delete-existing
    main.py process_files --source service_public --model BAAI/bge-m3
    main.py process_files --all --folder data/unprocessed --model BAAI/bge-m3
    main.py split_table --source legi
    main.py export_tables --output data/parquet
    main.py upload_dataset --input data/parquet/service_public.parquet --dataset-name service-public
"""

from docopt import docopt
import sys
from config import (
    setup_logging,
    get_logger,
    CNIL_DATA_FOLDER,
    LEGI_DATA_FOLDER,
    CONSTIT_DATA_FOLDER,
    DATA_GOUV_DATASETS_CATALOG_DATA_FOLDER,
    LOCAL_ADMINISTRATIONS_DIRECTORY_FOLDER,
    STATE_ADMINISTRATIONS_DIRECTORY_FOLDER,
    TRAVAIL_EMPLOI_DATA_FOLDER,
    SERVICE_PUBLIC_PRO_DATA_FOLDER,
    SERVICE_PUBLIC_PART_DATA_FOLDER,
    HF_TOKEN,
    DOLE_DATA_FOLDER,
    parquet_files_folder,
    config_file_path,
    data_history_path,
)
from database import create_all_tables, split_legi_table
from download_and_processing import download_files, get_data, get_all_data
from utils import export_tables_to_parquet

# Setup logging at the start
setup_logging()
logger = get_logger(__name__)


def main():
    try:
        args = docopt(__doc__)

        # Download files
        if args["download_files"]:
            logger.info(
                f"Downloading files using config: {config_file_path} and history: {data_history_path}"
            )

            download_files(
                config_file_path=config_file_path,
                data_history_path=data_history_path,
            )

        # Create tables
        elif args["create_tables"]:
            delete_existing = True if args["--delete-existing"] else False
            model = args["--model"] if args["--model"] else "BAAI/bge-m3"
            logger.info(
                f"Creating tables with model {model} (delete_existing={delete_existing})"
            )
            create_all_tables(delete_existing=delete_existing, model=model)

        # Process data
        elif args["process_files"]:
            model = args["--model"] if args["--model"] else "BAAI/bge-m3"
            if args["--all"]:
                folder = args["--folder"]
                logger.info(f"Processing all unprocessed data from folder: {folder}")
                get_all_data(unprocessed_data_folder=folder, model=model)
            else:
                source = args["--source"]
                source_map = {
                    "service_public": [
                        SERVICE_PUBLIC_PRO_DATA_FOLDER,
                        SERVICE_PUBLIC_PART_DATA_FOLDER,
                    ],
                    "travail_emploi": [TRAVAIL_EMPLOI_DATA_FOLDER],
                    "legi": [LEGI_DATA_FOLDER],
                    "cnil": [CNIL_DATA_FOLDER],
                    "state_administrations_directory": [
                        STATE_ADMINISTRATIONS_DIRECTORY_FOLDER
                    ],
                    "local_administrations_directory": [
                        LOCAL_ADMINISTRATIONS_DIRECTORY_FOLDER
                    ],
                    "constit": [CONSTIT_DATA_FOLDER],
                    "dole": [DOLE_DATA_FOLDER],
                    "data_gouv_datasets_catalog": [DATA_GOUV_DATASETS_CATALOG_DATA_FOLDER],
                }

                if source not in source_map:
                    logger.error(f"Unknown source: {source}")
                    return 1
                else:
                    logger.info(f"Processing data from source: {source}")
                    for data_folder in source_map[source]:
                        get_data(base_folder=data_folder, model=model)

        # Split table into smaller tables based on several criteria
        elif args["split_table"]:
            source = args["--source"] if args["--source"] else "unknown"
            if source == "legi":
                logger.info("Splitting LEGI table into smaller tables")
                split_legi_table()
            else:
                logger.error(f"Splitting is not implemented for the {source} source.")
                return 1
        # Export tables to parquet
        elif args["export_tables"]:
            output = args["--output"] if args["--output"] else parquet_files_folder
            logger.info(f"Exporting tables to Parquet in folder: {output}")
            export_tables_to_parquet(output_folder=output)

        # Upload dataset to Hugging Face
        elif args["upload_dataset"]:
            from utils import HuggingFace

            input_path = args["--input"]
            dataset_name = args["--dataset-name"]
            private = True if args["--private"] else False

            logger.info(
                f"Uploading dataset {dataset_name} from {input_path} to Hugging Face (private={private})"
            )
            hf = HuggingFace(hugging_face_repo="AgentPublic", token=HF_TOKEN)
            hf.upload_dataset(
                dataset_name=dataset_name, file_path=input_path, private=private
            )

        return 0

    except Exception as e:
        logger.error(f"An error occurred: {e}")
        return 1


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
