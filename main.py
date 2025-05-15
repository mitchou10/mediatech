from config import (
    setup_logging,
    get_logger,
    CNIL_DATA_FOLDER,
    LEGI_DATA_FOLDER,
    CONSTIT_DATA_FOLDER,
    LOCAL_DIRECTORY_FOLDER,
    NATIONAL_DIRECTORY_FOLDER,
    TRAVAIL_EMPLOI_DATA_FOLDER,
    SERVICE_PUBLIC_PRO_DATA_FOLDER,
    SERVICE_PUBLIC_PART_DATA_FOLDER,
    DOLE_DATA_FOLDER,
    parquet_files_folder,
    config_file_path,
    data_history_path,
)
from database import create_tables
from download_and_processing import download_files, get_data, get_all_data
from utils import (
    export_tables_to_parquet,
)

# Setup logging at the start
setup_logging()
logger = get_logger(__name__)

if __name__ == "__main__":
    # Download files
    download_files(
        config_file_path=config_file_path,
        data_history_path=data_history_path,
    )

    # # Create the tables if they do not exist
    create_tables(delete_existing=False)

    # Process XML files and insert data into the PostgreSQL database
    # get_all_data(unprocessed_data_folder="data/unprocessed")
    # get_data(base_folder=SERVICE_PUBLIC_PRO_DATA_FOLDER)
    # get_data(base_folder=SERVICE_PUBLIC_PART_DATA_FOLDER)
    # get_data(base_folder=TRAVAIL_EMPLOI_DATA_FOLDER)
    # get_data(base_folder=LEGI_DATA_FOLDER)
    # get_data(base_folder=CNIL_DATA_FOLDER)
    # get_data(base_folder=NATIONAL_DIRECTORY_FOLDER)

    # # Convert tables to Parquet files
    # export_tables_to_parquet(output_folder=parquet_files_folder)
