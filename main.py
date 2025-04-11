from config import (
    LEGI_DATA_FOLDER,
    CNIL_DATA_FOLDER,
    config_file_path,
    data_history_path,
)
from database import create_tables
from download_data import download_files, get_data


if __name__ == "__main__":
    # Download files
    download_files(
        config_file_path=config_file_path,
        data_history_path=data_history_path,
    )

    # Create the tables if they do not exist
    create_tables()

    # Process XML files and insert data into the PostgreSQL database
    get_data(base_folder=LEGI_DATA_FOLDER)
    get_data(base_folder=CNIL_DATA_FOLDER)
