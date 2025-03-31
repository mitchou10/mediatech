from config import LEGI_DATA_FOLDER, config_file_path, data_history_path
from database import create_table
from download_data import download_files, get_data


if __name__ == "__main__":
    # Download files
    download_files(
        config_file_path=config_file_path,
        data_history_path=data_history_path,
    )

    # Create the table if it does not exist
    create_table()

    # Process XML files and insert data into the PostgreSQL database
    get_data(base_folder=LEGI_DATA_FOLDER)
