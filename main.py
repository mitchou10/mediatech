from config import LEGI_DB_PATH, LEGI_DATA_FOLDER
from database import create_database, insert_data
from download_data import download_files, get_data


if __name__ == "__main__":
    # Download files
    download_files(
        config_file_path="download_data/data_config.json",
        log_file_path="download_data/data_history.json",
    )

    # Process XML files
    data = get_data(base_folder=LEGI_DATA_FOLDER)

    # Create the database if does not exist
    create_database(db_path=LEGI_DB_PATH)

    # Insert data into the database
    insert_data(db_path=LEGI_DB_PATH, data=data)
