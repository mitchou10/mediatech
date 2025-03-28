from config import LEGI_DB_PATH, LEGI_DATA_FOLDER, config_file_path, data_history_path
from database import create_database
from download_data import download_files, get_data


if __name__ == "__main__":
    # Download files
    download_files(
        config_file_path=config_file_path,
        data_history_path=data_history_path,
    )

    # Create the database if does not exist
    create_database(db_path=LEGI_DB_PATH)

    # Process XML files
    data = get_data(base_folder=LEGI_DATA_FOLDER, db_path=LEGI_DB_PATH)

    # # Insert data into the database
    # insert_data(db_path=LEGI_DB_PATH, data=data)
