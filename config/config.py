import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# PostgreSQL configuration
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5433")
POSTGRES_DB = os.getenv("POSTGRES_DB", "albert_data")
POSTGRES_USER = os.getenv("POSTGRES_USER", "user")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "password")

# Paths for configurations and data history
config_file_path = "config/data_config.json"
data_history_path = "config/data_history.json"
parquet_files_folder="data/parquet"

# Data folders
CNIL_DATA_FOLDER = os.getenv("CNIL_DATA_FOLDER", "data/unprocessed/cnil")
CONSTIT_DATA_FOLDER = os.getenv("CONSTIT_DATA_FOLDER", "data/unprocessed/constit")
DIRECTORIES_FOLDER = os.getenv("DIRECTORIES_FOLDER", "data/unprocessed/directories")
DOLE_DATA_FOLDER = os.getenv("DOLE_DATA_FOLDER", "data/unprocessed/dole")
LEGI_DATA_FOLDER = os.getenv("LEGI_DATA_FOLDER", "data/unprocessed/legi")

PARQUET_FOLDER = os.getenv("PARQUET_FOLDER", "data/parquet")

# OpenAI API configuration
STAGGING_URL = os.getenv("STAGGING_URL", "https://albert.api.staging.etalab.gouv.fr/v1")
STAGGING_API_KEY = os.getenv("STAGGING_API_KEY", "your_api_key_here")
