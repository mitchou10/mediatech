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

# Paths for configuration and data history
config_file_path = "config/data_config.json"
data_history_path = "config/data_history.json"
LEGI_DATA_FOLDER = os.getenv("LEGI_DATA_FOLDER", "data/legi")
CNIL_DATA_FOLDER = os.getenv("CNIL_DATA_FOLDER", "data/cnil")
