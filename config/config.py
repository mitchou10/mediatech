import os
from dotenv import load_dotenv


# Load environment variables
load_dotenv()

# Environment variables
LEGI_DB_PATH = os.getenv("LEGI_DB_PATH")
LEGI_DATA_FOLDER = os.getenv("LEGI_DATA_FOLDER")

# Paths for configuration and data history
config_file_path = "config/data_config.json"
data_history_path = "config/data_history.json"
