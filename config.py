import os
from dotenv import load_dotenv


# Load environment variables
load_dotenv()

LEGI_DB_PATH = os.getenv("LEGI_DB_PATH")
LEGI_DATA_FOLDER = os.getenv("LEGI_DATA_FOLDER")
