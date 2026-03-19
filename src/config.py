"""Project config"""

# Imports
import os
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables
load_dotenv()
neo4j_uri = os.getenv("LOCAL_NEO4J_URI")
neo4j_user = os.getenv("LOCAL_NEO4J_USERNAME")
neo4j_pwd = os.getenv("LOCAL_NEO4J_PASSWORD")

# Paths
PROJ_ROOT = Path(__file__).resolve().parents[1]
SRC = PROJ_ROOT / "src"
DATA_DIR = PROJ_ROOT / "data"
RAW_DATA_DIR = DATA_DIR / "raw"
PROCESSED_DATA_DIR = DATA_DIR / "processed"
INTERIM_DATA_DIR = DATA_DIR / "interim"
ARCHIVE_DATA_DIR = DATA_DIR / "archive"
CORE_DATA_DIR = DATA_DIR / "core"
SCHEMAS_DIR = PROJ_ROOT / "schemas"
