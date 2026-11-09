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

pg_host = os.getenv("LOCAL_PG_HOST", "localhost")
pg_db = os.getenv("LOCAL_PG_DB")
pg_user = os.getenv("LOCAL_PG_USER")
pg_pwd = os.getenv("LOCAL_PG_PASSWORD")
pg_port = int(os.getenv("LOCAL_PG_PORT", "5432"))

# Deterministic UUIDv5 namespace for CQC entities
CQC_UUID_NAMESPACE = "6ba7b810-9dad-11d1-80b4-00c04fd430c8"

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
