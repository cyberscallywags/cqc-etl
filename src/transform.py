"""Transform CQC directory data"""

# Imports
import time
import shutil
import datetime
from loguru import logger
from src.utils.queries import QUERIES
from src.utils.connectors import connect_duckdb
from src.utils.transforms import create_dfs, run_queries, export_tables
from src.config import RAW_DATA_DIR, CORE_DATA_DIR, PROCESSED_DATA_DIR, INTERIM_DATA_DIR


file_registry = {
    "onspd": CORE_DATA_DIR / "onspd",
    "directory": RAW_DATA_DIR / "18_February_2026_CQC_directory.csv",
    "ratings": RAW_DATA_DIR / "01_February_2026_Latest_ratings.ods",
    "counties": CORE_DATA_DIR / "lookups/CTY County names and codes UK as at 05_25.csv",
    "la_districts": CORE_DATA_DIR / "lookups/LAD_MAY_2025_UK_BUC_4725703192843186948.csv",
    "regions": CORE_DATA_DIR / "lookups/RGN Region names and codes EN as at 05_25.csv",
    "nhser": CORE_DATA_DIR / "lookups/NHSER NHS England Region names and codes EN as at 04_24.csv",
}


onspd_lookups = {
    "ctycd": "counties",
    "ladcd": "la_districts",
    "rgncd": "regions",
    "nhsercd": "nhser",
}


TABLES_TO_EXPORT = [
    # Main
    "locations",
    "providers",
    "location_ratings",
    "provider_ratings",

    # Geospatial
    "postcodes",
    "counties",
    "local_authorities",
    "regions",
    "nhser",

    # Ancillary
    "service_types",
    "service_groups",
    "services",
]


if __name__ == "__main__":
    start = time.perf_counter()
    started_at = datetime.datetime.now().isoformat()
    logger.info(f"Started transformation pipeline. Time: {started_at}")

    # Create timestamp
    timestamp = datetime.datetime.now().isoformat()

    # Connect to DuckDB and execute queries
    con = connect_duckdb()
    create_dfs(con, file_registry, CORE_DATA_DIR / "schema/schema_01.json")
    run_queries(con, QUERIES)
    export_tables(con, TABLES_TO_EXPORT, PROCESSED_DATA_DIR, timestamp)
    con.close()

    # Delete temp directory
    shutil.rmtree(INTERIM_DATA_DIR, ignore_errors=True)
    logger.info(f"Deleted interim directory: {INTERIM_DATA_DIR}")


    end = time.perf_counter()
    logger.success(f"Transformation pipeline complete. Runtime: {end - start} seconds")
