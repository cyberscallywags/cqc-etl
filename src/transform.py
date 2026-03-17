"""Transform CQC directory data"""

# Imports
import time
import shutil
import datetime
from loguru import logger
from src.utils.files import RawPipeline
from src.utils.connectors import connect_duckdb
from src.utils.queries import load_schema, run_queries
from src.utils.transforms import create_dfs, export_tables, transform_cleanup
from src.config import (
    RAW_DATA_DIR, CORE_DATA_DIR, PROCESSED_DATA_DIR, INTERIM_DATA_DIR, ARCHIVE_DATA_DIR
    )


file_registry = {
    "onspd": CORE_DATA_DIR / "lookups/ONSPD_NOV_2025_UK.parquet",
    "la_districts": CORE_DATA_DIR / "lookups/LAD_MAY_2025_UK_BUC_4725703192843186948.csv",
    "counties": CORE_DATA_DIR / "lookups/CTY County names and codes UK as at 05_25.csv",
    "icbs": CORE_DATA_DIR / "lookups/ICB Integrated Care Board names and codes UK as at 04_23.csv",
    "regions": CORE_DATA_DIR / "lookups/RGN Region names and codes EN as at 05_25.csv",
    "nhser": CORE_DATA_DIR / "lookups/NHSER NHS England Region names and codes EN as at 04_24.csv",
    "lad_pop": CORE_DATA_DIR / "lookups/LAD Populations by Age (1991-2021).csv",
    "past_location_ratings": ARCHIVE_DATA_DIR / "past_location_ratings.parquet",
    "past_provider_ratings": ARCHIVE_DATA_DIR / "past_provider_ratings.parquet",
}

no_rename = ["past_location_ratings", "past_provider_ratings", "location_bools"]



if __name__ == "__main__":
    start = time.perf_counter()
    started_at = datetime.datetime.now().isoformat()
    logger.info(f"Started transformation pipeline. Time: {started_at}")

    # Create timestamp
    timestamp = datetime.datetime.now().isoformat()

    # Convert Excel/ODS files if present and add to file registry
    file_registry = (file_registry | RawPipeline(RAW_DATA_DIR, load_schema("raw_01")).run())

    # Connect to DuckDB and execute queries
    con = connect_duckdb()
    create_dfs(con, file_registry, load_schema("transform_01"),
               exclude=no_rename, cleanup_fn=transform_cleanup)
    run_queries(con, select="transform")

    # Export tables
    TABLES_TO_EXPORT = list(load_schema("aura_01")["filemap"].keys())
    export_tables(TABLES_TO_EXPORT, PROCESSED_DATA_DIR, timestamp)
    con.close()

    # Delete temp directory
    shutil.rmtree(INTERIM_DATA_DIR, ignore_errors=True)
    logger.info(f"Deleted interim directory: {INTERIM_DATA_DIR}")

    end = time.perf_counter()
    logger.success(f"Transformation pipeline complete. Runtime: {end - start} seconds")
