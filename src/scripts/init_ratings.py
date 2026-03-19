"""Concatenate and upsert past location and provider ratings."""

# Imports
import os
import time
import shutil
import datetime
from pathlib import Path
from loguru import logger
from src.utils.connectors import connect_duckdb
from src.utils.queries import run_queries, load_schema
from src.utils.transforms import combine_parquet, create_dfs, export_tables
from src.config import ARCHIVE_DATA_DIR, INTERIM_DATA_DIR


# Initialise file registry
file_registry = {}
TABLES_TO_EXPORT = ["past_location_ratings", "past_provider_ratings"]
no_rename = TABLES_TO_EXPORT


if __name__ == "__main__":
    start = time.perf_counter()
    started_at = datetime.datetime.now().isoformat()
    logger.info(f"Started transformation pipeline. Time: {started_at}")

    # Define root and subfolders and create file list
    root_folder = ARCHIVE_DATA_DIR / "ratings_raw"
    subfolders = [str(i) for i in range(2015, 2026+1)]
    os.makedirs(INTERIM_DATA_DIR, exist_ok=True)

    prefixes = ["Locations", "Providers"]
    for prefix in prefixes:
        file_list = [
            f
            for folder in subfolders
            for f in (Path(root_folder) / folder).glob("*.parquet")
            if f.name.startswith(prefix)
            ]

        selected_cols = [
            f"{prefix[:-1]} ID", f"{prefix[:-1]} Name",
            "Service / Population Group", "Domain", "Latest Rating",
            "Publication Date", "Report Type", "Inherited Rating (Y/N)"
            ]

        # Combine sheets and write parquet file. Add file registry entry
        name = prefix[:-1].lower()
        out_file = INTERIM_DATA_DIR / f"past_{name}_ratings.parquet"
        combine_parquet(file_list, selected_cols).write_parquet(out_file)
        file_registry[f"{name}_ratings"] = out_file


    # Create timestamp
    timestamp = datetime.datetime.now().isoformat()

    # Connect to DuckDB and execute queries
    con = connect_duckdb()
    create_dfs(con, file_registry, load_schema("initialise_01"), exclude=no_rename)
    run_queries(con, select="init_ratings")
    export_tables(TABLES_TO_EXPORT, ARCHIVE_DATA_DIR, timestamp, "parquet")
    con.close()

    # Delete temp directory
    shutil.rmtree(INTERIM_DATA_DIR, ignore_errors=True)
    logger.info(f"Deleted interim directory: {INTERIM_DATA_DIR}")

    end = time.perf_counter()
    logger.success(f"Transformation pipeline complete. Runtime: {end - start} seconds")
