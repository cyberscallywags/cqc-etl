"""Transformation utlity functions"""

# Imports
import os
import json
from glob import glob
from pathlib import Path
import pandas as pd
import polars as pl
from loguru import logger
from .parsers import clean_documents



def combine_csvs(folder_path, skiprows=0):
    """Reads all CSV files in a folder and combines them into one pandas DataFrame."""
    csv_files = glob(os.path.join(folder_path, "*.csv"))
    logger.info(f"Found {len(csv_files)} files in folder.")
    df_list = [
        pd.read_csv(f, skiprows=skiprows, engine="pyarrow")
        for f in csv_files
    ]
    return pd.concat(df_list, ignore_index=True)


def run_queries(con, queries: list):
    """Run DuckDB queries."""
    n_queries = len(queries)

    for i, query in enumerate(queries, start=1):
        logger.info(f"Running queries: {i} of {n_queries}")
        con.execute(query)


def create_dfs(con, file_registry: dict, schema_path):
    """Create and filter raw dataframes using the registry and schema. Register in DuckDB"""
    logger.info("Creating dataframes...")

    dfs = {
        "onspd": combine_csvs(file_registry["onspd"]),
        "counties": pd.read_csv(file_registry["counties"]),
        "la_districts": pd.read_csv(file_registry["la_districts"]),
        "regions": pd.read_csv(file_registry["regions"]),
        "nhser": pd.read_csv(file_registry["nhser"]),
        "directory": pd.read_csv(file_registry["directory"], skiprows=4),
        "locations": pl.read_excel(file_registry["ratings"], sheet_name="Locations").to_pandas(),
        "providers": pl.read_excel(file_registry["ratings"], sheet_name="Providers").to_pandas(),
    }

    logger.success("Dataframe creation complete.") # pylint: disable=W0101

    # Load schema
    logger.info("Loading schema...")
    with open(schema_path, "r", encoding="utf-8") as f:
        schema: dict = json.load(f)

    # Apply schema-driven renaming
    logger.info("Filtering and renaming columns...")
    for name, df in dfs.items():
        if name not in schema:
            raise KeyError(f"Schema missing entry for dataframe '{name}'")

        cols_in = list(schema[name].values())
        cols_out = list(schema[name].keys())

        missing = set(cols_in) - set(df.columns)
        if missing:
            raise KeyError(f"Missing columns in {name}: {missing}")

        df = df[cols_in]
        df.columns = cols_out
        dfs[name] = df

    # Clean directory strings
    directory = dfs["directory"]
    for col in directory.columns:
        if pd.api.types.is_string_dtype(directory[col]):
            directory[col] = directory[col].str.strip()

    # Build address fields
    for key in ("providers", "locations"):
        df = dfs[key]
        df["address"] = df.apply(
            lambda row: ", ".join(
                part for part in [row.get("addressLine1"), row.get("addressLine2"), row.get("city")]
                if pd.notna(part) and str(part).strip() != ""
            ),
            axis=1,
        )

    logger.success("Sorting and renaming complete.")

    # Register dataframes in DuckDB
    for name, df in dfs.items():
        con.register(f"{name}_df", df)
        logger.info(f"Registered '{name}' dataframe in database.")

    logger.success(f"{len(dfs)} dataframes created and registered in DuckDB.")


def export_tables(con, table_names: list, output_dir: Path, ts: str):
    """
    Export specified DuckDB tables to JSON, adding an updated_at timestamp.

    Parameters
    - con: DuckDB database connection.
    - table_names (list): list of tables to export.
    - output_dir (Path): Folder to save JSON files to. Created if it doesn't exist.
    - ts (str): Datetime timestamp (isoformat) added to each subset dataframe as a new column.
    """

    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    for table in table_names:
        # Fetch table as a DataFrame
        df = con.execute(f"SELECT *, '{ts}' AS updated_at FROM {table}").df()

        # Convert to list of dicts and clean
        records = clean_documents(df.to_dict(orient="records"))

        # Write JSON
        out_path = output_dir / f"{table}.json"
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(records, f, indent=2)

        logger.success(f"Saved {table}.json: {len(records)} records.")
