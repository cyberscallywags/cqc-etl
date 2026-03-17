"""Transformation utlity functions"""

# Imports
import json
import uuid
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
import polars as pl
from loguru import logger
from .files import load_file
from .parsers import clean_documents
from .connectors import connect_duckdb



def create_dfs(con, file_registry: dict, schema: dict, exclude=None, cleanup_fn=None):
    """Generic dataframe creation, schema renaming, and registration in DuckDB."""
    logger.info("Creating dataframes...")

    # Load all dataframes in parallel
    with ThreadPoolExecutor() as ex:
        futures = {
            name: ex.submit(load_file, path)
            for name, path in file_registry.items()
            }
        dfs = {name: fut.result() for name, fut in futures.items()}

    logger.success("Dataframes created successfully.")

    # Schema-driven renaming
    logger.info("Filtering and renaming columns...")
    exclude = [] if exclude is None else exclude
    for name, df in dfs.items():
        if name in exclude:
            logger.info(f"Skipping schema renaming for '{name}'")
            continue

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
    logger.success("Schema renaming complete.")

    # Optional cleanup hook
    if cleanup_fn:
        logger.info("Running cleanup function...")
        cleanup_fn(dfs)
        logger.success("Cleanup complete.")

    # Register in DuckDB
    for name, df in dfs.items():
        con.register(f"{name}_df", df)
        logger.info(f"Registered '{name}' dataframe in database.")

    logger.success(f"{len(dfs)} dataframes created and registered in DuckDB.")

    return dfs


class TableExporter:
    """Handles exporting a single DuckDB table in various formats."""
    def __init__(self, fmt: str):
        self.fmt = fmt
        self.exporters = {
            "json": self.export_json,
            "parquet": self.export_parquet,
        }

        if fmt not in self.exporters:
            raise ValueError("Export format must be 'json' or 'parquet'.")

    def export(self, table, output_dir, ts):
        """Export file."""
        con = connect_duckdb()
        df = con.execute(f"SELECT *, '{ts}' AS updatedAt FROM {table}").df()
        return self.exporters[self.fmt](df, table, output_dir)

    # Format-specific methods

    def export_json(self, df, table, output_dir):
        """Export JSON."""
        records = clean_documents(df.to_dict(orient="records"))
        out_path = output_dir / f"{table}.json"
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(records, f, indent=2)
        return out_path.name, len(records)

    def export_parquet(self, df, table, output_dir):
        """Export Parquet."""
        df = df.apply(
            lambda col: col.astype(str)
            if col.map(lambda v: isinstance(v, uuid.UUID)).any()
            else col
            )

        out_path = output_dir / f"{table}.parquet"
        df.to_parquet(out_path, index=False, compression="snappy")
        return out_path.name, len(df)


def export_tables(table_names: list, output_dir: Path, ts: str, fmt: str="json"):
    """
    Export specified DuckDB tables, adding an updated_at timestamp.

    Parameters
    - con: DuckDB database connection.
    - table_names (list): list of tables to export.
    - output_dir (Path): Folder to save output files to.
    - ts (str): Datetime timestamp (isoformat) added to each subset dataframe.
    - fmt (str): Export each table as a .json or .parquet file.
    """
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    exporter = TableExporter(fmt)

    with ThreadPoolExecutor(max_workers=6) as executor:
        futures = {
            executor.submit(exporter.export, table, output_dir, ts): table
            for table in table_names
            }

        for future in as_completed(futures):
            table = futures[future]
            try:
                filename, count = future.result() # type: ignore
                logger.success(f"Saved {filename}: {count} records.")
            except Exception as e: # pylint: disable=W0718
                logger.error(f"Failed to export {table}: {e}")



# SPECIALISED FUNCTIONS

def combine_parquet(files: list, selected_columns: list) -> pl.DataFrame:
    """Combine rating Parquet files into one polars dataframe."""

    def load_and_normalise(fp: str, selected_columns: list) -> pl.DataFrame:
        df = pl.read_parquet(fp)

        # Rename Key Question to Domain
        if "Key Question" in df.columns:
            df = df.rename({"Key Question": "Domain"})

        # Normalise rating column names
        if "Latest Rating" not in df.columns:
            for alt in ["Latest Overall Rating", "Overall Rating"]:
                if alt in df.columns:
                    df = df.rename({alt: "Latest Rating"})
                    break

        # Add missing columns as empty str
        missing_cols = [
            pl.lit("").cast(pl.String).alias(col)
            for col in selected_columns
            if col not in df.columns
            ]

        df = df.with_columns(missing_cols)

        # Ensure all selected columns are str except Publication Date
        df = df.with_columns([
            pl.col(col).cast(pl.String)
            for col in selected_columns
            if col != "Publication Date"
            ])

        return df.select(selected_columns)

    dfs = [load_and_normalise(f, selected_columns) for f in files]
    return pl.concat(dfs, how="vertical")


def transform_cleanup(dfs: dict):
    """Add address column to locations data and coalesce boolean data."""

    # Concatenate addresses
    ldf = dfs["locations"]
    for pref in ["", "provider"]:
        ldf = ldf.with_columns(
            pl.concat_str(
                [
                    pl.when(pl.col(f"{pref}addressLine1").str.len_chars() > 0)
                    .then(pl.col(f"{pref}addressLine1"))
                    .otherwise(None),
                    pl.when(pl.col(f"{pref}addressLine2").str.len_chars() > 0)
                    .then(pl.col(f"{pref}addressLine2"))
                    .otherwise(None),
                    pl.when(pl.col(f"{pref}city").str.len_chars() > 0)
                    .then(pl.col(f"{pref}city"))
                    .otherwise(None),
                ],
                separator=", "
            ).alias(f"{pref}address")
        )

    # Coalesce boolean columns
    lbdf = dfs["location_bools"]
    col_map = {
        "Regulated activity -": "regulatedActivities",
        "Service type - ": "serviceTypes",
        "Service user band - ": "serviceUserBands"
        }
    for prefix, new in col_map.items():
        lbdf = lbdf.with_columns(
            pl.concat_list([
                pl.when(pl.col(col) == "Y")
                .then(pl.lit(col.replace(prefix, "")))
                .otherwise(None)
                for col in lbdf.columns if col.startswith(prefix)
            ])
            .list.drop_nulls()
            .alias(new)
        )

        ldf = ldf.with_columns(lbdf[new])

    # Apply changes
    dfs["locations"] = ldf
    del dfs["location_bools"]

    return dfs
