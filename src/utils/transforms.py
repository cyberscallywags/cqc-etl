"""Transformation utlity functions"""

# Imports
import os
import json
from pathlib import Path
from datetime import datetime
import pandas as pd
from loguru import logger
from .parsers import clean_document


def combine_csvs(folder_path):
    """
    Reads all CSV files in a folder and combines them into one pandas DataFrame.

    Parameters
    ----------
    folder_path : str
        Path to the folder containing CSV files.

    Returns
    -------
    pandas.DataFrame
        A single DataFrame containing all rows from all CSVs.
    """
    csv_files = [
        f for f in os.listdir(folder_path)
        if f.lower().endswith(".csv")
    ]
    logger.info(f"Found {len(csv_files)} file in folder.")

    if not csv_files:
        raise ValueError("No CSV files found in the folder.")

    df_list = []
    for file in csv_files:
        logger.info(f"Adding {file}...")
        full_path = os.path.join(folder_path, file)
        df = pd.read_csv(full_path, skiprows=4)
        df_list.append(df)

    combined_df = pd.concat(df_list, ignore_index=True)
    logger.success("Pandas dataframe created.")
    return combined_df


def add_timestamp(target, ts):
    """Add timestamp to Pandas dataframe or document."""
    target["updated_at"] = ts
    return target


def make_documents(df: pd.DataFrame, transform_specs: dict, ts: datetime, output_dir: Path):
    """
    Creates JSON document lists from specified dataframe using transform specs.

    Parameters
    - df (dataframe): Pandas dataframe to use as base for transformation and JSON creation.
    - transform_specs (dict): 
        Dictionary that defines the labels (list of documents) to be extracted from the dataframe.
        Has two lambda based key, value pairs:
        - data: Creates a subset dataframe, specifying the columns, filters, and sorting.
        - properties: Defines properties/fields in a transformed document.
    - ts (datetime): Datetime timestamp added to each subset dataframe as a new column.
    - output_dir (Path): Folder to save JSON files to. Created if it doesn't exist.
    """

    for label, specs in transform_specs.items():
        # Create subset dataframe and convert to list of dicts
        subset_df = specs["data"](df)
        subset_df["updated_at"] = ts.isoformat()
        raw_documents = list(subset_df.to_dict(orient="records"))

        # Transform list of dicts
        transformed_documents = []
        for raw_doc in raw_documents:
            transformed_doc, _ = clean_document(specs["properties"](raw_doc))
            transformed_documents.append(transformed_doc)

        # Save transformed list as JSON
        os.makedirs(output_dir, exist_ok=True)
        with open(output_dir / f"{label}.json", "w", encoding="utf-8") as f:
            json.dump(transformed_documents, f, indent=2)
        logger.info(f"Saved {label}.json: {len(transformed_documents)} records.")
