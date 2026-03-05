"""Transformation utlity functions"""

# Imports
import os
import pandas as pd
from loguru import logger


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
    logger.info(f"Found {len(csv_files)} in directory.")

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
