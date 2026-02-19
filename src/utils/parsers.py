"""Parsing utility functions"""

# Imports
from datetime import datetime
from loguru import logger
import numpy as np

def decompose_list(lst: list, separator: str) -> list:
    """
    Decompose separator separated entries in a list.
    Return new list of unique values.
    """
    new_lst = []
    for i in lst:
        new_lst.extend(str(i).split(separator))

    new_lst = list(set(new_lst))
    new_lst = [i for i in new_lst if i != "nan"]

    return new_lst

def clean_document(doc: dict, remove_ts: bool = False):
    """
    Removes keys with None, np.nan, 'nan', 'NaT', empty lists, or empty strings from a document.
    Also removes 'full_hash' and 'id_hash'. Optionally removes 'updated_at'.
    """
    # Keys to always remove
    keys_to_remove = {"full_hash", "id_hash"}

    # Optionally remove updated_at
    if remove_ts:
        keys_to_remove.add("updated_at")

    clean_doc = {
        k: v for k, v in doc.items()
        if v is not None and v != np.nan and v != 'nan' and v != 'NaT' and v != [] and v != ''
        and k not in keys_to_remove
    }

    old_keys = list(doc.keys())
    new_keys = list(clean_doc.keys())
    removed_keys = [k for k in old_keys if k not in new_keys]

    return clean_doc, removed_keys


def to_datetime(date_string, verbose=True):
    """
    Converts a date string to a datetime object.
    Returns None if the input is None, empty, or invalid.
    """
    if not date_string or not isinstance(date_string, str):
        return None

    formats = [
        '%Y-%m-%d %H:%M:%S.%f',
        '%Y-%m-%d %H:%M:%S',
        '%Y-%m-%d %H:%M',
        '%Y-%m-%d',
        '%d/%m/%Y',
    ]

    for fmt in formats:
        try:
            return datetime.strptime(date_string.strip(), fmt)
        except ValueError:
            continue

    if verbose:
        logger.error(f"Invalid date format for '{date_string}'")
    return None


def to_int(value):
    """
    Converts a value to an integer. Returns None if the input is None or an empty string.
    """
    if value is None or value == '':
        return None
    try:
        return int(value)
    except ValueError as e:
        logger.error(f"Invalid integer value '{value}': {e}")
        return None


def to_float(value):
    """
    Converts a value to a float. Returns None if the input is None or an empty string.
    """
    if value is None or value == '':
        return None
    try:
        return float(value)
    except ValueError as e:
        logger.error(f"Invalid float value '{value}': {e}")
        return None


def to_array(field_string, separator: str):
    """
    Converts a separator-separated string into a list of trimmed strings.
    """
    if not field_string:
        return []
    return [item.strip() for item in field_string.split(separator) if item.strip()]
