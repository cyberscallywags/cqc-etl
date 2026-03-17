"""Parsing utility functions"""

# Imports
import uuid
from datetime import datetime
from loguru import logger
import numpy as np
import pandas as pd


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


def normalise_value(v):
    """Normalise values."""
    # ndarray
    if isinstance(v, np.ndarray):
        return v.tolist()

    # datetime
    if isinstance(v, datetime):
        return v.isoformat()

    # uuid
    if isinstance(v, uuid.UUID):
        return str(v)

    return v


def is_empty_value(v):
    """Check for empty/invalid values."""
    # None
    if v is None:
        return True

    # Empty string, "NA" or "-"
    if isinstance(v, str) and v in ("", "-", "NA", "N/A"):
        return True

    # Empty list/dict/tuple
    if isinstance(v, (list, tuple, dict, set)):
        return len(v) == 0

    # Only call pd.isna on non-container values
    if not isinstance(v, (list, tuple, dict, set, np.ndarray, pd.Series)):
        return pd.isna(v)

    return False


def clean_document(doc: dict, remove_ts: bool = False, return_removed=True):
    """
    Removes keys with empty values from a document.
    Also removes 'full_hash' and 'id_hash'. Optionally removes 'updated_at'.
    Returns cleaned document or cleaned document and removed keys.
    """
    keys_to_remove = {"full_hash", "id_hash"}
    if remove_ts:
        keys_to_remove.add("updated_at")

    clean_doc = {
        k: normalise_value(v)
        for k, v in doc.items()
        if k not in keys_to_remove and not is_empty_value(v)
    }

    if return_removed:
        removed_keys = [k for k in doc if k not in clean_doc]
        return clean_doc, removed_keys

    return clean_doc


def clean_documents(docs: list, remove_ts: bool = False):
    """
    Applies 'clean_document' to multiple documents in a list.
    """
    return [clean_document(doc, remove_ts=remove_ts, return_removed=False) for doc in docs]


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
