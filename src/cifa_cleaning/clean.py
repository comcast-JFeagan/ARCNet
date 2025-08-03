"""
clean.py
--------
 This module provides functions for data normalization and cleaning.
It includes routines to clean manufacturer part numbers, manufacturer names,
and item descriptions. The clean_mfg_info function applies these cleaning
functions to a pandas DataFrame containing your supply chain data.
"""
 
import re
import pandas as pd
import logging
 
logger = logging.getLogger(__name__)
 
def clean_mfg_part(value: str) -> str:
    """
    Clean and normalize a manufacturer part number string.
    Parameters:
        value (str): The manufacturer part number.
    Returns:
        str: The cleaned manufacturer part number.
    """

    if not isinstance(value, str):
        return value
    value = value.strip()

    # Replace multiple spaces with a single space
    value = re.sub(r'\s+', ' ', value)

    # Remove punctuation (keeping alphanumerics and whitespace)
    value = re.sub(r'[^\w\s]', '', value)
    return value.lower()
 
def clean_mfg_name(value: str) -> str:
    """
    Clean and normalize a manufacturer name string.
    Parameters:
        value (str): The manufacturer name.
    Returns:
        str: The cleaned manufacturer name.
    """

    if not isinstance(value, str):
        return value

    value = value.strip()
    value = re.sub(r'\s+', ' ', value)
    value = re.sub(r'[^\w\s]', '', value)

    return value.lower()
 
def clean_description(value: str) -> str:
    """
    Clean and normalize an item description string.
    Parameters:
        value (str): The item description.
    Returns:
        str: The cleaned item description.
    """

    if not isinstance(value, str):
        return value

    value = value.strip()
    value = re.sub(r'\s+', ' ', value)
    value = re.sub(r'[^\w\s]', '', value)

    # Remove common stopwords (the, a, an, and, or, in)
    value = re.sub(r'\b(the|a|an|and|or|in)\b', '', value, flags=re.IGNORECASE)
    return value.lower()
 
def clean_mfg_info(data: pd.DataFrame) -> pd.DataFrame:
    """
    Apply cleaning functions to manufacturer-related columns in a DataFrame.
    This function cleans the 'mfg_part_number', 'item_description', and 'manufacturer_name'
    columns if they exist in the DataFrame.
    Parameters:
        data (pd.DataFrame): The DataFrame containing raw data.
    Returns:
        pd.DataFrame: The DataFrame with cleaned manufacturer information.
    """

    if 'mfg_part_number' in data.columns:
        data['mfg_part_number'] = data['mfg_part_number'].apply(clean_mfg_part)
        logger.info("Cleaned 'mfg_part_number' column.")
    else:
        logger.warning("'mfg_part_number' column not found in DataFrame.")
 
    if 'item_description' in data.columns:
        data['item_description'] = data['item_description'].apply(clean_description)
        logger.info("Cleaned 'item_description' column.")
    else:

        logger.warning("'item_description' column not found in DataFrame.")
 
    if 'manufacturer_name' in data.columns:
        data['manufacturer_name'] = data['manufacturer_name'].apply(clean_mfg_name)
        logger.info("Cleaned 'manufacturer_name' column.")
    else:
        logger.warning("'manufacturer_name' column not found in DataFrame.")
 
    return data 