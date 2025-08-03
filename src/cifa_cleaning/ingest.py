"""
ingest.py
---------
 
This module handles the ingestion of raw CSV files into pandas DataFrames.
It includes functions for loading data with standardized column names and performing
basic preprocessing tasks.
 
Functions:
    - load_data(file_path, **kwargs): Load a CSV file and normalize its column names.
    - preprocess_data(data): Apply initial preprocessing (e.g., filter out inactive items).
"""
 
import pandas as pd
import logging
 
# Set up a logger for this module
logger = logging.getLogger(__name__)
 
def load_data(file_path: str, **kwargs) -> pd.DataFrame:
    """
    Load a CSV file into a pandas DataFrame with normalized column names.
 
    Parameters:
        file_path (str): The path to the CSV file.
        **kwargs: Additional keyword arguments to pass to pd.read_csv().
 
    Returns:
        pd.DataFrame: The loaded DataFrame with cleaned column names.
    """
    try:
        df = pd.read_csv(file_path, **kwargs)
        # Normalize column names: remove extra spaces, lower-case, replace spaces and special characters
        df.columns = (
            df.columns
            .str.strip()
            .str.lower()
            .str.replace(' ', '_')
            .str.replace('(', '')
            .str.replace(')', '')
        )
        logger.info("Loaded '%s' with %d rows and %d columns.", file_path, len(df), len(df.columns))
        return df
    except Exception as e:
        logger.exception("Error loading file '%s': %s", file_path, e)
        raise
 
def preprocess_data(data: pd.DataFrame) -> pd.DataFrame:
    """
    Perform initial preprocessing on the DataFrame.
 
    For example, filter out rows where 'inventory_item_status_code' equals 'Inactive'.
 
    Parameters:
        data (pd.DataFrame): The DataFrame to preprocess.
 
    Returns:
        pd.DataFrame: The preprocessed DataFrame.
    """
    if 'inventory_item_status_code' in data.columns:
        initial_count = len(data)
        data = data[data['inventory_item_status_code'] != 'Inactive']
        logger.info("Filtered inactive items: %d -> %d rows.", initial_count, len(data))
    else:
        logger.warning("Column 'inventory_item_status_code' not found; no filtering applied.")
    return data