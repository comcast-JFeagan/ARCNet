"""
parallel.py
-----------

This module provides parallel processing wrappers for fuzzy matching and grouping functions.
It uses Python's multiprocessing Pool to distribute work across multiple CPU cores.
Functions:
    - parallel_match(small_list, catalog, num_chunks=4):
          Splits the small list into chunks and applies the matching routine in parallel.
    - parallel_grouping(data, key_columns, threshold=90, num_processes=4):
          Divides the data into blocks (by a blocking key) and processes each block in parallel.

"""
 
import numpy as np
import pandas as pd
import logging
from multiprocessing import Pool
 
# Import the core matching functions from matching.py.
# These functions should be defined in matching.py:
#    - process_chunk: processes a chunk of the small list against the master catalog.
#    - compare_group: groups similar records within a data block.

from .matching import process_chunk, compare_group
 
logger = logging.getLogger(__name__)
 
def parallel_match(small_list: pd.DataFrame, catalog: pd.DataFrame, num_chunks: int = 4) -> pd.DataFrame:

    """
    Perform fuzzy matching in parallel by splitting the small_list into chunks.
    Parameters:
        small_list (pd.DataFrame): DataFrame containing the small list of parts.
        catalog (pd.DataFrame): DataFrame containing the master catalog.
        num_chunks (int): Number of chunks/processes to use (default: 4).
    Returns:
        pd.DataFrame: Concatenated DataFrame with matching results.
    """

    # Split the small list into roughly equal chunks.
    chunks = np.array_split(small_list, num_chunks)
    logger.info("Split small_list into %d chunks for parallel matching.", len(chunks))

    # Use a Pool to process each chunk in parallel.
    with Pool(processes=num_chunks) as pool:
        results = pool.starmap(process_chunk, [(chunk, catalog) for chunk in chunks])
    combined_results = pd.concat(results, ignore_index=True)

    logger.info("Parallel matching completed; combined result has %d rows.", len(combined_results))
    return combined_results
 
def parallel_grouping(data: pd.DataFrame, key_columns: list, threshold: int = 90, num_processes: int = 4) -> list:
    """
    Parallelize the grouping of similar records by first blocking the data and then processing each block in parallel.
    Parameters:
        data (pd.DataFrame): The DataFrame containing supply chain records.
        key_columns (list): List of column names to use for fuzzy matching.
        threshold (int): Similarity threshold (default: 90).
        num_processes (int): Number of parallel processes to use (default: 4).
    Returns:
        list: Flattened list of grouping results (each item is a tuple with cifa_number and matches).
    """

    # Use a simple blocking strategy based on the first 3 characters of 'mfg_part_number'
    if 'mfg_part_number' not in data.columns:
        logger.error("'mfg_part_number' column not found in data; cannot perform grouping.")
        return []

    # Make a copy to avoid modifying the original DataFrame.
    data = data.copy()
    data['prefix_group'] = data['mfg_part_number'].str[:3]

    # Group data by the blocking key.
    groups = [group for _, group in data.groupby('prefix_group')]
    logger.info("Divided data into %d groups based on prefix blocking.", len(groups))

    # Process each group in parallel using the compare_group function.
    with Pool(processes=num_processes) as pool:
        results = pool.starmap(compare_group, [(group, data, key_columns, threshold) for group in groups])

    # Flatten the list of lists.
    flat_results = [item for sublist in results for item in sublist]
    logger.info("Parallel grouping completed; obtained %d grouping results.", len(flat_results))

    return flat_results 