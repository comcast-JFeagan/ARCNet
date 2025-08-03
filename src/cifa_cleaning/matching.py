"""
matching.py
-----------
 
This module provides fuzzy matching and grouping functions to identify similar records
based on key columns. It leverages rapidfuzz for string similarity matching.
Functions:
    - group_similar_records(data, key_columns, threshold=90):
          Groups records in the DataFrame by comparing key columns using fuzzy matching.
    - assign_parent_cifas(data, similarity_groups):
          Selects a parent CIFA for each group of similar records.
    - apply_cross_references(data, parent_mapping):
          Updates the DataFrame’s cross_reference field based on the parent mapping.
    - fuzzy_match_with_scores(small_list, master_catalog, threshold=90):
          Matches part numbers from a small list to the master catalog using fuzzy matching.
"""
 
import pandas as pd
import logging
from rapidfuzz import fuzz, process
 
logger = logging.getLogger(__name__)

def group_similar_records(data: pd.DataFrame, key_columns: list, threshold: int = 90) -> list:
    """
    Group similar records in the DataFrame based on fuzzy matching across specified key columns.
    Parameters:
        data (pd.DataFrame): The DataFrame containing supply chain records.
        key_columns (list): List of column names to use for matching (e.g., ['mfg_part_number', 'item_description']).
        threshold (int): Similarity score threshold (default 90).
    Returns:
        list: A list of tuples in the form (cifa_number, list of matching records as (cifa_number, score)).
    """

    results = []
    for idx, row in data.iterrows():
        matches = []
        for key_col in key_columns:
            if key_col not in data.columns:
                logger.warning("Column '%s' not found in data.", key_col)
                continue
            # Compare the current row's value against the full list in the column
            column_values = data[key_col].tolist()
            extracted = process.extract(row[key_col], column_values, scorer=fuzz.token_set_ratio, limit=10)
            # Record matches that meet the threshold
            matches.extend([(data.iloc[match[2]]['cifa_number'], match[1])
                            for match in extracted if match[1] >= threshold])

        # Deduplicate matches by keeping the maximum score for each candidate
        unique_matches = {}
        for cifa, score in matches:
            unique_matches[cifa] = max(score, unique_matches.get(cifa, 0))
        results.append((row['cifa_number'], list(unique_matches.items())))
    return results
 
def assign_parent_cifas(data: pd.DataFrame, similarity_groups: list) -> dict:
    """
    Assign parent CIFA numbers to records based on similarity groups.
    For each group, candidates are sorted by 'item_last_update' (if available) in descending order,
    and the record without an existing cross_reference and with a mapped_source other than 'description'
    is chosen as the parent. If no candidate meets the criteria, the first candidate is used.
    Parameters:
        data (pd.DataFrame): The original DataFrame.
        similarity_groups (list): The list of similarity groups from group_similar_records.
 
    Returns:
        dict: Mapping from child CIFA numbers to their parent's CIFA number.
    """

    parent_mapping = {}

    for cifa, group in similarity_groups:
        candidate_cifas = [rec[0] for rec in group]
        candidates = data[data['cifa_number'].isin(candidate_cifas)]
        if candidates.empty:
            continue
        if 'item_last_update' in candidates.columns:
            candidates = candidates.sort_values('item_last_update', ascending=False)

        # Select candidate that does not have a cross_reference and is not mapped from a description
        parent_candidate = candidates[(candidates['cross_reference'].isna()) &
                                      (candidates['mapped_source'] != 'description')]
        if parent_candidate.empty:
            parent = candidates.iloc[0]
        else:
            parent = parent_candidate.iloc[0]
        parent_cifa = parent['cifa_number']
        for candidate in candidates['cifa_number']:
            if candidate != parent_cifa:
                parent_mapping[candidate] = parent_cifa
    return parent_mapping
 
def apply_cross_references(data: pd.DataFrame, parent_mapping: dict) -> pd.DataFrame:
    """
    Update the DataFrame's 'cross_reference' column using the provided parent mapping.
    Parameters:
        data (pd.DataFrame): The DataFrame to update.
        parent_mapping (dict): Mapping from child CIFA to parent CIFA.
    Returns:
        pd.DataFrame: The updated DataFrame with applied cross references.
    """
    data['cross_reference'] = data['cifa_number'].map(parent_mapping)
    return data
 
def fuzzy_match_with_scores(small_list: pd.DataFrame, master_catalog: pd.DataFrame, threshold: int = 90) -> pd.DataFrame:
    """
    Perform fuzzy matching between part numbers in the small list and master catalog.
    Uses rapidfuzz to compare part numbers and returns a DataFrame containing
    matches along with similarity scores.
    Parameters:
        small_list (pd.DataFrame): DataFrame containing the smaller list of parts.
        master_catalog (pd.DataFrame): DataFrame of the master catalog.
        threshold (int): Minimum similarity score to consider a match (default 90).
    Returns:
        pd.DataFrame: A DataFrame with columns for input part number, matched part number,
                      corresponding CIFA number, and similarity scores.
    """
    results = []

    # Create a lookup for item descriptions from the small list (if needed for further filtering)
    small_list_descriptions = small_list.set_index('mfg_part_number')['item_description'].to_dict()
 
    for part_number in small_list['mfg_part_number']:
        matches = process.extract(
            part_number,
            master_catalog['mfg_part_number'].tolist(),
            scorer=fuzz.token_sort_ratio,
            score_cutoff=threshold,
            limit=5
        )

        for matched_part, similarity, idx in matches:
            cifa = master_catalog.iloc[idx]['cifa_number']
            cifa_description = master_catalog.iloc[idx]['item_description']

            # Optionally, skip matches if the description already exists in the small list
            if cifa_description in small_list_descriptions.values():
                continue
            results.append({
                'input_part_number': part_number,
                'matched_part_number': matched_part,
                'cifa_number': cifa,
                'similarity_score': similarity
            })

    return pd.DataFrame(results)

def process_chunk(chunk: pd.DataFrame, catalog: pd.DataFrame, threshold: int = 90) -> pd.DataFrame:
    """
    Process a chunk of the small list against the master catalog.
    For each row in the chunk, perform fuzzy matching on the 'mfg_part_number'
    column against the catalog's 'mfg_part_number' values. Returns a DataFrame
    containing the matching results.
    Parameters:
        chunk (pd.DataFrame): A subset of the small list.
        catalog (pd.DataFrame): The master catalog DataFrame.
        threshold (int): Similarity score threshold.
    Returns:
        pd.DataFrame: Matching results with columns for input part number, matched part number,
                      corresponding CIFA number, and similarity score.
    """
    results = []
    for idx, row in chunk.iterrows():
        part_number = row['mfg_part_number']
        # Get matches from the catalog based on fuzzy matching
        matches = process.extract(
            part_number,
            catalog['mfg_part_number'].tolist(),
            scorer=fuzz.token_sort_ratio,
            score_cutoff=threshold,
            limit=5
        )
        for matched_part, similarity, catalog_idx in matches:
            cifa = catalog.iloc[catalog_idx]['cifa_number']
            results.append({
                'input_part_number': part_number,
                'matched_part_number': matched_part,
                'cifa_number': cifa,
                'similarity_score': similarity
            })
    return pd.DataFrame(results)

def compare_group(group: pd.DataFrame, data: pd.DataFrame, key_columns: list, threshold: int = 90) -> list:
    """
    Compare records within a group to find similar entries based on key columns.
    Parameters:
        group (pd.DataFrame): A subset of the data (blocked by some key, e.g., prefix).
        data (pd.DataFrame): The full DataFrame (used for lookup if needed).
        key_columns (list): List of column names to use for matching.
        threshold (int): Similarity threshold.
    Returns:
        list: A list of tuples (cifa_number, list of matching (cifa_number, score)).
    """

    results = []

    for idx, row in group.iterrows():
        matches = []
        for key_col in key_columns:
            if key_col not in group.columns:
                continue

            candidates = data[key_col].tolist()
            extracted = process.extract(row[key_col], candidates, scorer=fuzz.token_sort_ratio, limit=5)
            matches.extend([
                (data.iloc[match[2]]['cifa_number'], match[1])
                for match in extracted if match[1] >= threshold
            ])

        # Deduplicate matches: keep the best score for each candidate
        unique_matches = {}
        for cifa, score in matches:
            unique_matches[cifa] = max(score, unique_matches.get(cifa, 0))
        results.append((row['cifa_number'], list(unique_matches.items())))
    return results 