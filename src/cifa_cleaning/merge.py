"""

merge.py

--------
 
This module provides functions for merging and combining datasets.
 
Functions:

    - combine_clean_data(mapped_data, unmapped_data):

          Combine mapped and unmapped data into a single dataset for final output.

    - merge_data(dca_data, inv_cat_data, pricing_data):

          Merge DCA data with an inventory catalog and pricing data to enrich the dataset.

"""
 
import pandas as pd

import logging
 
logger = logging.getLogger(__name__)
 
def combine_clean_data(mapped_data: pd.DataFrame, unmapped_data: pd.DataFrame) -> pd.DataFrame:

    """

    Combine mapped and unmapped data into a single dataset for final output.

    For mapped data, a 'mapped_source' is set to 'cross_reference'.

    For unmapped data, the source is determined based on whether a primary alternate CIFA is available.

    The function normalizes the primary alternate CIFA to a 9-digit string, moves it to the

    'cross_reference' column when applicable, and then concatenates both datasets.
 
    Parameters:

        mapped_data (pd.DataFrame): DataFrame containing rows with valid cross references.

        unmapped_data (pd.DataFrame): DataFrame containing rows without valid cross references.
 
    Returns:

        pd.DataFrame: The combined DataFrame.

    """

    # Work on copies to avoid modifying the original DataFrames

    mapped_data = mapped_data.copy()

    unmapped_data = unmapped_data.copy()
 
    # Mark mapped data source

    mapped_data['mapped_source'] = 'cross_reference'
 
    # Determine source for unmapped data based on primary_alternate_cifa

    unmapped_data['mapped_source'] = unmapped_data['primary_alternate_cifa'].apply(

        lambda x: 'description' if pd.notna(x) else 'unmapped'

    )
 
    # Normalize primary_alternate_cifa to 9-digit string if available

    unmapped_data['primary_alternate_cifa'] = unmapped_data['primary_alternate_cifa'].apply(

        lambda x: str(int(x)).zfill(9) if pd.notna(x) else None

    )
 
    # For rows mapped by description, move primary_alternate_cifa to cross_reference

    unmapped_data.loc[

        unmapped_data['mapped_source'] == 'description', 'cross_reference'

    ] = unmapped_data['primary_alternate_cifa']
 
    # Optionally update cross_reference_type for description-mapped rows

    unmapped_data.loc[

        unmapped_data['mapped_source'] == 'description', 'cross_reference_type'

    ] = 'description'
 
    # Combine both DataFrames

    combined_data = pd.concat([mapped_data, unmapped_data], ignore_index=True, sort=False)

    logger.info("Combined mapped (%d rows) and unmapped (%d rows) into %d rows.",

                len(mapped_data), len(unmapped_data), len(combined_data))

    return combined_data
 
def merge_data(dca_data: pd.DataFrame, inv_cat_data: pd.DataFrame, pricing_data: pd.DataFrame) -> pd.DataFrame:

    """

    Merge DCA data with inventory catalog and pricing data to enrich the dataset.

    The merge is performed in two steps:

      1. Merge dca_data with inv_cat_data using a model key (e.g., 'mfg_model' and 'mfg_part_number')

         to bring in the CIFA numbers and other inventory details.

      2. Merge the resulting DataFrame with pricing_data on the appropriate key (e.g., 'mfg_model' with 'item_id')

         to incorporate pricing information.

    Parameters:

        dca_data (pd.DataFrame): DataFrame containing DCA data.

        inv_cat_data (pd.DataFrame): DataFrame containing inventory catalog data.

        pricing_data (pd.DataFrame): DataFrame containing pricing information.

    Returns:

        pd.DataFrame: The merged DataFrame enriched with inventory and pricing details.

    """

    try:

        merged_data = pd.merge(

            dca_data, inv_cat_data,

            left_on='mfg_model', right_on='mfg_part_number',

            how='left'

        )

        logger.info("Merged DCA data with inventory catalog; resulting rows: %d", len(merged_data))

    except Exception as e:

        logger.exception("Error merging DCA data with inventory catalog: %s", e)

        raise
 
    try:

        merged_data = pd.merge(

            merged_data, pricing_data,

            left_on='mfg_model', right_on='item_id',

            how='left'

        )

        logger.info("Merged with pricing data; resulting rows: %d", len(merged_data))

    except Exception as e:

        logger.exception("Error merging with pricing data: %s", e)

        raise
 
    return merged_data

 