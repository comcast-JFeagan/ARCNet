"""

cifa_cleaning Package

---------------------
 
A package to ingest, clean, and match supply chain part data using CIFA numbers.
 
Modules:

    - ingest: Functions for loading and preprocessing raw data.

    - clean: Data normalization and cleaning routines (e.g., cleaning manufacturer parts,

             descriptions, etc.).

    - matching: Fuzzy matching and grouping functions to identify similar records.

    - parallel: Multiprocessing wrappers for matching routines.

    - merge: Functions for combining mapped/unmapped data and other merging tasks.
 
Usage:

    Import the package and call the required functions directly:
>>> from cifa_cleaning import load_data, clean_mfg_part, group_similar_records
>>> data = load_data("path/to/file.csv")
>>> cleaned = clean_mfg_part(data, "mfg_part_number")
>>> groups = group_similar_records(cleaned, ["mfg_part_number", "item_description"])

"""
 
__version__ = "0.1.0"
 
# Import key functions from submodules to expose them at the package level

from .ingest import load_data, preprocess_data

from .clean import clean_mfg_part, clean_description, clean_mfg_info

from .matching import (

    group_similar_records,

    assign_parent_cifas,

    apply_cross_references,

    fuzzy_match_with_scores

)

from .parallel import parallel_match, parallel_grouping

from .merge import combine_clean_data

 