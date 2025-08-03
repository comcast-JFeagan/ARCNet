import io
import pandas as pd
import pytest

from cifa_cleaning.ingest import load_data, preprocess_data

def test_load_data_normalize_columns():
    # Create a fake CSV file using StringIO
    data = io.StringIO(
        'Mfg Part Number,Item Description,Manufacturer Name,Inventory Item Status Code\n'
        '12345,Widget,Acme\n'
        '67890,Gadget,Widgets Inc.\n'
        'active, inactive, active\n'
    )
    # Call the load_data function
    df = load_data(data)
    # Manually simulate what load_data does to column
    expected_columns = ['mfg_part_number', 'item_description', 'manufacturer_name', 'inventory_item_status_code']
    assert list(df.columns) == expected_columns

def test_preprocess_data_filters_inactive():
    # Create a fake DataFrame
    data = pd.DataFrame({
        'mfg_part_number': ['12345', '67890'],
        'item_description': ['Widget', 'Gadget'],
        'manufacturer_name': ['Acme', 'Widgets Inc.'],
        'inventory_item_status_code': ['active', 'Inactive']
    })
    processed = preprocess_data(data)
    # Check that the inactive row was removed
    assert len(processed) == 1
    assert processed['mfg_part_number'].tolist() == ['12345']


