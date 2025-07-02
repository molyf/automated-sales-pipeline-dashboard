import pandas as pd
import numpy as np
from unittest.mock import patch, MagicMock
import pytest

from etl_pipeline.transform import *

def sample_data():
    return pd.DataFrame({
        'transaction_id': [1, 2, 3, None, 5],
        'day_of_week': ['Monday', np.nan, 'Tuesday', 'Monday', None],
        'product_name': ['Apple Watch', 'Samsung Phone', np.nan, 'Apple Watch', 'iPad'],
        'product_category': ['Smartwatch', None, 'Smartphone', 'Smartwatch', None],
        'price': [799.99, None, 999.99, 799.99, None],
        'quantity_sold': [2, 1, None, 2, None],
        'total_sale': [1599.98, None, None, 1599.98, None],
        'store_location': ['Pretoria', 'Johannesburg', None, 'Pretoria', None],
        'customer_name': ['Debby', 'Ailsun', None, 'Debby', 'Charlie']
    })

def test_handle_product_category():
    df = sample_data()
    result = handle_product_category(df)
    # Rows with missing product_category for which no match can be found are dropped
    assert 'Smartwatch' in result['product_category'].values
    assert 'Smartphone' in result['product_category'].values
    # iPad row (missing category, no match) should be dropped
    assert 'iPad' not in result['product_name'].values

def test_handle_price():
    df = sample_data()
    result = handle_price(df)
    # Prices missing for Samsung Phone and iPad:
    # Samsung Phone price filled if any match found (none, so dropped),
    # iPad row should be dropped
    assert 'Samsung Phone' not in result['product_name'].values
    assert 'iPad' not in result['product_name'].values
    # Apple Watch price remains
    assert all(result[result['product_name'] == 'Apple Watch']['price'] == 799.99)

def test_drop_invalid_rows():
    df = sample_data()
    result = drop_invalid_rows(df)
    # Rows with missing transaction_id, customer_name or product_name dropped
    assert result['transaction_id'].isnull().sum() == 0
    assert result['customer_name'].isnull().sum() == 0
    assert result['product_name'].isnull().sum() == 0

def test_handle_day_of_week():
    df = sample_data()
    result = handle_day_of_week(df)
    # Missing day_of_week filled with mode ("Monday")
    assert result['day_of_week'].isnull().sum() == 0
    assert result.loc[result['day_of_week'].isnull(), 'day_of_week'].empty

def test_handle_quantity():
    df = sample_data()
    result = handle_quantity(df)
    # Missing quantity_sold filled with mean (rounded)
    mean_quantity = round(df['quantity_sold'].mean())
    assert result['quantity_sold'].isnull().sum() == 0
    assert result.loc[2, 'quantity_sold'] == mean_quantity  # index 2 had None

def test_compute_total_sale():
    df = sample_data()

    # First, fill missing price and quantity as in your ETL
    df_filled_price = handle_price(df)
    df_filled_quantity = handle_quantity(df_filled_price)

    # Then compute total_sale
    result = compute_total_sale(df_filled_quantity)

    # Now, total_sale should have no missing values (because price and quantity are filled)
    assert result['total_sale'].isnull().sum() == 0

    # And total_sale values should be correct
    for idx in result.index:
        expected = result.loc[idx, 'price'] * result.loc[idx, 'quantity_sold']
        actual = result.loc[idx, 'total_sale']
        assert abs(expected - actual) < 1e-6


def test_handle_store_location():
    df = sample_data()
    result = handle_store_location(df)
    # Missing store_location filled with mode ('Pretoria')
    assert result['store_location'].isnull().sum() == 0
    assert result.loc[1, 'store_location'] == 'Johannesburg' or result.loc[4, 'store_location'] == 'Pretoria'

def test_standardize_types():
    df = sample_data()
    df['transaction_id'] = df['transaction_id'].astype(str) + " "
    df['price'] = df['price'].astype(str)
    df['quantity_sold'] = df['quantity_sold'].astype(str)
    df['total_sale'] = df['total_sale'].astype(str)
    result = standardize_types(df)
    # String columns stripped and numeric columns converted
    assert all(result['transaction_id'].apply(lambda x: isinstance(x, (int, float))))
    assert all(result['price'].apply(lambda x: isinstance(x, (int, float))))
    assert all(result['quantity_sold'].apply(lambda x: isinstance(x, (int, float))))
    assert all(result['total_sale'].apply(lambda x: isinstance(x, (int, float))))

@patch("etl_pipeline.transform.get_run_logger")  
def test_transform_sales_data(mock_get_logger):
    mock_logger = MagicMock()
    mock_get_logger.return_value = mock_logger

    df_raw = sample_data()
    cleaned_df, raw_df = transform_sales_data(df_raw)

    # Confirm no missing critical fields
    assert cleaned_df['transaction_id'].isnull().sum() == 0
    assert cleaned_df['customer_name'].isnull().sum() == 0
    assert cleaned_df['product_name'].isnull().sum() == 0
    assert cleaned_df['product_category'].isnull().sum() == 0
    assert cleaned_df['price'].isnull().sum() == 0
    assert cleaned_df['day_of_week'].isnull().sum() == 0
    assert cleaned_df['quantity_sold'].isnull().sum() == 0
    assert cleaned_df['total_sale'].isnull().sum() == 0
    assert cleaned_df['store_location'].isnull().sum() == 0

    # Confirm some rows are dropped (because of missing product_name, price fill, etc)
    assert cleaned_df.shape[0] < df_raw.shape[0]

    # Confirm raw_df remains unchanged
    pd.testing.assert_frame_equal(df_raw, raw_df)

    # Check that logger.info was called
    mock_logger.info.assert_called()

