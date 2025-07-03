import pandas as pd
import pytest
from unittest.mock import patch, MagicMock
from etl_pipeline.load import model_sales_data, upload_df_to_s3  


def sample_data():
    return pd.DataFrame({
        "customer_name": ["Alice", "Bob", "Alice"],
        "product_name": ["Widget", "Gadget", "Widget"],
        "product_category": ["Tools", "Electronics", "Tools"],
        "store_location": ["Store1", "Store2", "Store1"],
        "price": [10, 20, 10],
        "quantity_sold": [1, 2, 3],
        "total_sale": [10, 40, 30],
        "transaction_id": [1, 2, 3],
        "day_of_week": ["Monday", "Tuesday", "Wednesday"]
    })


def test_model_sales_data():
    df = sample_data()
    df_copy = df.copy()  # Keep original data untouched for comparison

    customers, products, stores, sales, raw_df = model_sales_data(df.copy(), df_copy)

    # --- Check customers ---
    assert set(customers["customer_name"]) == set(df["customer_name"])
    assert customers["customer_id"].is_unique

    # --- Check products ---
    assert set(products["product_name"]) == set(df["product_name"])
    # Check that the product category matches the original data for that product
    for _, row in products.iterrows():
        product_name = row["product_name"]
        product_category = row["product_category"]
        input_categories = df.loc[df["product_name"] == product_name, "product_category"].unique()
        assert product_category in input_categories
    assert products["product_id"].is_unique

    # --- Check stores ---
    assert set(stores["store_location"]) == set(df["store_location"])
    assert stores["store_id"].is_unique

    # --- Check sales dataframe ---
    for col in ["customer_name", "product_name", "product_category", "store_location"]:
        assert col not in sales.columns

    # --- Check raw dataframe unchanged ---
    pd.testing.assert_frame_equal(raw_df, df_copy)


@patch("etl_pipeline.load.get_run_logger")
def test_upload_df_to_s3(mock_get_logger):
    mock_logger = MagicMock()
    mock_get_logger.return_value = mock_logger

    mock_s3 = MagicMock()
    df = sample_data()
    bucket = "test-bucket"
    key = "test/key.csv"

    # Pass the mocked s3 client explicitly
    upload_df_to_s3(df, bucket, key, mock_s3)

    # Assert s3.put_object called once with correct parameters
    mock_s3.put_object.assert_called_once()
    args, kwargs = mock_s3.put_object.call_args
    assert kwargs["Bucket"] == bucket
    assert kwargs["Key"] == key
    assert isinstance(kwargs["Body"], str)
    assert df.iloc[0, 0] in kwargs["Body"]  # Check CSV string contains some data

    # Assert logger.info called for success
    mock_logger.info.assert_any_call(f"✅ Successfully uploaded {key} to s3 bucket")


@patch("etl_pipeline.load.get_run_logger")
def test_upload_df_to_s3_failure(mock_get_logger):
    mock_logger = MagicMock()
    mock_get_logger.return_value = mock_logger

    mock_s3 = MagicMock()
    mock_s3.put_object.side_effect = Exception("Upload failed")

    df = sample_data()
    bucket = "test-bucket"
    key = "test/key.csv"

    with pytest.raises(Exception, match="Upload failed"):
        upload_df_to_s3(df, bucket, key, mock_s3)

    # Assert logger.error called for failure
    mock_logger.error.assert_any_call(f"❌ Failed to upload {key} to s3 bucket")
