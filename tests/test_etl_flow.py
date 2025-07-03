import pytest
from unittest.mock import patch, MagicMock
import pandas as pd
from etl_pipeline.etl_flow import main  # fixed to main

@pytest.fixture
def dummy_df():
    return pd.DataFrame({"col": [1, 2, 3]})

@patch("etl_pipeline.etl_flow.Secret.load")
@patch("etl_pipeline.etl_flow.boto3.client")
@patch("etl_pipeline.etl_flow.extract_from_mockaroo")
@patch("etl_pipeline.etl_flow.transform_sales_data")
@patch("etl_pipeline.etl_flow.model_sales_data")
@patch("etl_pipeline.etl_flow.upload_customers_to_s3.submit")
@patch("etl_pipeline.etl_flow.upload_products_to_s3.submit")
@patch("etl_pipeline.etl_flow.upload_stores_to_s3.submit")
@patch("etl_pipeline.etl_flow.upload_sales_to_s3.submit")
@patch("etl_pipeline.etl_flow.upload_raw_df_to_s3.submit")
def test_etl_process(
    mock_upload_raw_submit,
    mock_upload_sales_submit,
    mock_upload_stores_submit,
    mock_upload_products_submit,
    mock_upload_customers_submit,
    mock_model_sales_data,
    mock_transform_sales_data,
    mock_extract_from_mockaroo,
    mock_boto3_client,
    mock_secret_load,
    dummy_df
):
    # Mock secret loading returns dict (not JSON string)
    mock_secret_instance = MagicMock()
    mock_secret_instance.get.return_value = {
        "MOCKAROO_API_KEY": "fake_api_key",
        "AWS_ACCESS_KEY_ID": "fake_aws_key",
        "AWS_SECRET_ACCESS_KEY": "fake_secret",
        "S3_BUCKET_NAME": "fake_bucket"
    }
    mock_secret_load.return_value = mock_secret_instance

    mock_boto3_client.return_value = MagicMock()

    mock_extract_from_mockaroo.return_value = dummy_df
    mock_transform_sales_data.return_value = (dummy_df, dummy_df)
    mock_model_sales_data.return_value = (dummy_df, dummy_df, dummy_df, dummy_df, dummy_df)

    mock_task = MagicMock()
    mock_task.result.return_value = None

    for mock_submit in [
        mock_upload_customers_submit,
        mock_upload_products_submit,
        mock_upload_stores_submit,
        mock_upload_sales_submit,
        mock_upload_raw_submit,
    ]:
        mock_submit.return_value = mock_task

    # Run the flow function by correct name
    main()

    # Assertions
    assert mock_secret_load.call_count >= 1
    mock_boto3_client.assert_called_once()
    mock_extract_from_mockaroo.assert_called_once()
    mock_transform_sales_data.assert_called_once()
    mock_model_sales_data.assert_called_once()

    mock_upload_customers_submit.assert_called_once()
    mock_upload_products_submit.assert_called_once()
    mock_upload_stores_submit.assert_called_once()
    mock_upload_sales_submit.assert_called_once()
    mock_upload_raw_submit.assert_called_once()

    mock_task.result.assert_called()
