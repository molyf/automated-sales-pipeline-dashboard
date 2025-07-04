import pytest
from unittest.mock import patch, MagicMock
import pandas as pd
from etl_pipeline.etl_flow import main

@pytest.fixture
def dummy_df():
    return pd.DataFrame({"col": [1, 2, 3]})

@patch("etl_pipeline.etl_flow.invoke_lambda_loader.submit")
@patch("etl_pipeline.etl_flow.confirm_s3_landing_complete.submit")
@patch("etl_pipeline.etl_flow.upload_raw_df_to_s3.submit")
@patch("etl_pipeline.etl_flow.upload_sales_to_s3.submit")
@patch("etl_pipeline.etl_flow.upload_stores_to_s3.submit")
@patch("etl_pipeline.etl_flow.upload_products_to_s3.submit")
@patch("etl_pipeline.etl_flow.upload_customers_to_s3.submit")
@patch("etl_pipeline.etl_flow.model_sales_data")
@patch("etl_pipeline.etl_flow.transform_sales_data")
@patch("etl_pipeline.etl_flow.extract_from_mockaroo")
@patch("etl_pipeline.etl_flow.boto3.client")
@patch("etl_pipeline.etl_flow.Secret.load")
def test_etl_process(
    mock_secret_load,
    mock_boto3_client,
    mock_extract_from_mockaroo,
    mock_transform_sales_data,
    mock_model_sales_data,
    mock_upload_customers_submit,
    mock_upload_products_submit,
    mock_upload_stores_submit,
    mock_upload_sales_submit,
    mock_upload_raw_submit,
    mock_confirm_submit,
    mock_lambda_submit,
    dummy_df
):
    # 🔐 Mock Secret block
    mock_secret_instance = MagicMock()
    mock_secret_instance.get.return_value = {
        "MOCKAROO_API_KEY": "fake_api_key",
        "AWS_ACCESS_KEY_ID": "fake_aws_key",
        "AWS_SECRET_ACCESS_KEY": "fake_secret",
        "S3_BUCKET_NAME": "fake_bucket"
    }
    mock_secret_load.return_value = mock_secret_instance

    # 🪣 Mock boto3 S3 client
    mock_boto3_client.return_value = MagicMock()

    # 🧪 Mock ETL steps
    mock_extract_from_mockaroo.return_value = dummy_df
    mock_transform_sales_data.return_value = (dummy_df, dummy_df)
    mock_model_sales_data.return_value = (dummy_df, dummy_df, dummy_df, dummy_df, dummy_df)

    # 📤 Mock S3 upload tasks
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

    # ✅ Mock confirm_s3_landing_complete
    mock_confirm_task = MagicMock()
    mock_confirm_task.result.return_value = True
    mock_confirm_submit.return_value = mock_confirm_task

    # ⚡ Mock Lambda invoke
    mock_lambda_response = MagicMock()
    mock_lambda_response.result.return_value = {"statusCode": 200, "body": "Success"}
    mock_lambda_submit.return_value = mock_lambda_response

    # 🚀 Run the flow
    main()

    # ✅ Assertions
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

    mock_confirm_submit.assert_called_once()
    mock_confirm_task.result.assert_called_once()

    mock_lambda_submit.assert_called_once_with("mapDataFunction")
    mock_lambda_response.result.assert_called_once()
