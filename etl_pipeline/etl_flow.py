import boto3
from prefect import flow, get_run_logger
from etl_pipeline.extract import extract_from_mockaroo
from etl_pipeline.transform import transform_sales_data
import json
from prefect.blocks.system import Secret
from etl_pipeline.load import (
    model_sales_data,
    upload_customers_to_s3,
    upload_products_to_s3,
    upload_stores_to_s3,
    upload_sales_to_s3,
    upload_raw_df_to_s3
)


@flow(name="ETL Pipeline Flow")
def main():
    logger = get_run_logger()
    logger.info("🚀 Starting ETL pipeline...")


    try:
        # Load secret block and parse JSON
        raw_secret = Secret.load("credentials").get()
        credentials = json.loads(raw_secret)
        logger.info("🔐 Loaded secret block: 'credentials'")

        # Extract secrets
        api_key = credentials["MOCKAROO_API_KEY"]
        aws_access_key = credentials["AWS_ACCESS_KEY_ID"]
        aws_secret_key = credentials["AWS_SECRET_ACCESS_KEY"]
        bucket_name = credentials["S3_BUCKET_NAME"]

        logger.info("✅ All secrets loaded successfully.")

        # Initialize S3 client
        s3 = boto3.client(
            's3',
            aws_access_key_id=aws_access_key,
            aws_secret_access_key=aws_secret_key
        )
        logger.info("✅ boto3 S3 client initialized.")

    except Exception as e:
        logger.error(f"❌ Failed to load secrets or initialize S3 client: {e}")
        raise

    # Extract
    df_raw = extract_from_mockaroo(api_key=api_key)
    
    # Transform
    transformed_df, raw_df = transform_sales_data(df_raw)
    
    # Split
    customers_df, products_df, stores_df, sales_df, raw_df = model_sales_data(transformed_df, raw_df)

    # Launch uploads in parallel 
    customer_task = upload_customers_to_s3.submit(customers_df, bucket_name)
    product_task = upload_products_to_s3.submit(products_df, bucket_name)
    store_task = upload_stores_to_s3.submit(stores_df, bucket_name)
    sales_task = upload_sales_to_s3.submit(sales_df, bucket_name)
    raw_task = upload_raw_df_to_s3.submit(raw_df, bucket_name)

    # Wait for all uploads to finish
    customer_task.result()
    product_task.result()
    store_task.result()
    sales_task.result()
    raw_task.result()

    logger.info("✅ ETL pipeline completed successfully.")
