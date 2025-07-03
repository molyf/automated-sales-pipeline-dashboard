import pandas as pd
import numpy as np
from prefect import task, get_run_logger
from prefect.tasks import NO_CACHE

def log_summary(df, label, logger):
    """Logs dataset summary: shape, missing values, duplicates."""
    logger.info(f"📋 Summary: {label}")
    logger.info(f"Shape: {df.shape[0]} rows × {df.shape[1]} columns")
    logger.info("Missing values:")
    logger.info(df.isnull().sum().to_string())
    logger.info(f"Duplicate rows: {df.duplicated().sum()}")

def drop_invalid_rows(df):
    # Drop rows missing critical identifiers or where product and category are both null
    return df.dropna(subset=["transaction_id", "customer_name", "product_name"]).loc[
        ~(df["product_name"].isnull() & df["product_category"].isnull())
    ]

def handle_day_of_week(df):
    # Fill missing 'day_of_week' with the most frequent value
    mode_day = df["day_of_week"].mode().iloc[0]
    df["day_of_week"] = df["day_of_week"].fillna(mode_day)
    return df

def handle_product_category(df):
    # Fill missing product categories based on product_name
    missing_cat = df["product_category"].isnull()
    rows_to_drop = []

    for idx in df[missing_cat].index:
        product = df.at[idx, "product_name"]
        match = df[(df["product_name"] == product) & df["product_category"].notnull()]
        
        if not match.empty:
            # Fill with most frequent matching category
            df.at[idx, "product_category"] = match["product_category"].mode().iloc[0]
        else:
            # If no match found, drop the row
            rows_to_drop.append(idx)
    
    df = df.drop(index=rows_to_drop).reset_index(drop=True)
    return df

def handle_price(df):
    # Fill missing prices based on product_name, or drop if no match
    missing_price = df["price"].isnull()
    rows_to_drop = []

    for idx in df[missing_price].index:
        product = df.at[idx, "product_name"]
        match = df[(df["product_name"] == product) & df["price"].notnull()]
        
        if not match.empty:
            df.at[idx, "price"] = match["price"].mode().iloc[0]
        else:
            rows_to_drop.append(idx)
    
    df = df.drop(index=rows_to_drop).reset_index(drop=True)
    return df

def handle_quantity(df):
    # Fill missing quantity_sold with rounded mean
    mean_quantity = round(df["quantity_sold"].mean())
    df["quantity_sold"] = df["quantity_sold"].fillna(mean_quantity)
    return df

def compute_total_sale(df):
    # Compute total_sale where missing using price × quantity
    mask = df["total_sale"].isnull()
    df.loc[mask, "total_sale"] = df.loc[mask, "price"] * df.loc[mask, "quantity_sold"]
    return df

def handle_store_location(df):
    # Fill missing store_location with the mode
    mode_location = df["store_location"].mode().iloc[0]
    df["store_location"] = df["store_location"].fillna(mode_location)
    return df

def standardize_types(df):
    # Strip strings and convert specific columns to numeric
    str_cols = df.select_dtypes(include="object").columns
    df[str_cols] = df[str_cols].apply(lambda x: x.str.strip())
    df["transaction_id"] = pd.to_numeric(df["transaction_id"], errors='coerce')
    df["price"] = pd.to_numeric(df["price"], errors='coerce')
    df["quantity_sold"] = pd.to_numeric(df["quantity_sold"], errors='coerce')
    df["total_sale"] = pd.to_numeric(df["total_sale"], errors='coerce')
    return df

@task(name="🧼 Transform Sales Data", cache_policy=NO_CACHE)
def transform_sales_data(df_raw):
    logger = get_run_logger()
    logger.info("🔧 Starting transformation...")

    # Create working copies
    df = df_raw.copy()
    raw_df = df.copy()

    log_summary(df, "Before Cleaning", logger)

    df = standardize_types(df)
    logger.info("✔️ Standardized data types.")

    df = drop_invalid_rows(df)
    logger.info("✔️ Dropped invalid rows.")

    df = handle_day_of_week(df)
    logger.info("✔️ Handled missing day_of_week.")

    df = handle_product_category(df)
    logger.info("✔️ Inferred missing product_category.")

    df = handle_price(df)
    logger.info("✔️ Filled missing price.")

    df = handle_quantity(df)
    logger.info("✔️ Filled missing quantity_sold.")

    df = compute_total_sale(df)
    logger.info("✔️ Computed missing total_sale.")

    df = handle_store_location(df)
    logger.info("✔️ Filled missing store_location.")

    log_summary(df, "After Cleaning", logger)
    logger.info("✅ Transformation completed.")

    return df, raw_df
