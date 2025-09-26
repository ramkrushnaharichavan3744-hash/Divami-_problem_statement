import pandas as pd
import logging
from datetime import datetime

# -----------------------
# Helper: Ensure required columns exist
# -----------------------
def ensure_columns(df, required_cols):
    for col, default in required_cols.items():
        if col not in df.columns:
            df[col] = default
    return df

# -----------------------
# Main Refinement Function
# -----------------------
def refine_sales_data(df):
    # 1. Standardize column names
    column_mapping = {
        "qty": "quantity",
        "sales_qty": "quantity",
        "prod_id": "product_id",
        "unit_price": "price_per_unit",
        "price": "price_per_unit",
        "date_time": "timestamp",
        "store": "store_id",
        "channel_name": "channel"
    }
    df = df.rename(columns=column_mapping)

    # 2. Ensure required schema
    required_cols = {
        "product_id": "Unknown",
        "channel": "Unknown",
        "store_id": "Unknown",
        "quantity": 0,
        "price_per_unit": 0.0,
        "timestamp": pd.NaT
    }
    df = ensure_columns(df, required_cols)

    # 3. Data type conversion
    df["quantity"] = pd.to_numeric(df["quantity"], errors="coerce").fillna(0).astype(int)
    df["price_per_unit"] = pd.to_numeric(df["price_per_unit"], errors="coerce").fillna(0).astype(float)
    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")

    # 4. Remove duplicates safely
    dup_subset = [c for c in ["product_id", "timestamp", "channel", "store_id"] if c in df.columns]
    if dup_subset:
        df = df.drop_duplicates(subset=dup_subset)

    # 5. Handle missing values
    df["product_id"] = df["product_id"].fillna("Unknown")
    df["channel"] = df["channel"].fillna("Unknown")
    df["store_id"] = df["store_id"].fillna("Unknown")

    # 6. Standardize string columns
    if "channel" in df.columns:
        df["channel"] = df["channel"].astype(str).str.lower().str.strip()
    if "product_id" in df.columns:
        df["product_id"] = df["product_id"].astype(str).str.strip()
    if "store_id" in df.columns:
        df["store_id"] = df["store_id"].astype(str).str.strip()

    # 7. Remove unrealistic values
    df = df[(df["quantity"] >= 0) & (df["price_per_unit"] >= 0) & (df["price_per_unit"] < 10000)]

    # 8. Derived columns
    if "timestamp" in df.columns:
        df["year"] = df["timestamp"].dt.year.fillna(0).astype(int)
        df["month"] = df["timestamp"].dt.month.fillna(0).astype(int)
        df["day"] = df["timestamp"].dt.day.fillna(0).astype(int)

    # ✅ Always calculate revenue here
    df["revenue"] = df["quantity"] * df["price_per_unit"]

    # 9. Add ingestion timestamp
    df["ingested_at"] = datetime.now()

    logging.info(f"Refined sales data: {len(df)} rows after cleaning")
    return df
