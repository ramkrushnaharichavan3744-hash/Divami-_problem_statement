import logging
import os
import pandas as pd
from datetime import datetime
from ingest_pipeline.ingest import read_sources
from ingest_pipeline.Data_cleaning import refine_sales_data


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("logs/pipeline.log"),
        logging.StreamHandler()
    ]
)

try:
    # Capture pipeline run timestamp
    pipeline_run_timestamp = datetime.now()

    # Step 1: Ingest all CSV/JSON files dynamically
    root_path = "data"
    df_all = read_sources(root_path)

    if df_all.empty:
        logging.warning("No data found in the specified folder.")
    else:
        # Step 2: Refine and clean data
        df_clean = refine_sales_data(df_all)

        # ✅ Step 2.1: Derive 'revenue' if missing
        if "revenue" not in df_clean.columns:
            if {"quantity", "price_per_unit"}.issubset(df_clean.columns):
                df_clean["revenue"] = df_clean["quantity"] * df_clean["price_per_unit"]
                logging.info("Derived 'revenue' column from quantity * price_per_unit")
            else:
                df_clean["revenue"] = 0
                logging.warning("'revenue' column missing and cannot be derived, filled with 0")

        # ✅ Step 2.2: Extract date for daily reports
        if "timestamp" in df_clean.columns:
            df_clean["date"] = df_clean["timestamp"].dt.date
        else:
            df_clean["date"] = pipeline_run_timestamp.date()
            logging.warning("'timestamp' column missing, using pipeline run date instead")

        # Step 3: Safe aggregations (check if columns exist)
        agg_columns = {}
        if "quantity" in df_clean.columns:
            agg_columns["total_sales"] = pd.NamedAgg(column="quantity", aggfunc="sum")
        if "revenue" in df_clean.columns:
            agg_columns["total_revenue"] = pd.NamedAgg(column="revenue", aggfunc="sum")

        if agg_columns:
            df_report = (
                df_clean.groupby(["date", "channel"])
                .agg(**agg_columns)
                .reset_index()
            )
            df_report["pipeline_run_time"] = pipeline_run_timestamp
        else:
            df_report = pd.DataFrame()
            logging.warning("No valid columns found for aggregation")

        # Top products
        if {"product_id", "quantity"}.issubset(df_clean.columns):
            df_top_products = (
                df_clean.groupby("product_id")
                .agg(
                    total_quantity=pd.NamedAgg(column="quantity", aggfunc="sum"),
                    total_revenue=pd.NamedAgg(column="revenue", aggfunc="sum"),
                )
                .sort_values(by="total_quantity", ascending=False)
                .head(5)
                .reset_index()
            )
            df_top_products["pipeline_run_time"] = pipeline_run_timestamp
        else:
            df_top_products = pd.DataFrame()
            logging.warning("Cannot generate top products report (missing product_id/quantity)")

        # Step 4: Save output
        report_date = pipeline_run_timestamp.strftime("%Y-%m-%d")
        output_path = f"output/Daily_reports/date={report_date}"
        os.makedirs(output_path, exist_ok=True)

        if not df_report.empty:
            df_report.to_csv(os.path.join(output_path, "report.csv"), index=False)
        if not df_top_products.empty:
            df_top_products.to_csv(os.path.join(output_path, "top_products.csv"), index=False)

        logging.info(f"Reports successfully written to {output_path}")

except Exception as e:
    logging.error(f"Pipeline failed: {e}")
