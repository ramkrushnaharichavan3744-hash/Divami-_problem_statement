# Sales Data Pipeline

## Oerview
This project implements a data pipeline to process sales data and generate daily reports for an e-commerce business. The pipeline ingests sales data from multiple sources (web app, mobile app, physical stores), cleans and refines the data, calculates total sales and revenue, and outputs aggregated daily reports.

The pipeline is designed to handle large data volumes, multiple files, and ensures robust error handling and logging.

## Business Context
The e-commerce company collects daily sales data from:
Web app and Mobile app (online sales)
Physical stores (uploaded CSV files)

## Reports Needed:
Total sales per channel
Top 5 best-selling products
Total revenue per day

```
Folder Structure
sales_data_pipeline/
│
├─ data/                 # Input CSV/JSON files (simulated source files)
│    ├─ sales_sample.csv
│
├─ ingest_pipeline/
│    ├─ __init__.py
│    ├─ ingest.py         # Handles dynamic file ingestion
│    ├─ Data_cleaning.py  # Data cleaning and refinement
│
├─ output/
│    ├─ Daily_reports/    # Output reports stored by date
│
├─ logs/
│    ├─ pipeline.log      # Log file
│
├─ run_pipeline.py        # Main pipeline script
├─ README.md              # Project documentation

```


## How the Pipeline Works

1. Data Ingestion
read_sources(root_path) dynamically reads all CSV/JSON files in the data/ folder.
All files are concatenated into a single Pandas DataFrame.
Logs the number of files read and any errors during ingestion.

2. Data Cleaning & Refinement
Standardizes column names to a unified schema (e.g., qty → quantity, price → price_per_unit).
Ensures required columns exist; fills missing columns with default values.

3. converts data types:
quantity → integer
price_per_unit → float
timestamp → datetime
Removes duplicates based on product_id, timestamp, channel, and store_id.

Handles missing or invalid values:
Missing product_id, channel, store_id → "Unknown"
Negative or unrealistically high values filtered out

## Adds derived columns:
revenue = quantity * price_per_unit
year, month, day extracted from timestamp
ingested_at → current timestamp for pipeline run


## Output Reports

Reports are saved as CSVs in: output/Daily_reports/date=<YYYY-MM-DD>/
Includes:
report.csv → Total sales and revenue per channel
top_products.csv → Top 5 products by quantity and revenue
Each report also includes the pipeline run timestamp.

## Logging & Error Handling
All pipeline steps are logged to logs/pipeline.log and console.

Handles:
Missing or empty input files
Missing columns
Invalid data types
Warnings are logged for data issues instead of failing the pipeline.
Exceptions are captured, and pipeline stops gracefully with an error message.

## Scalability Considerations
Designed to handle large volumes of files:
Reads all files dynamically
Can process multiple CSVs/JSONs simultaneously
Uses vectorized Pandas operations for speed

## Easily extendable to:
Add more channels
Support new data sources (e.g., APIs, databases)
Store output in databases (e.g., PostgreSQL, Snowflake)
Can be parallelized using Dask or Spark for very large datasets.

## Future Extensibility
Database Storage: Store refined data and reports in a database for historical queries.
Incremental Loading: Track last processed timestamp and load only new records.
Alerting: Send email alerts on missing data or pipeline failure.
Visualization: Generate dashboards automatically from daily reports.

## How to Run
Place all input CSV/JSON files in the data/ folder.

Ensure all required Python packages are installed:

pip install pandas

Run the pipeline:

python run_pipeline.py

Check the output/Daily_reports/ folder for the reports.

Logs are written to logs/pipeline.log.
