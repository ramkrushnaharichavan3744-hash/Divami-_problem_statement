import pandas as pd
import json
import glob
import logging
import os

def read_sources(root_path):
    """
    Reads all CSV and JSON files from the given root folder (recursively)
    and returns a list of DataFrames.
    """
    try:
        all_csv_files = glob.glob(os.path.join(root_path, "**", "*.csv"), recursive=True)
        all_json_files = glob.glob(os.path.join(root_path, "**", "*.json"), recursive=True)

        # Read all CSV files
        df_csv_list = [pd.read_csv(f) for f in all_csv_files]
        df_csv = pd.concat(df_csv_list, ignore_index=True) if df_csv_list else pd.DataFrame()

        # Read all JSON files
        df_json_list = []
        for f in all_json_files:
            with open(f, "r") as file:
                data = json.load(file)
                df_json_list.append(pd.json_normalize(data))
        df_json = pd.concat(df_json_list, ignore_index=True) if df_json_list else pd.DataFrame()

        # Combine CSV and JSON into a single DataFrame
        df_all = pd.concat([df_csv, df_json], ignore_index=True) if not df_csv.empty or not df_json.empty else pd.DataFrame()

        logging.info(f"Read {len(all_csv_files)} CSV files and {len(all_json_files)} JSON files successfully.")
        return df_all

    except Exception as e:
        logging.error(f"Error reading input files: {e}")
        raise
