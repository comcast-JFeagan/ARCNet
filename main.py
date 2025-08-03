import os
import logging
import pandas as pd
from src.normalize import normalize_single_report

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

def get_input_data():
    while True:
        file_path = input("📂 Enter the full path to the report file (.csv or .xlsx): ").strip()

        if not os.path.isfile(file_path):
            logging.error(f"File not found: {file_path}")
            continue

        ext = os.path.splitext(file_path)[1].lower()
        try:
            if ext == ".csv":
                df = pd.read_csv(file_path)
            elif ext in [".xlsx", ".xls"]:
                df = pd.read_excel(file_path)
            else:
                logging.warning("⚠️ Unsupported file type. Please use .csv or .xlsx.")
                continue

            logging.info(f"✅ Successfully loaded: {file_path}")
            return df, file_path
        except Exception as e:
            logging.error(f"❌ Error reading file: {e}")

if __name__ == "__main__":
    df_raw, input_path = get_input_data()
    config_path = r"config/Report_Config.xlsx"
    output = normalize_single_report(df_raw, input_path, config_path)
    logging.info(f"📄 Saved processed file to: {output}")
