import os
import argparse
import logging
import pandas as pd
from src.normalize import normalize_single_report

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")


def parse_args():
    parser = argparse.ArgumentParser(description="Normalize one or more report files.")
    group = parser.add_mutually_exclusive_group(required=False)
    group.add_argument("-f", "--file", help="Path to a single input report file")
    group.add_argument("-d", "--dir", help="Path to a folder containing multiple report files")

    parser.add_argument("-c", "--config", help="Path to the config Excel file",
                        default=os.path.join("config", "Report_Config.xlsx"))
    return parser.parse_args()


def load_file(file_path):
    if not os.path.isfile(file_path):
        raise FileNotFoundError(f"File not found: {file_path}")
    ext = os.path.splitext(file_path)[1].lower()
    if ext == ".csv":
        df = pd.read_csv(file_path)
    elif ext in [".xlsx", ".xls"]:
        df = pd.read_excel(file_path)
    else:
        raise ValueError("Unsupported file type")
    logging.info(f"✅ Loaded file: {file_path}")
    return df


def process_file(file_path, config_path):
    try:
        df = load_file(file_path)
        output = normalize_single_report(df, file_path, config_path)
        logging.info(f"📄 Saved processed file to: {output}")
    except Exception as e:
        logging.error(f"❌ Failed to process {file_path}: {e}")


def get_input_data_interactive():
    while True:
        file_path = input("📂 Enter the full path to the report file (.csv or .xlsx): ").strip()
        if os.path.isfile(file_path):
            try:
                df = load_file(file_path)
                return df, file_path
            except Exception as e:
                logging.error(f"❌ Error reading file: {e}")
        else:
            logging.error(f"File not found: {file_path}")


if __name__ == "__main__":
    args = parse_args()

    if not os.path.isfile(args.config):
        logging.error(f"❌ Config file not found: {args.config}")
        exit(1)

    if args.file:
        process_file(args.file, args.config)

    elif args.dir:
        if not os.path.isdir(args.dir):
            logging.error(f"❌ Folder not found: {args.dir}")
            exit(1)

        file_list = [
            os.path.join(args.dir, f)
            for f in os.listdir(args.dir)
            if f.lower().endswith((".csv", ".xlsx", ".xls"))
        ]

        if not file_list:
            logging.warning("⚠️ No .csv or .xlsx files found in the folder.")
        else:
            logging.info(f"📂 Found {len(file_list)} files to process.")
            for fpath in file_list:
                process_file(fpath, args.config)

    else:
        df_raw, input_path = get_input_data_interactive()
        output = normalize_single_report(df_raw, input_path, args.config)
        logging.info(f"📄 Saved processed file to: {output}")
