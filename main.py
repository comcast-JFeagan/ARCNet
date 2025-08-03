from src.normalize import normalize_single_report
import pandas as pd
import os

def get_input_data() -> tuple[pd.DataFrame, str]:
    while True:
        file_path = input("📂 Enter the full path to the report file (.csv or .xlsx): ").strip()

        if not os.path.isfile(file_path):
            print(f"❌ File not found: {file_path}")
            continue

        ext = os.path.splitext(file_path)[1].lower()

        try:
            if ext == ".csv":
                df = pd.read_csv(file_path)
            elif ext in [".xlsx", ".xls"]:
                df = pd.read_excel(file_path)
            else:
                print("⚠️ Unsupported file type. Please use .csv or .xlsx.")
                continue
            print(f"✅ Successfully loaded: {file_path}")
            return df, file_path
        except Exception as e:
            print(f"❌ Error reading file: {e}")

if __name__ == "__main__":
    df_raw, input_path = get_input_data()
    output = normalize_single_report(
        df_raw,
        input_path,
        config_path=r"config/Report Normal.xlsx"
    )
    print(f"📄 Saved processed file to: {output}")
