import os
from pathlib import Path
import pandas as pd

# 🔧 Set your root folder
root_folder = Path(r"C:\Users\jfeaga619\OneDrive - Comcast\2-Areas\DCAs\Upload Files")  
output_file = "combined_output.xlsx"

# 🧭 Recursively find all .xlsx files (ignores temp files)
excel_files = [f for f in root_folder.rglob("*.xlsx") if not f.name.startswith("~$")]

# 📦 Storage for DataFrames
all_data = []

for file_path in excel_files:
    try:
        # 🔍 Load the workbook and find the only sheet
        sheet_names = pd.ExcelFile(file_path).sheet_names
        if len(sheet_names) != 1:
            print(f"⚠️ Skipping {file_path.name}: contains {len(sheet_names)} sheets.")
            continue

        sheet = sheet_names[0]

        # 📥 Read the data
        df = pd.read_excel(file_path, sheet_name=sheet, engine='openpyxl')

        # 🧾 Add source file name (with relative path)
        df["SourceFile"] = str(file_path.relative_to(root_folder))

        all_data.append(df)

    except Exception as e:
        print(f"❌ Error processing {file_path.name}: {e}")

# 🧩 Merge and export
if all_data:
    combined_df = pd.concat(all_data, ignore_index=True, sort=False)
    output_path = root_folder / output_file
    combined_df.to_excel(output_path, index=False)
    print(f"\n✅ Combined {len(all_data)} files. Output saved to:\n{output_path}")
else:
    print("🚫 No valid Excel files found.")