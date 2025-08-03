"""
Normalize.py for report formatting
"""

import pandas as pd
import re
import os
import unicodedata

def detect_header_row(file_path, max_rows=5):
    import pandas as pd
    preview = pd.read_excel(file_path, header=None, nrows=max_rows)
    for i in range(max_rows):
        row = preview.iloc[i]
        if row.notna().sum() > 2 and row.apply(lambda x: isinstance(x, str)).sum() > 1:
            return i
    return 0

def clean_column_name(name):
    return unicodedata.normalize("NFKD", str(name)).encode("ascii", "ignore").decode("utf-8").strip().lower()

def load_config(config_path: str) -> pd.DataFrame:
    df = pd.read_excel(config_path)
    df.columns = df.columns.str.strip()
    df.columns = [clean_column_name(col) for col in df.columns]
    return df

def apply_rule(series: pd.Series, rule: str) -> pd.Series:
    if rule == "Text":
        return series.astype(str).str.strip()
    elif rule == "Short Date":
        return pd.to_datetime(series, errors='coerce').dt.strftime('%m/%d/%Y')
    elif rule == "Whole Number":
        def clean_whole_number(value):
            try:
                if pd.isna(value):
                    return 0
                value_str = str(value).replace(",", "")
                return int(float(value_str))
            except Exception:
                return 0
        return series.apply(clean_whole_number)
    elif rule == "Pad9":
        def pad9(value):
            try:
                if pd.isna(value) or str(value).strip().lower() == 'nan':
                    return ''
                digits_only = re.sub(r'\D', '', str(int(float(value))))
                return digits_only.zfill(9)
            except Exception:
                return ''
        return series.apply(pad9)
    elif rule == "TONo":
        def tono(value):
            try:
                if pd.isna(value) or str(value).strip().lower() == 'nan':
                    return ''
                value_str = str(value).strip()
                #Remove Trailing Zero if it's a float-like string
                if re.match(r'^\d+\.0$', value_str):
                    return value_str.split('.')[0]
                return value_str
            except Exception:
                return ''
        return series.apply(tono)
    elif rule == "Price":
        def price(value):
            try:
                if pd.isna(value) or str(value).strip().lower() == 'nan':
                    return 0.00
                value_str = str(value).strip()
                # Handle accounting-style negatives: ($1,234.56) → -1234.56
                if re.match(r'^\(\$?[\d,]+\.\d{2}\)$', value_str):
                    value_str = '-' + value_str.strip('()')
                # Remove $ and commas
                value_str = value_str.replace('$', '').replace(',', '')
                return round(float(value_str), 2)
            except Exception:
                return 0.00
        return series.apply(price)
    return series

def clean_modelno(series: pd.Series) -> pd.Series:
    return series.astype(str).apply(lambda x: re.sub(r'[^a-zA-Z0-9\+=]', '', x.replace(" ", "").strip().lower()))

def clean_mfgname(series: pd.Series) -> pd.Series:
    return series.astype(str).str.strip().apply(lambda x: re.sub(r'[^a-zA-Z0-9]', '', x)).str.lower()

def normalize_report(df_raw: pd.DataFrame, config: pd.DataFrame) -> (pd.DataFrame, pd.DataFrame, pd.DataFrame):
    # Apply to both raw and config columns
    df_raw.columns = df_raw.columns.str.strip()
    df_raw.columns = [clean_column_name(col) for col in df_raw.columns]
    raw_col_map = {col.lower(): col for col in df_raw.columns} 
    config_map = {
        clean_column_name(row['original column name']): (row['desired standard name'], row['rule'])
        for _, row in config.iterrows()
    }


    processed = pd.DataFrame()
    model_clean = None
    item_padded = None

    for col in df_raw.columns:
        if col in config_map:
            std_col, rule = config_map[col]
            series = df_raw[col]

            if rule in ["Text", "Short Date", "Whole Number", "TONo", "Price"]:
                processed[std_col] = apply_rule(series, rule)
            elif rule == "Pad9":
                item_padded = apply_rule(series, rule)
                processed[std_col] = item_padded
            elif rule == "ModelNo":
                processed[std_col] = series  # original
                model_clean = clean_modelno(series)
                processed[std_col + "_Clean"] = model_clean
            elif rule == "MfgName":
                processed[std_col] = series  # original
                processed[std_col + "_Clean"] = clean_mfgname(series)
            
    # Add INDEX if both cleaned ModelNo and padded ItemNo exist
    if model_clean is not None and item_padded is not None:
        processed.insert(0, "INDEX", model_clean + "|" + item_padded)

    # Ignored columns
    ignored_cols = [col for col in df_raw.columns if col not in config_map]
    df_ignored = df_raw[ignored_cols]
    df_raw_full = df_raw.copy()

    return processed, df_ignored, df_raw_full

def save_to_excel(processed: pd.DataFrame, ignored: pd.DataFrame, raw: pd.DataFrame, original_file_path: str):
    from pandas import ExcelWriter

    base_name = os.path.splitext(os.path.basename(original_file_path))[0]
    output_dir = os.path.join(os.path.dirname(__file__), "Output")
    os.makedirs(output_dir, exist_ok=True)
    output_path = os.path.join(output_dir, f"{base_name}_processed.xlsx")

    with ExcelWriter(output_path, engine="xlsxwriter") as writer:
        for df, sheet_name in [(processed, "Processed"), (ignored, "Ignored"), (raw, "Raw")]:
            df.to_excel(writer, sheet_name=sheet_name, index=False)
            workbook  = writer.book
            worksheet = writer.sheets[sheet_name]
            worksheet.add_table(0, 0, df.shape[0], df.shape[1]-1,
                {'name': sheet_name, 'columns': [{'header': col} for col in df.columns]})
    
    return output_path

def normalize_single_report(data_path: str, config_path: str) -> str:
    import pandas as pd
    import os

    ext = os.path.splitext(data_path)[1].lower()

    if ext == ".csv":
        df = pd.read_csv(data_path)
    elif ext in [".xlsx", ".xls"]:
        header_row = detect_header_row(data_path)
        df = pd.read_excel(data_path, header=header_row)
    else:
        raise ValueError(f"Unsupported file type: {data_path}")

    config = load_config(config_path)
    processed, ignored, raw = normalize_report(df, config)
    output_path = save_to_excel(processed, ignored, raw, data_path)
    print(f"✅ Excel file saved to: {output_path}")
    return output_path

