"""
normalize.py — Report normalization utilities
"""

import os
import re
import pandas as pd
import unicodedata
import logging
import platform

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

def detect_header_row(file_path, max_rows=5):
    preview = pd.read_excel(file_path, header=None, nrows=max_rows)
    for i in range(max_rows):
        row = preview.iloc[i]
        if row.notna().sum() > 2 and row.apply(lambda x: isinstance(x, str)).sum() > 1:
            return i
    return 0

def clean_column_name(name):
    return unicodedata.normalize("NFKD", str(name)).encode("ascii", "ignore").decode("utf-8").strip().lower()

def load_config(config_path):
    df = pd.read_excel(config_path)
    df.columns = [clean_column_name(col) for col in df.columns.str.strip()]
    return df

def get_default_output_folder():
    system = platform.system()
    if system == "Darwin":  # macOS
        return "/Users/jeremyfeagan/arcnet_outputs"
    elif system == "Windows":
        return os.path.join(os.environ["USERPROFILE"], "Documents", "ARCNetOutputs")
    else:
        return os.path.join(os.path.dirname(__file__), "Output")

# Rule functions
def rule_text(series):
    return series.astype(str).str.strip()

def rule_short_date(series):
    return pd.to_datetime(series, errors='coerce').dt.strftime('%m/%d/%Y')

def rule_whole_number(series):
    def clean(value):
        try:
            if pd.isna(value):
                return 0
            return int(float(str(value).replace(",", "")))
        except Exception:
            return 0
    return series.apply(clean)

def rule_pad9(series):
    def pad(value):
        try:
            if pd.isna(value) or str(value).strip().lower() == 'nan':
                return ''
            digits = re.sub(r'\D', '', str(int(float(value))))
            return digits.zfill(9)
        except Exception:
            return ''
    return series.apply(pad)

def rule_tono(series):
    def tono(value):
        try:
            if pd.isna(value) or str(value).strip().lower() == 'nan':
                return ''
            value_str = str(value).strip()
            if re.match(r'^\d+\.0$', value_str):
                return value_str.split('.')[0]
            return value_str
        except Exception:
            return ''
    return series.apply(tono)

def rule_price(series):
    def price(value):
        try:
            if pd.isna(value) or str(value).strip().lower() == 'nan':
                return 0.00
            value_str = str(value).strip()
            if re.match(r'^\(\$?[\d,]+\.\d{2}\)$', value_str):
                value_str = '-' + value_str.strip('()')
            value_str = value_str.replace('$', '').replace(',', '')
            return round(float(value_str), 2)
        except Exception:
            return 0.00
    return series.apply(price)

def clean_modelno(series):
    return series.astype(str).apply(lambda x: re.sub(r'[^a-zA-Z0-9+=]', '', x.replace(" ", "").strip().lower()))

def clean_mfgname(series):
    return series.astype(str).str.strip().apply(lambda x: re.sub(r'[^a-zA-Z0-9]', '', x)).str.lower()

# Rule dispatcher
RULE_DISPATCH = {
    "text": rule_text,
    "short date": rule_short_date,
    "whole number": rule_whole_number,
    "pad9": rule_pad9,
    "tono": rule_tono,
    "price": rule_price,
}

def normalize_report(df_raw, config):
    df_raw.columns = [clean_column_name(col) for col in df_raw.columns.str.strip()]
    config_map = {
        clean_column_name(row['original column name']): (row['desired standard name'], row['rule'].lower())
        for _, row in config.iterrows()
    }

    processed = pd.DataFrame()
    model_clean = None
    item_padded = None

    for col in df_raw.columns:
        if col in config_map:
            std_col, rule = config_map[col]
            series = df_raw[col]

            if rule in RULE_DISPATCH:
                processed[std_col] =RULE_DISPATCH[rule](series)
                if rule == "pad9":
                    item_padded = processed[std_col]
                elif rule == "modelno":
                    processed[std_col] = series
                    model_clean = clean_modelno(series)
                    processed[f"{std_col}_Clean"] = model_clean
                elif rule == "mfgname":
                    processed[std_col] = series
                    processed[f"{std_col}_Clean"] = clean_mfgname(series)

    if model_clean is not None and item_padded is not None:
        processed.insert(0, "INDEX", model_clean + "\n" + item_padded)

    ignored_cols = [col for col in df_raw.columns if col not in config_map]
    df_ignored = df_raw[ignored_cols]
    df_raw_full = df_raw.copy()

    return processed, df_ignored, df_raw_full

def save_to_excel(processed, ignored, raw, original_file_path, output_folder=None):
    base_name = os.path.splitext(os.path.basename(original_file_path))[0]
    #output_dir = output_folder or os.path.join(os.path.dirname(__file__), "Output")
    output_dir = output_folder or get_default_output_folder()
    if not os.path.exists(output_dir):
        os.makedirs(output_dir, exist_ok=True)
        logging.info(f"Creating output directory: {output_dir}")
    else:
        logging.info(f"Using existing output directory: {output_dir}")
    if not os.path.isdir(output_dir):
        raise NotADirectoryError(f"Output path is not a directory: {output_dir}")
    output_path = os.path.join(output_dir, f"{base_name}_processed.xlsx")

    with pd.ExcelWriter(output_path, engine="xlsxwriter") as writer:
        for df, sheet_name in [(processed, "Processed"), (ignored, "Ignored"), (raw, "Raw")]:
            df.to_excel(writer, sheet_name=sheet_name, index=False)
            worksheet = writer.sheets[sheet_name]
            worksheet.add_table(0, 0, df.shape[0], df.shape[1] - 1, {
                'name': sheet_name,
                'columns': [{'header': col} for col in df.columns]
            })

    return output_path

def normalize_single_report(df_raw, data_path, config_or_path, output_folder=None):
    if isinstance(config_or_path, str):
        config = load_config(config_or_path)
    else:
        config = config_or_path
    processed, ignored, raw = normalize_report(df_raw, config)
    output_path = save_to_excel(processed, ignored, raw, data_path, output_folder=output_folder)
    logging.info(f"✅ Excel file saved to: {output_path}")
    return output_path
