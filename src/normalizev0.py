import pandas as pd
import re
import os

def load_config(config_path: str, report_name: str) -> pd.DataFrame:
    df = pd.read_excel(config_path)
    df.columns = df.columns.str.strip()
    df = df[df['Desired Standard Name'].str.lower() != 'not used']
    df['Is new Field?'] = df['Is new Field?'].astype(str).str.lower().str.strip() == 'yes'
    return df[df['Report Name'].str.lower() == report_name.lower()]

def normalize_report(df: pd.DataFrame, config: pd.DataFrame) -> pd.DataFrame:
    df.columns = df.columns.str.strip()
    result_df = pd.DataFrame()
    included_cols = set()

    for _, row in config.iterrows():
        is_new = row['Is new Field?']
        orig_col = str(row['Original Column Name']).strip() if pd.notna(row['Original Column Name']) else ''
        std_col = str(row['Desired Standard Name']).strip()
        derived_from = str(row['Derived From']).strip() if pd.notna(row['Derived From']) else ''
        rule = str(row['Format Rule/Notes']).strip().lower() if pd.notna(row['Format Rule/Notes']) else ''

        if is_new and std_col.lower() == 'index':
            model_col = next((c for c in df.columns if c.lower().strip() == 'itemid'), None)
            item_col = next((c for c in df.columns if c.lower().strip() == 'cifa id'), None)
            if model_col and item_col:
                model_series = df[model_col].astype(str).apply(lambda x: re.sub(r'[^a-zA-Z0-9\+\=]', '', x)).str.lower()
                item_series = df[item_col].astype(str).str.zfill(9)
                result_df[std_col] = model_series + "|" + item_series
                included_cols.update([model_col, item_col])
            continue

        if not is_new:
            match = next((c for c in df.columns if c.strip().lower() == orig_col.lower()), None)
            if not match:
                continue

            series = df[match]

            if 'text' in rule:
                series = series.astype(str).str.strip()
            if 'short date' in rule:
                series = pd.to_datetime(series, errors='coerce').dt.strftime('%m/%d/%Y')
            if 'whole number' in rule:
                series = pd.to_numeric(series, errors='coerce').fillna(0).astype(int)
            if 'pad' in rule and '9' in rule:
                series = series.astype(str).str.zfill(9)
            if 'alnum' in rule:
                series = series.astype(str).apply(lambda x: re.sub(r'[^a-zA-Z0-9\+\=]', '', x))
            if 'lowercase' in rule:
                series = series.astype(str).str.lower()

            if std_col.lower() in ['modelno', 'mfgname']:
                result_df[match] = df[match]
                included_cols.add(match)

            result_df[std_col] = series
            included_cols.add(match)

    for col in df.columns:
        if col not in result_df.columns:
            result_df[col] = df[col]

    return result_df

def save_output(df: pd.DataFrame, input_path: str) -> str:
    base, _ = os.path.splitext(input_path)
    output_path = f"{base}_processed.csv"
    df.to_csv(output_path, index=False)
    return output_path

def normalize_single_report(*, df: pd.DataFrame, data_path: str, config_path: str, report_name: str) -> str:
    config = load_config(config_path, report_name)
    normalized = normalize_report(df, config)
    output_path = save_output(normalized, data_path)
    print(f"✅ Normalized file saved to: {output_path}")
    return output_path
