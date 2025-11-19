from pathlib import Path
import sys
import pandas as pd
from src.utils.csv_utils import load_csv, save_csv
from src.utils.path_utils import get_skuld_root


def long_to_wide(long_csv_path: str, wide_csv_path: str):
    """
    Convert long-format CSV to wide-format CSV with forward-fill:
    - Macro data (no ticker) forward-filled globally
    - Ticker-specific data forward-filled per ticker
    - Macro features merged onto ticker rows
    """
    # Load CSV, skip malformed lines
    df = load_csv(long_csv_path)

    # Separate macro and ticker-specific data
    df_macro = df[df['ticker'].isna()].sort_values('timestamp')
    df_ticker = df[df['ticker'].notna()].sort_values(['ticker', 'timestamp'])

    # Pivot macro data: forward-fill globally
    df_macro_wide = df_macro.pivot_table(
        index='timestamp',
        columns='feature',
        values='value'
    ).ffill().reset_index()

    # Pivot ticker data: forward-fill per ticker
    df_ticker_wide = df_ticker.pivot_table(
        index=['timestamp', 'ticker'],
        columns='feature',
        values='value'
    ).groupby('ticker').ffill().reset_index()

    # Merge macro features onto ticker rows
    df_final = df_ticker_wide.merge(df_macro_wide, on='timestamp', how='left')

    # Flatten column names
    df_final.columns.name = None

    # Save wide CSV
    save_csv(df_final, wide_csv_path)
    print(f"Wide CSV saved to {wide_csv_path}")


if __name__ == "__main__":
    skuld_root = get_skuld_root()

    long_csv = skuld_root / "data" / "data_long.csv"  # outward-facing raw CSV
    wide_csv = skuld_root / "python-ml" / "data" / "data_wide.csv"  # internal processed CSV

    print("Loading from:", long_csv)
    print("Saving to:", wide_csv)

    long_to_wide(str(long_csv), str(wide_csv))
