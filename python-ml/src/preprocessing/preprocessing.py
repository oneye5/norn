from pathlib import Path
import pandas as pd

from src.config.config import *
from src.utils.csv_utils import load_csv, save_csv
from src.utils.path_utils import get_skuld_root


def create_future_labels(df: pd.DataFrame) -> pd.DataFrame:
    """
    For each row, create a label indicating whether the Close price increases
    by at least THRESHOLD_PCT within FUTURE_DELTA_SEC seconds.
    The label is 1 if it increases, 0 otherwise.
    """
    df = df.sort_values([TICKER_COL, TIMESTAMP_COL]).reset_index(drop=True)
    df["future_ts"] = df[TIMESTAMP_COL] + FUTURE_DELTA_SEC

    labeled_frames = []

    for ticker, group in df.groupby(TICKER_COL):
        g = group.copy()
        # Find index of the first row at or after the future timestamp
        future_idx = g[TIMESTAMP_COL].searchsorted(g["future_ts"], side="left")

        # Compute future prices
        future_prices = [
            g.iloc[idx][CLOSE_COL] if idx < len(g) else None
            for idx in future_idx
        ]
        g["future_close"] = future_prices

        # Compute label as 1 if future_close >= threshold increase, else 0
        g[LABEL_COL] = (
            ((g["future_close"] - g[CLOSE_COL]) / g[CLOSE_COL]) >= THRESHOLD_PCT
        ).astype(int)

        labeled_frames.append(g)

    df_out = pd.concat(labeled_frames, ignore_index=True)

    # Drop rows without a valid future price
    df_out = df_out.dropna(subset=["future_close"])

    # Drop helper columns
    df_out = df_out.drop(columns=["future_ts", "future_close"])

    # Ensure label column is integer type
    df_out[LABEL_COL] = df_out[LABEL_COL].astype("int8")

    return df_out


def one_hot_encode(df: pd.DataFrame) -> pd.DataFrame:
    """
    One-hot encode the ticker column.
    """
    df = pd.get_dummies(df, columns=[TICKER_COL], prefix="ticker", dtype="int8")
    return df


def preprocess(wide_csv_path: str, output_csv_path: str):
    """
    Full preprocessing pipeline:
    - Load wide CSV
    - Generate future labels (1/0)
    - One-hot encode tickers
    - Save preprocessed CSV
    """
    df = load_csv(wide_csv_path)

    # Generate labels
    df = create_future_labels(df)

    # One-hot encode tickers
    df = one_hot_encode(df)

    save_csv(df, output_csv_path)
    print(f"Preprocessed CSV saved to {output_csv_path}")


if __name__ == "__main__":
    root = get_skuld_root()
    input_csv = root / "python-ml" / "data" / "data_wide_imputed.csv"
    output_csv = root / "python-ml" / "data" / "data_preprocessed.csv"

    print("Loading:", input_csv)
    print("Saving:", output_csv)

    preprocess(str(input_csv), str(output_csv))
