from pathlib import Path
import pandas as pd

from src.config.config import *
from src.utils.csv_utils import load_csv, save_csv
from src.utils.path_utils import get_skuld_root


def time_based_split(df: pd.DataFrame):
    """
    Split the dataframe into train and test sets based on time.
    The last TEST_DELTA_SEC seconds of data go into the test set.
    """
    # Ensure sorted by timestamp
    df = df.sort_values(TIMESTAMP_COL).reset_index(drop=True)

    # Determine split timestamp
    max_ts = df[TIMESTAMP_COL].max()
    split_ts = max_ts - TEST_DELTA_SEC

    # Split
    train_df = df[df[TIMESTAMP_COL] <= split_ts].copy()
    test_df = df[df[TIMESTAMP_COL] > split_ts].copy()

    return train_df, test_df


def split_and_save(preprocessed_csv_path: str):
    """
    Load the preprocessed data, split into train/test based on time,
    and save each as CSV.
    """
    df = load_csv(preprocessed_csv_path)

    train_df, test_df = time_based_split(df)

    root = get_skuld_root()
    train_csv = root / "python-ml" / "data" / "train.csv"
    test_csv = root / "python-ml" / "data" / "test.csv"

    save_csv(train_df, train_csv)
    save_csv(test_df, test_csv)

    print(f"Train CSV saved to: {train_csv} ({len(train_df)} rows)")
    print(f"Test CSV saved to:  {test_csv} ({len(test_df)} rows)")


if __name__ == "__main__":
    root = get_skuld_root()
    preprocessed_csv = root / "python-ml" / "data" / "data_preprocessed.csv"

    print("Loading preprocessed data:", preprocessed_csv)
    split_and_save(str(preprocessed_csv))
