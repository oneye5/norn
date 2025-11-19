import pandas as pd
from pathlib import Path

def load_csv(path: str, index_col=None) -> pd.DataFrame:
    """
    Load a CSV into a pandas DataFrame.

    Args:
        path (str): Path to the CSV file.
        index_col (str or int, optional): Column to use as index.
        parse_dates (list[str], optional): List of columns to parse as datetime.

    Returns:
        pd.DataFrame
    """
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"CSV file not found: {path}")

    df = pd.read_csv(path, index_col=index_col)
    return df

def save_csv(df: pd.DataFrame, path: str, index=False):
    """
    Save a DataFrame to CSV.

    Args:
        df (pd.DataFrame): Data to save.
        path (str): Destination path.
        index (bool): Whether to save the index as a column.
    """
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=index)
