
from src.preprocessing.long_to_wide_csv import long_to_wide_and_impute
from src.preprocessing.preprocessing import preprocess
from src.preprocessing.train_test_split import split_and_save
from src.utils.csv_utils import load_csv, save_csv
from src.utils.path_utils import get_skuld_root
from src.config.config import *

from src.learner.learner import train_model, predict

_root = get_skuld_root()
_train_csv = _root / "python-ml" / "data" / "train.csv"
_test_csv = _root / "python-ml" / "data" / "test.csv"
_full_csv = _root / "python-ml" / "data" / "data_preprocessed.csv"
_model_file = _root / "python-ml" / "data" / "model.pkl"
_prediction_file = _root / "python-ml" / "data" / "predictions"
_raw_data_csv = _root / "data" / "data_long.csv"
_wide_imputed = _root / "python-ml" / "data" / "data_wide_imputed.csv"


def run():
    long_to_wide_and_impute(str(_raw_data_csv), str(_wide_imputed))
    preprocess(str(_wide_imputed), str(_full_csv))

    full_df = load_csv(str(_full_csv))
    data_end_ts = full_df[TIMESTAMP_COL].max()

    print(f"Raw Data End: {load_csv(str(_raw_data_csv))[TIMESTAMP_COL].max()}")
    print(f"Preprocessed Data End (Anchor): {data_end_ts}")

    for i in range(0, EVAL_TEST_ITERATIONS):
        run_iteration(i, data_end_ts)


def run_iteration(i, anchor_ts):
    to_ts = anchor_ts - TEST_SPLIT_DURATION_MILLIS * i
    from_ts = to_ts - TEST_SPLIT_DURATION_MILLIS
    split_and_save(str(_full_csv), from_ts, to_ts)
    train_model(str(_train_csv), str(_model_file))
    predict(str(_model_file), str(_test_csv), str(_prediction_file) + str(i) + ".csv")


if __name__ == "__main__":
    run()
