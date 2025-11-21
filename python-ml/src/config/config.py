# Percentage change required to label = 1
# Example: 0.02 = +2% price increase
THRESHOLD_PCT = 0.02


_day = 1000 * 60 * 60 * 24
_year = _day * 365

# milliseconds into the future to check for price movement
FUTURE_DELTA_MILLIS = _day * 128

# Test duration
TEST_DELTA_SEC = _day * 30


# Column names
TIMESTAMP_COL = "timestamp"
TICKER_COL = "ticker"
CLOSE_COL = "Close"

# Output label column
LABEL_COL = "label"
PRICE_COL = "Close"
PREDICTION_COL = "pred_prob"
TICKER_PREFIX = "#TICKER#"

ANALYSIS_TRAIN_DELTA_MILLIS = _day * 90   # 90 days
ANALYSIS_TEST_DELTA_MILLIS = _day * 30    # 30 days
ANALYSIS_STEP_MILLIS = _day * 1           # 1 day



print("This file is not intended to be runnable")