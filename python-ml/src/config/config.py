# Percentage change required to label = 1
# Example: 0.02 = +2% price increase
THRESHOLD_PCT = 0.02


_day = 1000 * 60 * 60 * 24
_year = _day * 365

# milliseconds into the future to check for price movement
LABEL_LOOKAHEAD_MILLIS = _year

# Test duration
TEST_SPLIT_DURATION_MILLIS = _day * 128


# Column names
TIMESTAMP_COL = "timestamp"
TICKER_COL = "ticker"
CLOSE_COL = "Close"

# Output label column
LABEL_COL = "label"
PRICE_COL = "Close"
PREDICTION_COL = "pred_prob"
TICKER_PREFIX = "#TICKER#"

# Analysis & evaluation
EVAL_TEST_ITERATIONS = 100




print("This file is not intended to be runnable")