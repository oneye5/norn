# Percentage change required to label = 1
# Example: 0.02 = +2% price increase
THRESHOLD_PCT = 0.02

# Seconds into the future to check for price movement
# Example: 86400 = 1 day
FUTURE_DELTA_SEC = 86400 * 128

# Test duration
TEST_DELTA_SEC = 30 * 24 * 3600  # last 30 days


# Column names
TIMESTAMP_COL = "timestamp"
TICKER_COL = "ticker"
CLOSE_COL = "Close"

# Output label column
LABEL_COL = "label"
