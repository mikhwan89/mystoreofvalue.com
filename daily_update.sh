#!/bin/bash

# Daily Asset Price Update Script
# Runs at 2 AM US Eastern Time
# Updates last 10 days of price data for all configured symbols
# Updates asset metadata (crypto, commodities, indices, stocks), exchanges data, and holidays

SCRIPT_DIR="/home/$(whoami)/projects/mystoreofvalue.com"
LOG_DIR="$SCRIPT_DIR/logs"
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
LOG_FILE="$LOG_DIR/daily_update_$TIMESTAMP.log"

# Create logs directory if it doesn't exist
mkdir -p "$LOG_DIR"

# Log start time
echo "========================================" >> "$LOG_FILE"
echo "Daily Asset Price Update Started" >> "$LOG_FILE"
echo "Time: $(date)" >> "$LOG_FILE"
echo "========================================" >> "$LOG_FILE"

# Change to script directory
cd "$SCRIPT_DIR"

# Step 1: Update exchanges data
echo "" >> "$LOG_FILE"
echo "--- Updating Exchanges Data ---" >> "$LOG_FILE"
python3 populate_exchanges.py >> "$LOG_FILE" 2>&1

# Step 2: Update exchange holidays
echo "" >> "$LOG_FILE"
echo "--- Updating Exchange Holidays ---" >> "$LOG_FILE"
python3 populate_exchange_holidays.py >> "$LOG_FILE" 2>&1

# Step 3: Update asset metadata (crypto, commodities, indices)
echo "" >> "$LOG_FILE"
echo "--- Updating Asset Metadata (Crypto, Commodities, Indices) ---" >> "$LOG_FILE"
python3 populate_asset_metadata.py >> "$LOG_FILE" 2>&1

# Step 4: Update stocks metadata
echo "" >> "$LOG_FILE"
echo "--- Updating Stocks Metadata ---" >> "$LOG_FILE"
python3 populate_stocks_metadata.py >> "$LOG_FILE" 2>&1

# Step 5: Run the Python script in daily update mode
echo "" >> "$LOG_FILE"
echo "--- Updating Price Data (Last 10 Days) ---" >> "$LOG_FILE"
python3 fetch_asset_light.py --daily >> "$LOG_FILE" 2>&1

# Log completion
echo "========================================" >> "$LOG_FILE"
echo "Daily Asset Price Update Completed" >> "$LOG_FILE"
echo "Time: $(date)" >> "$LOG_FILE"
echo "========================================" >> "$LOG_FILE"

# Keep only last 30 days of logs
find "$LOG_DIR" -name "daily_update_*.log" -type f -mtime +30 -delete

echo "Daily update completed. Log: $LOG_FILE"
