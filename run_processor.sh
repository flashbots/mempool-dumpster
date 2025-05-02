#!/bin/sh
set -e

# --- Configuration ---
# Read from environment variables set in docker-compose
AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID}"
AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY}"
AWS_REGION="${AWS_REGION}"
R2_ACCOUNT_ID="${R2_ACCOUNT_ID}"
S3_BUCKET="${S3_BUCKET}" # Base bucket URL (e.g., s3://your-bucket)
CHECK_NODE_URI="${CHECK_NODE_URI}"
PROCESSING_DATE="${PROCESSING_DATE}" # Expects YYYY-MM-DD format

# --- Validate Configuration ---
if [ -z "$PROCESSING_DATE" ] || [ -z "$R2_ACCOUNT_ID" ] || [ -z "$S3_BUCKET" ] || \ 
   [ -z "$AWS_ACCESS_KEY_ID" ] || [ -z "$AWS_SECRET_ACCESS_KEY" ] || \ 
   [ -z "$AWS_REGION" ] || [ -z "$CHECK_NODE_URI" ]; then
  echo "Error: Missing required environment variables for processor."
  echo "Need: PROCESSING_DATE, R2_ACCOUNT_ID, S3_BUCKET, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION, CHECK_NODE_URI"
  exit 1
fi

# --- Derived Variables ---
YEAR_MONTH=$(echo "$PROCESSING_DATE" | cut -d'-' -f1,2)
YESTERDAY=$(date -I -d "$PROCESSING_DATE - 1 day") # For blacklist lookup
LOCAL_DATA_DIR="/data/${PROCESSING_DATE}"
LOCAL_MERGED_DIR="/merged/${PROCESSING_DATE}"
R2_ENDPOINT="https://${R2_ACCOUNT_ID}.r2.cloudflarestorage.com"

# Source and destination paths in S3/R2
# Assumes hourly data is under 'hourly_raw' and processed data goes to 'daily_processed'
HOURLY_RAW_S3_PATH="${S3_BUCKET%/}/hourly_raw/${PROCESSING_DATE}/"
DAILY_PROCESSED_S3_PATH="${S3_BUCKET%/}/daily_processed/${YEAR_MONTH}/"

# Create local directories
mkdir -p "${LOCAL_DATA_DIR}/transactions"
mkdir -p "${LOCAL_DATA_DIR}/sourcelog"
mkdir -p "${LOCAL_MERGED_DIR}"

# --- Download Hourly Data from R2 ---
echo "[$(date)] Downloading hourly data for ${PROCESSING_DATE} from R2..."
echo "Source: ${HOURLY_RAW_S3_PATH}"
echo "Destination: ${LOCAL_DATA_DIR}"
# Sync transactions and sourcelog directories separately (adjust based on actual structure in R2)
aws s3 sync "${HOURLY_RAW_S3_PATH}transactions/" "${LOCAL_DATA_DIR}/transactions/" --endpoint-url "${R2_ENDPOINT}" --no-progress
aws s3 sync "${HOURLY_RAW_S3_PATH}sourcelog/" "${LOCAL_DATA_DIR}/sourcelog/" --endpoint-url "${R2_ENDPOINT}" --no-progress
echo "Download complete."

# --- Merge Data ---
echo "[$(date)] Merging sourcelog for ${PROCESSING_DATE}..."
/app/merge sourcelog \
    --out "${LOCAL_MERGED_DIR}" \
    --fn-prefix "${PROCESSING_DATE}" \
    "${LOCAL_DATA_DIR}/sourcelog/"*.csv
echo "Sourcelog merge complete."

echo "[$(date)] Merging transactions for ${PROCESSING_DATE}..."
# Note: Need path to yesterday's merged CSV for blacklist. Assuming it's available at daily path.
YESTERDAY_CSV_PATH="${DAILY_PROCESSED_S3_PATH}${YESTERDAY}.csv.zip"
LOCAL_YESTERDAY_CSV="/tmp/${YESTERDAY}.csv.zip"

echo "Downloading yesterday's blacklist CSV: ${YESTERDAY_CSV_PATH}"
aws s3 cp "${YESTERDAY_CSV_PATH}" "${LOCAL_YESTERDAY_CSV}" --endpoint-url "${R2_ENDPOINT}" --no-progress || echo "Warning: Yesterday's blacklist CSV not found or failed to download."

BLACKLIST_ARG=""
if [ -f "${LOCAL_YESTERDAY_CSV}" ]; then
    BLACKLIST_ARG="--tx-blacklist ${LOCAL_YESTERDAY_CSV}"
fi

/app/merge transactions \
    --out "${LOCAL_MERGED_DIR}" \
    --fn-prefix "${PROCESSING_DATE}" \
    --write-tx-csv \
    --write-summary \
    --check-node "${CHECK_NODE_URI}" \
    ${BLACKLIST_ARG} \
    --sourcelog "${LOCAL_MERGED_DIR}/${PROCESSING_DATE}_sourcelog.csv" \
    "${LOCAL_DATA_DIR}/transactions/"*.csv
echo "Transaction merge complete."

# --- Compress Files ---
echo "[$(date)] Compressing output files..."
cd "${LOCAL_MERGED_DIR}"
zip "${PROCESSING_DATE}.csv.zip" "${PROCESSING_DATE}.csv"
gzip -k "${PROCESSING_DATE}.csv" # Keep original .csv
zip "${PROCESSING_DATE}_sourcelog.csv.zip" "${PROCESSING_DATE}_sourcelog.csv"
# Optional: Compress transactions.csv if needed (was commented out in original script)
# zip "${PROCESSING_DATE}_transactions.csv.zip" "${PROCESSING_DATE}_transactions.csv"
# Optional: Handle trash files if needed
cd -
echo "Compression complete."

# --- Upload Daily Processed Files to R2 & S3 ---
echo "[$(date)] Uploading processed files for ${PROCESSING_DATE} to R2 and S3..."
cd "${LOCAL_MERGED_DIR}"

# Upload Parquet
echo "Uploading ${PROCESSING_DATE}.parquet ..."
aws s3 cp --no-progress "${PROCESSING_DATE}.parquet" "${DAILY_PROCESSED_S3_PATH}" --endpoint-url "${R2_ENDPOINT}"
aws --profile aws s3 cp --no-progress "${PROCESSING_DATE}.parquet" "${DAILY_PROCESSED_S3_PATH}" # Assuming 'aws' profile for S3

# Upload CSV Zip
echo "Uploading ${PROCESSING_DATE}.csv.zip ..."
aws s3 cp --no-progress "${PROCESSING_DATE}.csv.zip" "${DAILY_PROCESSED_S3_PATH}" --endpoint-url "${R2_ENDPOINT}"
aws --profile aws s3 cp --no-progress "${PROCESSING_DATE}.csv.zip" "${DAILY_PROCESSED_S3_PATH}"

# Upload CSV Gzip
echo "Uploading ${PROCESSING_DATE}.csv.gz ..."
aws s3 cp --no-progress "${PROCESSING_DATE}.csv.gz" "${DAILY_PROCESSED_S3_PATH}" --endpoint-url "${R2_ENDPOINT}"
# aws --profile aws s3 cp --no-progress "${PROCESSING_DATE}.csv.gz" "${DAILY_PROCESSED_S3_PATH}" # Optional: Upload gz to S3 too

# Upload Sourcelog Zip
echo "Uploading ${PROCESSING_DATE}_sourcelog.csv.zip ..."
aws s3 cp --no-progress "${PROCESSING_DATE}_sourcelog.csv.zip" "${DAILY_PROCESSED_S3_PATH}" --endpoint-url "${R2_ENDPOINT}"
aws --profile aws s3 cp --no-progress "${PROCESSING_DATE}_sourcelog.csv.zip" "${DAILY_PROCESSED_S3_PATH}"

# Upload Summary Txt
echo "Uploading ${PROCESSING_DATE}_summary.txt ..."
aws s3 cp --no-progress "${PROCESSING_DATE}_summary.txt" "${DAILY_PROCESSED_S3_PATH}" --endpoint-url "${R2_ENDPOINT}"
aws --profile aws s3 cp --no-progress "${PROCESSING_DATE}_summary.txt" "${DAILY_PROCESSED_S3_PATH}"

cd -
echo "Upload complete."

# --- Cleanup ---
echo "[$(date)] Cleaning up local files..."
rm -rf "${LOCAL_DATA_DIR}"
rm -rf "${LOCAL_MERGED_DIR}"
rm -f "${LOCAL_YESTERDAY_CSV}"
echo "Cleanup complete."

echo "[$(date)] Processor run for ${PROCESSING_DATE} finished successfully."

exit 0 