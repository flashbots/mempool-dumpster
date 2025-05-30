#!/bin/sh
set -e

# Default interval 1 hour (3600 seconds)
UPLOAD_INTERVAL=${UPLOAD_INTERVAL:-3600}

# Check required variables
if [ -z "$R2_ACCOUNT_ID" ] || [ -z "$S3_BUCKET" ] || [ -z "$AWS_ACCESS_KEY_ID" ] || [ -z "$AWS_SECRET_ACCESS_KEY" ] || [ -z "$AWS_REGION" ]; then
  echo "Error: Missing required environment variables."
  echo "Need: R2_ACCOUNT_ID, S3_BUCKET, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION"
  exit 1
fi

# Construct R2 endpoint URL
R2_ENDPOINT="https://${R2_ACCOUNT_ID}.r2.cloudflarestorage.com"

# Destination paths (ensure S3_BUCKET starts with s3://)
S3_DESTINATION="${S3_BUCKET%/}/hourly_raw/"
R2_DESTINATION="${S3_DESTINATION}" # Using the same path structure for R2

echo "Starting hourly uploader..."
echo "Local Source: /data"
echo "S3 Destination: ${S3_DESTINATION}"
echo "R2 Destination: ${R2_DESTINATION}"
echo "R2 Endpoint: ${R2_ENDPOINT}"
echo "Upload Interval: ${UPLOAD_INTERVAL} seconds"

while true; do
  echo "($(date)) Starting sync cycle..."

  # Sync to R2
  echo "Syncing to R2..."
  aws s3 sync /data "${R2_DESTINATION}" --endpoint-url "${R2_ENDPOINT}" --no-progress --exclude "*tmp*" --exclude "*.tmp"
  echo "Sync to R2 finished."

  # Sync to S3 (optional, maybe only R2 is needed? Add condition if needed)
  # echo "Syncing to S3..."
  # aws s3 sync /data "${S3_DESTINATION}" --no-progress --exclude "*tmp*" --exclude "*.tmp"
  # echo "Sync to S3 finished."

  echo "Sync cycle complete. Sleeping for ${UPLOAD_INTERVAL} seconds..."
  sleep $UPLOAD_INTERVAL
done 