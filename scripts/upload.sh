#!/bin/bash
#
# This is a quick and dirty script to:
#
# 1. create a daily archive
# 2. upload to Cloudflare R2 and AWS S3
#
# Requires awscli with (1) default profile for R2 and (2) profile "aws" for AWS S3.
#
# Usage:
#
#    ./scripts/upload.sh /mnt/data/mempool-dumpster/2023-08-28
#
set -o errexit
set -o nounset
set -o pipefail
if [[ "${TRACE-0}" == "1" ]]; then
    set -o xtrace
fi

# requires directory as argument
if [ $# -eq 0 ]
  then
    echo "Usage: $0 <directory>"
    exit 1
fi

# requires directory to exist
if [ ! -d "$1" ]; then
  echo "Directory $1 does not exist"
  exit 1
fi

# extract date from directory name
date=$(basename $1)
ym=${date:0:7}

# confirm
if [ -z ${YES:-} ]; then
  echo "Uploading $1 for date $date"
  read -p "Are you sure? " -n 1 -r
  echo
  if [[ ! $REPLY =~ ^[Yy]$ ]]
  then
    exit 1
  fi
fi

# summarize raw transactions
echo "Running summarizer"
/root/mempool-dumpster/build/summerizer -out $1 --out-date $date $1/transactions/*.csv

# compress
cd $1
echo "Compressing transaction files..."
zip "${date}_transactions.csv.zip" "${date}_transactions.csv"
zip "${date}.csv.zip" "${date}.csv"

# upload to Cloudflare R2 and AWS S3
echo "Uploading parquet file..."
aws s3 cp --no-progress "${date}.parquet" "s3://flashbots-mempool-dumpster/ethereum/mainnet/${ym}/" --endpoint-url "https://${CLOUDFLARE_R2_ACCOUNT_ID}.r2.cloudflarestorage.com"
aws --profile aws s3 cp --no-progress "${date}.parquet" "s3://flashbots-mempool-dumpster/ethereum/mainnet/${ym}/"

echo "Uploading zipped summary CSV file..."
aws s3 cp --no-progress "${date}.csv.zip" "s3://flashbots-mempool-dumpster/ethereum/mainnet/${ym}/" --endpoint-url "https://${CLOUDFLARE_R2_ACCOUNT_ID}.r2.cloudflarestorage.com"
aws --profile aws s3 cp --no-progress "${date}.csv.zip" "s3://flashbots-mempool-dumpster/ethereum/mainnet/${ym}/"

echo "Uploading transactions file..."
aws s3 cp --no-progress "${date}_transactions.csv.zip" "s3://flashbots-mempool-dumpster/ethereum/mainnet/${ym}/" --endpoint-url "https://${CLOUDFLARE_R2_ACCOUNT_ID}.r2.cloudflarestorage.com"
aws --profile aws s3 cp --no-progress "${date}_transactions.csv.zip" "s3://flashbots-mempool-dumpster/ethereum/mainnet/${ym}/"

# finally, remove the raw transactions directory
if [ -z ${YES:-} ]; then
  read -p "Upload successful. Remove the raw transactions directory? " -n 1 -r
  echo
  if [[ ! $REPLY =~ ^[Yy]$ ]]
  then
    exit 0
  fi
fi

rm -rf "transactions" "${date}_transactions.csv" "${date}.csv"
echo "All done!"
echo ""
