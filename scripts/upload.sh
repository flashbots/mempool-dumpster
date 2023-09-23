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
yesterday=$(date -I -d "$date - 1 day")

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

#
# PROCESS RAW FILES
#
echo "Merging sourcelog..."
/server/mempool-dumpster/build/merge sourcelog --out $1 --fn-prefix $date $1/sourcelog/*.csv

echo "Merging transactions..."
/server/mempool-dumpster/build/merge transactions \
  --check-node /mnt/data/geth/geth.ipc \
  --write-tx-csv \
  --tx-blacklist "$1/../${yesterday}/${yesterday}.csv.zip" \
  --sourcelog "$1/${date}_sourcelog.csv" \
  --out $1 \
  --fn-prefix $date \
  $1/transactions/*.csv

#
# Compress & upload
#
cd $1
echo "Compressing transaction files..."
zip "${date}_transactions.csv.zip" "${date}_transactions.csv"
zip "${date}.csv.zip" "${date}.csv"
zip "${date}_sourcelog.csv.zip" "${date}_sourcelog.csv"

# upload to Cloudflare R2 and AWS S3
echo "Uploading ${date}.parquet ..."
aws s3 cp --no-progress "${date}.parquet" "s3://flashbots-mempool-dumpster/ethereum/mainnet/${ym}/" --endpoint-url "https://${CLOUDFLARE_R2_ACCOUNT_ID}.r2.cloudflarestorage.com"
aws --profile aws s3 cp --no-progress "${date}.parquet" "s3://flashbots-mempool-dumpster/ethereum/mainnet/${ym}/"

echo "Uploading ${date}.csv.zip CSV ..."
aws s3 cp --no-progress "${date}.csv.zip" "s3://flashbots-mempool-dumpster/ethereum/mainnet/${ym}/" --endpoint-url "https://${CLOUDFLARE_R2_ACCOUNT_ID}.r2.cloudflarestorage.com"
aws --profile aws s3 cp --no-progress "${date}.csv.zip" "s3://flashbots-mempool-dumpster/ethereum/mainnet/${ym}/"

# echo "Uploading ${date}_transactions.csv.zip ..."
# aws s3 cp --no-progress "${date}_transactions.csv.zip" "s3://flashbots-mempool-dumpster/ethereum/mainnet/${ym}/" --endpoint-url "https://${CLOUDFLARE_R2_ACCOUNT_ID}.r2.cloudflarestorage.com"
# aws --profile aws s3 cp --no-progress "${date}_transactions.csv.zip" "s3://flashbots-mempool-dumpster/ethereum/mainnet/${ym}/"

echo "Uploading ${date}_sourcelog.csv.zip ..."
aws s3 cp --no-progress "${date}_sourcelog.csv.zip" "s3://flashbots-mempool-dumpster/ethereum/mainnet/${ym}/" --endpoint-url "https://${CLOUDFLARE_R2_ACCOUNT_ID}.r2.cloudflarestorage.com"
aws --profile aws s3 cp --no-progress "${date}_sourcelog.csv.zip" "s3://flashbots-mempool-dumpster/ethereum/mainnet/${ym}/"

#
# Create analysis
#
echo "Creating summary..."
cd $1
/server/mempool-dumpster/build/analyze \
  --out "${date}_summary.txt" \
  --input-parquet "${date}.parquet" \
  --input-sourcelog "${date}_sourcelog.csv.zip"

# /server/mempool-dumpster/build/analyze test \
#   --tx-blacklist "../${yesterday}/${yesterday}.csv.zip" \
#   --tx-whitelist "${date}.csv.zip" \
#   --out "${date}_summary.txt" \
#   "${date}_sourcelog.csv"

echo "Uploading ${date}_summary.txt ..."
aws s3 cp --no-progress "${date}_summary.txt" "s3://flashbots-mempool-dumpster/ethereum/mainnet/${ym}/" --endpoint-url "https://${CLOUDFLARE_R2_ACCOUNT_ID}.r2.cloudflarestorage.com"
aws --profile aws s3 cp --no-progress "${date}_summary.txt" "s3://flashbots-mempool-dumpster/ethereum/mainnet/${ym}/"

#
# CLEANUP
#
if [ -z ${YES:-} ]; then
  read -p "Upload successful. Remove the raw transactions directory? " -n 1 -r
  echo
  if [[ ! $REPLY =~ ^[Yy]$ ]]
  then
    exit 0
  fi
fi

# rm -rf transactions sourcelog
rm -rf "${date}_transactions.csv" "${date}.csv" "${date}_sourcelog.csv"
echo "All done!"
echo ""
