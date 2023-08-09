#!/bin/bash
#
# This is a quick and dirty script to create a daily archive and upload to S3.
# Only meant for development purposes, not production
#

# requires directory as argument
if [ $# -eq 0 ]
  then
    echo "No arguments supplied"
    exit 1
fi

# requires directory to exist
if [ ! -d "$1" ]; then
  echo "Directory $1 does not exist"
  exit 1
fi

# extract date from directory name
date=$(basename $1)

# confirm
echo "Uploading $1 for date $date"
read -p "Are you sure? " -n 1 -r
echo
if [[ ! $REPLY =~ ^[Yy]$ ]]
then
  exit 0
fi

# summarize raw transactions
echo "Running summarizer"
go run cmd/summarizer/main.go -dir $1

# rename
mv $1/transactions.parquet "$1/${date}.parquet"
mv $1/transactions "$1/${date}_transactions"
cd $1
zip -r "${date}_transactions.zip" "${date}_transactions"

# upload
echo "Uploading parquet file..."
aws s3 cp "${date}.parquet" "s3://flashbots-mempool-dumpster/ethereum/mainnet/${date}/"

echo "Uploading transactions file..."
aws s3 cp "${date}_transactions.zip" "s3://flashbots-mempool-dumpster/ethereum/mainnet/${date}/"