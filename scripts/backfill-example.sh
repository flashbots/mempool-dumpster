#!/bin/bash
#
# Example to backfill (or fix) data for a date range
#
set -o errexit
set -o nounset
set -o pipefail

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
DATA_DIR="/mnt/data/mempool-dumpster"

date_from="2023-09-23"
date_to="2023-09-24" # exclusive
# date_to=$(date -I)

date=$date_from
while [ "$date" != $date_to ]; do
    echo $date

    ym=${date:0:7}
    yesterday=$(date -I -d "$date - 1 day")

    cd $DATA_DIR/$date
    ls

    mkdir -p orig
    mv * orig/ || true
    ls

    sourcelog_arg=""
    if [ -f "${DATA_DIR}/${date}/orig/${date}_sourcelog.csv.zip" ]; then
        sourcelog_arg="--sourcelog ${DATA_DIR}/${date}/orig/${date}_sourcelog.csv.zip"
    elif [ -f "${DATA_DIR}/${date}/orig/${date}_sourcelog.csv" ]; then
        sourcelog_arg="--sourcelog ${DATA_DIR}/${date}/orig/${date}_sourcelog.csv"
    fi

    $SCRIPT_DIR/../../build/mempool-dumpster merge t \
        --out "${DATA_DIR}/${date}/" \
        --fn-prefix $date \
        --write-tx-csv \
        --write-summary \
        --check-node /mnt/data/geth/geth.ipc \
        --tx-blacklist "${DATA_DIR}/${yesterday}/${yesterday}.csv.zip" \
        $sourcelog_arg \
        "${DATA_DIR}/${date}/orig/${date}_transactions.csv.zip"

    # create new transactions.csv.zip
    zip "${date}_transactions.csv.zip" "${date}_transactions.csv"
    rm -f "${date}_transactions.csv"

    # create new csv.zip
    zip "${date}.csv.zip" "${date}.csv"
    rm -f "${date}.csv"

    echo "[s3] Uploading ${date}.parquet ..."
    aws s3 cp "${date}.parquet" "s3://flashbots-mempool-dumpster/ethereum/mainnet/${ym}/" --endpoint-url "https://${CLOUDFLARE_R2_ACCOUNT_ID}.r2.cloudflarestorage.com"
    aws --profile aws s3 cp "${date}.parquet" "s3://flashbots-mempool-dumpster/ethereum/mainnet/${ym}/"

    echo "[s3] Uploading ${date}.csv.zip ..."
    aws s3 cp "${date}.csv.zip" "s3://flashbots-mempool-dumpster/ethereum/mainnet/${ym}/" --endpoint-url "https://${CLOUDFLARE_R2_ACCOUNT_ID}.r2.cloudflarestorage.com"
    aws --profile aws s3 cp "${date}.csv.zip" "s3://flashbots-mempool-dumpster/ethereum/mainnet/${ym}/"

    echo "[s3] Uploading ${date}_summary.txt ..."
    aws s3 cp "${date}_summary.txt" "s3://flashbots-mempool-dumpster/ethereum/mainnet/${ym}/" --endpoint-url "https://${CLOUDFLARE_R2_ACCOUNT_ID}.r2.cloudflarestorage.com"
    aws --profile aws s3 cp "${date}_summary.txt" "s3://flashbots-mempool-dumpster/ethereum/mainnet/${ym}/"

    date=$(date -I -d "$date + 1 day")
    # exit 0
done