#!/bin/bash
# require two arguments
if [ $# -ne 2 ]; then
    echo "Usage: $0 <local_file> <s3_folder>"
    exit 1
fi

echo "uploading $1 to S3 $2 ..."
aws s3 cp $1 s3://flashbots-mempool-dumpster/$2 --endpoint-url "https://${CLOUDFLARE_R2_ACCOUNT_ID}.r2.cloudflarestorage.com"