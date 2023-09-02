#!/bin/bash
aws s3 ls s3://flashbots-mempool-dumpster/$1 --endpoint-url "https://${CLOUDFLARE_R2_ACCOUNT_ID}.r2.cloudflarestorage.com" | awk '{ print $2 }'