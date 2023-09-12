#!/bin/bash
aws s3 cp website/static s3://flashbots-mempool-dumpster/static --endpoint-url "https://${CLOUDFLARE_R2_ACCOUNT_ID}.r2.cloudflarestorage
.com" --recursive
