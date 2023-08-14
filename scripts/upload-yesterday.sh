#!/bin/bash
#
# This is a quick and dirty script to create a daily archive for yesterday and upload to S3.
#
set -o errexit
set -o nounset
set -o pipefail
if [[ "${TRACE-0}" == "1" ]]; then
    set -o xtrace
fi

# print current date
echo "now: $(date)"

# get yesterday's date
d=$(date -d yesterday '+%Y-%m-%d')
echo "upload for: $d"

# change to project root directory
cd "$(dirname "$0")"
cd ..
YES=1 ./scripts/upload.sh "/mnt/data/mempool-archiver/$d"