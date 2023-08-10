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

d=$(date -d yesterday '+%Y-%m-%d')
echo $d

# change to script directory
cd "$(dirname "$0")"
YES=1 ./upload.sh "/mnt/data/mempool-archiver/$d"