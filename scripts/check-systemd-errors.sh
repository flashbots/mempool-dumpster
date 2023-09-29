#!/bin/bash
#
# Checks systemd for errors
#
set -o errexit
set -o nounset
set -o pipefail
if [[ "${TRACE-0}" == "1" ]]; then
    set -o xtrace
fi

# load environment variables $PUSHOVER_APP_TOKEN and $PUSHOVER_APP_KEY
source "$(dirname "$0")/../.env.prod"

# Maximum number of allowed errors (above a notification is sent)
ERROR_LIMIT=2
SINCE="1d ago"

function error() {
    # write to /tmp/mempool-collector-error-current.log but only if not already exists
    if [ -f /tmp/mempool-collector-error-current.log ]; then
        return
    fi

    echo "$1" > /tmp/mempool-collector-error-current.log
    echo "NEW ERRORS" $1

    curl -s \
    --form-string "token=$PUSHOVER_APP_TOKEN" \
    --form-string "user=$PUSHOVER_APP_KEY" \
    --form-string "message=$1 errors in mempool-collector service found" \
    https://api.pushover.net/1/messages.json
}

function reset() {
    rm -f /tmp/mempool-collector-error-current.log
}

date
journalctl -u mempool-collector -o cat --since "$SINCE" | grep ERROR | tee /tmp/mempool-collector-errors.log
lines=$(wc -l /tmp/mempool-collector-errors.log | awk '{print $1}')
# echo "Found $lines errors in mempool-collector service"

# if more errors than threshold
if [ "$lines" -gt $ERROR_LIMIT ]; then
  error $lines
elif [ "$lines" -eq 0 ]; then
  reset
fi
