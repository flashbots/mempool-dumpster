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
ERROR_LIMIT=3
SINCE="2h ago"

function error() {
    # write to /tmp/mempool-collector-error-current.log but only if not already exists
    if [ -f /tmp/mempool-collector-error-current.log ]; then
        return
    fi

    cp /tmp/mempool-collector-errors.log /tmp/mempool-collector-error-current.log

    lines=$(wc -l /tmp/mempool-collector-errors.log | awk '{print $1}')
    err=$( head -n 5 /tmp/mempool-collector-errors.log )
    echo "NEW ERRORS"
    echo "$err"

    # send pushover message
    curl -s \
    --form-string "token=$PUSHOVER_APP_TOKEN" \
    --form-string "user=$PUSHOVER_APP_KEY" \
    --form-string "message=mempool-collector $lines errors: $err" \
    https://api.pushover.net/1/messages.json
}

function reset() {
    if [ ! -f /tmp/mempool-collector-error-current.log ]; then
        return
    fi

    # remove previous file
    rm -f /tmp/mempool-collector-error-current.log

    # send pushover message about resolution
    curl -s \
    --form-string "token=$PUSHOVER_APP_TOKEN" \
    --form-string "user=$PUSHOVER_APP_KEY" \
    --form-string "message=mempool-collector errors resolved" \
    https://api.pushover.net/1/messages.json
}

date
journalctl -u mempool-collector -o cat --since "$SINCE" | grep ERROR | tee /tmp/mempool-collector-errors.log  || true
lines=$(wc -l /tmp/mempool-collector-errors.log | awk '{print $1}')
# echo "Found $lines errors in mempool-collector service"

# alert if more errors than threshold
if [ "$lines" -gt $ERROR_LIMIT ]; then
  error
elif [ "$lines" -eq 0 ]; then
  reset
fi
