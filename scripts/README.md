This directory contains a bunch of useful scripts.

More helpers:

```bash
# pretty-print tx source stats
journalctl -u mempool-collector -o cat --since "1h ago" | grep "source_stats" | awk '{ $1=""; $2=""; $3=""; print $0}' | jq
```