This directory contains a bunch of useful scripts.

More helpers:

```bash
#
# source stats from journal
#

# all tx
journalctl -u mempool-collector -o cat --since "10m ago" | grep "source_stats_all" | awk '{ $1=""; $2=""; $3=""; print $0}' | jq

# only specific ones, side by side
journalctl -u mempool-collector -o cat --since "10m ago" | grep "source_stats_all" | awk '{ $1=""; $2=""; $3=""; print $0}' | jq '.local + "   " + .apool'

journalctl -u mempool-collector -o cat --since "1h ago" | grep "source_stats_first" | awk '{ $1=""; $2=""; $3=""; print $0}' | jq
```