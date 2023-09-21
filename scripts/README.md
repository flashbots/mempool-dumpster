This directory contains a bunch of useful scripts.

---

## Clickhouse Local

It's extremely fast and versatily, and can even query across multiple parquet files:

- https://clickhouse.com/docs/en/operations/utilities/clickhouse-local
- https://clickhouse.com/docs/en/sql-reference/statements

```bash
# You can query parquet files with SQL:
clickhouse local -q "select hash,hex(rawTx) from 'out/out/transactions.parquet' limit 1;"
clickhouse local -q "select count(*) from 'out/out/transactions.parquet';"

# Show schema
clickhouse local -q "DESCRIBE TABLE 'out/out/transactions.parquet';"

# Get exclusive transactions for bloxroute
clickhouse local -q "SELECT COUNT(*) FROM 'out/out/transactions.parquet' WHERE length(sources) == 1 AND sources[1] == 'bloxroute';"

# Get number of included transactions (beta)
clickhouse local -q "SELECT COUNT(*) FROM 'out/out/transactions.parquet' WHERE blockNumberIncluded>0;"
```

More helpers:

```bash
# source stats - all tx
journalctl -u mempool-collector -o cat --since "10m ago" | grep "source_stats_all" | awk '{ $1=""; $2=""; $3=""; print $0}' | jq

# source stats - only specific sources
journalctl -u mempool-collector -o cat --since "10m ago" | grep "source_stats_all" | awk '{ $1=""; $2=""; $3=""; print $0}' | jq '.local + "   " + .apool'

# source stats - tx first
journalctl -u mempool-collector -o cat --since "1h ago" | grep "source_stats_first" | awk '{ $1=""; $2=""; $3=""; print $0}' | jq
```

CSV tricks

```bash
# get unique hashes from sourcelog or main csv file
cat file.csv | sed 's/,/ /g' | awk '{ print $2}' | sort | uniq > unique.txt

# who sent trash?
cat /mnt/data/mempool-dumpster/2023-09-13/trash/*.csv | sed 's/,/ /g' | awk '{ print $3}' | sort | uniq -c
```
