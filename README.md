# Mempool Dumpster 🗑️♻️

[![Goreport status](https://goreportcard.com/badge/github.com/flashbots/mempool-dumpster)](https://goreportcard.com/report/github.com/flashbots/mempool-dumpster)
[![Test status](https://github.com/flashbots/mempool-dumpster/workflows/Checks/badge.svg?branch=main)](https://github.com/flashbots/mempool-dumpster/actions?query=workflow%3A%22Checks%22)

Dump mempool transactions from EL nodes, and archive them in [Parquet](https://github.com/apache/parquet-format) and CSV format.

Notes:

- **The data is freely available at https://mempool-dumpster.flashbots.net**
- Observing about 1M - 1.3M unique transactions per day
- This project is under active development, although relatively stable and ready to use in production
- Related tooling: https://github.com/dvush/mempool-dumpster-rs

---

## Available mempool sources

1. Generic EL nodes (`newPendingTransactions`) (i.e. go-ethereum, Infura, etc.)
2. Alchemy ([`alchemy_pendingTransactions`](https://docs.alchemy.com/reference/alchemy-pendingtransactions))
3. [bloXroute](https://docs.bloxroute.com/streams/newtxs-and-pendingtxs) (at least ["Professional" plan](https://bloxroute.com/pricing/))
4. [Chainbound Fiber](https://fiber.chainbound.io/docs/usage/getting-started/)
5. [Eden](https://docs.edennetwork.io/eden-rpc/speed-rpc)

---

## Output files

Daily files uploaded by mempool-dumpster (i.e. for [September 2023](https://mempool-dumpster.flashbots.net/ethereum/mainnet/2023-09/index.html)):

1. Parquet file with [transaction metadata and raw transactions](/common/types.go#L7) (~800MB/day, i.e. [`2023-09-08.parquet`](https://mempool-dumpster.flashbots.net/ethereum/mainnet/2023-09/2023-09-08.csv.zip))
1. CSV file with only the transaction metadata (~100MB/day zipped, i.e. [`2023-09-08.csv.zip`](https://mempool-dumpster.flashbots.net/ethereum/mainnet/2023-09/2023-09-08.csv.zip))
1. CSV file with details about when each transaction was received by any source (~100MB/day zipped, i.e. [`2023-09-08_sourcelog.csv.zip`](https://mempool-dumpster.flashbots.net/ethereum/mainnet/2023-09/2023-09-08_sourcelog.csv.zip))
1. Summary in text format (~2kB, i.e. [`2023-09-08_summary.txt`](https://mempool-dumpster.flashbots.net/ethereum/mainnet/2023-09/2023-09-08_summary.txt))

---

## FAQ

- _What is a-pool?_ ... A-Pool is a regular geth node with some optimized peering settings, subscribed to over the network.
- _What are exclusive transactions?_ ... a transaction that was seen from no other source (transaction only provided by a single source)

---

# Working with Parquet

[Apache Parquet](https://parquet.apache.org/) is a column-oriented data file format designed for efficient data storage and retrieval. It provides efficient data compression and encoding schemes with enhanced performance to handle complex data in bulk (more [here](https://www.databricks.com/glossary/what-is-parquet#:~:text=What%20is%20Parquet%3F,handle%20complex%20data%20in%20bulk.)).

We recommend to use [clickhouse local](https://clickhouse.com/docs/en/operations/utilities/clickhouse-local) (as well as [DuckDB](https://duckdb.org/)) to work with Parquet files, it makes it easy to run [queries](https://clickhouse.com/docs/en/sql-reference/statements) like:

```bash
# count rows
$ clickhouse local -q "SELECT count(*) FROM 'transactions.parquet' LIMIT 1;"

# get the first hash+rawTx
$ clickhouse local -q "SELECT hash,hex(rawTx) FROM 'transactions.parquet' LIMIT 1;"

# get details of a particular hash
$ clickhouse local -q "SELECT timestamp,hash,from,to,hex(rawTx) FROM 'transactions.parquet' WHERE hash='0x152065ad73bcf63f68572f478e2dc6e826f1f434cb488b993e5956e6b7425eed';"

# show the schema
$ clickhouse local -q "DESCRIBE TABLE 'transactions.parquet';"
timestamp	Nullable(DateTime64(3))
hash	Nullable(String)
chainId	Nullable(String)
from	Nullable(String)
to	Nullable(String)
value	Nullable(String)
nonce	Nullable(String)
gas	Nullable(String)
gasPrice	Nullable(String)
gasTipCap	Nullable(String)
gasFeeCap	Nullable(String)
dataSize	Nullable(Int64)
data4Bytes	Nullable(String)
rawTx	Nullable(String)
```

---

# System architecture

1. [Collector](cmd/collect/main.go): Connects to EL nodes and writes new mempool transactions and sourcelog to hourly CSV files. Multiple collector instances can run without colliding.
2. [Merger](cmd/merge/main.go): Takes collector CSV files as input, de-duplicates, sorts by timestamp and writes CSV + Parquet output files.
3. [Analyzer](cmd/analyze/main.go): Analyzes sourcelog CSV files and produces summary report.
4. [Website](cmd/website/main.go): Website dev-mode as well as build + upload.

---

# Getting started

## Mempool Collector

1. Subscribes to new pending transactions at various data sources
1. Writes `timestamp_ms` + `hash` + `raw_tx` to CSV file (one file per hour [by default](collector/consts.go))
1. Note: the collector can store transactions repeatedly, and only the merger will properly deduplicate them later

**Default filenames:**

Transactions
- Schema: `<out_dir>/<date>/transactions/txs_<date>_<uid>.csv`
- Example: `out/2023-08-07/transactions/txs_2023-08-07-10-00_collector1.csv`

Sourcelog
- Schema: `<out_dir>/<date>/sourcelog/src_<date>_<uid>.csv`
- Example: `out/2023-08-07/sourcelog/src_2023-08-07-10-00_collector1.csv`

**Running the mempool collector:**

```bash
# print help
go run cmd/collect/main.go -help

# Connect to ws://localhost:8546 and write CSVs into ./out
go run cmd/collect/main.go -out ./out

# Connect to multiple nodes
go run cmd/collect/main.go -out ./out -nodes ws://server1.com:8546,ws://server2.com:8546
```

## Merger

- Iterates over collector output directory / CSV files
- Deduplicates transactions, sorts them by timestamp

```bash
go run cmd/merge/main.go -h
```


---

# Architecture

## General design goals

- Keep it simple and stupid
- Vendor-agnostic (main flow should work on any server, independent of a cloud provider)
- Downtime-resilience to minimize any gaps in the archive
- Multiple collector instances can run concurrently, without getting into each others way
- Merger produces the final archive (based on the input of multiple collector outputs)
- The final archive:
  - Includes (1) parquet file with transaction metadata, and (2) compressed file of raw transaction CSV files
  - Compatible with [Clickhouse](https://clickhouse.com/docs/en/integrations/s3) and [S3 Select](https://docs.aws.amazon.com/AmazonS3/latest/userguide/selecting-content-from-objects.html) (Parquet using gzip compression)
  - Easily distributable as torrent

## Collector

- `NodeConnection`
    - One for each EL connection
    - New pending transactions are sent to `TxProcessor` via a channel
- `TxProcessor`
    - Check if it already processed that tx
    - Store it in the output directory

## Merger

- Uses https://github.com/xitongsys/parquet-go to write Parquet format

## Transaction RLP format

- encoding transactions in typed EIP-2718 envelopes:
  - https://medium.com/@markodayansa/a-comprehensive-guide-to-rlp-encoding-in-ethereum-6bd75c126de0
  - https://blog.mycrypto.com/new-transaction-types-on-ethereum
  - https://eips.ethereum.org/EIPS/eip-2718

---

# Contributing

Install dependencies

```bash
go install mvdan.cc/gofumpt@latest
go install honnef.co/go/tools/cmd/staticcheck@latest
go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest
go install github.com/daixiang0/gci@latest
```

Lint, test, format

```bash
make lint
make test
make fmt
```

---

# Further notes

- See also: [discussion about compression](https://github.com/flashbots/mempool-dumpster/issues/2) and [storage](https://github.com/flashbots/mempool-dumpster/issues/1)

---

# License

MIT

---

# Maintainers

- [metachris](https://twitter.com/metachris)
- [0x416e746f6e](https://github.com/0x416e746f6e)
