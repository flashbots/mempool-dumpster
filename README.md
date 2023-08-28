# Mempool Dumpster 🗑️♻️

[![Goreport status](https://goreportcard.com/badge/github.com/flashbots/mempool-dumpster)](https://goreportcard.com/report/github.com/flashbots/mempool-dumpster)
[![Test status](https://github.com/flashbots/mempool-dumpster/workflows/Checks/badge.svg?branch=main)](https://github.com/flashbots/mempool-dumpster/actions?query=workflow%3A%22Checks%22)

Dump mempool transactions from EL nodes, and archive them in [Parquet](https://github.com/apache/parquet-format) and CSV format.

Output files:

1. CSV: Raw transactions (timestamp in milliseconds, tx hash, RLP hex; about 1GB / day zipped)
2. CSV: [Transaction metadata](/common/types.go#L5-L22) (~100MB / day zipped)
3. Parquet: [Transaction metadata](/common/types.go#L5-L22) (~100MB / day)

Mempool sources:

1. Generic EL nodes (`newPendingTransactions`) (i.e. go-ethereum, Infura, etc.)
2. Alchemy ([`alchemy_pendingTransactions`](https://docs.alchemy.com/reference/alchemy-pendingtransactions))
3. [bloXroute](https://docs.bloxroute.com/streams/newtxs-and-pendingtxs)

Notes:

- This project is under active development, although relatively stable and ready to use in production
- Observing about 30k - 100k mempool transactions per hour (1M - 1.5M transactions per day)

---

# System architecture

1. [Mempool Collector](cmd/collector/main.go): Connects to EL nodes and writes new mempool transactions to CSV files. Multiple collector instances can run without colliding.
2. [Summarizer](cmd/summarizer/main.go): Takes collector CSV files as input, de-duplicates, sorts by timestamp and writes CSV + Parquet output files

---

# Getting started

## Mempool Collector

1. Subscribes to new pending transactions at various data sources
    1. EL nodes via websocket (local (i.e. geth) or remote (i.e. Infura))
    1. Can also connect to [bloXroute (need "Professional" plan or above)](https://docs.bloxroute.com/streams/newtxs-and-pendingtxs)
1. Writes `timestamp` + `hash` + `rawTx` to CSV file (one file per hour [by default](collector/consts.go))
1. Note: the collector can store transactions repeatedly, and only the summarizer will properly deduplicate them later

Default filename:

- Schema: `<out_dir>/<date>/transactions/txs_<date>_<uid>.csv`
- Example: `out/2023-08-07/transactions/txs_2023-08-07-10-00_collector1.csv`

**Running the mempool collector:**

```bash
# Connect to ws://localhost:8546 and write CSVs into ./out
go run cmd/collector/main.go -out ./out

# Connect to multiple nodes
go run cmd/collector/main.go -out ./out -nodes ws://server1.com:8546,ws://server2.com:8546
```

## Summarizer

- Iterates over collector output directory / CSV files
- Deduplicates transactions, sorts them by timestamp
- Creates:
  - Summary file in Parquet and CSV format with [key transaction attributes](summarizer/types.go)
  - A single, sorted and deduplicated transactions CSV file

```bash
go run cmd/summarizer/main.go -h

go run cmd/summarizer/main.go -out /mnt/data/mempool-dumpster/2023-08-12/ --out-date 2023-08-12 /mnt/data/mempool-dumpster/2023-08-12/2023-08-12_transactions/*.csv
```


---

# Architecture

## General design goals

- Keep it simple and stupid
- Vendor-agnostic (main flow should work on any server, independent of a cloud provider)
- Downtime-resilience to minimize any gaps in the archive
- Multiple collector instances can run concurrently, without getting into each others way
- Summarizer script produces the final archive (based on the input of multiple collector outputs)
- The final archive:
  - Includes (1) parquet file with transaction metadata, and (2) compressed file of raw transaction CSV files
  - Compatible with [Clickhouse](https://clickhouse.com/docs/en/integrations/s3) and [S3 Select](https://docs.aws.amazon.com/AmazonS3/latest/userguide/selecting-content-from-objects.html) (Parquet using gzip compression)
  - Easily distributable as torrent

## Mempool Collector

- `NodeConnection`
    - One for each EL connection
    - New pending transactions are sent to `TxProcessor` via a channel
- `TxProcessor`
    - Check if it already processed that tx
    - Store it in the output directory

## Summarizer

- Uses https://github.com/xitongsys/parquet-go to write Parquet format

## Transaction RLP format

- encoding transactions in typed EIP-2718 envelopes:
  - https://medium.com/@markodayansa/a-comprehensive-guide-to-rlp-encoding-in-ethereum-6bd75c126de0
  - https://blog.mycrypto.com/new-transaction-types-on-ethereum
  - https://eips.ethereum.org/EIPS/eip-2718

---

## Contributing

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

## Further notes

- See also: [discussion about compression](https://github.com/flashbots/mempool-dumpster/issues/2) and [storage](https://github.com/flashbots/mempool-dumpster/issues/1)

---

## License

MIT

---

## Maintainers

- [metachris](https://twitter.com/metachris)
- [0x416e746f6e](https://github.com/0x416e746f6e)
