# Mempool Dumpster 🗑️♻️

[![Goreport status](https://goreportcard.com/badge/github.com/flashbots/mempool-dumpster)](https://goreportcard.com/report/github.com/flashbots/mempool-dumpster)
[![Test status](https://github.com/flashbots/mempool-dumpster/workflows/Checks/badge.svg?branch=main)](https://github.com/flashbots/mempool-dumpster/actions?query=workflow%3A%22Checks%22)

Dump mempool transactions from EL nodes, and archive them in [Parquet](https://github.com/apache/parquet-format) and CSV format.

- Parquet: [Transaction metadata](summarizer/types.go) (timestamp in millis, hash, [attributes](summarizer/types.go); about 100MB / day)
- CSV: Raw transactions (RLP hex + timestamp in millis + tx hash; about 1GB / day zipped)
- This project is under active development, although relatively stable and ready to use
- Observing about 30k - 100k mempool transactions per hour (1M - 1.5M transactions per day)

---

# System architecture

1. [Mempool Collector](cmd/collector/main.go): Connects to EL nodes and writes new mempool transactions to CSV files. Multiple collector instances can run without colliding.
2. [summarizer](cmd/summarizer/main.go): Takes collector CSV files as input, dedupes, sorts by timestamp and writes to CSV + Parquet output files

---

# Getting started

## Mempool Collector

1. Connects to one or more EL nodes via websocket
2. Listens for new pending transactions
3. Writes `timestamp` + `hash` + `rawTx` to CSV file (one file per hour [by default](collector/consts.go))

Default filename:

- Schema: `<out_dir>/<year-month>/transactions/txs_<date>_<uid>.csv`
- Example: `out/2023-08/transactions/txs_2023-08-07-10-00_collector1.csv`

**Running the mempool collector:**

```bash
# Connect to ws://localhost:8546 and write CSVs into ./out
go run cmd/collector/main.go -out ./out

# Connect to multiple nodes
go run cmd/collector/main.go -out ./out -nodes ws://server1.com:8546,ws://server2.com:8546
```

## Summarizer

- Iterates over collector output directory / CSV files
- Creates summary file in Parquet format with [key transaction attributes](summarizer/types.go)
- TODO: create archive from output of multiple collectors
  - Take several files/directories as input

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

## TODO

Lots, this is WIP

should:

- collector support multiple `-node` cli args (like [mev-boost](https://github.com/flashbots/mev-boost/blob/ci-fix/cli/main.go#L87))

could:

- stats about which node saw how many tx first
- http server to add/remove nodes, see stats, pprof?

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
