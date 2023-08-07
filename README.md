# Mempool Archiver

[![Goreport status](https://goreportcard.com/badge/github.com/flashbots/mempool-archiver)](https://goreportcard.com/report/github.com/flashbots/mempool-archiver)
[![Test status](https://github.com/flashbots/mempool-archiver/workflows/Checks/badge.svg?branch=main)](https://github.com/flashbots/mempool-archiver/actions?query=workflow%3A%22Checks%22)

Collect mempool transactions from EL nodes, and archive them in [Parquet](https://github.com/apache/parquet-format)/CSV/JSON format.

---

**Notes:**

- This is work in progress and under heavy development.
- Seeing about 80k - 100k unique new mempool transactions per hour, on average ~1.2kb per rawTx (as of 2023-08-07).
- See also: [discussion about storage size and other considerations](https://github.com/flashbots/mempool-archiver/issues/1)

---

# Getting started

## Mempool Collector

1. Connects to one or more EL nodes via websocket
2. Listens for new pending transactions
3. Writes `timestamp` + `hash` + `rawTx` to CSV file (one file per hour [by default](collector/consts.go))

Default filename:

- Schema: `<out_dir>/<date>/transactions/tx-txs-<bucket_start_datetime>.csv`
- Example: `out/2023-08-07/transactions/txs-2023-08-07-10-00.csv`

**Running the mempool collector:**

```bash
# Connect to ws://localhost:8546 and write CSVs into ./out
go run cmd/collector/main.go -out ./out

# Connect to multiple nodes
go run cmd/collector/main.go -out ./out -nodes ws://server1.com:8546,ws://server2.com:8546
```

## Summarizer

(not yet working)

Iterates over an collector output directory, and creates summary file in Parquet / CSV format with extracted transaction data: `Timestamp`, `Hash`, `ChainID`, `From`, `To`, `Value`, `Nonce`, `Gas`, `GasPrice`, `GasTipCap`, `GasFeeCap`, `DataSize`, `Data4Bytes`

```bash
go run cmd/summarizer/main.go -h
```


---

# Architecture

## General design goals

- Keep it simple and stupid
- Vendor-agnostic (main flow should work on any server, independent of a cloud provider)
- Downtime-resilience to minimize any gaps in the archive
- Multiple collector instances can run concurrently without getting into each others way

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

## Open questions

Storage & compression:

1. Summary files (CSV, Parquet)
    a. Store with or without signature (~160b which is often about 50% of an entry)
    b. Compress? (might impact usability as Clickhouse backend or S3 Select)
1. Parquet files: store with fields as strings (like in JSON), or in native data types? (native might be smaller size, but harder to query/parse)


---

## TODO

Lots, this is WIP
