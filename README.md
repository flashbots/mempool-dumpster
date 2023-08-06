# Mempool Archiver

[![Goreport status](https://goreportcard.com/badge/github.com/flashbots/mempool-archiver)](https://goreportcard.com/report/github.com/flashbots/mempool-archiver)
[![Test status](https://github.com/flashbots/mempool-archiver/workflows/Checks/badge.svg)](https://github.com/flashbots/mempool-archiver/actions?query=workflow%3A%22Checks%22)

Save mempool transactions from one or more EL nodes, and archive them in JSON, CSV and Parquet format.

---

**Notes:**

- This is work in progress - some parts work (i.e. collector) and others are under heavy development (i.e. summarizer).
- See also discussion about storage size and considerations here: https://github.com/flashbots/mempool-archiver/issues/1

---

# Getting started

## Mempool Collector

1. Connects to EL nodes (websocket)
2. Listens for new pending transactions
3. Writes a JSON file for each transaction (incl. timestamp in milliseconds, hash, raw tx and various transaction details - see [example here](docs/example-tx-summary.json))
4. TODO: Write to S3

Default JSON filename: `<out_dir>/<date>/transactions/h<hour>/<tx_hash>.json`

**Running the mempool collector:**

```bash
# Connect to ws://localhost:8546 and only print hashes
go run cmd/collector/main.go

# Connect to ws://localhost:8546 and write to JSON files
go run cmd/collector/main.go -out-dir ./out

# Connect to multiple nodes
go run cmd/collector/main.go -nodes ws://server1.com:8546,ws://server2.com:8546
```

Running `go run cmd/collector/main.go -out-dir ./out` will store files like this: `out/2023-08-03/transactions/h14/0xa342b33104151418155d6bcb25d44ee99fa175f5ef3998f5b3e94eeb3ad38503.json`

```json
{
  "timestamp": 1691074457173,
  "hash": "0xa342b33104151418155d6bcb25d44ee99fa175f5ef3998f5b3e94eeb3ad38503",
  "rawTx": "0xb87502f8720102841dcd65008502540be40082520894b2d513b9a54a999912a57b705bcadf7e71ed595c8702a2317dbc220080c001a0a4163068b0963cfe96d4a56bd39f98fda914ad7f7de9b7ee6cd4d52bce14da80a0620c70c21c87250e746d1055b644c39a1dcc033dc4bef2677f8263251e167924",
  "chainId": "1",
  "from": "0x0000000000000000000000000000000000000000",
  "to": "0xb2d513b9a54A999912A57b705bcaDf7e71ed595c",
  "value": "741283400000000",
  "nonce": 2,
  "gas": 21000,
  "gasPrice": "10000000000",
  "gasTipCap": "500000000",
  "gasFeeCap": "10000000000",
  "dataSize": 0,
  "data4Bytes": "0xf7a16963",
  "v": "1",
  "r": "74218511909336679248134793498318422809493748978359085964217109365158694935168",
  "s": "44348639554762280135880091897521071467049468565939127343180930962632954247460"
}
```

## Summarizer

Iterates over an collector output directory, and creates summary file in Parquet / CSV format.

```bash
go run cmd/summarizer/main.go -h
```

---

# Architecture

## General design goals

- Keep it simple and stupid
- Vendor-agnostic (main flow should work on any server, independent of a cloud provider)
- Downtime-resilience to minimize any gaps in the archive
- A collector instance can connect to many EL nodes
- Multiple collector instances can run concurrently without getting into each others way

## Mempool Collector

- `NodeConnection`
    - One for each EL connection
    - New pending transactions are sent to `TxProcessor` via a channel
- `TxProcessor`
    - Check if it already processed that tx
    - Store it in the output directory

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
2. Raw transaction files:
    a. Keep all meta fields in JSON as-is, or strip down to _only_ raw tx? (usually between 50%-80% of the JSON bytes)
    b. Compress the whole transactions folder (should do that)


---

## TODO

1. Write to S3
2. `eth_subscribe` for `newPendingTransactions` which only returns the hash (i.e. [Infura](https://docs.infura.io/networks/ethereum/json-rpc-methods/subscription-methods/eth_subscribe) -> wait a second or two, check if local node hasn't found it, and then request the hash)
3. Parquet writer (in progress)
4. Summarizer service