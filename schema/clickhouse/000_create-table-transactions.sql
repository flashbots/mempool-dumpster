CREATE TABLE IF NOT EXISTS transactions (
    hash String,
    chainId String,
    txType Int64,
    from String,
    to String,
    value String,
    nonce String,
    gas String,
    gasPrice String,
    gasTipCap String,
    gasFeeCap String,
    dataSize Int64,
    data4Bytes String,
    rawTx String
)
ENGINE = ReplacingMergeTree
PRIMARY KEY (hash)
ORDER BY (hash);

