CREATE TABLE IF NOT EXISTS transactions (
    received_at DateTime64(3, 'UTC'),
    hash String,
    chain_id String,
    tx_type Int64,
    from String,
    to String,
    value String,
    nonce String,
    gas String,
    gas_price String,
    gas_tip_cap String,
    gas_fee_cap String,
    data_size Int64,
    data_4bytes String,
    raw_tx String,

    ver Int64 MATERIALIZED -toUnixTimestamp(received_at)
)
ENGINE = ReplacingMergeTree(ver)
PRIMARY KEY (hash)
ORDER BY (hash)
PARTITION BY toDate(received_at);
