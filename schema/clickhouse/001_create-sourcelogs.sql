CREATE TABLE IF NOT EXISTS sourcelogs (
    received_at DateTime64(3),
    hash String,
    source String,
    location String,
)
ENGINE = MergeTree
PRIMARY KEY (received_at, hash)
ORDER BY (received_at, hash)
PARTITION BY toDate(received_at);
