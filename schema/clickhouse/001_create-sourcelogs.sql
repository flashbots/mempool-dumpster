CREATE TABLE IF NOT EXISTS sourcelogs (
    receivedAt DateTime64(3),
    hash String,
    source String,
    location String,
)
ENGINE = MergeTree
PRIMARY KEY (receivedAt, hash)
ORDER BY (receivedAt, hash);
