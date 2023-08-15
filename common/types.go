package common

type TxSummaryEntry struct {
	Timestamp int64  `parquet:"name=timestamp, type=INT64, convertedtype=TIMESTAMP_MILLIS"`
	Hash      string `parquet:"name=hash, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`

	ChainID string `parquet:"name=chainId, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	From    string `parquet:"name=from, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	To      string `parquet:"name=to, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Value   string `parquet:"name=value, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Nonce   uint64 `json:"nonce"`

	Gas       uint64 `json:"gas"`
	GasPrice  string `parquet:"name=gasPrice, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	GasTipCap string `parquet:"name=gasTipCap, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	GasFeeCap string `parquet:"name=gasFeeCap, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`

	DataSize   int64  `parquet:"name=dataSize, type=INT64"`
	Data4Bytes string `parquet:"name=data4Bytes, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
}
