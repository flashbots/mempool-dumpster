// Package summarizer contains stuff for the summarizer script
package summarizer

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

// func TxDetailToParquet(t collector.TxDetail) TxSummaryEntry {
// 	return TxSummaryEntry{
// 		Timestamp: t.Timestamp,
// 		Hash:      t.Hash,

// 		ChainID: t.ChainID,
// 		From:    t.From,
// 		To:      t.To,
// 		Value:   t.Value,
// 		Nonce:   t.Nonce,

// 		Gas:       t.Gas,
// 		GasPrice:  t.GasPrice,
// 		GasTipCap: t.GasTipCap,
// 		GasFeeCap: t.GasFeeCap,

// 		DataSize:   t.DataSize,
// 		Data4Bytes: t.Data4Bytes,
// 	}
// }

// // CSVHeader is a CSV header for TxDetail
// var CSVHeader []string = []string{
// 	"timestamp",
// 	"hash",

// 	"chainId",
// 	"from",
// 	"to",
// 	"value",
// 	"nonce",

// 	"gas",
// 	"gasPrice",
// 	"gasTipCap",
// 	"gasFeeCap",

// 	"dataSize",
// 	"data4Bytes",
// }

// func TxDetailToCSV(t collector.TxDetail) []string {
// 	ret := []string{
// 		fmt.Sprint(t.Timestamp),
// 		t.Hash,

// 		t.ChainID,
// 		t.From,
// 		t.To,
// 		t.Value,
// 		fmt.Sprint(t.Nonce),

// 		fmt.Sprint(t.Gas),
// 		t.GasPrice,
// 		t.GasTipCap,
// 		t.GasFeeCap,

// 		fmt.Sprint(t.DataSize),
// 		t.Data4Bytes,
// 	}

// 	return ret
// }
