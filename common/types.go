package common

import (
	"fmt"
	"strings"
)

// TxSummaryEntry is a struct that represents a single transaction in the summary CSV and Parquet file
// see also https://github.com/xitongsys/parquet-go for more details on parquet tags
type TxSummaryEntry struct {
	Timestamp int64  `parquet:"name=timestamp, type=INT64, convertedtype=TIMESTAMP_MILLIS"`
	Hash      string `parquet:"name=hash, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN, omitstats=true"`

	ChainID string `parquet:"name=chainId, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN"`
	From    string `parquet:"name=from, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN, omitstats=true"`
	To      string `parquet:"name=to, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Value   string `parquet:"name=value, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN, omitstats=true"`
	Nonce   string `parquet:"name=nonce, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN, omitstats=true"`

	Gas       string `parquet:"name=gas, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN, omitstats=true"`
	GasPrice  string `parquet:"name=gasPrice, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN, omitstats=true"`
	GasTipCap string `parquet:"name=gasTipCap, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN, omitstats=true"`
	GasFeeCap string `parquet:"name=gasFeeCap, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN, omitstats=true"`

	DataSize   int64  `parquet:"name=dataSize, type=INT64"`
	Data4Bytes string `parquet:"name=data4Bytes, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`

	RawTx string `parquet:"name=rawTx, type=BYTE_ARRAY, encoding=PLAIN, omitstats=true"`
}

func (t TxSummaryEntry) RawTxHex() string {
	return fmt.Sprintf("0x%x", t.RawTx)
}

func (t TxSummaryEntry) ToCSVRow() []string {
	return []string{
		fmt.Sprint(t.Timestamp),
		t.Hash,
		t.ChainID,
		t.From,
		t.To,
		t.Value,
		fmt.Sprint(t.Nonce),
		fmt.Sprint(t.Gas),
		t.GasPrice,
		t.GasTipCap,
		t.GasFeeCap,
		fmt.Sprint(t.DataSize),
		t.Data4Bytes,
	}
}

var TxSummaryEntryCSVHeader = []string{
	"timestamp_ms",
	"hash",
	"chain_id",
	"from",
	"to",
	"value",
	"nonce",
	"gas",
	"gas_price",
	"gas_tip_cap",
	"gas_fee_cap",
	"data_size",
	"data_4bytes",
}

type BlxRawTxMsg struct { //nolint:musttag
	Params struct {
		Result struct {
			RawTx string
		}
	}
}

type EdenRawTxMsg struct { //nolint:musttag
	Params struct {
		Result struct {
			RLP string
		}
	}
}

type SourceComp struct {
	Source    string
	Reference string
}

func NewSourceComps(args []string) (srcComp []SourceComp) {
	srcComp = make([]SourceComp, 0)

	for _, entries := range args {
		parts := strings.Split(entries, "-")
		if len(parts) != 2 {
			continue
		}
		srcComp = append(srcComp, SourceComp{
			Source:    parts[0],
			Reference: parts[1],
		})
	}

	return
}
