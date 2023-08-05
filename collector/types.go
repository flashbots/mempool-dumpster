package collector

import (
	"fmt"
	"time"

	"github.com/ethereum/go-ethereum/core/types"
)

type TxIn struct {
	uri string
	tx  *types.Transaction
	t   time.Time
}

type TxSummaryJSON struct {
	Timestamp int64  `json:"timestamp"`
	Hash      string `json:"hash"`
	RawTx     string `json:"rawTx"`

	ChainID string `json:"chainId"`
	From    string `json:"from"`
	To      string `json:"to"`
	Value   string `json:"value"`
	Nonce   uint64 `json:"nonce"`

	Gas       uint64 `json:"gas"`
	GasPrice  string `json:"gasPrice"`
	GasTipCap string `json:"gasTipCap"`
	GasFeeCap string `json:"gasFeeCap"`

	DataSize   int64  `json:"dataSize"`
	Data4Bytes string `json:"data4Bytes"`
	// AccessList string `json:"accessList"`
	// BlobGas       string `json:"blobGas"`
	// BlobGasFeeCap string `json:"blobGasFeeCap"`
	// BlobHashes    string `json:"blobHashes"`

	// Signature
	V string `json:"v"`
	R string `json:"r"`
	S string `json:"s"`
}

// TxSummaryCSVHeader is a CSV header for TxSummaryJSON
var TxSummaryCSVHeader []string = []string{
	"timestamp",
	"hash",
	// "rawTx",

	"chainId",
	"from",
	"to",
	"value",
	"nonce",

	"gas",
	"gasPrice",
	"gasTipCap",
	"gasFeeCap",

	"dataSize",
	"data4Bytes",

	"v",
	"r",
	"s",
}

func (t TxSummaryJSON) ToCSV(withSignature bool) []string {
	ret := []string{
		fmt.Sprint(t.Timestamp),
		t.Hash,
		// t.RawTx,

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

	if withSignature {
		ret = append(ret, t.V, t.R, t.S)
	}

	return ret
}
