package collector

import (
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
