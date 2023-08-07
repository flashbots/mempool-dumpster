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

type TxDetail struct {
	Timestamp int64  `json:"timestamp"`
	Hash      string `json:"hash"`
	RawTx     string `json:"rawTx"`

	// ChainID string `json:"-"` // `json:"chainId"`
	// From    string `json:"-"` // `json:"from"`
	// To      string `json:"-"` // `json:"to"`
	// Value   string `json:"-"` // `json:"value"`
	// Nonce   uint64 `json:"-"` // `json:"nonce"`

	// Gas       uint64 `json:"-"` // `json:"gas"`
	// GasPrice  string `json:"-"` // `json:"gasPrice"`
	// GasTipCap string `json:"-"` // `json:"gasTipCap"`
	// GasFeeCap string `json:"-"` // `json:"gasFeeCap"`

	// DataSize   int64  `json:"-"` // `json:"dataSize"`
	// Data4Bytes string `json:"-"` // `json:"data4Bytes"`
	// // AccessList string `json:"accessList"`
	// // BlobGas       string `json:"blobGas"`
	// // BlobGasFeeCap string `json:"blobGasFeeCap"`
	// // BlobHashes    string `json:"blobHashes"`

	// // Signature
	// V string `json:"-"`
	// R string `json:"-"`
	// S string `json:"-"`
}

// TxDetailCSVHeader is a CSV header for TxDetail
var TxDetailCSVHeader []string = []string{
	"timestamp",
	"hash",
	"rawTx",
}
