package collector

import (
	"time"

	"github.com/ethereum/go-ethereum/core/types"
)

type TxIn struct {
	T  time.Time
	Tx *types.Transaction
	// URI    string
	URITag string
}

type TxDetail struct {
	Timestamp int64  `json:"timestamp"`
	Hash      string `json:"hash"`
	RawTx     string `json:"rawTx"`
}
