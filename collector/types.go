package collector

import (
	"time"

	"github.com/ethereum/go-ethereum/core/types"
)

type TxIn struct {
	URI string
	Tx  *types.Transaction
	T   time.Time
}

type TxDetail struct {
	Timestamp int64  `json:"timestamp"`
	Hash      string `json:"hash"`
	RawTx     string `json:"rawTx"`
}
