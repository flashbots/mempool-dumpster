package collector

import (
	"time"

	"github.com/ethereum/go-ethereum/core/types"
)

type TxIn struct {
	T      time.Time
	Tx     *types.Transaction
	Source string
}

type TxDetail struct {
	Timestamp int64  `json:"timestamp"`
	Hash      string `json:"hash"`
	RawTx     string `json:"rawTx"`
}
