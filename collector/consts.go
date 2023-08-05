package collector

import (
	"time"
)

const (
	// txCacheTime is the amount of time before TxProcessor removes transactions from the "already processed" list
	txCacheTime = time.Minute * 5
)
