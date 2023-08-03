package collector

import "time"

const (
	datePath = time.DateOnly
	// datePath = "2006-01-02_15"

	// txCacheTime is the amount of time before TxProcessor removes transactions from the "already processed" list
	txCacheTime = time.Minute * 5
)
