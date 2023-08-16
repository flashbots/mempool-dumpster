package collector

import (
	"time"
)

const (
	// txCacheTime is the amount of time before TxProcessor removes transactions from the "already processed" list
	txCacheTime = time.Minute * 30

	// bucketMinutes is the number of minutes to write into each CSV file (i.e. new file created for every X minutes bucket)
	bucketMinutes = 60
)
