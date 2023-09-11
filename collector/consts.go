package collector

import (
	"time"

	"github.com/flashbots/mempool-dumpster/common"
)

const (
	// txCacheTime is the amount of time before TxProcessor removes transactions from the "already processed" list
	txCacheTime = time.Minute * 30

	// bucketMinutes is the number of minutes to write into each CSV file (i.e. new file created for every X minutes bucket)
	bucketMinutes = 60

	// exponential backoff settings
	initialBackoffSec = 5
	maxBackoffSec     = 120
)

var (
	// Bloxroute URL - should point to local Gateway GRPC port
	blxDefaultURL = common.GetEnv("BLX_URI", "127.0.0.1:1001")

	// Chainbound Fiber URL
	chainboundDefaultURL = common.GetEnv("CHAINBOUND_URI", "beta.fiberapi.io:8080")
)
