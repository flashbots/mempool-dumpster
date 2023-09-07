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

	initialBackoffSec = 5
	maxBackoffSec     = 120
)

var (
	// blxDefaultURL options - via https://docs.bloxroute.com/introduction/cloud-api-ips
	// wss://virginia.eth.blxrbdn.com/ws
	// wss://uk.eth.blxrbdn.com/ws
	// wss://singapore.eth.blxrbdn.com/ws
	// wss://germany.eth.blxrbdn.com/ws
	blxDefaultURL        = common.GetEnv("BLX_URI", "wss://virginia.eth.blxrbdn.com/ws")
	chainboundDefaultURL = common.GetEnv("CHAINBOUND_URI", "beta.fiberapi.io:8080")
)
