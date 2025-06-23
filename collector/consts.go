package collector

import (
	"time"

	"github.com/flashbots/mempool-dumpster/common"
)

const (
	// txCacheTime is the amount of time before TxProcessor removes transactions from the "already processed" list
	txCacheTime = time.Minute * 30

	// exponential backoff settings
	initialBackoffSec = 5
	maxBackoffSec     = 120
)

var (
	// bucketMinutes is the number of minutes to write into each CSV file (i.e. new file created for every X minutes bucket)
	bucketMinutes = common.GetEnvInt("BUCKET_MINUTES", 60)

	// Bloxroute URL - Websocket URI or Gateway GRPC URI (https://docs.bloxroute.com/introduction/cloud-api-ips)
	blxDefaultURL = common.GetEnv("BLX_URI", "wss://germany.eth.blxrbdn.com/ws")

	// Eden URL - https://docs.edennetwork.io/eden-rpc/speed-rpc
	edenDefaultURL = common.GetEnv("EDEN_URI", "wss://speed-eu-west.edennetwork.io")

	// Chainbound Fiber URL
	chainboundDefaultURL = common.GetEnv("CHAINBOUND_URI", "beta.fiberapi.io:8080")

	// https://healthchecks.io link (optional)
	healthChecksIOURL = common.GetEnv("HEALTHCHECKS_IO_URI", "")

	// https://clickhouse.com/docs/best-practices/selecting-an-insert-strategy
	clickhouseBatchSize    = common.GetEnvInt("CLICKHOUSE_BATCH_SIZE", 1_000)
	clickhouseSaveRetries  = common.GetEnvInt("CLICKHOUSE_SAVE_RETRIES", 5)
	clickhouseApplySQLPath = common.GetEnv("CLICKHOUSE_APPLY_SQL_PATH", "")
)
