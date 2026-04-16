package collector

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

const (
	redisKeyPrefix    = "mempool-dumpster:"
	redisTTL          = 5 * time.Minute
	redisPingTimeout  = 30 * time.Second
	redisAddTxTimeout = 10 * time.Second
	redisQueueSize    = 4096
	redisNumWorkers   = 4
)

type Redis struct {
	log    *zap.SugaredLogger
	client *redis.Client
	queue  chan string
	wg     sync.WaitGroup
}

func NewRedis(log *zap.SugaredLogger, endpoint string) (*Redis, error) {
	opts, err := redis.ParseURL(endpoint)
	if err != nil {
		return nil, fmt.Errorf("failed to parse redis endpoint: %w", err)
	}

	client := redis.NewClient(opts)

	ctx, cancel := context.WithTimeout(context.Background(), redisPingTimeout)
	defer cancel()

	if err := client.Ping(ctx).Err(); err != nil {
		client.Close()
		return nil, fmt.Errorf("failed to ping redis: %w", err)
	}

	rd := &Redis{
		log:    log,
		client: client,
		queue:  make(chan string, redisQueueSize),
	}

	rd.wg.Add(redisNumWorkers)
	for range redisNumWorkers {
		go rd.worker()
	}

	return rd, nil
}

// AddTx sends a tx hash to the background queue for async Redis write.
// Drops the hash if the queue is full to avoid blocking the caller.
func (r *Redis) AddTx(hash string) {
	select {
	case r.queue <- hash:
	default:
		r.log.Warnw("redis queue full, dropping tx", "tx", hash)
	}
}

func (r *Redis) worker() {
	defer r.wg.Done()
	for hash := range r.queue {
		r.processHash(hash)
	}
}

func (r *Redis) processHash(hash string) {
	ctx, cancel := context.WithTimeout(context.Background(), redisAddTxTimeout)
	defer cancel()
	if err := r.client.Set(ctx, redisKeyPrefix+hash, "1", redisTTL).Err(); err != nil {
		r.log.Errorw("failed to add tx to redis", "error", err, "tx", hash)
	}
}

func (r *Redis) Close() error {
	close(r.queue)
	r.wg.Wait()
	return r.client.Close()
}
