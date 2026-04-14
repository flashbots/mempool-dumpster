package collector

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

const (
	redisKeyPrefix      = "mempool-dumpster:"
	redisTTL            = 5 * time.Minute
	redisPingTimeout    = 30 * time.Second
	redisAddTxTimeout   = 10 * time.Second
)

type Redis struct {
	log    *zap.SugaredLogger
	client *redis.Client
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

	return &Redis{
		log:    log,
		client: client,
	}, nil
}

func (r *Redis) AddTx(ctx context.Context, hash string) error {
	return r.client.Set(ctx, redisKeyPrefix+hash, "1", redisTTL).Err()
}

func (r *Redis) Close() error {
	return r.client.Close()
}
