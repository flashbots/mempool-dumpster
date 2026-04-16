package collector

import (
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestRedis_AddTx(t *testing.T) {
	mr := miniredis.RunT(t)
	log := zap.NewNop().Sugar()

	r, err := NewRedis(log, "redis://"+mr.Addr())
	require.NoError(t, err)

	hash := "0xabc123"
	r.AddTx(hash)

	// Close flushes the queue and waits for workers to finish
	require.NoError(t, r.Close())

	// key exists with correct prefix
	require.True(t, mr.Exists(redisKeyPrefix+hash))

	// value is "1"
	val, err := mr.Get(redisKeyPrefix + hash)
	require.NoError(t, err)
	require.Equal(t, "1", val)

	// TTL is set
	ttl := mr.TTL(redisKeyPrefix + hash)
	require.Equal(t, redisTTL, ttl)
}

func TestRedis_TTLExpiry(t *testing.T) {
	mr := miniredis.RunT(t)
	log := zap.NewNop().Sugar()

	r, err := NewRedis(log, "redis://"+mr.Addr())
	require.NoError(t, err)

	hash := "0xdef456"
	r.AddTx(hash)
	require.NoError(t, r.Close())

	require.True(t, mr.Exists(redisKeyPrefix+hash))

	// fast-forward past TTL
	mr.FastForward(redisTTL + time.Second)
	require.False(t, mr.Exists(redisKeyPrefix+hash))
}

func TestRedis_BadEndpoint(t *testing.T) {
	log := zap.NewNop().Sugar()
	_, err := NewRedis(log, "redis://localhost:1")
	require.Error(t, err)
}
