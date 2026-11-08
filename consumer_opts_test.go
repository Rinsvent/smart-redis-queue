package redisqueue

import (
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewConsumer_Options(t *testing.T) {
	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379", DB: 7})
	require.NoError(t, rdb.Ping(t.Context()).Err())
	t.Cleanup(func() { _ = rdb.Close() })

	c := NewConsumer(rdb, "opts-q", "c-opts",
		WithPing(false),
		WithPrefetchCount(10),
		WithPollInterval(200*time.Millisecond),
		WithSamePartitionBatch(true),
		WithDLQMaxSize(50),
	)
	t.Cleanup(c.Close)

	assert.False(t, c.enablePing)
	assert.Equal(t, 10, c.prefetchCount)
	assert.Equal(t, 200*time.Millisecond, c.pollInterval)
	assert.True(t, c.samePartitionBatch)
	assert.Equal(t, 50, c.dlqMaxSize)
}
