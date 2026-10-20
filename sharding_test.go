package redisqueue

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setupShardClients(t *testing.T, dbs ...int) []*redis.Client {
	t.Helper()
	clients := make([]*redis.Client, len(dbs))
	ctx := context.Background()
	for i, db := range dbs {
		c, err := newClient(RedisConfig{Addr: "localhost:6379", DB: db})
		if err != nil {
			t.Skipf("Redis not available: %v", err)
		}
		require.NoError(t, c.FlushDB(ctx).Err())
		clients[i] = c
		t.Cleanup(func() { _ = c.Close() })
	}
	return clients
}

func shardCfgFromClients(strategy ShardStrategy, clients []*redis.Client) *ShardConfig {
	shards := make([]Shard, len(clients))
	for i, c := range clients {
		shards[i] = Shard{Name: fmt.Sprintf("s%d", i), Client: c}
	}
	return &ShardConfig{Strategy: strategy, Shards: shards}
}

func TestShardRouter_StickyByGroup(t *testing.T) {
	clients := setupShardClients(t, 10, 11)
	cfg := shardCfgFromClients(ShardByGroup, clients)
	r, err := NewShardRouter(cfg)
	require.NoError(t, err)
	defer r.Close()

	_, s1, i1, err := r.ResolveWrite("acct:A")
	require.NoError(t, err)
	_, s2, i2, err := r.ResolveWrite("acct:A")
	require.NoError(t, err)
	assert.Equal(t, i1, i2)
	assert.Equal(t, s1.Name, s2.Name)

	seen := map[int]bool{i1: true}
	for _, key := range []string{"acct:B", "acct:C", "acct:D", "acct:E", "acct:F", "acct:G"} {
		_, _, idx, err := r.ResolveWrite(key)
		require.NoError(t, err)
		seen[idx] = true
	}
	assert.True(t, len(seen) >= 1)
}

func TestShardRouter_KeyMapAndReadOnly(t *testing.T) {
	clients := setupShardClients(t, 10, 11)
	cfg := &ShardConfig{
		Strategy: ShardByManual,
		Shards: []Shard{
			{Name: "s0", Client: clients[0], ReadOnly: true},
			{Name: "s1", Client: clients[1]},
		},
		KeyMap: map[string]string{"vip": "s0"},
	}

	r, err := NewShardRouter(cfg)
	require.NoError(t, err)
	defer r.Close()

	_, _, _, err = r.ResolveWrite("vip")
	require.Error(t, err)

	_, s, _, err := r.ResolveAny("vip")
	require.NoError(t, err)
	assert.Equal(t, "s0", s.Name)

	_, s, _, err = r.ResolveWrite("other")
	require.NoError(t, err)
	assert.Equal(t, "s1", s.Name)
}

func TestShardRouter_ByManualRequiresKey(t *testing.T) {
	clients := setupShardClients(t, 10)
	r, err := NewShardRouter(shardCfgFromClients(ShardByManual, clients))
	require.NoError(t, err)
	defer r.Close()

	_, err = r.ShardKeyForTask("q", &Task{ID: "t"}, "")
	require.Error(t, err)

	key, err := r.ShardKeyForTask("q", &Task{ID: "t"}, "tenant:1")
	require.NoError(t, err)
	assert.Equal(t, "tenant:1", key)
}

func TestDistributeWorkers(t *testing.T) {
	assert.Equal(t, []int{4, 4, 4}, DistributeWorkers(12, 3))
	assert.Equal(t, []int{3, 2, 2}, DistributeWorkers(7, 3))
	assert.Equal(t, []int{1, 1, 0}, DistributeWorkers(2, 3))
	assert.Equal(t, []int{1}, DistributeWorkers(0, 1))
}

func TestProducerConsumer_SetShards_ByGroup(t *testing.T) {
	clients := setupShardClients(t, 10, 11)
	cfg := shardCfgFromClients(ShardByGroup, clients)
	queue := "shard-q-group"

	producer := NewProducer(nil, queue)
	require.NoError(t, producer.SetShards(cfg))
	defer producer.Close()

	ctx := context.Background()
	require.NoError(t, producer.Publish(ctx,
		&Task{ID: "t1", Groups: []string{"g1"}, Payload: []byte("a"), Scheduled: time.Now()},
		&Task{ID: "t2", Groups: []string{"g2"}, Payload: []byte("b"), Scheduled: time.Now()},
		&Task{ID: "t3", Groups: []string{"g1"}, Payload: []byte("c"), Scheduled: time.Now()},
	))

	_, _, i1, err := producer.ShardRouter().ResolveWrite("g1")
	require.NoError(t, err)
	assert.Equal(t, int64(1), clients[i1].Exists(ctx, payloadKey(queue, "t1")).Val())
	assert.Equal(t, int64(1), clients[i1].Exists(ctx, payloadKey(queue, "t3")).Val())

	pool := NewConsumerPool(nil, queue)
	require.NoError(t, pool.SetShards(cfg))
	defer pool.Close()
	pool.SetCount(4)
	pool.SetPollInterval(50 * time.Millisecond)

	var got atomic.Int64
	cctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	done := make(chan struct{})
	go func() {
		pool.Consume(cctx, func(task *Task) error {
			got.Add(1)
			if got.Load() >= 3 {
				cancel()
			}
			return nil
		})
		close(done)
	}()
	<-done
	assert.GreaterOrEqual(t, got.Load(), int64(3))
}

func TestProducer_SetShards_ByManualAndPublishTo(t *testing.T) {
	clients := setupShardClients(t, 10, 11)
	cfg := shardCfgFromClients(ShardByManual, clients)
	queue := "shard-q-manual"

	producer := NewProducer(nil, queue)
	require.NoError(t, producer.SetShards(cfg))
	defer producer.Close()

	ctx := context.Background()
	err := producer.Publish(ctx, &Task{ID: "x", Payload: []byte("no-key"), Scheduled: time.Now()})
	require.Error(t, err)

	require.NoError(t, producer.PublishManual(ctx, "part:42",
		&Task{ID: "m1", Partition: "part:42", Payload: []byte("1"), Scheduled: time.Now()},
	))
	require.NoError(t, producer.PublishTo(ctx, "s0",
		&Task{ID: "pin", Payload: []byte("2"), Scheduled: time.Now()},
	))

	assert.Equal(t, int64(1), clients[0].Exists(ctx, payloadKey(queue, "pin")).Val())

	_, _, idx, err := producer.ShardRouter().ResolveWrite("part:42")
	require.NoError(t, err)
	assert.Equal(t, int64(1), clients[idx].Exists(ctx, payloadKey(queue, "m1")).Val())
}

func TestProducer_SetShards_ReadOnlyDrain(t *testing.T) {
	clients := setupShardClients(t, 10, 11)
	cfg := &ShardConfig{
		Strategy: ShardByQueue,
		Shards: []Shard{
			{Name: "s0", Client: clients[0], ReadOnly: true},
			{Name: "s1", Client: clients[1]},
		},
	}
	producer := NewProducer(nil, "drain-q")
	require.NoError(t, producer.SetShards(cfg))
	defer producer.Close()

	ctx := context.Background()
	require.NoError(t, producer.Publish(ctx, &Task{
		ID: "only-s1", Payload: []byte("x"), Scheduled: time.Now(),
	}))
	assert.Equal(t, int64(0), clients[0].Exists(ctx, payloadKey("drain-q", "only-s1")).Val())
	assert.Equal(t, int64(1), clients[1].Exists(ctx, payloadKey("drain-q", "only-s1")).Val())
}

func TestShardByPartition_Strategy(t *testing.T) {
	clients := setupShardClients(t, 10, 11)
	r, err := NewShardRouter(shardCfgFromClients(ShardByPartition, clients))
	require.NoError(t, err)
	defer r.Close()

	key, err := r.ShardKeyForTask("q", &Task{Partition: "!order:1"}, "")
	require.NoError(t, err)
	assert.Equal(t, "!order:1", key)
}
