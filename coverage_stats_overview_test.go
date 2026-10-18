package redisqueue

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestErrBackpressure_Error(t *testing.T) {
	e := &ErrBackpressure{Tag: "tg", Count: 3, Limit: 2}
	assert.Contains(t, e.Error(), "backpressure")
	assert.Contains(t, e.Error(), "tg")
}

func TestAdmin_QueueOverviewAndStatsBounds(t *testing.T) {
	admin, rdb := setupAdminTest(t)
	defer rdb.Close()
	ctx := context.Background()
	q := "overview-q"
	producer := NewProducer(rdb, q)
	consumer := NewConsumer(rdb, q, "ov-c")
	defer consumer.Close()
	consumer.SetPrefetchCount(1)

	require.NoError(t, producer.Publish(ctx, &Task{
		ID: "o1", Partition: "p1", Groups: []string{"g1"},
		Payload: []byte("x"), Scheduled: time.Now().Add(-time.Second),
	}))
	require.NoError(t, producer.Publish(ctx, &Task{
		ID: "o2", Partition: "p2", Groups: []string{"g1"},
		Payload: []byte("y"), Scheduled: time.Now().Add(-time.Second),
	}))

	got, err := consumer.Get(ctx)
	require.NoError(t, err)
	require.Len(t, got, 1)

	ov, err := admin.QueueOverview(ctx, q)
	require.NoError(t, err)
	assert.Equal(t, q, ov.QueueName)
	assert.GreaterOrEqual(t, ov.PartitionTotal, 1)
	assert.GreaterOrEqual(t, ov.TotalPending, int64(1))
	assert.GreaterOrEqual(t, ov.TotalInProgress, int64(1))
	assert.NotEmpty(t, ov.Groups)

	from, to, err := admin.QueueStatsBounds(ctx, q)
	require.NoError(t, err)
	assert.Equal(t, int64(0), from)
	assert.Equal(t, int64(0), to)

	sec1 := time.Now().Unix() - 10
	sec2 := time.Now().Unix() - 1
	require.NoError(t, rdb.ZAdd(ctx, "queue:"+q+":stats:index",
		redis.Z{Score: float64(sec1), Member: strconv.FormatInt(sec1, 10)},
		redis.Z{Score: float64(sec2), Member: strconv.FormatInt(sec2, 10)},
	).Err())

	from, to, err = admin.QueueStatsBounds(ctx, q)
	require.NoError(t, err)
	assert.Equal(t, sec1, from)
	assert.Equal(t, sec2, to)

	require.NoError(t, consumer.Ack(ctx, got[0].ID, 0))
}

func TestConsumer_StatsRetentionLoopAndReject(t *testing.T) {
	producer, consumer, rdb := setupTestQueue(t)
	ctx := context.Background()
	consumer.SetPrefetchCount(1)
	consumer.SetStatsRetention(time.Hour)
	consumer.SetTagsStats("all")

	require.NoError(t, rdb.SAdd(ctx, "queue:test-queue:tag:all", "seed").Err())
	require.NoError(t, producer.Publish(ctx, &Task{
		ID: "st-1", Payload: []byte("x"), Tags: []string{"all"},
		Scheduled: time.Now().Add(-time.Second),
	}))

	got, err := consumer.Get(ctx)
	require.NoError(t, err)
	require.Len(t, got, 1)
	require.NoError(t, consumer.Reject(ctx, got[0].ID, 0))

	require.NoError(t, producer.Publish(ctx, &Task{
		ID: "st-2", Payload: []byte("y"), Scheduled: time.Now().Add(-time.Second),
	}))
	got, err = consumer.Get(ctx)
	require.NoError(t, err)
	require.Len(t, got, 1)
	require.NoError(t, consumer.Ack(ctx, got[0].ID, 0))

	// дать loop сделать tick/flush (ensureLoop уже запущен SetStatsRetention)
	time.Sleep(1100 * time.Millisecond)
	consumer.Close()

	from, to, err := NewAdmin(rdb).QueueStatsBounds(ctx, "test-queue")
	require.NoError(t, err)
	// после flush индекс может быть непустым
	t.Logf("stats bounds %d..%d", from, to)
}

func TestConsumerPool_SetStatsRetentionAndTags(t *testing.T) {
	_, rdb := setupAdminTest(t)
	defer rdb.Close()
	pool := NewConsumerPool(rdb, "pool-stats-q")
	pool.SetStatsRetention(time.Minute)
	pool.SetTagsStats("t1", "t2")
	assert.True(t, pool.stats.enabled())
	assert.Equal(t, []string{"t1", "t2"}, pool.stats.tagsCopy())
}

func TestStatsSink_AddRejectAndStop(t *testing.T) {
	_, rdb := setupAdminTest(t)
	defer rdb.Close()
	sink := newStatsSink(rdb, "sink-reject")
	sink.setRetention(time.Hour)
	sink.ensureLoop()
	sink.addReject(2)
	sink.addReject(0) // no-op branch
	sink.addGet(0)
	sink.addAck(0)
	time.Sleep(50 * time.Millisecond)
	sink.Stop()
	sink.Stop() // second Stop: loop already stopped
}
