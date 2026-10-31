package redisqueue

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestExchange_DirectFanoutAtomic(t *testing.T) {
	_, _, rdb := setupTestQueue(t)
	ctx := context.Background()

	ex := NewExchange(rdb, "orders").
		Bind("order.created", "billing").
		Bind("order.created", "analytics").
		Bind("", "audit")
	require.NoError(t, ex.Persist(ctx))
	require.NoError(t, ex.Load(ctx))
	assert.NotEqual(t, "0", ex.Version())

	past := time.Now().Add(-time.Second)
	require.NoError(t, ex.Publish(ctx, ExchangeDirect, "order.created",
		&Task{ID: "t1", Partition: "p1", Payload: []byte("x"), Scheduled: past},
	))

	for _, q := range []string{"billing", "analytics"} {
		c := NewConsumer(rdb, q, "")
		got, err := c.Get(ctx)
		require.NoError(t, err)
		require.Len(t, got, 1)
		assert.Equal(t, "t1", got[0].ID)
		_ = c.Ack(ctx, got[0].ID, 0)
		c.Close()
	}

	// fanout → все bound, включая audit
	require.NoError(t, ex.Publish(ctx, ExchangeFanout, "",
		&Task{ID: "t2", Payload: []byte("y"), Scheduled: past},
	))
	for _, q := range []string{"billing", "analytics", "audit"} {
		c := NewConsumer(rdb, q, "")
		got, err := c.Get(ctx)
		require.NoError(t, err)
		require.Len(t, got, 1, "queue %s", q)
		assert.Equal(t, "t2", got[0].ID)
		_ = c.Ack(ctx, got[0].ID, 0)
		c.Close()
	}
}

func TestExchange_TopicAndNoRoute(t *testing.T) {
	_, _, rdb := setupTestQueue(t)
	ctx := context.Background()

	ex := NewExchange(rdb, "topic-ex").
		Bind("order.*.eu", "eu-q")
	require.NoError(t, ex.Persist(ctx))
	_ = ex.Load(ctx)

	past := time.Now().Add(-time.Second)
	err := ex.Publish(ctx, ExchangeTopic, "order.paid.us",
		&Task{ID: "nope", Payload: []byte("x"), Scheduled: past},
	)
	require.ErrorIs(t, err, ErrNoRoute)

	require.NoError(t, ex.Publish(ctx, ExchangeTopic, "order.paid.eu",
		&Task{ID: "ok", Payload: []byte("x"), Scheduled: past},
	))
	c := NewConsumer(rdb, "eu-q", "")
	defer c.Close()
	got, err := c.Get(ctx)
	require.NoError(t, err)
	require.Len(t, got, 1)
}

func TestExchange_CheckVersionReload(t *testing.T) {
	_, _, rdb := setupTestQueue(t)
	ctx := context.Background()

	ex := NewExchange(rdb, "ver-ex").Bind("k", "q1")
	require.NoError(t, ex.Persist(ctx))
	require.NoError(t, ex.Load(ctx))
	ver1 := ex.Version()

	// другой клиент меняет bindings
	ex2 := NewExchange(rdb, "ver-ex").Bind("k", "q2")
	require.NoError(t, ex2.Persist(ctx))

	past := time.Now().Add(-time.Second)
	// без checkVersion — уйдёт в устаревший q1
	require.NoError(t, ex.Publish(ctx, ExchangeDirect, "k",
		&Task{ID: "stale", Payload: []byte("a"), Scheduled: past},
	))
	c1 := NewConsumer(rdb, "q1", "")
	got, err := c1.Get(ctx)
	require.NoError(t, err)
	require.Len(t, got, 1)
	c1.Close()

	// с checkVersion — подтянет q2
	require.Equal(t, ver1, ex.Version()) // ещё старая до publish
	require.NoError(t, ex.PublishWith(ctx, ExchangeDirect, "k",
		[]ExchangePublishOption{WithCheckVersion()},
		&Task{ID: "fresh", Payload: []byte("b"), Scheduled: past},
	))
	assert.NotEqual(t, ver1, ex.Version())
	c2 := NewConsumer(rdb, "q2", "")
	defer c2.Close()
	got, err = c2.Get(ctx)
	require.NoError(t, err)
	require.Len(t, got, 1)
	assert.Equal(t, "fresh", got[0].ID)
}

func TestExchange_BestEffortLocal(t *testing.T) {
	_, _, rdb := setupTestQueue(t)
	ctx := context.Background()

	ex := NewExchange(rdb, "local").Bind("x", "lq1").Bind("x", "lq2")
	past := time.Now().Add(-time.Second)
	require.NoError(t, ex.PublishWith(ctx, ExchangeDirect, "x",
		[]ExchangePublishOption{WithDeliveryBestEffort()},
		&Task{ID: "be1", Payload: []byte("z"), Scheduled: past},
	))
	for _, q := range []string{"lq1", "lq2"} {
		c := NewConsumer(rdb, q, "")
		got, err := c.Get(ctx)
		require.NoError(t, err)
		require.Len(t, got, 1)
		c.Close()
	}
}

func TestExchange_InMemoryAtomicWithoutPersist(t *testing.T) {
	_, _, rdb := setupTestQueue(t)
	ctx := context.Background()
	ex := NewExchange(rdb, "mem").Bind("a", "m1").Bind("a", "m2")
	past := time.Now().Add(-time.Second)
	require.NoError(t, ex.Publish(ctx, ExchangeDirect, "a",
		&Task{ID: "m", Payload: []byte("1"), Scheduled: past},
	))
	for _, q := range []string{"m1", "m2"} {
		c := NewConsumer(rdb, q, "")
		got, err := c.Get(ctx)
		require.NoError(t, err)
		require.Len(t, got, 1)
		c.Close()
	}
}

func TestExchange_LocalBindingsAndOptions(t *testing.T) {
	_, _, rdb := setupTestQueue(t)
	ex := NewExchange(rdb, "local-ex")
	assert.Equal(t, "local-ex", ex.Name())

	ex.Bind("a.*", "q1").Bind("b.*", "q2")
	ex.Unbind("a.*", "q1")
	ex.SetBindings([]Binding{{Pattern: "c.*", Queue: "q3"}})
	bs := ex.Bindings()
	require.Len(t, bs, 1)
	assert.Equal(t, "q3", bs[0].Queue)

	ex.SetCompression(CompressionZstd)
	ex.SetCompressMinBytes(64)

	cfg := &exchangePublishConfig{}
	cfg.apply([]ExchangePublishOption{
		WithDeliveryAtomic(),
		WithDeliveryBestEffort(),
		WithCheckVersion(),
		WithExchangeTagBackpressure("t", 10),
		WithExchangeCompression(CompressionZstd),
		nil,
	})
	opts := cfg.asPublishOptions()
	assert.NotEmpty(t, opts)
}
