package gateway

import (
	"context"
	"fmt"
	"testing"
	"time"

	redisqueue "github.com/Rinsvent/smart-redis-queue"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// stopGatewayHealth — отключает probe/ping, чтобы тест сам контролировал recover
// (без гонки с probeRedis → recover).
func stopGatewayHealth(g *Gateway) {
	g.mu.Lock()
	started := g.healthStarted
	if started {
		select {
		case <-g.stopHealth:
		default:
			close(g.stopHealth)
		}
		g.healthStarted = false
	}
	g.mu.Unlock()
	if started {
		g.healthWG.Wait()
	}
	g.mu.Lock()
	g.stopHealth = make(chan struct{})
	g.mu.Unlock()
}

func newLossTestGateway(t *testing.T, rdb *redis.Client) *Gateway {
	t.Helper()
	ctx := context.Background()
	gw, err := New(Config{
		Redis:              rdb,
		SkipPersistCheck:   true,
		LossWindowOverride: time.Second,
	})
	require.NoError(t, err)
	require.NoError(t, gw.Boot(ctx))
	stopGatewayHealth(gw)
	t.Cleanup(func() { gw.Close() })
	return gw
}

func publishReady(t *testing.T, gw *Gateway, q string, ids ...string) {
	t.Helper()
	ctx := context.Background()
	tasks := make([]*redisqueue.Task, 0, len(ids))
	for _, id := range ids {
		tasks = append(tasks, &redisqueue.Task{
			ID:        id,
			Partition: "p1",
			Payload:   []byte("body:" + id),
			Scheduled: time.Now().Add(-time.Second),
		})
	}
	require.NoError(t, gw.PublishTasks(ctx, q, tasks, nil))
}

// drainOnce — Get+Ack до пусто; возвращает счётчик доставок по id (для проверки дублей).
func drainOnce(t *testing.T, gw *Gateway, q, consumer string) map[string]int {
	t.Helper()
	ctx := context.Background()
	got := map[string]int{}
	empty := 0
	for empty < 5 {
		tasks, err := gw.GetTasks(ctx, q, consumer, 10)
		require.NoError(t, err)
		if len(tasks) == 0 {
			empty++
			time.Sleep(5 * time.Millisecond)
			continue
		}
		empty = 0
		for _, task := range tasks {
			got[task.ID]++
			require.NoError(t, gw.AckTask(ctx, q, consumer, task.ID, nil))
		}
	}
	return got
}

func assertDeliveredOnce(t *testing.T, got map[string]int, wantIDs ...string) {
	t.Helper()
	require.Len(t, got, len(wantIDs), "got=%v want=%v", got, wantIDs)
	for _, id := range wantIDs {
		assert.Equal(t, 1, got[id], "id %s delivery count", id)
	}
}

func opKinds(g *Gateway) (pub, ack, rej int, pending int) {
	for _, op := range g.ops.snapshot() {
		switch op.Kind {
		case OpPublish:
			pub++
		case OpAck:
			ack++
		case OpReject:
			rej++
		}
		if !op.RedisOK {
			pending++
		}
	}
	return
}

func simulateAOFLoss(t *testing.T, rdb *redis.Client) {
	t.Helper()
	require.NoError(t, rdb.FlushDB(context.Background()).Err())
}

// TestGateway_Integration_LossWindowRecover — полноценный сценарий:
// publish/get/ack/reject → имитация потери AOF (FlushDB) → recover →
// проверка op-log, восстановления и отсутствия дублей.
func TestGateway_Integration_LossWindowRecover(t *testing.T) {
	rdb := gatewayRedis(t)
	ctx := context.Background()
	q := fmt.Sprintf("gw-loss-%d", time.Now().UnixNano())

	t.Run("re_publish_unsynced_after_flush", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		gw := newLossTestGateway(t, rdb)

		ids := []string{"u1", "u2", "u3"}
		publishReady(t, gw, q+"-unsync", ids...)
		assert.Equal(t, 3, gw.loss.len())
		pub, _, _, pending := opKinds(gw)
		assert.Equal(t, 3, pub)
		assert.Equal(t, 0, pending)

		// Redis «не успел» засинкать — данные пропали, gateway RAM цела.
		simulateAOFLoss(t, rdb)
		require.NoError(t, gw.recover(ctx))
		assert.Equal(t, StateRunning, gw.State())

		got := drainOnce(t, gw, q+"-unsync", "c-unsync")
		assertDeliveredOnce(t, got, ids...)
	})

	t.Run("acked_not_resurrected", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		gw := newLossTestGateway(t, rdb)
		qq := q + "-ack"

		publishReady(t, gw, qq, "ack-me", "keep-me")
		tasks, err := gw.GetTasks(ctx, qq, "c-ack", 10)
		require.NoError(t, err)
		require.Len(t, tasks, 2)
		require.NoError(t, gw.AckTask(ctx, qq, "c-ack", "ack-me", nil))

		assert.Equal(t, 1, gw.loss.len(), "only keep-me stays in loss after Ack")
		_, ackN, _, _ := opKinds(gw)
		assert.GreaterOrEqual(t, ackN, 1)

		simulateAOFLoss(t, rdb)
		require.NoError(t, gw.recover(ctx))

		got := drainOnce(t, gw, qq, "c-ack2")
		assert.Equal(t, 0, got["ack-me"], "Acked task must not resurrect")
		assert.Equal(t, 1, got["keep-me"])
		assert.Len(t, got, 1)
	})

	t.Run("reject_keeps_loss_and_restores", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		gw := newLossTestGateway(t, rdb)
		qq := q + "-rej"

		publishReady(t, gw, qq, "rej-1")
		tasks, err := gw.GetTasks(ctx, qq, "c-rej", 1)
		require.NoError(t, err)
		require.Len(t, tasks, 1)
		require.NoError(t, gw.RejectTask(ctx, qq, "c-rej", "rej-1", &RejectRec{Delay: 0, DeadLetterMax: -1}))

		assert.Equal(t, 1, gw.loss.len(), "Reject must keep publish in loss-window")
		_, _, rejN, _ := opKinds(gw)
		assert.GreaterOrEqual(t, rejN, 1)

		simulateAOFLoss(t, rdb)
		require.NoError(t, gw.recover(ctx))

		got := drainOnce(t, gw, qq, "c-rej2")
		assertDeliveredOnce(t, got, "rej-1")
	})

	t.Run("no_duplicates_when_data_intact", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		gw := newLossTestGateway(t, rdb)
		qq := q + "-intact"

		ids := []string{"i1", "i2"}
		publishReady(t, gw, qq, ids...)
		require.NoError(t, gw.recover(ctx)) // re-publish → AlreadyExist
		require.NoError(t, gw.recover(ctx)) // второй раз тоже идемпотентно

		got := drainOnce(t, gw, qq, "c-intact")
		assertDeliveredOnce(t, got, ids...)

		// повторный drain — пусто (не копятся дубли в ready)
		got2 := drainOnce(t, gw, qq, "c-intact2")
		assert.Empty(t, got2)
	})

	t.Run("redis_down_buffer_ack_reject_then_loss", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		gw := newLossTestGateway(t, rdb)
		qq := q + "-buf"

		publishReady(t, gw, qq, "buf-ack", "buf-rej", "buf-open")
		tasks, err := gw.GetTasks(ctx, qq, "c-buf", 10)
		require.NoError(t, err)
		require.Len(t, tasks, 3)
		byID := map[string]*redisqueue.Task{}
		for _, task := range tasks {
			byID[task.ID] = task
		}

		// Redis «упал» для gateway: terminal ops буферизуются.
		gw.state.set(StateRedisDown)
		require.ErrorIs(t, gw.PublishTasks(ctx, qq, []*redisqueue.Task{{ID: "x", Payload: []byte("1")}}, nil), ErrRedisDown)
		require.NoError(t, gw.AckTask(ctx, qq, "c-buf", "buf-ack", nil))
		require.NoError(t, gw.RejectTask(ctx, qq, "c-buf", "buf-rej", &RejectRec{DeadLetterMax: -1}))

		_, _, _, pending := opKinds(gw)
		assert.Equal(t, 2, pending, "Ack+Reject buffered as RedisOK=false")
		// Ack drops buf-ack; Reject keeps buf-rej; buf-open never terminal → 2 in loss.
		assert.Equal(t, 2, gw.loss.len())
		lossIDs := map[string]bool{}
		for _, e := range gw.loss.snapshot() {
			lossIDs[e.Rec.ID] = true
		}
		assert.False(t, lossIDs["buf-ack"])
		assert.True(t, lossIDs["buf-rej"])
		assert.True(t, lossIDs["buf-open"])

		simulateAOFLoss(t, rdb)
		require.NoError(t, gw.recover(ctx))
		assert.Equal(t, StateRunning, gw.State())
		_, _, _, pending = opKinds(gw)
		assert.Equal(t, 0, pending, "buffered ops marked RedisOK after replay")

		got := drainOnce(t, gw, qq, "c-buf2")
		assert.Equal(t, 0, got["buf-ack"], "buffered Ack + loss drop → no resurrect")
		assert.Equal(t, 1, got["buf-rej"], "Reject kept in loss → restored after AOF loss")
		assert.Equal(t, 1, got["buf-open"], "untouched publish restored")
		assert.Len(t, got, 2)
		_ = byID
	})

	t.Run("flush_mid_inflight_get_then_recover", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		gw := newLossTestGateway(t, rdb)
		qq := q + "-inflight"

		publishReady(t, gw, qq, "in-1", "in-2")
		tasks, err := gw.GetTasks(ctx, qq, "c-in", 1)
		require.NoError(t, err)
		require.Len(t, tasks, 1)
		held := tasks[0].ID

		simulateAOFLoss(t, rdb)
		require.NoError(t, gw.recover(ctx))

		// оба снова в ready (в т.ч. held — был в processing, Redis стёрт)
		got := drainOnce(t, gw, qq, "c-in2")
		assertDeliveredOnce(t, got, "in-1", "in-2")
		assert.Contains(t, got, held)
	})
}
