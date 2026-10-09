package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"testing"
	"time"

	redisqueue "github.com/Rinsvent/smart-redis-queue"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/urfave/cli/v3"
)

const managerTestDB = 6 // отдельная DB: go test ./... параллелит пакеты с DB 5

func testRedis(t *testing.T) *redis.Client {
	t.Helper()
	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379", DB: managerTestDB})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := rdb.Ping(ctx).Err(); err != nil {
		t.Skipf("Redis not available: %v", err)
	}
	require.NoError(t, rdb.FlushDB(context.Background()).Err())
	t.Cleanup(func() { _ = rdb.Close() })
	return rdb
}

func captureStdout(t *testing.T, fn func()) string {
	t.Helper()
	old := os.Stdout
	r, w, err := os.Pipe()
	require.NoError(t, err)
	os.Stdout = w
	fn()
	_ = w.Close()
	os.Stdout = old
	var buf bytes.Buffer
	_, _ = io.Copy(&buf, r)
	_ = r.Close()
	return buf.String()
}

func rootCmd(args ...string) *cli.Command {
	cmd := &cli.Command{
		Name: "manager",
		Flags: []cli.Flag{
			&cli.StringFlag{Name: "addr", Value: "localhost:6379"},
			&cli.StringFlag{Name: "password", Value: ""},
			&cli.IntFlag{Name: "db", Value: managerTestDB},
		},
		Commands: []*cli.Command{
			{
				Name: "info",
				Flags: []cli.Flag{
					&cli.StringFlag{Name: "queue", Value: ""},
					&cli.StringFlag{Name: "partition", Value: ""},
				},
				Action: runInfo,
			},
			{
				Name: "purge",
				Flags: []cli.Flag{
					&cli.StringFlag{Name: "queue", Required: true},
					&cli.StringFlag{Name: "partition", Value: ""},
				},
				Action: runPurge,
			},
			{
				Name: "retry",
				Flags: []cli.Flag{
					&cli.StringFlag{Name: "queue", Required: true},
				},
				Action: runRetry,
			},
		},
	}
	return cmd
}

func TestRedisClient(t *testing.T) {
	_ = testRedis(t)
	cmd := rootCmd()
	db := fmt.Sprintf("%d", managerTestDB)
	require.NoError(t, cmd.Run(context.Background(), []string{"manager", "--addr", "localhost:6379", "--db", db, "info"}))
}

func TestRunInfo_Empty(t *testing.T) {
	_ = testRedis(t)
	db := fmt.Sprintf("%d", managerTestDB)
	out := captureStdout(t, func() {
		cmd := rootCmd()
		require.NoError(t, cmd.Run(context.Background(), []string{"manager", "--db", db, "info"}))
	})
	assert.Contains(t, out, "Очереди не найдены")
}

func TestRunInfo_WithQueue(t *testing.T) {
	rdb := testRedis(t)
	ctx := context.Background()
	db := fmt.Sprintf("%d", managerTestDB)
	p := redisqueue.NewProducer(rdb, "mgr-info")
	require.NoError(t, p.Publish(ctx, &redisqueue.Task{
		ID: "t1", Partition: "!p1", Payload: []byte("x"), Scheduled: time.Now().Add(-time.Second),
	}))
	c := redisqueue.NewConsumer(rdb, "mgr-info", "c1")
	t.Cleanup(c.Close)
	c.SetPrefetchCount(1)
	got, err := c.Get(ctx)
	require.NoError(t, err)
	require.Len(t, got, 1)

	out := captureStdout(t, func() {
		cmd := rootCmd()
		require.NoError(t, cmd.Run(ctx, []string{"manager", "--db", db, "info", "--queue", "mgr-info"}))
	})
	assert.Contains(t, out, "mgr-info")
	assert.Contains(t, out, "pending=")
	assert.Contains(t, out, "in-progress=")
}

func TestRunPurge_QueueAndPartition(t *testing.T) {
	rdb := testRedis(t)
	ctx := context.Background()
	db := fmt.Sprintf("%d", managerTestDB)
	p := redisqueue.NewProducer(rdb, "mgr-purge")
	require.NoError(t, p.Publish(ctx,
		&redisqueue.Task{ID: "a", Partition: "p1", Payload: []byte("1"), Scheduled: time.Now()},
		&redisqueue.Task{ID: "b", Partition: "p2", Payload: []byte("2"), Scheduled: time.Now()},
	))

	out := captureStdout(t, func() {
		cmd := rootCmd()
		require.NoError(t, cmd.Run(ctx, []string{"manager", "--db", db, "purge", "--queue", "mgr-purge", "--partition", "p1"}))
	})
	assert.Contains(t, out, "Purged partition")

	out = captureStdout(t, func() {
		cmd := rootCmd()
		require.NoError(t, cmd.Run(ctx, []string{"manager", "--db", db, "purge", "--queue", "mgr-purge"}))
	})
	assert.Contains(t, out, "Purged queue")
}

func TestRunRetry(t *testing.T) {
	rdb := testRedis(t)
	ctx := context.Background()
	db := fmt.Sprintf("%d", managerTestDB)
	p := redisqueue.NewProducer(rdb, "mgr-retry")
	c := redisqueue.NewConsumer(rdb, "mgr-retry", "c-retry")
	t.Cleanup(c.Close)
	c.SetPrefetchCount(1)
	require.NoError(t, p.Publish(ctx, &redisqueue.Task{
		ID: "r1", Payload: []byte("x"), Scheduled: time.Now().Add(-time.Second),
	}))
	got, err := c.Get(ctx)
	require.NoError(t, err)
	require.Len(t, got, 1)

	out := captureStdout(t, func() {
		cmd := rootCmd()
		require.NoError(t, cmd.Run(ctx, []string{"manager", "--db", db, "retry", "--queue", "mgr-retry"}))
	})
	assert.Contains(t, out, "Retried 1")
}
