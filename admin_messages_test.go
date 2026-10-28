package redisqueue

import (
	"context"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func messagesTestRedis(t *testing.T) *redis.Client {
	t.Helper()
	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379", DB: 13})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := rdb.Ping(ctx).Err(); err != nil {
		t.Skipf("redis: %v", err)
	}
	t.Cleanup(func() { _ = rdb.Close() })
	return rdb
}

func TestAdminListMessages(t *testing.T) {
	rdb := messagesTestRedis(t)
	ctx := context.Background()
	q := "msg-q"
	prefix := "queue:" + q + ":"
	_ = rdb.FlushDB(ctx)

	_ = rdb.Set(ctx, prefix+"payload:billing", "{}", 0)
	_ = rdb.Set(ctx, prefix+"partition:billing", "default", 0)
	_ = rdb.Set(ctx, prefix+"step:billing", "import#3/10@1700000000000", 0)
	_ = rdb.Set(ctx, prefix+"reject_count:billing", "2", 0)
	_ = rdb.SAdd(ctx, prefix+"tag:cron", "billing")

	admin := NewAdmin(rdb)

	byID, err := admin.ListMessages(ctx, q, ListMessagesOptions{Query: "billing"})
	if err != nil {
		t.Fatal(err)
	}
	if len(byID) != 1 || byID[0].RejectCount != 2 || byID[0].Progress == nil || byID[0].Progress.Curr != 3 {
		t.Fatalf("byID: %+v", byID)
	}

	byTag, err := admin.ListMessages(ctx, q, ListMessagesOptions{Tag: "cron"})
	if err != nil || len(byTag) != 1 {
		t.Fatalf("byTag: %v %#v", err, byTag)
	}

	scan, err := admin.ListMessages(ctx, q, ListMessagesOptions{})
	if err != nil || len(scan) < 1 {
		t.Fatalf("scan: %v %#v", err, scan)
	}

	ti, err := admin.InspectTask(ctx, q, "billing")
	if err != nil || !ti.Exists || ti.Progress == nil {
		t.Fatalf("inspect: %v %+v", err, ti)
	}
}
