package tasks

import (
	"context"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	redisqueue "github.com/Rinsvent/smart-redis-queue"
)

func testRDB(t *testing.T) *redis.Client {
	t.Helper()
	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379", DB: 14})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := rdb.Ping(ctx).Err(); err != nil {
		t.Skipf("redis: %v", err)
	}
	_ = rdb.FlushDB(ctx)
	t.Cleanup(func() { _ = rdb.Close() })
	return rdb
}

func TestRegistryCatalogAndDispatch(t *testing.T) {
	rdb := testRDB(t)
	ctx := context.Background()
	reg, err := New(rdb, Config{Namespace: "svc", DefaultQueue: "cron-q"})
	if err != nil {
		t.Fatal(err)
	}
	called := false
	if err := reg.Register(Def{
		Name: "billing",
		Args: []ArgDef{{Name: "day", Default: "1"}},
		Handle: func(ctx context.Context, inv Invocation) error {
			called = true
			if inv.Args["day"] != "1" {
				t.Fatalf("args=%v", inv.Args)
			}
			return nil
		},
	}); err != nil {
		t.Fatal(err)
	}
	if err := reg.SyncCatalog(ctx); err != nil {
		t.Fatal(err)
	}
	ents, err := CatalogForQueue(ctx, rdb, "svc", "cron-q")
	if err != nil || len(ents) != 1 || ents[0].Name != "billing" {
		t.Fatalf("catalog: %v %#v", err, ents)
	}

	p := redisqueue.NewProducer(rdb, "cron-q")
	reg.SetProducer("cron-q", p)
	id, err := reg.Enqueue(ctx, "billing", RunOptions{})
	if err != nil || id != "billing" {
		t.Fatalf("enqueue: %v %q", err, id)
	}
	id2, err := reg.Enqueue(ctx, "billing", RunOptions{BypassLock: true})
	if err != nil || BaseName(id2) != "billing" || id2 == "billing" {
		t.Fatalf("bypass: %v %q", err, id2)
	}

	task := &redisqueue.Task{ID: "billing", Payload: []byte(`{"day":"1"}`)}
	if err := reg.Dispatch(ctx, task); err != nil || !called {
		t.Fatalf("dispatch: %v called=%v", err, called)
	}
	run, _ := reg.GetRun(ctx, "billing")
	if run["status"] != "ok" {
		t.Fatalf("run=%v", run)
	}
}

func TestBaseName(t *testing.T) {
	if BaseName("import~ab12") != "import" {
		t.Fatal(BaseName("import~ab12"))
	}
	if BaseName("import") != "import" {
		t.Fatal()
	}
}
