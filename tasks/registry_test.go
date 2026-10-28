package tasks

import (
	"context"
	"strings"
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
	id3, err := BuildTaskID("billing", map[string]string{"day": "1"}, false, true)
	id4, err2 := BuildTaskID("billing", map[string]string{"day": "1"}, false, true)
	id5, _ := BuildTaskID("billing", map[string]string{"day": "2"}, false, true)
	if err != nil || err2 != nil || id3 != id4 || !strings.HasPrefix(id3, "billing~o") || id3 == id5 {
		t.Fatalf("byOptions: %v %v %q %q %q", err, err2, id3, id4, id5)
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

func TestCronParseAndTwoDefs(t *testing.T) {
	next, err := NextRunAfter("*/5 * * * *", time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC))
	if err != nil || next.Minute()%5 != 0 {
		t.Fatalf("next=%v err=%v", next, err)
	}

	rdb := testRDB(t)
	ctx := context.Background()
	reg, _ := New(rdb, Config{Namespace: "svc2", DefaultQueue: "q"})
	h := func(ctx context.Context, inv Invocation) error { return nil }
	if err := reg.Register(Def{
		Name: "full", Cron: "0 3 * * *", Concurrency: ConcurrencyForbid,
		Args: []ArgDef{{Name: "mode", Default: "full"}}, Handle: h,
	}); err != nil {
		t.Fatal(err)
	}
	if err := reg.Register(Def{
		Name: "delta", Cron: "*/15 * * * *", Concurrency: ConcurrencyAfter,
		Args: []ArgDef{{Name: "mode", Default: "delta"}}, Handle: h,
	}); err != nil {
		t.Fatal(err)
	}
	if err := reg.SyncCatalog(ctx); err != nil {
		t.Fatal(err)
	}
	ents, _ := CatalogForQueue(ctx, rdb, "svc2", "q")
	if len(ents) != 2 {
		t.Fatalf("ents=%d", len(ents))
	}
	var sawAfter bool
	for _, e := range ents {
		if e.Concurrency == ConcurrencyAfter {
			sawAfter = true
		}
		if e.Cron == "" {
			t.Fatalf("missing cron: %+v", e)
		}
	}
	if !sawAfter {
		t.Fatalf("catalog: %+v", ents)
	}
}

func TestConcurrencyAfterPendingRerunImmediate(t *testing.T) {
	rdb := testRDB(t)
	ctx := context.Background()
	reg, _ := New(rdb, Config{Namespace: "svc3", DefaultQueue: "q"})
	reg.SetProducer("q", redisqueue.NewProducer(rdb, "q"))
	_ = reg.Register(Def{
		Name: "job", Cron: "0 * * * *", Concurrency: ConcurrencyAfter,
		Handle: func(ctx context.Context, inv Invocation) error { return nil },
	})
	// как после пропущенного cron-тика
	_ = rdb.HSet(ctx, runKey("svc3", "job"), "pending_rerun", "1").Err()
	d, _ := reg.def("job")
	if err := reg.flushPendingRerun(ctx, d); err != nil {
		t.Fatal(err)
	}
	if rdb.Exists(ctx, "queue:q:payload:job").Val() != 1 {
		t.Fatal("expected immediate re-enqueue after finish")
	}
	if rdb.HGet(ctx, runKey("svc3", "job"), "pending_rerun").Val() != "" {
		t.Fatal("pending_rerun should be cleared")
	}
}
