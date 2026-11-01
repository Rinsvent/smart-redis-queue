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
	var gotBase, gotRun string
	if err := reg.Register(Def{
		Name: "billing",
		Args: []ArgDef{{Name: "day", Default: "1"}},
		Handle: func(ctx context.Context, inv Invocation) error {
			called = true
			gotBase, gotRun = inv.BaseName, inv.RunID
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
	if n, _ := rdb.SIsMember(ctx, "srq:tasks:namespaces", "svc").Result(); !n {
		t.Fatal("namespaces SET missing svc")
	}

	id, err := reg.Enqueue(ctx, "billing", RunOptions{})
	if err != nil || id != "billing" {
		t.Fatalf("enqueue: %v %q", err, id)
	}
	if _, err := reg.Enqueue(ctx, "billing", RunOptions{}); !isAlreadyExists(err) {
		t.Fatalf("want AlreadyExist, got %v", err)
	}

	id2, err := reg.Enqueue(ctx, "billing", RunOptions{BypassLock: true})
	if err != nil || !strings.HasPrefix(id2, "billing~r") || BaseName(id2) != "billing" {
		t.Fatalf("bypass: %v %q", err, id2)
	}
	id3, err := reg.Enqueue(ctx, "billing", RunOptions{
		Args: map[string]string{"day": "1"}, ByOptions: true,
	})
	if err != nil || !strings.HasPrefix(id3, "billing~o") {
		t.Fatalf("byOptions enqueue: %v %q", err, id3)
	}
	id4, err := BuildTaskID("billing", map[string]string{"day": "1"}, false, true)
	if err != nil || id3 != id4 {
		t.Fatalf("byOptions stable: %v %q %q", err, id3, id4)
	}
	id5, _ := BuildTaskID("billing", map[string]string{"day": "2"}, false, true)
	if id3 == id5 {
		t.Fatal("different args must differ")
	}
	// BypassLock wins over ByOptions
	id6, err := BuildTaskID("billing", map[string]string{"day": "1"}, true, true)
	if err != nil || !strings.HasPrefix(id6, "billing~r") {
		t.Fatalf("bypass ignores byOptions: %v %q", err, id6)
	}

	if err := reg.Dispatch(ctx, &redisqueue.Task{ID: id2, Payload: []byte(`{"day":"1"}`)}); err != nil || !called {
		t.Fatalf("dispatch ~r: %v called=%v", err, called)
	}
	if gotBase != "billing" || gotRun != id2 {
		t.Fatalf("inv base=%q run=%q", gotBase, gotRun)
	}
	run, _ := reg.GetRun(ctx, "billing")
	if run["status"] != "ok" {
		t.Fatalf("run=%v", run)
	}
}

func TestBaseName(t *testing.T) {
	if BaseName("import~rdeadbeef") != "import" {
		t.Fatal(BaseName("import~rdeadbeef"))
	}
	if BaseName("import~oabcd1234") != "import" {
		t.Fatal(BaseName("import~oabcd1234"))
	}
	if BaseName("import") != "import" {
		t.Fatal()
	}
}

func TestCronParseAndCatalog(t *testing.T) {
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

func TestScheduleTickConcurrencyPolicies(t *testing.T) {
	rdb := testRDB(t)
	ctx := context.Background()
	reg, _ := New(rdb, Config{Namespace: "pol", DefaultQueue: "q"})
	_ = reg.Register(Def{
		Name: "forbid-job", Cron: "* * * * *", Concurrency: ConcurrencyForbid,
		Handle: func(ctx context.Context, inv Invocation) error { return nil },
	})
	_ = reg.Register(Def{
		Name: "after-job", Cron: "* * * * *", Concurrency: ConcurrencyAfter,
		Handle: func(ctx context.Context, inv Invocation) error { return nil },
	})
	_ = reg.Register(Def{
		Name: "allow-job", Cron: "* * * * *", Concurrency: ConcurrencyAllow,
		Handle: func(ctx context.Context, inv Invocation) error { return nil },
	})

	now := time.Now()
	nextDue := map[string]time.Time{
		"forbid-job": now.Add(-time.Second),
		"after-job":  now.Add(-time.Second),
		"allow-job":  now.Add(-time.Second),
	}
	// seed in-flight payloads (как во время длинного прогона)
	for _, name := range []string{"forbid-job", "after-job", "allow-job"} {
		if _, err := reg.Enqueue(ctx, name, RunOptions{}); err != nil {
			t.Fatal(err)
		}
	}

	dForbid, _ := reg.def("forbid-job")
	dAfter, _ := reg.def("after-job")
	dAllow, _ := reg.def("allow-job")
	reg.scheduleTick(ctx, dForbid, now, nextDue)
	reg.scheduleTick(ctx, dAfter, now, nextDue)
	reg.scheduleTick(ctx, dAllow, now, nextDue)

	if rdb.HGet(ctx, runKey("pol", "forbid-job"), "pending_rerun").Val() != "" {
		t.Fatal("forbid must not set pending_rerun")
	}
	if rdb.HGet(ctx, runKey("pol", "after-job"), "pending_rerun").Val() != "1" {
		t.Fatal("after must arm pending_rerun")
	}
	// allow: второй payload с ~r
	var allowExtra int
	iter := rdb.Scan(ctx, 0, "queue:q:payload:allow-job~r*", 100).Iterator()
	for iter.Next(ctx) {
		allowExtra++
	}
	if err := iter.Err(); err != nil {
		t.Fatal(err)
	}
	if allowExtra < 1 {
		t.Fatal("allow tick must Publish ~r bypass id")
	}
}

func TestConcurrencyAfterPendingRerunAfterAck(t *testing.T) {
	rdb := testRDB(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	reg, _ := New(rdb, Config{Namespace: "svc3", DefaultQueue: "q"})
	runs := 0
	block := make(chan struct{})
	started := make(chan struct{})
	_ = reg.Register(Def{
		Name: "job", Cron: "0 * * * *", Concurrency: ConcurrencyAfter,
		Handle: func(ctx context.Context, inv Invocation) error {
			runs++
			if runs == 1 {
				close(started)
				<-block // держим in-flight, пока «тик» ставит pending_rerun
			}
			return nil
		},
	})
	if _, err := reg.Enqueue(ctx, "job", RunOptions{}); err != nil {
		t.Fatal(err)
	}

	go func() { _ = reg.Consume(ctx, ConsumeOptions{Workers: 1}) }()

	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("handler did not start")
	}
	// имитация cron AlreadyExists → pending_rerun
	_ = rdb.HSet(ctx, runKey("svc3", "job"), "pending_rerun", "1").Err()
	d, _ := reg.def("job")
	// flush во время in-flight: NX fail → флаг остаётся
	if err := reg.flushPendingRerun(ctx, d); err != nil {
		t.Fatal(err)
	}
	if rdb.HGet(ctx, runKey("svc3", "job"), "pending_rerun").Val() != "1" {
		t.Fatal("flag must stay while payload NX-blocks Enqueue")
	}

	close(block) // handler finishes → Ack → AfterAck flush

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if runs >= 2 {
			cancel()
			return
		}
		time.Sleep(30 * time.Millisecond)
	}
	t.Fatalf("want 2 runs after Ack+pending_rerun, got %d; flag=%q",
		runs, rdb.HGet(ctx, runKey("svc3", "job"), "pending_rerun").Val())
}

func TestRegistryConsume(t *testing.T) {
	rdb := testRDB(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	reg, err := New(rdb, Config{Namespace: "svc-c", DefaultQueue: "q-c"})
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{}, 1)
	if err := reg.Register(Def{
		Name: "job",
		Handle: func(ctx context.Context, inv Invocation) error {
			done <- struct{}{}
			return nil
		},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := reg.Enqueue(ctx, "job", RunOptions{}); err != nil {
		t.Fatal(err)
	}
	go func() {
		_ = reg.Consume(ctx, ConsumeOptions{Workers: 1})
	}()
	select {
	case <-done:
		cancel()
	case <-time.After(5 * time.Second):
		t.Fatal("Consume did not Dispatch in time")
	}
}

func TestFlushPendingRerunIgnoresNonAfter(t *testing.T) {
	rdb := testRDB(t)
	ctx := context.Background()
	reg, _ := New(rdb, Config{Namespace: "x", DefaultQueue: "q"})
	_ = reg.Register(Def{
		Name: "job", Concurrency: ConcurrencyForbid,
		Handle: func(ctx context.Context, inv Invocation) error { return nil },
	})
	_ = rdb.HSet(ctx, runKey("x", "job"), "pending_rerun", "1").Err()
	d, _ := reg.def("job")
	if err := reg.flushPendingRerun(ctx, d); err != nil {
		t.Fatal(err)
	}
	if rdb.Exists(ctx, "queue:q:payload:job").Val() != 0 {
		t.Fatal("forbid must not flush-enqueue")
	}
}
