package redisqueue

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func stepsTestRedis(t *testing.T) *redis.Client {
	t.Helper()
	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379", DB: 12})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := rdb.Ping(ctx).Err(); err != nil {
		t.Skipf("redis: %v", err)
	}
	t.Cleanup(func() { _ = rdb.Close() })
	return rdb
}

func TestParseEncodeStepValue(t *testing.T) {
	started := time.UnixMilli(1_700_000_000_000)

	cases := []struct {
		raw     string
		name    string
		curr    int
		total   int
		unk     bool
		hasProg bool
		hasTime bool
	}{
		{"import", "import", 0, 0, false, false, false},
		{"import#3/10", "import", 3, 10, false, true, false},
		{"import#3/?", "import", 3, 0, true, true, false},
		{"import#3/10@1700000000000", "import", 3, 10, false, true, true},
		{"import#7/?@1700000000000", "import", 7, 0, true, true, true},
		{"bad#x/y", "bad#x/y", 0, 0, false, false, false},
	}
	for _, tc := range cases {
		n, c, tot, unk, st, hp, ctrl := parseStepValue(tc.raw)
		if n != tc.name || c != tc.curr || tot != tc.total || unk != tc.unk || hp != tc.hasProg {
			t.Fatalf("parse %q: got name=%q %d/%d unk=%v hp=%v", tc.raw, n, c, tot, unk, hp)
		}
		if ctrl != "" {
			t.Fatalf("parse %q unexpected control %q", tc.raw, ctrl)
		}
		if tc.hasTime {
			if st.UnixMilli() != started.UnixMilli() {
				t.Fatalf("parse %q started: got %v", tc.raw, st)
			}
		} else if !st.IsZero() {
			t.Fatalf("parse %q unexpected started %v", tc.raw, st)
		}
	}

	enc := encodeStepValue("import", 3, 10, false, started, "")
	if enc != "import#3/10@1700000000000" {
		t.Fatalf("encode: %q", enc)
	}
	encU := encodeStepValue("import", 3, 0, true, started, "")
	if encU != "import#3/?@1700000000000" {
		t.Fatalf("encode unk: %q", encU)
	}
	encP := encodeStepValue("import", 3, 10, false, started, StepControlPause)
	if encP != "import#3/10@1700000000000!pause" {
		t.Fatalf("encode pause: %q", encP)
	}
	cp := ParseStepCheckpoint(encP)
	if !cp.HasProgress || cp.Control != StepControlPause || cp.Curr != 3 {
		t.Fatalf("ParseStepCheckpoint: %+v", cp)
	}
}

func TestStepsSkipCompleted(t *testing.T) {
	rdb := stepsTestRedis(t)
	ctx := context.Background()
	q, id := "steps-q", "t1"
	_ = rdb.Del(ctx, stepKey(q, id))
	key := stepKey(q, id)
	if err := rdb.Set(ctx, key, "a", 0).Err(); err != nil {
		t.Fatal(err)
	}

	s, err := NewStepsRaw(ctx, rdb, q, id)
	if err != nil {
		t.Fatal(err)
	}
	order := []string{}
	for _, name := range []string{"a", "b", "c"} {
		if err := s.Do(ctx, name, func(ctx context.Context) error {
			order = append(order, name)
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}
	if strings.Join(order, ",") != "b,c" {
		t.Fatalf("order=%v want b,c", order)
	}
	if got, _ := rdb.Get(ctx, key).Result(); got != "c" {
		t.Fatalf("checkpoint=%q", got)
	}
}

func TestStepsProgressResume(t *testing.T) {
	rdb := stepsTestRedis(t)
	ctx := context.Background()
	q, id := "steps-prog", "t1"
	key := stepKey(q, id)
	_ = rdb.Del(ctx, key)

	s1, err := NewStepsRaw(ctx, rdb, q, id)
	if err != nil {
		t.Fatal(err)
	}
	failAfter := 3
	err = s1.Do(ctx, "import", func(ctx context.Context) error {
		bar, err := s1.Progress(ctx, StepProgressConfig{})
		if err != nil {
			return err
		}
		bar.SetTotal(10)
		for i := 0; i < 10; i++ {
			if err := bar.Advance(ctx, 1); err != nil {
				return err
			}
			if bar.Current() >= failAfter {
				return errors.New("boom")
			}
		}
		return nil
	})
	if err == nil || err.Error() != "boom" {
		t.Fatalf("want boom, got %v", err)
	}
	raw, _ := rdb.Get(ctx, key).Result()
	name, curr, total, unk, started, hp, _ := parseStepValue(raw)
	if !hp || name != "import" || curr != 3 || total != 10 || unk {
		t.Fatalf("mid key %q → %s %d/%d", raw, name, curr, total)
	}
	if started.IsZero() {
		t.Fatal("started missing")
	}
	startMS := started.UnixMilli()

	s2, err := NewStepsRaw(ctx, rdb, q, id)
	if err != nil {
		t.Fatal(err)
	}
	c, tot, u, st, ok := s2.LastProgress()
	if !ok || c != 3 || tot != 10 || u || st.UnixMilli() != startMS {
		t.Fatalf("LastProgress: %d/%d unk=%v st=%v ok=%v", c, tot, u, st, ok)
	}

	advanced := 0
	err = s2.Do(ctx, "import", func(ctx context.Context) error {
		bar, err := s2.Progress(ctx, StepProgressConfig{})
		if err != nil {
			return err
		}
		bar.SetTotal(10)
		if bar.Current() != 3 {
			t.Fatalf("resume current=%d", bar.Current())
		}
		if bar.StartedAt().UnixMilli() != startMS {
			t.Fatalf("started not preserved")
		}
		for bar.Current() < 10 {
			if err := bar.Advance(ctx, 1); err != nil {
				return err
			}
			advanced++
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if advanced != 7 {
		t.Fatalf("advanced=%d want 7", advanced)
	}
	if got, _ := rdb.Get(ctx, key).Result(); got != "import" {
		t.Fatalf("after End want bare name, got %q", got)
	}
	// completed — повтор пропускается
	ran := false
	s3, _ := NewStepsRaw(ctx, rdb, q, id)
	_ = s3.Do(ctx, "import", func(ctx context.Context) error {
		ran = true
		return nil
	})
	if ran {
		t.Fatal("completed step should skip")
	}
}

func TestStepsProgressBeginEnd(t *testing.T) {
	rdb := stepsTestRedis(t)
	ctx := context.Background()
	q, id := "steps-be", "t1"
	_ = rdb.Del(ctx, stepKey(q, id))

	s, err := NewStepsRaw(ctx, rdb, q, id)
	if err != nil {
		t.Fatal(err)
	}
	ok, err := s.Begin(ctx, "import")
	if err != nil || !ok {
		t.Fatalf("Begin: ok=%v err=%v", ok, err)
	}
	bar, err := s.Progress(ctx, StepProgressConfig{FlushEvery: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	bar.SetTotal(5)
	_ = bar.Advance(ctx, 1) // flushEvery hour but first Advance always flushes
	raw, _ := rdb.Get(ctx, stepKey(q, id)).Result()
	if !strings.Contains(raw, "#1/5@") {
		t.Fatalf("want progress marker, got %q", raw)
	}
	if err := s.End(ctx); err != nil {
		t.Fatal(err)
	}
	if got, _ := rdb.Get(ctx, stepKey(q, id)).Result(); got != "import" {
		t.Fatalf("End: %q", got)
	}
}

func TestStepsProgressOutsideBegin(t *testing.T) {
	rdb := stepsTestRedis(t)
	ctx := context.Background()
	_ = rdb.Del(ctx, stepKey("q", "id"))
	s, err := NewStepsRaw(ctx, rdb, "q", "id")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Progress(ctx, StepProgressConfig{}); err == nil {
		t.Fatal("expected error")
	}
}

func TestStepsETA(t *testing.T) {
	p := &StepProgress{
		total:   10,
		unknown: false,
		current: 2,
		started: time.Now().Add(-2 * time.Second),
	}
	eta := p.ETA()
	if eta < time.Second || eta > 20*time.Second {
		t.Fatalf("ETA=%v", eta)
	}
}

func TestStepProgressFlushPreservesControl(t *testing.T) {
	rdb := stepsTestRedis(t)
	ctx := context.Background()
	key := stepKey("steps-lua", "t1")
	_ = rdb.Del(ctx, key)

	started := time.UnixMilli(1_700_000_000_000)
	base1 := encodeStepValue("import", 1, 10, false, started, "")
	ctrl, err := stepProgressFlushScript.Run(ctx, rdb, []string{key}, base1, 0).Text()
	if err != nil || ctrl != "" {
		t.Fatalf("flush1: ctrl=%q err=%v", ctrl, err)
	}
	if err := SetStepControl(ctx, rdb, "steps-lua", "t1", StepControlPause); err != nil {
		t.Fatal(err)
	}
	base2 := encodeStepValue("import", 5, 10, false, started, "")
	ctrl, err = stepProgressFlushScript.Run(ctx, rdb, []string{key}, base2, 0).Text()
	if err != nil || ctrl != StepControlPause {
		t.Fatalf("flush2: ctrl=%q err=%v", ctrl, err)
	}
	raw, _ := rdb.Get(ctx, key).Result()
	cp := ParseStepCheckpoint(raw)
	if cp.Curr != 5 || cp.Control != StepControlPause {
		t.Fatalf("got %q %+v", raw, cp)
	}
}
