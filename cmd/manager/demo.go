package main

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	redisqueue "github.com/Rinsvent/smart-redis-queue"
	"github.com/redis/go-redis/v9"
	"github.com/urfave/cli/v3"
)

func demoCommands() []*cli.Command {
	return []*cli.Command{
		{
			Name:  "seed",
			Usage: "Наполнить несколько Redis DB демо-очередями (группы, теги, ordered, blocked, in-flight)",
			Flags: []cli.Flag{
				&cli.StringFlag{
					Name:  "dbs",
					Value: "0,1,2",
					Usage: "список DB через запятую",
				},
				&cli.BoolFlag{
					Name:  "purge",
					Value: true,
					Usage: "перед seed очистить демо-очереди на этих DB",
				},
			},
			Action: runSeed,
		},
		{
			Name:  "publish",
			Usage: "Публиковать быстрее consumer’ов; потолок глубины — backpressure (~10k ± jitter)",
			Flags: []cli.Flag{
				&cli.StringFlag{Name: "queue", Aliases: []string{"q"}, Value: "demo-mail", Usage: "имя очереди"},
				&cli.IntFlag{Name: "count", Aliases: []string{"n"}, Value: 0, Usage: "сколько задач (0 = бесконечно до Ctrl+C)"},
				&cli.DurationFlag{Name: "interval", Value: 0, Usage: "пауза между батчами (0 = крутить без паузы)"},
				&cli.StringFlag{Name: "groups", Value: "acct:0,acct:1,acct:2,acct:3,acct:4,acct:5,acct:6,acct:7", Usage: "группы/аккаунты через запятую"},
				&cli.StringFlag{Name: "tags", Value: "all,demo,campaign:live", Usage: "теги (всегда +all; первый — ключ backpressure)"},
				&cli.StringFlag{Name: "partition", Value: "", Usage: "фиксированная партиция; пусто = !{group}:res:{i}"},
				&cli.BoolFlag{Name: "ordered", Value: true, Usage: "партиции с префиксом ! (если --partition пуст)"},
				&cli.IntFlag{Name: "partitions", Value: 2000, Usage: "число логических партиций (ротация res внутри групп)"},
				&cli.IntFlag{Name: "priority", Value: 0, Usage: "priority задач"},
				&cli.IntFlag{Name: "batch", Value: 100, Usage: "размер Publish batch"},
				&cli.IntFlag{Name: "backpressure", Value: 10000, Usage: "лимит глубины по тегу (0 = выкл); к нему добавляется jitter"},
				&cli.IntFlag{Name: "backpressure-jitter", Value: 500, Usage: "случайный ± к --backpressure на каждый Publish"},
				&cli.DurationFlag{Name: "backpressure-wait", Value: 50 * time.Millisecond, Usage: "пауза при ErrBackpressure перед ретраем"},
			},
			Action: runPublish,
		},
		{
			Name:  "consume",
			Usage: "Пул воркеров: sleep 20ms–2s (95% ≤100ms), BlockGroups при stub-429, stats для UI",
			Flags: []cli.Flag{
				&cli.StringFlag{Name: "queue", Aliases: []string{"q"}, Value: "demo-mail", Usage: "имя очереди"},
				&cli.IntFlag{Name: "workers", Aliases: []string{"w"}, Value: 8, Usage: "размер ConsumerPool"},
				&cli.Float64Flag{Name: "fail-rate", Value: 0.02, Usage: "доля stub-429 → RejectWithDelay+BlockGroups (0..1)"},
				&cli.DurationFlag{Name: "block-for", Value: 800 * time.Millisecond, Usage: "на сколько парковать группу при stub-429"},
				&cli.IntFlag{Name: "group-rate", Value: 40, Usage: "Get rate limit на группу (token/s, 0 = выкл)"},
				&cli.DurationFlag{Name: "stats", Value: 5 * time.Minute, Usage: "retention метрик (0 = выкл)"},
				&cli.StringFlag{Name: "stats-tags", Value: "all,demo,campaign:live", Usage: "теги для SCARD-сэмпла"},
				&cli.IntFlag{Name: "prefetch", Value: 5, Usage: "prefetch на воркер"},
				&cli.DurationFlag{Name: "poll", Value: 200 * time.Millisecond, Usage: "poll interval"},
				&cli.DurationFlag{Name: "for", Value: 0, Usage: "сколько работать (0 = до Ctrl+C)"},
			},
			Action: runConsume,
		},
	}
}

func parseDBList(s string) ([]int, error) {
	parts := strings.Split(s, ",")
	out := make([]int, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		n, err := strconv.Atoi(p)
		if err != nil {
			return nil, fmt.Errorf("bad db %q: %w", p, err)
		}
		out = append(out, n)
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("empty --dbs")
	}
	return out, nil
}

func splitCSV(s string) []string {
	if s == "" {
		return nil
	}
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}

// ensureAllTag — тег all первым (удобно для CountByTag / backpressure / stats).
func ensureAllTag(tags []string) []string {
	out := make([]string, 0, len(tags)+1)
	seen := map[string]struct{}{}
	add := func(t string) {
		if t == "" {
			return
		}
		if _, ok := seen[t]; ok {
			return
		}
		seen[t] = struct{}{}
		out = append(out, t)
	}
	add("all")
	for _, t := range tags {
		add(t)
	}
	return out
}

func clientOnDB(c *cli.Command, db int) *redis.Client {
	root := c.Root()
	return redis.NewClient(&redis.Options{
		Addr:     root.String("addr"),
		Password: root.String("password"),
		DB:       db,
	})
}

func runSeed(ctx context.Context, c *cli.Command) error {
	dbs, err := parseDBList(c.String("dbs"))
	if err != nil {
		return err
	}
	purge := c.Bool("purge")
	now := time.Now().Add(-time.Second)

	type seedSpec struct {
		queue string
		build func(db int, producer *redisqueue.Producer, admin *redisqueue.Admin, rdb *redis.Client) error
	}

	specs := []seedSpec{
		{
			queue: "demo-mail",
			build: func(db int, producer *redisqueue.Producer, admin *redisqueue.Admin, rdb *redis.Client) error {
				tasks := []*redisqueue.Task{}
				for i := 0; i < 12; i++ {
					g := []string{"smtp:1"}
					if i%3 == 0 {
						g = []string{"smtp:2"}
					}
					if i%5 == 0 {
						g = []string{"smtp:1", "smtp:2"}
					}
					tasks = append(tasks, &redisqueue.Task{
						ID:        fmt.Sprintf("mail-db%d-%d", db, i),
						Partition: fmt.Sprintf("!user:%d", i%5),
						Priority:  i % 3,
						Payload:   []byte(fmt.Sprintf(`{"db":%d,"kind":"mail","i":%d,"text":"hello-mail"}`, db, i)),
						Scheduled: now,
						Groups:    g,
						Tags:      []string{"all", "campaign:spring", "channel:email", fmt.Sprintf("batch:%d", i/4)},
					})
				}
				tasks = append(tasks,
					&redisqueue.Task{
						ID: fmt.Sprintf("mail-plain-db%d", db), Partition: "inbox",
						Payload: []byte(`{"kind":"plain"}`), Scheduled: now, Groups: []string{"smtp:1"},
						Tags: []string{"all", "campaign:spring", "plain"},
					},
					&redisqueue.Task{
						ID: fmt.Sprintf("mail-later-db%d", db), Partition: "!user:99",
						Payload: []byte(`{"kind":"delayed"}`), Scheduled: time.Now().Add(30 * time.Second),
						Groups: []string{"smtp:2"}, Tags: []string{"all", "campaign:spring", "delayed"},
					},
				)
				if err := producer.Publish(ctx, tasks...); err != nil {
					return err
				}
				_ = admin.BlockGroups(ctx, "demo-mail", 20, "smtp:2")
				cons := redisqueue.NewConsumer(rdb, "demo-mail", fmt.Sprintf("seed-hold-db%d", db))
				defer cons.Close()
				cons.SetPrefetchCount(2)
				got, err := cons.Get(ctx)
				if err != nil {
					return err
				}
				fmt.Printf("  db=%d queue=demo-mail published=%d in-flight=%d blocked-group=smtp:2\n", db, len(tasks), len(got))
				return nil
			},
		},
		{
			queue: "demo-tg",
			build: func(db int, producer *redisqueue.Producer, admin *redisqueue.Admin, rdb *redis.Client) error {
				_ = admin
				tasks := make([]*redisqueue.Task, 0, 16)
				for i := 0; i < 10; i++ {
					token := "token:aaa"
					if i%2 == 1 {
						token = "token:bbb"
					}
					tasks = append(tasks, &redisqueue.Task{
						ID:        fmt.Sprintf("tg-db%d-%d", db, i),
						Partition: fmt.Sprintf("!%s:chat:%d", token, i%4),
						Payload:   []byte(fmt.Sprintf(`{"db":%d,"adapter":"tg","i":%d}`, db, i)),
						Scheduled: now,
						Groups:    []string{token},
						Tags:      []string{"all", "broadcast", "adapter:tg", fmt.Sprintf("prio:%d", i%2)},
					})
				}
				for i := 0; i < 4; i++ {
					tasks = append(tasks, &redisqueue.Task{
						ID: fmt.Sprintf("tg-fan-db%d-%d", db, i), Partition: "bulk",
						Payload: []byte(`{"kind":"bulk"}`), Scheduled: now,
						Groups: []string{"token:aaa"}, Tags: []string{"all", "broadcast", "bulk"},
					})
				}
				if err := producer.Publish(ctx, tasks...); err != nil {
					return err
				}
				fmt.Printf("  db=%d queue=demo-tg published=%d\n", db, len(tasks))
				return nil
			},
		},
		{
			queue: "demo-mixed",
			build: func(db int, producer *redisqueue.Producer, admin *redisqueue.Admin, rdb *redis.Client) error {
				tasks := []*redisqueue.Task{
					{ID: fmt.Sprintf("mix-a-db%d", db), Partition: "!shop:1", Payload: []byte(`{"x":1}`), Scheduled: now,
						Groups: []string{"conn:east"}, Tags: []string{"all", "region:east", "mix"}},
					{ID: fmt.Sprintf("mix-b-db%d", db), Partition: "!shop:2", Payload: []byte(`{"x":2}`), Scheduled: now,
						Groups: []string{"conn:west"}, Tags: []string{"all", "region:west", "mix"}},
					{ID: fmt.Sprintf("mix-c-db%d", db), Partition: "jobs", Payload: []byte(`{"x":3}`), Scheduled: now,
						Groups: []string{"conn:east", "conn:west"}, Tags: []string{"all", "region:multi", "mix", "race!file:logo"}},
					{ID: fmt.Sprintf("mix-d-db%d", db), Partition: "!shop:1", Priority: 2, Payload: []byte(`{"x":4,"prio":2}`), Scheduled: now,
						Groups: []string{"conn:east"}, Tags: []string{"all", "region:east", "mix", "priority"}},
				}
				if err := producer.Publish(ctx, tasks...); err != nil {
					return err
				}
				_ = admin.BlockGroups(ctx, "demo-mixed", 45, "conn:west")
				fmt.Printf("  db=%d queue=demo-mixed published=%d blocked-group=conn:west race-tag=race!file:logo\n", db, len(tasks))
				return nil
			},
		},
	}

	rootDB := c.Root().Int("db")
	_ = rootDB

	for _, db := range dbs {
		rdb := clientOnDB(c, db)
		admin := redisqueue.NewAdmin(rdb)
		if purge {
			for _, sp := range specs {
				if _, err := admin.Purge(ctx, sp.queue, ""); err != nil {
					_ = rdb.Close()
					return fmt.Errorf("purge db=%d %s: %w", db, sp.queue, err)
				}
			}
		}
		fmt.Printf("Seeding Redis DB %d @ %s\n", db, c.Root().String("addr"))
		for _, sp := range specs {
			producer := redisqueue.NewProducer(rdb, sp.queue)
			if err := sp.build(db, producer, admin, rdb); err != nil {
				_ = rdb.Close()
				return fmt.Errorf("seed db=%d %s: %w", db, sp.queue, err)
			}
		}
		_ = rdb.Close()
	}
	fmt.Println("Done. Open desktop UI → Local Redis → очереди demo-* на DB 0/1/2.")
	return nil
}

func demoPartition(idx, nParts int, groups []string, fixedPart string, ordered bool) (part string, g []string) {
	if fixedPart != "" {
		g = groups
		if len(groups) > 0 {
			g = []string{groups[idx%len(groups)]}
		}
		return fixedPart, g
	}
	if len(groups) == 0 {
		if ordered {
			return fmt.Sprintf("!res:%d", idx%nParts), nil
		}
		return fmt.Sprintf("res:%d", idx%nParts), nil
	}
	gName := groups[idx%len(groups)]
	perGroup := nParts / len(groups)
	if perGroup < 1 {
		perGroup = 1
	}
	res := (idx / len(groups)) % perGroup
	if ordered {
		part = fmt.Sprintf("!%s:res:%d", gName, res)
	} else {
		part = fmt.Sprintf("%s:res:%d", gName, res)
	}
	return part, []string{gName}
}

func backpressureLimit(base, jitter int) int64 {
	if base <= 0 {
		return 0
	}
	lim := base
	if jitter > 0 {
		lim += rand.Intn(2*jitter+1) - jitter
	}
	if lim < 1 {
		lim = 1
	}
	return int64(lim)
}

func runPublish(ctx context.Context, c *cli.Command) error {
	rdb := redisClient(c)
	defer rdb.Close()

	queue := c.String("queue")
	count := c.Int("count")
	interval := c.Duration("interval")
	groups := splitCSV(c.String("groups"))
	tags := ensureAllTag(splitCSV(c.String("tags")))
	fixedPart := c.String("partition")
	ordered := c.Bool("ordered")
	nParts := c.Int("partitions")
	if nParts < 1 {
		nParts = 1
	}
	priority := c.Int("priority")
	batch := c.Int("batch")
	if batch < 1 {
		batch = 1
	}
	bpBase := c.Int("backpressure")
	bpJitter := c.Int("backpressure-jitter")
	bpWait := c.Duration("backpressure-wait")
	bpTag := "all"
	if bpBase > 0 {
		bpTag = tags[0] // ensureAllTag → always "all" first
	}

	producer := redisqueue.NewProducer(rdb, queue)
	ctx, stop := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
	defer stop()

	published := 0
	bpHits := 0
	started := time.Now()
	lastReport := started
	lastPublished := 0
	fmt.Printf("Publish → %s db=%d parts≈%d groups=%d batch=%d interval=%s bp=%d±%d tag=%q. Ctrl+C to stop.\n",
		queue, c.Root().Int("db"), nParts, len(groups), batch, interval, bpBase, bpJitter, bpTag)

	for count == 0 || published < count {
		select {
		case <-ctx.Done():
			fmt.Printf("Stopped. published=%d backpressure_hits=%d\n", published, bpHits)
			return nil
		default:
		}

		n := batch
		if count > 0 && published+n > count {
			n = count - published
		}
		tasks := make([]*redisqueue.Task, 0, n)
		now := time.Now().Add(-time.Second)
		for i := 0; i < n; i++ {
			idx := published + i
			part, g := demoPartition(idx, nParts, groups, fixedPart, ordered)
			tasks = append(tasks, &redisqueue.Task{
				ID:        fmt.Sprintf("live-%d-%d", time.Now().UnixNano(), idx),
				Partition: part,
				Priority:  priority,
				Payload:   []byte(fmt.Sprintf(`{"i":%d,"ts":%d,"stub":true}`, idx, time.Now().Unix())),
				Scheduled: now,
				Groups:    g,
				Tags:      tags,
			})
		}

		var opts []redisqueue.PublishOption
		if lim := backpressureLimit(bpBase, bpJitter); lim > 0 && bpTag != "" {
			opts = append(opts, redisqueue.WithTagBackpressure(bpTag, lim))
		}

		var err error
		if len(opts) > 0 {
			err = producer.PublishWith(ctx, opts, tasks...)
		} else {
			err = producer.Publish(ctx, tasks...)
		}
		if err != nil {
			var bp *redisqueue.ErrBackpressure
			if errors.As(err, &bp) {
				bpHits++
				if bpHits == 1 || bpHits%50 == 0 {
					fmt.Printf("  backpressure tag=%s depth=%d limit=%d (hits=%d published=%d) — ждём consumer\n",
						bp.Tag, bp.Count, bp.Limit, bpHits, published)
				}
				select {
				case <-ctx.Done():
					fmt.Printf("Stopped. published=%d backpressure_hits=%d\n", published, bpHits)
					return nil
				case <-time.After(bpWait):
				}
				continue
			}
			return err
		}

		published += len(tasks)
		if time.Since(lastReport) >= time.Second {
			delta := published - lastPublished
			fmt.Printf("  published=%d (+%d/s) bp_hits=%d elapsed=%s\n",
				published, delta, bpHits, time.Since(started).Truncate(time.Second))
			lastReport = time.Now()
			lastPublished = published
		}
		if interval > 0 {
			select {
			case <-ctx.Done():
				fmt.Printf("Stopped. published=%d backpressure_hits=%d\n", published, bpHits)
				return nil
			case <-time.After(interval):
			}
		}
	}
	fmt.Printf("Done. published=%d backpressure_hits=%d\n", published, bpHits)
	return nil
}

// demoHandlerDelay: 95% — 20..100ms; 5% — 100ms..2s.
func demoHandlerDelay() time.Duration {
	if rand.Float64() < 0.95 {
		return time.Duration(20+rand.Intn(81)) * time.Millisecond
	}
	return time.Duration(100+rand.Intn(1901)) * time.Millisecond
}

func runConsume(ctx context.Context, c *cli.Command) error {
	rdb := redisClient(c)
	defer rdb.Close()

	queue := c.String("queue")
	workers := c.Int("workers")
	if workers < 1 {
		workers = 1
	}
	failRate := c.Float64("fail-rate")
	blockFor := c.Duration("block-for")
	groupRate := c.Int("group-rate")
	statsRet := c.Duration("stats")
	statsTags := ensureAllTag(splitCSV(c.String("stats-tags")))
	prefetch := c.Int("prefetch")
	poll := c.Duration("poll")
	forDur := c.Duration("for")

	if forDur > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, forDur)
		defer cancel()
	}
	ctx, stop := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
	defer stop()

	pool := redisqueue.NewConsumerPool(rdb, queue)
	pool.SetCount(workers)
	pool.SetPrefetchCount(prefetch)
	pool.SetPollInterval(poll)
	if statsRet > 0 {
		pool.SetStatsRetention(statsRet)
		if len(statsTags) > 0 {
			pool.SetTagsStats(statsTags...)
		}
	}
	if groupRate > 0 {
		if err := pool.SetRateLimit(&redisqueue.RateLimitConfig{
			Algorithm: redisqueue.RateLimitTokenBucket,
			Group: &redisqueue.RateLimitLayer{
				Rates: []redisqueue.Rate{{Duration: time.Second, Value: groupRate}},
			},
		}); err != nil {
			return fmt.Errorf("SetRateLimit: %w", err)
		}
	}

	var acked, rejected atomic.Int64
	fmt.Printf("Consume ← %s db=%d workers=%d fail-rate=%.2f group-rate=%d/s block-for=%s stats=%s. Ctrl+C to stop.\n",
		queue, c.Root().Int("db"), workers, failRate, groupRate, blockFor, statsRet)

	pool.Consume(ctx, func(task *redisqueue.Task) error {
		time.Sleep(demoHandlerDelay())
		if failRate > 0 && rand.Float64() < failRate {
			rejected.Add(1)
			n := rejected.Load()
			delaySec := blockFor.Seconds()
			if delaySec < 0.05 {
				delaySec = 0.05
			}
			groups := task.Groups
			if n%20 == 1 {
				fmt.Printf("  stub-429 %s groups=%v (acked=%d rejected=%d) → BlockGroups\n",
					task.ID, groups, acked.Load(), n)
			}
			return redisqueue.NewRejectWithDelay(fmt.Errorf("stub upstream 429"), delaySec, groups...)
		}
		acked.Add(1)
		if a := acked.Load(); a%100 == 0 {
			fmt.Printf("  ack progress acked=%d rejected=%d last=%s\n", a, rejected.Load(), task.ID)
		}
		return nil
	})

	fmt.Printf("Stopped. acked=%d rejected=%d\n", acked.Load(), rejected.Load())
	return nil
}
