package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"strings"
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
			Usage: "Публиковать заглушечные задачи (для живого UI / статистики)",
			Flags: []cli.Flag{
				&cli.StringFlag{Name: "queue", Aliases: []string{"q"}, Value: "demo-mail", Usage: "имя очереди"},
				&cli.IntFlag{Name: "count", Aliases: []string{"n"}, Value: 20, Usage: "сколько задач (0 = бесконечно до Ctrl+C)"},
				&cli.DurationFlag{Name: "interval", Value: 200 * time.Millisecond, Usage: "пауза между задачами в потоке"},
				&cli.StringFlag{Name: "groups", Value: "smtp:1,smtp:2", Usage: "группы через запятую (ротация)"},
				&cli.StringFlag{Name: "tags", Value: "campaign:live,batch:demo", Usage: "теги через запятую"},
				&cli.StringFlag{Name: "partition", Value: "", Usage: "фиксированная партиция; пусто = !chat:{i%N} / plain:{i}"},
				&cli.BoolFlag{Name: "ordered", Value: true, Usage: "партиции с префиксом ! (если --partition пуст)"},
				&cli.IntFlag{Name: "partitions", Value: 8, Usage: "число ротируемых партиций"},
				&cli.IntFlag{Name: "priority", Value: 0, Usage: "priority задач"},
				&cli.IntFlag{Name: "batch", Value: 1, Usage: "размер Publish batch"},
			},
			Action: runPublish,
		},
		{
			Name:  "consume",
			Usage: "Consume с заглушкой handler + stats (график Get/Ack/Reject в UI)",
			Flags: []cli.Flag{
				&cli.StringFlag{Name: "queue", Aliases: []string{"q"}, Value: "demo-mail", Usage: "имя очереди"},
				&cli.DurationFlag{Name: "delay", Value: 50 * time.Millisecond, Usage: "искусственная задержка handler"},
				&cli.Float64Flag{Name: "fail-rate", Value: 0.05, Usage: "доля Reject (0..1)"},
				&cli.DurationFlag{Name: "stats", Value: 5 * time.Minute, Usage: "retention метрик (0 = выкл)"},
				&cli.StringFlag{Name: "stats-tags", Value: "campaign:live,batch:demo", Usage: "теги для SCARD-сэмпла"},
				&cli.IntFlag{Name: "prefetch", Value: 5, Usage: "prefetch"},
				&cli.DurationFlag{Name: "poll", Value: 300 * time.Millisecond, Usage: "poll interval"},
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
						Tags:      []string{"campaign:spring", "channel:email", fmt.Sprintf("batch:%d", i/4)},
					})
				}
				// unordered + delayed
				tasks = append(tasks,
					&redisqueue.Task{
						ID: fmt.Sprintf("mail-plain-db%d", db), Partition: "inbox",
						Payload: []byte(`{"kind":"plain"}`), Scheduled: now, Groups: []string{"smtp:1"},
						Tags: []string{"campaign:spring", "plain"},
					},
					&redisqueue.Task{
						ID: fmt.Sprintf("mail-later-db%d", db), Partition: "!user:99",
						Payload: []byte(`{"kind":"delayed"}`), Scheduled: time.Now().Add(30 * time.Second),
						Groups: []string{"smtp:2"}, Tags: []string{"campaign:spring", "delayed"},
					},
				)
				if err := producer.Publish(ctx, tasks...); err != nil {
					return err
				}
				// заблокировать группу на 20s — видно в UI Groups
				_ = admin.BlockGroups(ctx, "demo-mail", 20, "smtp:2")
				// взять 2 задачи in-flight без Ack
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
						Tags:      []string{"broadcast", "adapter:tg", fmt.Sprintf("prio:%d", i%2)},
					})
				}
				for i := 0; i < 4; i++ {
					tasks = append(tasks, &redisqueue.Task{
						ID: fmt.Sprintf("tg-fan-db%d-%d", db, i), Partition: "bulk",
						Payload: []byte(`{"kind":"bulk"}`), Scheduled: now,
						Groups: []string{"token:aaa"}, Tags: []string{"broadcast", "bulk"},
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
						Groups: []string{"conn:east"}, Tags: []string{"region:east", "mix"}},
					{ID: fmt.Sprintf("mix-b-db%d", db), Partition: "!shop:2", Payload: []byte(`{"x":2}`), Scheduled: now,
						Groups: []string{"conn:west"}, Tags: []string{"region:west", "mix"}},
					{ID: fmt.Sprintf("mix-c-db%d", db), Partition: "jobs", Payload: []byte(`{"x":3}`), Scheduled: now,
						Groups: []string{"conn:east", "conn:west"}, Tags: []string{"region:multi", "mix", "race!file:logo"}},
					{ID: fmt.Sprintf("mix-d-db%d", db), Partition: "!shop:1", Priority: 2, Payload: []byte(`{"x":4,"prio":2}`), Scheduled: now,
						Groups: []string{"conn:east"}, Tags: []string{"region:east", "mix", "priority"}},
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

	rootDB := c.Root().Int("db") // ignored; seed uses --dbs
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

func runPublish(ctx context.Context, c *cli.Command) error {
	rdb := redisClient(c)
	defer rdb.Close()

	queue := c.String("queue")
	count := c.Int("count")
	interval := c.Duration("interval")
	groups := splitCSV(c.String("groups"))
	tags := splitCSV(c.String("tags"))
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

	producer := redisqueue.NewProducer(rdb, queue)
	ctx, stop := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
	defer stop()

	published := 0
	fmt.Printf("Publish → %s db=%d (count=%d batch=%d interval=%s). Ctrl+C to stop.\n",
		queue, c.Root().Int("db"), count, batch, interval)

	for count == 0 || published < count {
		select {
		case <-ctx.Done():
			fmt.Printf("Stopped. published=%d\n", published)
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
			part := fixedPart
			if part == "" {
				if ordered {
					part = fmt.Sprintf("!chat:%d", idx%nParts)
				} else {
					part = fmt.Sprintf("plain:%d", idx%nParts)
				}
			}
			g := groups
			if len(groups) > 0 {
				g = []string{groups[idx%len(groups)]}
			}
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
		if err := producer.Publish(ctx, tasks...); err != nil {
			return err
		}
		published += len(tasks)
		if published%50 == 0 || (count > 0 && published >= count) {
			fmt.Printf("  published=%d\n", published)
		}
		if interval > 0 {
			select {
			case <-ctx.Done():
				fmt.Printf("Stopped. published=%d\n", published)
				return nil
			case <-time.After(interval):
			}
		}
	}
	fmt.Printf("Done. published=%d\n", published)
	return nil
}

func runConsume(ctx context.Context, c *cli.Command) error {
	rdb := redisClient(c)
	defer rdb.Close()

	queue := c.String("queue")
	delay := c.Duration("delay")
	failRate := c.Float64("fail-rate")
	statsRet := c.Duration("stats")
	statsTags := splitCSV(c.String("stats-tags"))
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

	consumer := redisqueue.NewConsumer(rdb, queue, "")
	defer consumer.Close()
	consumer.SetPrefetchCount(prefetch)
	consumer.SetPollInterval(poll)
	if statsRet > 0 {
		consumer.SetStatsRetention(statsRet)
		if len(statsTags) > 0 {
			consumer.SetTagsStats(statsTags...)
		}
	}

	var acked, rejected int
	fmt.Printf("Consume ← %s db=%d delay=%s fail-rate=%.2f stats=%s. Ctrl+C to stop.\n",
		queue, c.Root().Int("db"), delay, failRate, statsRet)

	n := 0
	err := consumer.Consume(ctx, func(task *redisqueue.Task) error {
		if delay > 0 {
			time.Sleep(delay)
		}
		n++
		if failRate > 0 && float64(n%100) < failRate*100 {
			rejected++
			if rejected%10 == 1 {
				fmt.Printf("  reject %s (acked=%d rejected=%d)\n", task.ID, acked, rejected)
			}
			return redisqueue.NewRejectWithDelay(fmt.Errorf("stub fail"), 0.2)
		}
		acked++
		if acked%20 == 0 {
			fmt.Printf("  ack progress acked=%d rejected=%d last=%s\n", acked, rejected, task.ID)
		}
		return nil
	})
	if err != nil && ctx.Err() == nil {
		return err
	}
	fmt.Printf("Stopped. acked=%d rejected=%d\n", acked, rejected)
	return nil
}
