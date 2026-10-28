package tasks

import (
	"context"
	"fmt"
	"sync"

	redisqueue "github.com/Rinsvent/smart-redis-queue"
)

// ConsumeOptions — параметры Registry.Consume.
type ConsumeOptions struct {
	// Workers — число воркеров ConsumerPool на каждую очередь (default 5).
	Workers int
	// Queues — ограничить очередями; пусто = все Queue из Def + DefaultQueue.
	Queues []string
}

// Consume запускает воркеры на очередях registry: Get → Dispatch по base name.
// Блокируется до ctx.Done().
//
// Параллелизм — пул на очередь (не consumer-per-task и не «создать consumer
// при появлении задачи»): UI/Scheduler Publish в ту же очередь; свободный
// воркер берёт сразу. Лок только через payload-ключ (NX); без лока — в работу.
func (r *Registry) Consume(ctx context.Context, opts ...ConsumeOptions) error {
	if r == nil {
		return fmt.Errorf("tasks: nil registry")
	}
	var o ConsumeOptions
	if len(opts) > 0 {
		o = opts[0]
	}
	if o.Workers < 1 {
		o.Workers = 5
	}
	queues := o.Queues
	if len(queues) == 0 {
		queues = r.queues()
	}
	if len(queues) == 0 {
		return fmt.Errorf("tasks: no queues (Register Def or set DefaultQueue)")
	}
	for _, q := range queues {
		r.ensureProducer(q)
	}

	var wg sync.WaitGroup
	for _, q := range queues {
		q := q
		wg.Add(1)
		go func() {
			defer wg.Done()
			pool := redisqueue.NewConsumerPool(r.rdb, q)
			pool.SetCount(o.Workers)
			pool.Consume(ctx, func(task *redisqueue.Task) error {
				return r.Dispatch(ctx, task)
			})
		}()
	}
	<-ctx.Done()
	wg.Wait()
	return ctx.Err()
}

func (r *Registry) queues() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	seen := map[string]struct{}{}
	var out []string
	add := func(q string) {
		if q == "" {
			return
		}
		if _, ok := seen[q]; ok {
			return
		}
		seen[q] = struct{}{}
		out = append(out, q)
	}
	add(r.cfg.DefaultQueue)
	for _, d := range r.defs {
		add(d.Queue)
	}
	return out
}
