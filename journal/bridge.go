package journal

import (
	"context"
	"fmt"
	"time"

	redisqueue "github.com/Rinsvent/smart-redis-queue"
	"github.com/redis/go-redis/v9"
)

// Bridge качает journal → одна Redis-очередь.
// rdb — и pump-lock/cursor, и цель Publish: должен быть тот же инстанс, куда пишутся задачи.
// При шардинге очереди передайте клиент нужного шарда (или SetPublishOptions(WithShardName) на sharded Producer —
// тогда rdb всё равно обязан быть этим шардом, иначе cursor и очередь разъедутся).
type Bridge struct {
	rdb      *redis.Client
	prod     *redisqueue.Producer
	name     string
	src      Source
	resume   ResumeMode
	holderID string
	pubOpts  []redisqueue.PublishOption
}

func NewBridge(rdb *redis.Client, journalName, queueName string, src Source) *Bridge {
	return &Bridge{
		rdb:      rdb,
		prod:     redisqueue.NewProducer(rdb, queueName),
		name:     journalName,
		src:      src,
		resume:   ResumeAuto,
		holderID: fmt.Sprintf("bridge-%d", time.Now().UnixNano()),
	}
}

func (b *Bridge) SetResumeMode(m ResumeMode) { b.resume = m }

// SetPublishOptions — доп. опции к каждому Pump (compression, backpressure, WithShardName…).
// WithJournal добавляется самим Bridge; передавать его сюда не нужно.
func (b *Bridge) SetPublishOptions(opts ...redisqueue.PublishOption) {
	b.pubOpts = append([]redisqueue.PublishOption(nil), opts...)
}

// SetProducer подменяет publisher (например после SetShards). Cursor/lock по-прежнему на rdb из NewBridge.
func (b *Bridge) SetProducer(p *redisqueue.Producer) {
	if p != nil {
		b.prod = p
	}
}

func (b *Bridge) Cursor(ctx context.Context) (Cursor, error) {
	s, err := b.rdb.Get(ctx, redisqueue.JournalCursorKey(b.name)).Result()
	if err == redis.Nil {
		// ещё не ставили cursor — читать с начала (offset -1 → драйвер стартует с oldest / 0)
		return Cursor{Offset: -1}, nil
	}
	if err != nil {
		return Cursor{}, err
	}
	return DecodeCursor(s)
}

func (b *Bridge) Pump(ctx context.Context, n int) (int, error) {
	if n < 1 {
		n = 32
	}
	cur, err := b.Cursor(ctx)
	if err != nil {
		return 0, err
	}
	recs, err := b.src.ReadBatch(ctx, cur, n)
	if err != nil {
		return 0, err
	}
	if len(recs) == 0 {
		return 0, nil
	}
	last := recs[len(recs)-1].Cursor
	if cur.Offset >= 0 && !last.After(cur) {
		return 0, redisqueue.ErrJournalCursorRewind
	}
	tasks := make([]*redisqueue.Task, len(recs))
	for i := range recs {
		tasks[i] = recs[i].Task
	}
	opts := make([]redisqueue.PublishOption, 0, len(b.pubOpts)+1)
	opts = append(opts, b.pubOpts...)
	opts = append(opts, redisqueue.WithJournal(b.name, last.Encode()))
	if err := b.prod.PublishWith(ctx, opts, tasks...); err != nil {
		return 0, err
	}
	return len(recs), nil
}

func (b *Bridge) RecoverAfterRedis(ctx context.Context, waitAcks func(context.Context) error) error {
	unlock, err := acquirePumpLock(ctx, b.rdb, b.name, b.holderID)
	if err != nil {
		return err
	}
	defer unlock()
	if waitAcks != nil {
		if err := waitAcks(ctx); err != nil {
			return err
		}
	}
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		n, err := b.Pump(ctx, 256)
		if err != nil {
			return err
		}
		if n == 0 {
			break
		}
	}
	if b.resume == ResumeAuto {
		return redisqueue.SetResumeAfter(ctx, b.rdb, time.Now())
	}
	return nil
}

func (b *Bridge) Loop(ctx context.Context, batch int, idle time.Duration) error {
	unlock, err := acquirePumpLock(ctx, b.rdb, b.name, b.holderID)
	if err != nil {
		return fmt.Errorf("journal pump lock: %w", err)
	}
	defer unlock()
	if idle <= 0 {
		idle = 200 * time.Millisecond
	}
	for {
		n, err := b.Pump(ctx, batch)
		if err != nil {
			return err
		}
		if n == 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(idle):
			}
		}
	}
}
