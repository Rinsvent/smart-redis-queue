package journal

import (
	"context"
	"fmt"
	"time"

	redisqueue "github.com/Rinsvent/smart-redis-queue"
	"github.com/redis/go-redis/v9"
)

type Bridge struct {
	rdb      *redis.Client
	name     string
	queue    string
	src      Source
	resume   ResumeMode
	holderID string
}

func NewBridge(rdb *redis.Client, journalName, queueName string, src Source) *Bridge {
	return &Bridge{
		rdb:      rdb,
		name:     journalName,
		queue:    queueName,
		src:      src,
		resume:   ResumeAuto,
		holderID: fmt.Sprintf("bridge-%d", time.Now().UnixNano()),
	}
}

func (b *Bridge) SetResumeMode(m ResumeMode) { b.resume = m }

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
	if err := redisqueue.PublishJournalBatch(ctx, b.rdb, b.queue, b.name, last.Encode(), tasks); err != nil {
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
