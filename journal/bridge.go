package journal

import (
	"context"
	"errors"
	"fmt"
	"time"

	redisqueue "github.com/Rinsvent/smart-redis-queue"
	"github.com/redis/go-redis/v9"
)

// Bridge качает journal → Redis-очередь (одна или sharded).
//
// rdb — meta: pump-lock + journal cursor (всегда здесь).
// Producer по умолчанию пишет в rdb; после SetShards задачи раскладываются по шардам,
// а cursor по-прежнему на rdb (двухфазно: Publish на шарды → SET cursor). Crash между
// фазами безопасен: replay NX-skip уже лежащих id.
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

// SetPublishOptions — compression, backpressure, WithShardName… WithJournal добавляет Bridge сам
// (только single-Redis); при шардинге cursor пишется на meta rdb отдельно.
func (b *Bridge) SetPublishOptions(opts ...redisqueue.PublishOption) {
	b.pubOpts = append([]redisqueue.PublishOption(nil), opts...)
}

// SetProducer подменяет publisher. Cursor/lock остаются на rdb из NewBridge.
func (b *Bridge) SetProducer(p *redisqueue.Producer) {
	if p != nil {
		b.prod = p
	}
}

// SetShards — тот же ShardConfig, что у ConsumerPool. Курсор journal не переезжает на шарды.
func (b *Bridge) SetShards(cfg *redisqueue.ShardConfig) error {
	return b.prod.SetShards(cfg)
}

func (b *Bridge) Producer() *redisqueue.Producer { return b.prod }

func (b *Bridge) Cursor(ctx context.Context) (Cursor, error) {
	s, err := b.rdb.Get(ctx, redisqueue.JournalCursorKey(b.name)).Result()
	if err == redis.Nil {
		return Cursor{Offset: -1}, nil
	}
	if err != nil {
		return Cursor{}, err
	}
	return DecodeCursor(s)
}

func (b *Bridge) promoteDue(ctx context.Context, n int) error {
	p, ok := b.src.(DuePromoter)
	if !ok {
		return nil
	}
	_, err := p.PromoteDue(ctx, n)
	return err
}

func (b *Bridge) commitCursor(ctx context.Context, encoded string) error {
	return b.rdb.Set(ctx, redisqueue.JournalCursorKey(b.name), encoded, 0).Err()
}

func (b *Bridge) Pump(ctx context.Context, n int) (int, error) {
	if n < 1 {
		n = 32
	}
	if err := b.promoteDue(ctx, n); err != nil {
		return 0, err
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

	sharded := b.prod.ShardRouter() != nil
	var opts []redisqueue.PublishOption
	if sharded {
		opts = redisqueue.StripJournalPublishOptions(b.pubOpts)
	} else {
		opts = append(opts, b.pubOpts...)
		opts = append(opts, redisqueue.WithJournal(b.name, last.Encode()))
	}
	if err := b.prod.PublishWith(ctx, opts, tasks...); err != nil {
		var exists *redisqueue.ErrTasksAlreadyExist
		if !errors.As(err, &exists) {
			return 0, err
		}
	}
	if sharded {
		// cursor на meta после успешного (или NX-partial) publish по шардам
		if err := b.commitCursor(ctx, last.Encode()); err != nil {
			return 0, err
		}
	}
	return len(recs), nil
}

func (b *Bridge) lockDoneAcks(ctx context.Context) error {
	from := Cursor{Offset: -1}
	for {
		acks, err := b.src.ReadAcks(ctx, from, 500)
		if err != nil {
			return err
		}
		if len(acks) == 0 {
			return nil
		}
		ids := make([]string, 0, len(acks))
		for _, a := range acks {
			if a.TaskID != "" {
				ids = append(ids, a.TaskID)
			}
		}
		if len(ids) > 0 {
			if err := b.prod.LockOnAllShards(ctx, ids...); err != nil {
				var exists *redisqueue.ErrTasksAlreadyExist
				if !errors.As(err, &exists) {
					return err
				}
			}
		}
		from = acks[len(acks)-1].Cursor
	}
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
	if err := b.lockDoneAcks(ctx); err != nil {
		return fmt.Errorf("journal recover lock acks: %w", err)
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
