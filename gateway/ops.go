package gateway

import (
	"context"
	"fmt"
	"strings"
	"time"

	redisqueue "github.com/Rinsvent/smart-redis-queue"
)

// PublishTasks — Publish после Redis OK; запись в op-log + loss-window.
func (g *Gateway) PublishTasks(ctx context.Context, queue string, tasks []*redisqueue.Task, opts []redisqueue.PublishOption) error {
	if queue == "" {
		return ErrEmptyQueue
	}
	if err := g.state.allowPublish(); err != nil {
		return err
	}
	recs := make([]PublishRec, 0, len(tasks))
	for _, t := range tasks {
		if t == nil || t.ID == "" {
			return ErrEmptyTaskID
		}
		recs = append(recs, publishRecFromTask(t))
	}

	p := g.producer(queue)
	var err error
	if len(opts) > 0 {
		err = p.PublishWith(ctx, opts, tasks...)
	} else {
		err = p.Publish(ctx, tasks...)
	}
	if err != nil {
		g.noteRedisErr(err)
		return err
	}
	now := time.Now()
	for i := range recs {
		rec := recs[i]
		g.ops.append(Op{
			Kind:    OpPublish,
			Queue:   queue,
			TaskID:  rec.ID,
			RedisOK: true,
			Publish: &rec,
			At:      now,
		})
		g.loss.onPublishOK(queue, rec, now)
	}
	return nil
}

// GetTasks — Get только в Running.
func (g *Gateway) GetTasks(ctx context.Context, queue, consumerID string, prefetch int) ([]*redisqueue.Task, error) {
	if queue == "" {
		return nil, ErrEmptyQueue
	}
	if err := g.state.allowGet(); err != nil {
		return nil, err
	}
	c := g.consumer(queue, consumerID)
	if prefetch > 0 {
		c.SetPrefetchCount(prefetch)
	}
	tasks, err := c.Get(ctx)
	if err != nil {
		g.noteRedisErr(err)
		return nil, err
	}
	return tasks, nil
}

// AckTask — Ack; при RedisDown копит в op-log без вызова Redis.
func (g *Gateway) AckTask(ctx context.Context, queue, consumerID, taskID string, ack *AckRec) error {
	if queue == "" {
		return ErrEmptyQueue
	}
	if taskID == "" {
		return ErrEmptyTaskID
	}
	if err := g.state.allowTerminal(); err != nil {
		return err
	}
	rec := ack
	if rec == nil {
		rec = &AckRec{}
	}
	op := Op{
		Kind:     OpAck,
		Queue:    queue,
		TaskID:   taskID,
		Consumer: consumerID,
		Ack:      rec,
	}

	if g.state.Get() == StateRedisDown {
		op.RedisOK = false
		g.ops.append(op)
		g.loss.onAckOK(taskID)
		return nil
	}

	c := g.consumer(queue, consumerID)
	if err := c.Ack(ctx, taskID, ackOptions(rec)...); err != nil {
		g.noteRedisErr(err)
		if g.state.Get() == StateRedisDown {
			op.RedisOK = false
			g.ops.append(op)
			g.loss.onAckOK(taskID)
			return nil
		}
		return err
	}
	op.RedisOK = true
	g.ops.append(op)
	g.loss.onAckOK(taskID)
	return nil
}

// RejectTask — Reject; publish остаётся / продлевается в loss-window.
func (g *Gateway) RejectTask(ctx context.Context, queue, consumerID, taskID string, rej *RejectRec) error {
	if queue == "" {
		return ErrEmptyQueue
	}
	if taskID == "" {
		return ErrEmptyTaskID
	}
	if err := g.state.allowTerminal(); err != nil {
		return err
	}
	rec := rej
	if rec == nil {
		rec = &RejectRec{DeadLetterMax: -1}
	}
	op := Op{
		Kind:     OpReject,
		Queue:    queue,
		TaskID:   taskID,
		Consumer: consumerID,
		Reject:   rec,
	}

	if g.state.Get() == StateRedisDown {
		now := time.Now()
		op.RedisOK = false
		op.At = now
		g.ops.append(op)
		g.loss.onRejectOK(taskID, now)
		return nil
	}

	c := g.consumer(queue, consumerID)
	if err := c.Reject(ctx, taskID, rejectOptions(rec)...); err != nil {
		g.noteRedisErr(err)
		if g.state.Get() == StateRedisDown {
			now := time.Now()
			op.RedisOK = false
			op.At = now
			g.ops.append(op)
			g.loss.onRejectOK(taskID, now)
			return nil
		}
		return err
	}
	now := time.Now()
	op.RedisOK = true
	op.At = now
	g.ops.append(op)
	g.loss.onRejectOK(taskID, now)
	return nil
}

func (g *Gateway) noteRedisErr(err error) {
	if err == nil {
		return
	}
	msg := strings.ToLower(err.Error())
	if strings.Contains(msg, "connection refused") ||
		strings.Contains(msg, "i/o timeout") ||
		strings.Contains(msg, "broken pipe") ||
		strings.Contains(msg, "eof") ||
		strings.Contains(msg, "loading") {
		if g.state.Get() == StateRunning {
			g.log.Printf("redis fault on op: %v → redis_down", err)
			g.state.set(StateRedisDown)
		}
	}
}

func publishRecFromTask(t *redisqueue.Task) PublishRec {
	return PublishRec{
		ID:                t.ID,
		Partition:         t.Partition,
		PartitionPriority: t.PartitionPriority,
		QueuePriority:     t.QueuePriority,
		Payload:           append([]byte(nil), t.Payload...),
		Tags:              append([]string(nil), t.Tags...),
		Groups:            append([]string(nil), t.Groups...),
		Scheduled:         t.Scheduled,
	}
}

func taskFromPublishRec(r PublishRec) *redisqueue.Task {
	return &redisqueue.Task{
		ID:                r.ID,
		Partition:         r.Partition,
		PartitionPriority: r.PartitionPriority,
		QueuePriority:     r.QueuePriority,
		Payload:           append([]byte(nil), r.Payload...),
		Tags:              append([]string(nil), r.Tags...),
		Groups:            append([]string(nil), r.Groups...),
		Scheduled:         r.Scheduled,
	}
}

func ackOptions(r *AckRec) []redisqueue.AckOption {
	if r == nil {
		return nil
	}
	var opts []redisqueue.AckOption
	if r.IdempotencyTTLSec > 0 {
		opts = append(opts, redisqueue.WithIdempotency(time.Duration(r.IdempotencyTTLSec*float64(time.Second))))
	}
	if len(r.Shared) > 0 {
		opts = append(opts, redisqueue.WithShared(r.Shared))
	}
	if r.SharedTTLSec > 0 {
		opts = append(opts, redisqueue.WithSharedTTL(time.Duration(r.SharedTTLSec*float64(time.Second))))
	}
	if len(r.OpenBlocks) > 0 {
		opts = append(opts, redisqueue.WithOpenBlocks(r.OpenBlocks...))
	}
	return opts
}

func rejectOptions(r *RejectRec) []redisqueue.RejectOption {
	if r == nil {
		return nil
	}
	var opts []redisqueue.RejectOption
	if r.Delay > 0 {
		opts = append(opts, redisqueue.WithDelay(r.Delay))
	}
	if len(r.BlockGroups) > 0 {
		opts = append(opts, redisqueue.WithBlockGroups(r.BlockGroups...))
	}
	if len(r.BlockTags) > 0 {
		opts = append(opts, redisqueue.WithBlockTags(r.BlockTags...))
	}
	if r.DeadLetter {
		if r.DeadLetterMax < 0 {
			opts = append(opts, redisqueue.WithDeadLetter())
		} else {
			opts = append(opts, redisqueue.WithDeadLetter(r.DeadLetterMax))
		}
	}
	return opts
}

// recover — Get закрыт; replay ops с RedisOK=false, затем loss-window publishes.
func (g *Gateway) recover(ctx context.Context) error {
	g.state.set(StateRecovering)
	g.log.Printf("recover: begin ops=%d loss=%d", g.ops.len(), g.loss.len())

	for _, op := range g.ops.snapshot() {
		if op.RedisOK {
			continue
		}
		if err := g.replayOp(ctx, op); err != nil {
			return fmt.Errorf("gateway: replay offset=%d: %w", op.Offset, err)
		}
	}

	for _, e := range g.loss.snapshot() {
		if e.Queue == "" {
			g.log.Printf("recover: loss entry %s without queue, skip", e.Rec.ID)
			continue
		}
		t := taskFromPublishRec(e.Rec)
		if err := g.producer(e.Queue).Publish(ctx, t); err != nil {
			g.log.Printf("recover: re-publish %s: %v", e.Rec.ID, err)
		}
	}

	g.state.set(StateRunning)
	g.log.Printf("recover: done → running")
	g.ensureHealth()
	return nil
}

func (g *Gateway) replayOp(ctx context.Context, op Op) error {
	switch op.Kind {
	case OpPublish:
		if op.Publish == nil {
			return nil
		}
		t := taskFromPublishRec(*op.Publish)
		return g.producer(op.Queue).Publish(ctx, t)
	case OpAck:
		c := g.consumer(op.Queue, op.Consumer)
		return c.Ack(ctx, op.TaskID, ackOptions(op.Ack)...)
	case OpReject:
		c := g.consumer(op.Queue, op.Consumer)
		return c.Reject(ctx, op.TaskID, rejectOptions(op.Reject)...)
	default:
		return nil
	}
}
