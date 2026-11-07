package redisqueue

import (
	"context"
	"fmt"

	"github.com/redis/go-redis/v9"
)

// Shared — mid-handler publish prepare-once blob’ов для race!s! (без Consumer в хэндлере).
// Пишет queue:{q}:shared:{id}, снимает лок, перезаписывает уже лежащие значения.
type Shared struct {
	rdb             *redis.Client
	queue           string
	taskID          string
	consumerID      string
	concurrencyTags bool
	task            *Task // чтобы обновить Task.Shared после Publish
}

// NewShared создаёт publisher из Task после Get/Consume (как NewSteps).
func NewShared(_ context.Context, task *Task) (*Shared, error) {
	if task == nil {
		return nil, fmt.Errorf("shared: task is nil")
	}
	if task.bindRedis == nil || task.bindQueue == "" || task.bindConsumerID == "" {
		return nil, fmt.Errorf("shared: task has no queue bind (use task from Consumer.Get / Consume, or NewSharedRaw)")
	}
	return &Shared{
		rdb:             task.bindRedis,
		queue:           task.bindQueue,
		taskID:          task.ID,
		consumerID:      task.bindConsumerID,
		concurrencyTags: task.bindConcurrencyTags,
		task:            task,
	}, nil
}

// NewSharedRaw — без Task: явные redis/queue/ids.
func NewSharedRaw(rdb *redis.Client, queueName, taskID, consumerID string, concurrencyTags bool) (*Shared, error) {
	if rdb == nil {
		return nil, fmt.Errorf("shared: redis client is nil")
	}
	if queueName == "" || taskID == "" || consumerID == "" {
		return nil, fmt.Errorf("shared: queue, task id and consumer id required")
	}
	return &Shared{
		rdb:             rdb,
		queue:           queueName,
		taskID:          taskID,
		consumerID:      consumerID,
		concurrencyTags: concurrencyTags,
	}, nil
}

// Publish пишет Shared blob’ы (ключ = id из race!s!{id}) и снимает соответствующие локи.
// TTL: WithSharedTTL; иначе DefaultSharedTTL. Можно сразу WithOpenBlocks.
func (s *Shared) Publish(ctx context.Context, data map[string][]byte, opts ...AckOption) error {
	if s == nil {
		return fmt.Errorf("shared: nil receiver")
	}
	cfg := applyAckOptions(opts)
	if len(data) == 0 && len(cfg.openBlocks) == 0 {
		return nil
	}
	return s.runShare(ctx, data, cfg)
}

// Open помечает race!b!{id} как ready (parallel Get без лока) на TTL.
// ids — суффиксы или полные race!b!… теги.
func (s *Shared) Open(ctx context.Context, ids []string, opts ...AckOption) error {
	if s == nil {
		return fmt.Errorf("shared: nil receiver")
	}
	if len(ids) == 0 {
		return nil
	}
	cfg := applyAckOptions(opts)
	cfg.openBlocks = append(append([]string(nil), cfg.openBlocks...), ids...)
	return s.runShare(ctx, nil, cfg)
}

func (s *Shared) runShare(ctx context.Context, data map[string][]byte, cfg ackConfig) error {
	args := []interface{}{
		s.queue,
		s.taskID,
		s.consumerID,
		sharedTTLSeconds(cfg.sharedTTL),
		bool01(s.concurrencyTags),
	}
	args = appendSharedArgs(args, data)
	args = appendOpenBlockArgs(args, cfg.openBlocks)

	result, err := getShareScript().Run(ctx, s.rdb, []string{}, args...).Result()
	if err != nil {
		return fmt.Errorf("shared: publish: %w", err)
	}
	if result.(int64) == 0 {
		return fmt.Errorf("shared: task not found in processing or not owned by this consumer")
	}
	if s.task != nil && len(data) > 0 {
		if s.task.Shared == nil {
			s.task.Shared = make(map[string][]byte, len(data))
		}
		for id, blob := range data {
			s.task.Shared[id] = blob
		}
	}
	return nil
}
