package redisqueue

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

func stepKey(queueName, taskID string) string {
	return "queue:" + queueName + ":step:" + taskID
}

// StepFunc — тело одного шага recovery.
type StepFunc func(ctx context.Context) error

// StepsOptions — опции NewSteps / NewStepsRaw.
type StepsOptions struct {
	// TTL — EXPIRE на ключ step при каждом Commit. 0 = без TTL (ключ живёт до Ack/Remove/Release).
	TTL time.Duration
}

// StepRunner — checkpoint по строковому step.
// Ключ чистится на Ack/Admin.Remove; опционально Release или TTL.
type StepRunner struct {
	last      string
	skipUntil string
	active    bool
	rdb       *redis.Client
	queue     string
	taskID    string
	ttl       time.Duration
}

// NewSteps создаёт runner из Task после Get/Consume (redis и queue подставляет consumer).
func NewSteps(ctx context.Context, task *Task, opts ...StepsOptions) (*StepRunner, error) {
	if task == nil {
		return nil, fmt.Errorf("steps: task is nil")
	}
	if task.bindRedis == nil || task.bindQueue == "" {
		return nil, fmt.Errorf("steps: task has no queue bind (use task from Consumer.Get / Consume, or NewStepsRaw)")
	}
	var o StepsOptions
	if len(opts) > 0 {
		o = opts[0]
	}
	return newSteps(ctx, task.bindRedis, task.bindQueue, task.ID, o)
}

// NewStepsRaw — без Task: явные redis/queue/id (скрипты, тесты, свой Get-путь).
func NewStepsRaw(ctx context.Context, rdb *redis.Client, queueName, taskID string, opts ...StepsOptions) (*StepRunner, error) {
	var o StepsOptions
	if len(opts) > 0 {
		o = opts[0]
	}
	return newSteps(ctx, rdb, queueName, taskID, o)
}

func newSteps(ctx context.Context, rdb *redis.Client, queueName, taskID string, opts StepsOptions) (*StepRunner, error) {
	if rdb == nil {
		return nil, fmt.Errorf("steps: redis client is nil")
	}
	if queueName == "" || taskID == "" {
		return nil, fmt.Errorf("steps: queue and task id required")
	}
	last := ""
	val, err := rdb.Get(ctx, stepKey(queueName, taskID)).Result()
	if err == nil {
		last = val
	} else if err != redis.Nil {
		return nil, fmt.Errorf("steps: load checkpoint: %w", err)
	}
	return &StepRunner{
		rdb:       rdb,
		queue:     queueName,
		taskID:    taskID,
		last:      last,
		skipUntil: last,
		active:    last == "",
		ttl:       opts.TTL,
	}, nil
}

// Last — текущий checkpoint ("" если нет).
func (s *StepRunner) Last() string {
	if s == nil {
		return ""
	}
	return s.last
}

// Commit записывает имя завершённого шага.
func (s *StepRunner) Commit(ctx context.Context, name string) error {
	if s == nil {
		return fmt.Errorf("steps: nil runner")
	}
	name = strings.TrimSpace(name)
	if name == "" {
		return fmt.Errorf("step: empty name")
	}
	key := stepKey(s.queue, s.taskID)
	if err := s.rdb.Set(ctx, key, name, s.ttl).Err(); err != nil {
		return fmt.Errorf("step commit %q: %w", name, err)
	}
	s.last = name
	return nil
}

// Do выполняет fn, если шаг ещё не пройден checkpoint'ом; при успехе Commit(name).
func (s *StepRunner) Do(ctx context.Context, name string, fn StepFunc) error {
	if s == nil {
		return fmt.Errorf("steps: nil runner")
	}
	name = strings.TrimSpace(name)
	if name == "" {
		return fmt.Errorf("step: empty name")
	}
	if fn == nil {
		return fmt.Errorf("step %q: nil func", name)
	}

	if !s.active {
		if name != s.skipUntil {
			return nil
		}
		s.active = true
		return nil
	}

	if name == s.last {
		return nil
	}

	if err := fn(ctx); err != nil {
		return err
	}
	return s.Commit(ctx, name)
}

// Release удаляет checkpoint вручную. Обычно не нужен: Ack и Admin.Remove чистят ключ сами.
func (s *StepRunner) Release(ctx context.Context) error {
	if s == nil {
		return nil
	}
	if err := s.rdb.Del(ctx, stepKey(s.queue, s.taskID)).Err(); err != nil {
		return fmt.Errorf("step release: %w", err)
	}
	s.last = ""
	s.skipUntil = ""
	s.active = true
	return nil
}
