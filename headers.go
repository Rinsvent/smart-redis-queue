package redisqueue

import (
	"context"
	"fmt"
	"strings"

	"github.com/redis/go-redis/v9"
)

func headersKey(queueName, taskID string) string {
	return "queue:" + queueName + ":headers:" + taskID
}

func (t *Task) requireBind() (*redis.Client, string, string, error) {
	if t == nil {
		return nil, "", "", fmt.Errorf("headers: task is nil")
	}
	if t.bindRedis == nil || t.bindQueue == "" {
		return nil, "", "", fmt.Errorf("headers: task has no queue bind (use task from Consumer.Get / Consume)")
	}
	if t.ID == "" {
		return nil, "", "", fmt.Errorf("headers: empty task id")
	}
	return t.bindRedis, t.bindQueue, t.ID, nil
}

// Headers читает мета-заголовки задачи (отдельный Redis HASH, не на горячем пути Get).
// Нет ключа → пустая map, nil error.
func (t *Task) Headers(ctx context.Context) (map[string]string, error) {
	rdb, queue, id, err := t.requireBind()
	if err != nil {
		return nil, err
	}
	m, err := rdb.HGetAll(ctx, headersKey(queue, id)).Result()
	if err != nil {
		return nil, fmt.Errorf("headers get: %w", err)
	}
	if m == nil {
		m = map[string]string{}
	}
	return m, nil
}

// SetHeaders полностью заменяет headers. Пустая map — удаляет ключ.
func (t *Task) SetHeaders(ctx context.Context, headers map[string]string) error {
	rdb, queue, id, err := t.requireBind()
	if err != nil {
		return err
	}
	key := headersKey(queue, id)
	if len(headers) == 0 {
		return rdb.Del(ctx, key).Err()
	}
	clean := make(map[string]string, len(headers))
	for k, v := range headers {
		k = strings.TrimSpace(k)
		if k == "" {
			return fmt.Errorf("headers: empty key")
		}
		clean[k] = v
	}
	pipe := rdb.TxPipeline()
	pipe.Del(ctx, key)
	pipe.HSet(ctx, key, clean)
	_, err = pipe.Exec(ctx)
	if err != nil {
		return fmt.Errorf("headers set: %w", err)
	}
	return nil
}

// SetHeader пишет одно поле (создаёт HASH при необходимости).
func (t *Task) SetHeader(ctx context.Context, key, value string) error {
	rdb, queue, id, err := t.requireBind()
	if err != nil {
		return err
	}
	key = strings.TrimSpace(key)
	if key == "" {
		return fmt.Errorf("headers: empty key")
	}
	if err := rdb.HSet(ctx, headersKey(queue, id), key, value).Err(); err != nil {
		return fmt.Errorf("headers set %q: %w", key, err)
	}
	return nil
}

// DeleteHeader удаляет одно поле.
func (t *Task) DeleteHeader(ctx context.Context, key string) error {
	rdb, queue, id, err := t.requireBind()
	if err != nil {
		return err
	}
	key = strings.TrimSpace(key)
	if key == "" {
		return fmt.Errorf("headers: empty key")
	}
	return rdb.HDel(ctx, headersKey(queue, id), key).Err()
}
