package redisqueue

import (
	"context"
	"time"
)

// ConsumerOption — конфигурация NewConsumer(...opts). Старый вызов без opts без изменений.
type ConsumerOption func(*Consumer)

// WithPing — запускать pingLoop (дефолт true). Gateway: WithPing(false).
func WithPing(enabled bool) ConsumerOption {
	return func(c *Consumer) {
		c.enablePing = enabled
	}
}

// WithPollInterval — интервал ожидания при пустом Get.
func WithPollInterval(d time.Duration) ConsumerOption {
	return func(c *Consumer) { c.pollInterval = d }
}

// WithPrefetchCount — размер пачки Get (минимум 1).
func WithPrefetchCount(n int) ConsumerOption {
	return func(c *Consumer) {
		if n < 1 {
			n = 1
		}
		c.prefetchCount = n
	}
}

// WithCheckDeadConsumerLocksOnGet — проверка чужих lock на Get.
func WithCheckDeadConsumerLocksOnGet(enabled bool) ConsumerOption {
	return func(c *Consumer) { c.checkDeadConsumerLocksOnGet = enabled }
}

// WithSamePartitionBatch — пачка Get из одной партиции.
func WithSamePartitionBatch(enabled bool) ConsumerOption {
	return func(c *Consumer) { c.samePartitionBatch = enabled }
}

// WithExtendGroupBlocksOnly — Reject/BlockGroups только продлевают паузу (ZADD GT).
func WithExtendGroupBlocksOnly(enabled bool) ConsumerOption {
	return func(c *Consumer) { c.extendGroupBlocksOnly = enabled }
}

// WithDLQMaxSize — дефолт ёмкости DLQ для WithDeadLetter() без аргумента.
func WithDLQMaxSize(n int) ConsumerOption {
	return func(c *Consumer) {
		if n < 0 {
			n = 0
		}
		c.dlqMaxSize = n
	}
}

// WithIdempotencyTTL — TTL stub после Ack (дедуп Publish того же ID).
func WithIdempotencyTTL(d time.Duration) ConsumerOption {
	return func(c *Consumer) {
		if d < 0 {
			d = 0
		}
		c.idempotencyTtl = d
	}
}

// WithConcurrencyTags — проверка race! на Get / release на Ack·Reject.
func WithConcurrencyTags(enabled bool) ConsumerOption {
	return func(c *Consumer) { c.concurrencyTags = enabled }
}

// WithRateLimit — транспортный RL на Get. Невалидный cfg игнорируется (nil = выкл).
// Для ошибки валидации по-прежнему можно вызвать SetRateLimit.
func WithRateLimit(cfg *RateLimitConfig) ConsumerOption {
	return func(c *Consumer) {
		_ = c.SetRateLimit(cfg)
	}
}

// WithStatsRetention — запись g/a/r (+ теги) с TTL точек. 0 = выкл.
func WithStatsRetention(d time.Duration) ConsumerOption {
	return func(c *Consumer) {
		c.SetStatsRetention(d)
	}
}

// WithTagsStats — теги для SCARD-сэмпла (нужен WithStatsRetention / SetStatsRetention).
func WithTagsStats(tags ...string) ConsumerOption {
	return func(c *Consumer) {
		c.SetTagsStats(tags...)
	}
}

// WithMiddleware — middleware вокруг handler в Consume (порядок = Use).
func WithMiddleware(mw ...HandlerMiddleware) ConsumerOption {
	return func(c *Consumer) {
		c.middlewares = append(c.middlewares, mw...)
	}
}

// WithJournalAck — колбэк после успешного SRQ Ack (journal).
func WithJournalAck(fn func(ctx context.Context, taskID string) error) ConsumerOption {
	return func(c *Consumer) { c.journalAck = fn }
}

// WithAfterAck — колбэк после Ack (после journalAck).
func WithAfterAck(fn func(ctx context.Context, taskID string) error) ConsumerOption {
	return func(c *Consumer) { c.afterAck = fn }
}

// WithEmergencyResume — см. SetEmergencyResume.
func WithEmergencyResume(on bool) ConsumerOption {
	return func(c *Consumer) { c.emergencyResume = on }
}
