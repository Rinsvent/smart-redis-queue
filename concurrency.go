package redisqueue

import (
	"strings"
	"time"
)

// RaceTagPrefix — concurrency-ресурс в Task.Tags. Producer всегда индексирует
// такие теги в race:tags:{id}; проверка/локи в Get — только при SetConcurrencyTags(true).
const RaceTagPrefix = "race!"

// RaceSharedTagPrefix — prepare-once поверх race: после Share/Ack+Shared лок не нужен,
// Get отдаёт blob в Task.Shared. Полный тег: RaceSharedTagPrefix + id.
const RaceSharedTagPrefix = "race!s!"

// DefaultSharedTTL — TTL ключа queue:{q}:shared:{id}, если не задан WithSharedTTL.
const DefaultSharedTTL = 24 * time.Hour

// HasRaceTagPrefix — тег является concurrency-ресурсом (включая race!s!).
func HasRaceTagPrefix(tag string) bool {
	return strings.HasPrefix(tag, RaceTagPrefix)
}

// IsRaceSharedTag — prepare-once тег race!s!{id}.
func IsRaceSharedTag(tag string) bool {
	return strings.HasPrefix(tag, RaceSharedTagPrefix)
}

// RaceSharedTag собирает тег race!s!{id}.
func RaceSharedTag(id string) string {
	return RaceSharedTagPrefix + id
}

// RaceSharedID возвращает id из race!s!{id}; ok=false если не shared-тег.
func RaceSharedID(tag string) (id string, ok bool) {
	if !strings.HasPrefix(tag, RaceSharedTagPrefix) {
		return "", false
	}
	return tag[len(RaceSharedTagPrefix):], true
}

// SetConcurrencyTags включает проверку race!-тегов в Get (и release в Ack/Reject/Share).
// Выкл (дефолт) — ноль доп. redis.call на горячем пути. Ping-reclaim снимает
// чужие race-локи всегда (редкий путь, иначе утечка после смерти consumer).
func (c *Consumer) SetConcurrencyTags(enabled bool) {
	c.concurrencyTags = enabled
}

// SetConcurrencyTags — то же для пула (наследуется consumer’ами при Consume).
func (p *ConsumerPool) SetConcurrencyTags(enabled bool) {
	p.concurrencyTags = enabled
}

// ackConfig — накопитель для Ack(...AckOption).
type ackConfig struct {
	idempotencyTTL time.Duration
	shared         map[string][]byte
	sharedTTL      time.Duration
}

// AckOption — опция Ack (WithIdempotency, WithShared, WithSharedTTL).
type AckOption func(*ackConfig)

// WithIdempotency — окно dedupe после Ack (payload:{id} = "0" + EXPIRE).
func WithIdempotency(ttl time.Duration) AckOption {
	return func(c *ackConfig) {
		if ttl < 0 {
			ttl = 0
		}
		c.idempotencyTTL = ttl
	}
}

// WithShared пишет prepare-once blob’ы (ключ = id из race!s!{id}) и снимает
// соответствующие race-локи. Перезаписывает уже лежащий Shared.
func WithShared(shared map[string][]byte) AckOption {
	return func(c *ackConfig) {
		c.shared = shared
	}
}

// WithSharedTTL — TTL для ключей shared при этом Ack/Share; 0 → DefaultSharedTTL.
func WithSharedTTL(ttl time.Duration) AckOption {
	return func(c *ackConfig) {
		if ttl < 0 {
			ttl = 0
		}
		c.sharedTTL = ttl
	}
}

func applyAckOptions(opts []AckOption) ackConfig {
	cfg := ackConfig{}
	for _, opt := range opts {
		if opt != nil {
			opt(&cfg)
		}
	}
	return cfg
}

func sharedTTLSeconds(ttl time.Duration) float64 {
	if ttl <= 0 {
		ttl = DefaultSharedTTL
	}
	return ttl.Seconds()
}
