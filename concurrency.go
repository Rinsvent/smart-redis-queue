package redisqueue

import (
	"strings"
	"time"
)

// RaceTagPrefix — concurrency-ресурс в Task.Tags. Producer всегда индексирует
// такие теги в race:tags:{id}; проверка/локи в Get — только при SetConcurrencyTags(true).
const RaceTagPrefix = "race!"

// RaceSharedTagPrefix — prepare-once: после Shared.Publish / Ack+WithShared лок не нужен,
// Get отдаёт blob в Task.Shared. Ключ: queue:{q}:shared:{id}.
const RaceSharedTagPrefix = "race!s!"

// RaceBlockTagPrefix — wait-gate: block:{id} отсутствует → mutex (один probe);
// значение не ready → Get skip; ready → parallel без лока. Ключ: queue:{q}:block:{id}.
const RaceBlockTagPrefix = "race!b!"

// BlockWaitValue / BlockReadyValue — значения ключа block:{id}.
const (
	BlockWaitValue  = "1"
	BlockReadyValue = "ready"
)

// DefaultSharedTTL — TTL shared:{id} / block:{id} ready, если не задан WithSharedTTL.
const DefaultSharedTTL = 24 * time.Hour

// HasRaceTagPrefix — concurrency-ресурс (race! / race!s! / race!b!).
func HasRaceTagPrefix(tag string) bool {
	return strings.HasPrefix(tag, RaceTagPrefix)
}

// IsRaceSharedTag — prepare-once тег race!s!{id}.
func IsRaceSharedTag(tag string) bool {
	return strings.HasPrefix(tag, RaceSharedTagPrefix)
}

// IsRaceBlockTag — wait-gate тег race!b!{id}.
func IsRaceBlockTag(tag string) bool {
	return strings.HasPrefix(tag, RaceBlockTagPrefix)
}

// RaceSharedTag собирает тег race!s!{id}.
func RaceSharedTag(id string) string {
	return RaceSharedTagPrefix + id
}

// RaceBlockTag собирает тег race!b!{id}.
func RaceBlockTag(id string) string {
	return RaceBlockTagPrefix + id
}

// RaceSharedID возвращает id из race!s!{id}; ok=false если не shared-тег.
func RaceSharedID(tag string) (id string, ok bool) {
	if !strings.HasPrefix(tag, RaceSharedTagPrefix) {
		return "", false
	}
	return tag[len(RaceSharedTagPrefix):], true
}

// RaceBlockID возвращает id из race!b!{id}; ok=false если не block-тег.
func RaceBlockID(tag string) (id string, ok bool) {
	if !strings.HasPrefix(tag, RaceBlockTagPrefix) {
		return "", false
	}
	return tag[len(RaceBlockTagPrefix):], true
}

// NormalizeRaceBlockID — id или полный race!b!{id} → id.
func NormalizeRaceBlockID(idOrTag string) string {
	if id, ok := RaceBlockID(idOrTag); ok {
		return id
	}
	return idOrTag
}

// SetConcurrencyTags включает проверку race!-тегов в Get (и release в Ack/Reject/Share).
// Выкл (дефолт) — ноль доп. redis.call на горячем пути. Ping-reclaim снимает
// чужие race-локи всегда (редкий путь, иначе утечка после смерти consumer).
// Deprecated: NewConsumer(..., WithConcurrencyTags(enabled)).
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
	openBlocks     []string // race!b! ids → SET block:{id} ready
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

// WithSharedTTL — TTL для shared / Open(block ready) при Ack/Shared; 0 → DefaultSharedTTL.
func WithSharedTTL(ttl time.Duration) AckOption {
	return func(c *ackConfig) {
		if ttl < 0 {
			ttl = 0
		}
		c.sharedTTL = ttl
	}
}

// WithOpenBlocks помечает race!b!{id} как ready (parallel без лока) на TTL.
// ids — суффиксы или полные race!b!… теги.
func WithOpenBlocks(ids ...string) AckOption {
	return func(c *ackConfig) {
		if len(ids) == 0 {
			return
		}
		c.openBlocks = append([]string(nil), ids...)
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
