package redisqueue

import (
	"fmt"
	"time"
)

func bool01(v bool) string {
	if v {
		return "1"
	}
	return "0"
}

// Поддерживаемые Algorithm (как в github.com/Rinsvent/rate-limiter).
const (
	RateLimitTokenBucket          = "token-bucket"
	RateLimitLeakyBucket          = "leaky-bucket"
	RateLimitFixedWindowCounter   = "fixed-window-counter"
	RateLimitSlidingWindowLog     = "sliding-window-log"
	RateLimitSlidingWindowCounter = "sliding-window-counter"
)

// Rate — окно лимита: не более Value событий за Duration (AND по списку окон).
type Rate struct {
	Duration time.Duration
	Value    int
}

// RateLimitLayer — один слой (group или partition). nil / пустые Rates = слой выключен.
type RateLimitLayer struct {
	Rates []Rate
}

// RateLimitConfig — opt-in лимит на Consumer Get (два слоя AND).
// Algorithm: см. константы RateLimit*; пусто = token-bucket.
type RateLimitConfig struct {
	Algorithm string
	Group     *RateLimitLayer
	Partition *RateLimitLayer
}

func (c *RateLimitConfig) enabled() bool {
	return c != nil && (layerEnabled(c.Group) || layerEnabled(c.Partition))
}

func layerEnabled(l *RateLimitLayer) bool {
	return l != nil && len(l.Rates) > 0
}

func normalizeAlgorithm(algo string) string {
	if algo == "" {
		return RateLimitTokenBucket
	}
	return algo
}

func (c *RateLimitConfig) validate() error {
	if c == nil || !c.enabled() {
		return nil
	}
	switch normalizeAlgorithm(c.Algorithm) {
	case RateLimitTokenBucket, RateLimitLeakyBucket, RateLimitFixedWindowCounter,
		RateLimitSlidingWindowLog, RateLimitSlidingWindowCounter:
	default:
		return fmt.Errorf("rate limit: unsupported algorithm %q", c.Algorithm)
	}
	for _, l := range []*RateLimitLayer{c.Group, c.Partition} {
		if !layerEnabled(l) {
			continue
		}
		for _, r := range l.Rates {
			if r.Value <= 0 || r.Duration <= 0 {
				return fmt.Errorf("rate limit: each Rate needs Value>0 and Duration>0")
			}
		}
	}
	return nil
}

// appendGetRateLimitArgs: algorithm, gRateN, (durMs, value)*, pRateN, (durMs, value)*
// Выкл: "", 0, 0 — без RL call в Lua.
func appendGetRateLimitArgs(args []interface{}, cfg *RateLimitConfig) []interface{} {
	if cfg == nil || !cfg.enabled() {
		return append(args, "", 0, 0)
	}
	args = append(args, normalizeAlgorithm(cfg.Algorithm))
	args = appendLayerRates(args, cfg.Group)
	args = appendLayerRates(args, cfg.Partition)
	return args
}

func appendLayerRates(args []interface{}, l *RateLimitLayer) []interface{} {
	if !layerEnabled(l) {
		return append(args, 0)
	}
	args = append(args, len(l.Rates))
	for _, r := range l.Rates {
		args = append(args, r.Duration.Milliseconds(), r.Value)
	}
	return args
}

// SetRateLimit включает транспортный rate limit на Get (group и/или partition).
// nil или пустые слои — выключить. Копия конфига; изменение cfg после вызова не влияет.
func (c *Consumer) SetRateLimit(cfg *RateLimitConfig) error {
	if cfg == nil || !cfg.enabled() {
		c.rateLimit = nil
		return nil
	}
	if err := cfg.validate(); err != nil {
		return err
	}
	c.rateLimit = cloneRateLimitConfig(cfg)
	return nil
}

func cloneRateLimitConfig(cfg *RateLimitConfig) *RateLimitConfig {
	out := &RateLimitConfig{Algorithm: normalizeAlgorithm(cfg.Algorithm)}
	if layerEnabled(cfg.Group) {
		out.Group = &RateLimitLayer{Rates: append([]Rate(nil), cfg.Group.Rates...)}
	}
	if layerEnabled(cfg.Partition) {
		out.Partition = &RateLimitLayer{Rates: append([]Rate(nil), cfg.Partition.Rates...)}
	}
	return out
}

// SetRateLimit — то же для пула (наследуется каждым consumer при старте Consume).
func (p *ConsumerPool) SetRateLimit(cfg *RateLimitConfig) error {
	if cfg == nil || !cfg.enabled() {
		p.rateLimit = nil
		return nil
	}
	if err := cfg.validate(); err != nil {
		return err
	}
	p.rateLimit = cloneRateLimitConfig(cfg)
	return nil
}
