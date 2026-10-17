package redisqueue

import (
	"fmt"
	"strings"
)

// ErrBackpressure — Publish отклонён: глубина тега (SCARD) + размер батча превысили лимит.
// Ни одна задача из вызова не добавлена.
type ErrBackpressure struct {
	Tag   string
	Count int64 // SCARD до попытки
	Limit int64
}

func (e *ErrBackpressure) Error() string {
	return fmt.Sprintf("backpressure: tag %q count=%d limit=%d", e.Tag, e.Count, e.Limit)
}

// PublishOption — opt-in опции PublishWith (backpressure, compression и т.п.).
type PublishOption func(*publishConfig)

type publishConfig struct {
	tagLimits        map[string]int64 // tag → max pending (SCARD+batch ≤ max)
	compression      Compression
	compressMinBytes int
}

// WithCompression — сжатие payload для этого PublishWith (перекрывает Producer.SetCompression).
func WithCompression(c Compression) PublishOption {
	return func(cfg *publishConfig) {
		cfg.compression = c
	}
}

// WithCompressMinBytes — порог длины для WithCompression / SetCompression на этом вызове.
func WithCompressMinBytes(n int) PublishOption {
	return func(cfg *publishConfig) {
		cfg.compressMinBytes = n
	}
}

// WithTagBackpressure ограничивает Publish: если SCARD(tag)+len(batch) > max, весь батч отклоняется.
func WithTagBackpressure(tag string, max int64) PublishOption {
	return func(c *publishConfig) {
		if tag == "" || max <= 0 {
			return
		}
		if c.tagLimits == nil {
			c.tagLimits = make(map[string]int64)
		}
		c.tagLimits[tag] = max
	}
}

// WithTagLimits — несколько лимитов (AND: срабатывает первый превышенный).
// Можно комбинировать с WithTagBackpressure.
func WithTagLimits(limits map[string]int64) PublishOption {
	return func(c *publishConfig) {
		if len(limits) == 0 {
			return
		}
		if c.tagLimits == nil {
			c.tagLimits = make(map[string]int64, len(limits))
		}
		for tag, max := range limits {
			if tag == "" || max <= 0 {
				continue
			}
			c.tagLimits[tag] = max
		}
	}
}

func (c *publishConfig) apply(opts []PublishOption) {
	for _, opt := range opts {
		if opt != nil {
			opt(c)
		}
	}
}

func (c *publishConfig) validate() error {
	for tag := range c.tagLimits {
		if strings.Contains(tag, TagSeparator) {
			return fmt.Errorf("backpressure tag %q contains forbidden separator %q", tag, TagSeparator)
		}
	}
	return nil
}

// bpArgs — ARGV-хвост для Lua: bpCount, tag, limit, ...
func (c *publishConfig) bpArgs() []interface{} {
	if len(c.tagLimits) == 0 {
		return []interface{}{0}
	}
	args := make([]interface{}, 0, 1+2*len(c.tagLimits))
	args = append(args, len(c.tagLimits))
	for tag, max := range c.tagLimits {
		args = append(args, tag, max)
	}
	return args
}
