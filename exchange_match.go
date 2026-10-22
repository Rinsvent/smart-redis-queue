package redisqueue

import (
	"strings"
)

// ExchangeKind — алгоритм матчинга bindings (не хранится в Redis).
type ExchangeKind int

const (
	// ExchangeDirect — точное совпадение routing key с pattern.
	ExchangeDirect ExchangeKind = iota + 1
	// ExchangeFanout — все bound очереди; routing key игнорируется.
	ExchangeFanout
	// ExchangeTopic — AMQP-like: * = один сегмент, # = ноль или более сегментов (через '.').
	ExchangeTopic
)

// Binding — привязка pattern → очередь.
type Binding struct {
	Pattern string
	Queue   string
}

const bindingFieldSep = "\x1f"

func bindingField(pattern, queue string) string {
	return pattern + bindingFieldSep + queue
}

func parseBindingField(field string) (pattern, queue string, ok bool) {
	i := strings.IndexByte(field, bindingFieldSep[0])
	if i < 0 {
		return "", "", false
	}
	return field[:i], field[i+1:], true
}

// matchBindings возвращает уникальные имена очередей для kind+routingKey.
func matchBindings(kind ExchangeKind, routingKey string, bindings []Binding) []string {
	seen := make(map[string]struct{}, len(bindings))
	out := make([]string, 0, len(bindings))
	add := func(q string) {
		if q == "" {
			return
		}
		if _, ok := seen[q]; ok {
			return
		}
		seen[q] = struct{}{}
		out = append(out, q)
	}

	switch kind {
	case ExchangeFanout:
		for _, b := range bindings {
			add(b.Queue)
		}
	case ExchangeDirect:
		for _, b := range bindings {
			if b.Pattern == routingKey {
				add(b.Queue)
			}
		}
	case ExchangeTopic:
		for _, b := range bindings {
			if topicMatch(b.Pattern, routingKey) {
				add(b.Queue)
			}
		}
	}
	return out
}

// topicMatch — AMQP topic: сегменты через '.'; * один сегмент; # остаток (в т.ч. пустой).
func topicMatch(pattern, key string) bool {
	return topicMatchParts(splitDot(pattern), splitDot(key))
}

func splitDot(s string) []string {
	if s == "" {
		return []string{}
	}
	return strings.Split(s, ".")
}

func topicMatchParts(pat, key []string) bool {
	pi, ki := 0, 0
	for pi < len(pat) && ki < len(key) {
		p := pat[pi]
		if p == "#" {
			// # в конце — съедает всё
			if pi == len(pat)-1 {
				return true
			}
			// # не в конце: пробуем съесть 0..n сегментов key, пока хвост не совпадёт
			for n := 0; n <= len(key)-ki; n++ {
				if topicMatchParts(pat[pi+1:], key[ki+n:]) {
					return true
				}
			}
			return false
		}
		if p == "*" || p == key[ki] {
			pi++
			ki++
			continue
		}
		return false
	}
	// остаток pattern только из #
	for pi < len(pat) {
		if pat[pi] != "#" {
			return false
		}
		pi++
	}
	return ki == len(key)
}
