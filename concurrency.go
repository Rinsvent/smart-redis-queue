package redisqueue

import "strings"

// RaceTagPrefix — concurrency-ресурс в Task.Tags. Producer всегда индексирует
// такие теги в race:tags:{id}; проверка/локи в Get — только при SetConcurrencyTags(true).
const RaceTagPrefix = "race!"

// HasRaceTagPrefix — тег является concurrency-ресурсом.
func HasRaceTagPrefix(tag string) bool {
	return strings.HasPrefix(tag, RaceTagPrefix)
}

// SetConcurrencyTags включает проверку race!-тегов в Get (и release в Ack/Reject).
// Выкл (дефолт) — ноль доп. redis.call на горячем пути. Ping-reclaim снимает
// чужие race-локи всегда (редкий путь, иначе утечка после смерти consumer).
func (c *Consumer) SetConcurrencyTags(enabled bool) {
	c.concurrencyTags = enabled
}

// SetConcurrencyTags — то же для пула (наследуется consumer’ами при Consume).
func (p *ConsumerPool) SetConcurrencyTags(enabled bool) {
	p.concurrencyTags = enabled
}
