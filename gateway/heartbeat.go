package gateway

import (
	"context"
	"fmt"

	redisqueue "github.com/Rinsvent/smart-redis-queue"
)

// trackConsumer регистрирует consumer для централизованного ping.
func (g *Gateway) trackConsumer(c *redisqueue.Consumer) {
	if c == nil {
		return
	}
	g.hbMu.Lock()
	if g.hbConsumers == nil {
		g.hbConsumers = make(map[*redisqueue.Consumer]struct{})
	}
	g.hbConsumers[c] = struct{}{}
	g.hbMu.Unlock()
}

func (g *Gateway) pingAll(ctx context.Context) {
	g.hbMu.Lock()
	list := make([]*redisqueue.Consumer, 0, len(g.hbConsumers))
	for c := range g.hbConsumers {
		list = append(list, c)
	}
	g.hbMu.Unlock()
	for _, c := range list {
		if err := c.Ping(ctx); err != nil {
			g.noteRedisErr(err)
		}
	}
}

// fenceLocks — renew heartbeat известных consumer’ов до replay (reclaim не должен обогнать Ack).
// Любой transport-error → fail recover (fail-closed).
func (g *Gateway) fenceLocks(ctx context.Context) error {
	seen := map[string]struct{}{}
	for _, op := range g.ops.snapshot() {
		if op.Consumer == "" || op.Queue == "" {
			continue
		}
		key := op.Queue + "\x00" + op.Consumer
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		clients := g.redisClients()
		for i, rdb := range clients {
			idx := -1
			if len(clients) > 1 {
				idx = i
			}
			c := g.consumerOn(rdb, op.Queue, op.Consumer, idx)
			if err := c.Ping(ctx); err != nil {
				return fmt.Errorf("fence ping %s/%s shard=%d: %w", op.Queue, op.Consumer, i, err)
			}
		}
	}
	return nil
}
