package redisqueue

import (
	"context"
	"sync"
)

// SetShards включает шардинг на пуле: Consume слушает все шарды (включая ReadOnly).
// redis-клиент из NewConsumerPool после этого не используется.
func (p *ConsumerPool) SetShards(cfg *ShardConfig) error {
	if p.ownRouter && p.router != nil {
		_ = p.router.Close()
	}
	router, err := NewShardRouter(cfg)
	if err != nil {
		return err
	}
	p.applyShardRouter(router, true)
	return nil
}

// SetShardRouter — общий роутер (Close не закрывает его).
func (p *ConsumerPool) SetShardRouter(router *ShardRouter) {
	if p.ownRouter && p.router != nil {
		_ = p.router.Close()
	}
	p.applyShardRouter(router, false)
}

func (p *ConsumerPool) applyShardRouter(router *ShardRouter, own bool) {
	p.router = router
	p.ownRouter = own
	p.stats = nil
	if router == nil {
		p.shardStats = nil
		return
	}
	clients := router.Clients()
	p.shardStats = make([]*StatsSink, len(clients))
	for i, c := range clients {
		s := newStatsSink(c, p.queueName)
		if p.statsRetention > 0 {
			s.setRetention(p.statsRetention)
		}
		if len(p.statsTags) > 0 {
			s.setTags(p.statsTags)
		}
		p.shardStats[i] = s
	}
	if p.count < len(clients) {
		p.count = len(clients) // минимум по воркеру на шард по умолчанию при позднем SetShards
	}
}

// ShardRouter возвращает роутер или nil.
func (p *ConsumerPool) ShardRouter() *ShardRouter {
	return p.router
}

// Close закрывает роутер, созданный через SetShards.
func (p *ConsumerPool) Close() error {
	if p.ownRouter && p.router != nil {
		err := p.router.Close()
		p.router = nil
		p.shardStats = nil
		return err
	}
	return nil
}

func (p *ConsumerPool) consumeSharded(ctx context.Context, handler func(*Task) error) {
	clients := p.router.Clients()
	counts := DistributeWorkers(p.count, len(clients))

	for _, s := range p.shardStats {
		if s != nil && s.enabled() {
			s.ensureLoop()
		}
	}
	defer func() {
		for _, s := range p.shardStats {
			if s != nil {
				s.Stop()
			}
		}
	}()

	var wg sync.WaitGroup
	for i, client := range clients {
		n := counts[i]
		if n < 1 {
			continue
		}
		var stats *StatsSink
		if i < len(p.shardStats) {
			stats = p.shardStats[i]
		}
		for w := 0; w < n; w++ {
			wg.Add(1)
			go p.runConsumer(ctx, &wg, handler, client, stats)
		}
	}
	wg.Wait()
}
