package redisqueue

import (
	"context"
	"fmt"
	"sort"
)

// SetShards включает шардинг на Producer. redis-клиент из NewProducer после этого не используется.
// Один и тот же ShardConfig должен быть у producer и ConsumerPool.
func (p *Producer) SetShards(cfg *ShardConfig) error {
	if p.ownRouter && p.router != nil {
		_ = p.router.Close()
	}
	router, err := NewShardRouter(cfg)
	if err != nil {
		return err
	}
	clients := router.Clients()
	pubs := make([]*Producer, len(clients))
	for i, c := range clients {
		pubs[i] = &Producer{
			redis:            c,
			queueName:        p.queueName,
			compression:      p.compression,
			compressMinBytes: p.compressMinBytes,
		}
	}
	p.router = router
	p.ownRouter = true
	p.shardPubs = pubs
	return nil
}

// SetShardRouter — общий роутер (Close не закрывает его).
func (p *Producer) SetShardRouter(router *ShardRouter) {
	if p.ownRouter && p.router != nil {
		_ = p.router.Close()
	}
	p.router = router
	p.ownRouter = false
	if router == nil {
		p.shardPubs = nil
		return
	}
	clients := router.Clients()
	pubs := make([]*Producer, len(clients))
	for i, c := range clients {
		pubs[i] = &Producer{
			redis:            c,
			queueName:        p.queueName,
			compression:      p.compression,
			compressMinBytes: p.compressMinBytes,
		}
	}
	p.shardPubs = pubs
}

// ShardRouter возвращает роутер или nil.
func (p *Producer) ShardRouter() *ShardRouter {
	return p.router
}

// Close закрывает роутер, созданный через SetShards.
func (p *Producer) Close() error {
	if p.ownRouter && p.router != nil {
		err := p.router.Close()
		p.router = nil
		p.shardPubs = nil
		return err
	}
	return nil
}

// PublishManual — явный shard key для всего батча (нужен при ShardByManual).
func (p *Producer) PublishManual(ctx context.Context, shardKey string, tasks ...*Task) error {
	return p.PublishWith(ctx, []PublishOption{WithShardKey(shardKey)}, tasks...)
}

// PublishTo пишет все задачи на именованный шард.
func (p *Producer) PublishTo(ctx context.Context, shardName string, tasks ...*Task) error {
	return p.PublishWith(ctx, []PublishOption{WithShardName(shardName)}, tasks...)
}

// LockTo — Lock на именованном шарде.
func (p *Producer) LockTo(ctx context.Context, shardName string, ids ...string) error {
	if p.router == nil {
		return fmt.Errorf("LockTo: sharding is not configured")
	}
	_, _, idx, err := p.router.ResolveByName(shardName, true)
	if err != nil {
		return err
	}
	return p.shardPubs[idx].lockLocal(ctx, ids)
}

func (p *Producer) publishSharded(ctx context.Context, opts []PublishOption, tasks []*Task) error {
	cfg := &publishConfig{compression: p.compression, compressMinBytes: p.compressMinBytes}
	cfg.apply(opts)
	if err := cfg.validate(); err != nil {
		return err
	}
	if err := validateManualKeyIfSet(cfg.shardKey); err != nil {
		return err
	}

	type group struct {
		idx   int
		tasks []*Task
	}
	byIdx := make(map[int]*group)
	for _, task := range tasks {
		idx, err := p.resolveTaskIndex(task, cfg)
		if err != nil {
			return err
		}
		g, ok := byIdx[idx]
		if !ok {
			g = &group{idx: idx}
			byIdx[idx] = g
		}
		g.tasks = append(g.tasks, task)
	}

	order := make([]int, 0, len(byIdx))
	for idx := range byIdx {
		order = append(order, idx)
	}
	sort.Ints(order)

	for _, idx := range order {
		if err := p.shardPubs[idx].publishLocal(ctx, opts, byIdx[idx].tasks); err != nil {
			return err
		}
	}
	return nil
}

func (p *Producer) resolveTaskIndex(task *Task, cfg *publishConfig) (int, error) {
	if cfg.shardName != "" {
		_, _, idx, err := p.router.ResolveByName(cfg.shardName, true)
		return idx, err
	}
	key, err := p.router.ShardKeyForTask(p.queueName, task, cfg.shardKey)
	if err != nil {
		return -1, err
	}
	_, _, idx, err := p.router.ResolveWrite(key)
	return idx, err
}

func validateManualKeyIfSet(key string) error {
	if key == "" {
		return nil
	}
	return validateManualKey(key)
}

func (p *Producer) lockSharded(ctx context.Context, ids []string) error {
	key, err := p.router.ShardKeyForTask(p.queueName, nil, "")
	if err != nil {
		return fmt.Errorf("Lock: %w (use LockTo for explicit shard)", err)
	}
	_, _, idx, err := p.router.ResolveWrite(key)
	if err != nil {
		return err
	}
	return p.shardPubs[idx].lockLocal(ctx, ids)
}
