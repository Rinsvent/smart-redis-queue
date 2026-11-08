// Package client — Go SDK к SRQ Gateway (gRPC). Прямой Redis SDK не заменяется.
package client

import (
	"context"
	"fmt"
	"time"

	redisqueue "github.com/Rinsvent/smart-redis-queue"
	gatewayv1 "github.com/Rinsvent/smart-redis-queue/gateway/internal/pb/gateway/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Conn — gRPC-клиент gateway.
type Conn struct {
	cc  *grpc.ClientConn
	api gatewayv1.GatewayClient
}

// Dial подключается к gateway gRPC (например "127.0.0.1:50051"), не к HTTP ops.
func Dial(ctx context.Context, addr string) (*Conn, error) {
	cc, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	_ = ctx
	return &Conn{cc: cc, api: gatewayv1.NewGatewayClient(cc)}, nil
}

func (c *Conn) Close() error {
	if c.cc == nil {
		return nil
	}
	return c.cc.Close()
}

func (c *Conn) Health(ctx context.Context) (*gatewayv1.HealthResponse, error) {
	return c.api.Health(ctx, &gatewayv1.HealthRequest{})
}

// Producer публикует задачи через gateway.
type Producer struct {
	conn  *Conn
	queue string
}

func (c *Conn) Producer(queue string) *Producer {
	return &Producer{conn: c, queue: queue}
}

func (p *Producer) Publish(ctx context.Context, tasks ...*redisqueue.Task) error {
	req := &gatewayv1.PublishRequest{Queue: p.queue}
	for _, t := range tasks {
		req.Tasks = append(req.Tasks, taskToProto(t))
	}
	_, err := p.conn.api.Publish(ctx, req)
	return err
}

// Consumer получает / Ack / Reject через gateway.
type Consumer struct {
	conn       *Conn
	queue      string
	consumerID string
	prefetch   int32
}

func (c *Conn) Consumer(queue, consumerID string) *Consumer {
	return &Consumer{conn: c, queue: queue, consumerID: consumerID, prefetch: 5}
}

func (c *Consumer) SetPrefetchCount(n int) {
	if n < 1 {
		n = 1
	}
	c.prefetch = int32(n)
}

func (c *Consumer) Get(ctx context.Context) ([]*redisqueue.Task, error) {
	resp, err := c.conn.api.Get(ctx, &gatewayv1.GetRequest{
		Queue:      c.queue,
		ConsumerId: c.consumerID,
		Prefetch:   c.prefetch,
	})
	if err != nil {
		return nil, err
	}
	out := make([]*redisqueue.Task, 0, len(resp.Tasks))
	for _, t := range resp.Tasks {
		out = append(out, protoToTask(t))
	}
	return out, nil
}

// Ack подтверждает задачу. Опции зеркалят gateway AckRequest.
func (c *Consumer) Ack(ctx context.Context, taskID string, opts ...AckOption) error {
	req := &gatewayv1.AckRequest{
		Queue:      c.queue,
		ConsumerId: c.consumerID,
		TaskId:     taskID,
	}
	for _, o := range opts {
		o(req)
	}
	_, err := c.conn.api.Ack(ctx, req)
	return err
}

// Reject отклоняет задачу.
func (c *Consumer) Reject(ctx context.Context, taskID string, opts ...RejectOption) error {
	req := &gatewayv1.RejectRequest{
		Queue:         c.queue,
		ConsumerId:    c.consumerID,
		TaskId:        taskID,
		DeadLetterMax: -1,
	}
	for _, o := range opts {
		o(req)
	}
	_, err := c.conn.api.Reject(ctx, req)
	return err
}

type AckOption func(*gatewayv1.AckRequest)

func WithIdempotencyTTL(sec float64) AckOption {
	return func(r *gatewayv1.AckRequest) { r.IdempotencyTtlSec = sec }
}

func WithShared(shared map[string][]byte) AckOption {
	return func(r *gatewayv1.AckRequest) { r.Shared = shared }
}

func WithSharedTTL(sec float64) AckOption {
	return func(r *gatewayv1.AckRequest) { r.SharedTtlSec = sec }
}

func WithOpenBlocks(ids ...string) AckOption {
	return func(r *gatewayv1.AckRequest) { r.OpenBlocks = append(r.OpenBlocks, ids...) }
}

type RejectOption func(*gatewayv1.RejectRequest)

func WithDelay(sec float64) RejectOption {
	return func(r *gatewayv1.RejectRequest) { r.Delay = sec }
}

func WithBlockGroups(groups ...string) RejectOption {
	return func(r *gatewayv1.RejectRequest) { r.BlockGroups = append(r.BlockGroups, groups...) }
}

func WithBlockTags(tags ...string) RejectOption {
	return func(r *gatewayv1.RejectRequest) { r.BlockTags = append(r.BlockTags, tags...) }
}

func WithDeadLetter(maxSize ...int) RejectOption {
	return func(r *gatewayv1.RejectRequest) {
		r.DeadLetter = true
		if len(maxSize) == 0 {
			r.DeadLetterMax = -1
			return
		}
		r.DeadLetterMax = int32(maxSize[0])
	}
}

func taskToProto(t *redisqueue.Task) *gatewayv1.Task {
	if t == nil {
		return nil
	}
	var ms int64
	if !t.Scheduled.IsZero() {
		ms = t.Scheduled.UnixMilli()
	}
	return &gatewayv1.Task{
		Id:                t.ID,
		Partition:         t.Partition,
		PartitionPriority: int32(t.PartitionPriority),
		QueuePriority:     int32(t.QueuePriority),
		Payload:           t.Payload,
		Tags:              t.Tags,
		Groups:            t.Groups,
		ScheduledUnixMs:   ms,
		RejectCount:       int32(t.RejectCount),
	}
}

func protoToTask(t *gatewayv1.Task) *redisqueue.Task {
	if t == nil {
		return nil
	}
	var sched time.Time
	if t.ScheduledUnixMs > 0 {
		sched = time.UnixMilli(t.ScheduledUnixMs)
	}
	return &redisqueue.Task{
		ID:                t.Id,
		Partition:         t.Partition,
		PartitionPriority: int(t.PartitionPriority),
		QueuePriority:     int(t.QueuePriority),
		Payload:           t.Payload,
		Tags:              t.Tags,
		Groups:            t.Groups,
		Scheduled:         sched,
		RejectCount:       int(t.RejectCount),
	}
}

// MustDial — Dial или panic (удобно в примерах).
func MustDial(addr string) *Conn {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	c, err := Dial(ctx, addr)
	if err != nil {
		panic(fmt.Errorf("gateway dial %s: %w", addr, err))
	}
	return c
}
