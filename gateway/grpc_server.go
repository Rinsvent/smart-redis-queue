package gateway

import (
	"context"
	"net"
	"time"

	redisqueue "github.com/Rinsvent/smart-redis-queue"
	gatewayv1 "github.com/Rinsvent/smart-redis-queue/gateway/internal/pb/gateway/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type grpcAPI struct {
	gatewayv1.UnimplementedGatewayServer
	g *Gateway
}

// ListenAndServeGRPC поднимает gRPC на addr (например ":50051").
func (g *Gateway) ListenAndServeGRPC(ctx context.Context, addr string) error {
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	srv := grpc.NewServer()
	gatewayv1.RegisterGatewayServer(srv, &grpcAPI{g: g})
	g.log.Printf("gRPC listen %s", addr)

	errCh := make(chan error, 1)
	go func() {
		errCh <- srv.Serve(lis)
	}()
	select {
	case <-ctx.Done():
		srv.GracefulStop()
		return ctx.Err()
	case err := <-errCh:
		return err
	}
}

func (a *grpcAPI) Health(ctx context.Context, _ *gatewayv1.HealthRequest) (*gatewayv1.HealthResponse, error) {
	p := a.g.Persist()
	return &gatewayv1.HealthResponse{
		State:        a.g.State().String(),
		AppendFsync:  p.AppendFsync,
		LossWindowNs: int64(p.LossWindow),
	}, nil
}

func (a *grpcAPI) Publish(ctx context.Context, req *gatewayv1.PublishRequest) (*gatewayv1.PublishResponse, error) {
	tasks := make([]*redisqueue.Task, 0, len(req.Tasks))
	for _, t := range req.Tasks {
		tasks = append(tasks, protoToTask(t))
	}
	if err := a.g.PublishTasks(ctx, req.Queue, tasks, nil); err != nil {
		return nil, mapErr(err)
	}
	return &gatewayv1.PublishResponse{}, nil
}

func (a *grpcAPI) Get(ctx context.Context, req *gatewayv1.GetRequest) (*gatewayv1.GetResponse, error) {
	tasks, err := a.g.GetTasks(ctx, req.Queue, req.ConsumerId, int(req.Prefetch))
	if err != nil {
		return nil, mapErr(err)
	}
	out := make([]*gatewayv1.Task, 0, len(tasks))
	for _, t := range tasks {
		out = append(out, taskToProto(t))
	}
	return &gatewayv1.GetResponse{Tasks: out}, nil
}

func (a *grpcAPI) Ack(ctx context.Context, req *gatewayv1.AckRequest) (*gatewayv1.AckResponse, error) {
	ack := &AckRec{
		IdempotencyTTLSec: req.IdempotencyTtlSec,
		Shared:            req.Shared,
		SharedTTLSec:      req.SharedTtlSec,
		OpenBlocks:        req.OpenBlocks,
	}
	if err := a.g.AckTask(ctx, req.Queue, req.ConsumerId, req.TaskId, ack); err != nil {
		return nil, mapErr(err)
	}
	return &gatewayv1.AckResponse{}, nil
}

func (a *grpcAPI) Reject(ctx context.Context, req *gatewayv1.RejectRequest) (*gatewayv1.RejectResponse, error) {
	rej := &RejectRec{
		Delay:         req.Delay,
		BlockGroups:   req.BlockGroups,
		BlockTags:     req.BlockTags,
		DeadLetter:    req.DeadLetter,
		DeadLetterMax: int(req.DeadLetterMax),
	}
	if err := a.g.RejectTask(ctx, req.Queue, req.ConsumerId, req.TaskId, rej); err != nil {
		return nil, mapErr(err)
	}
	return &gatewayv1.RejectResponse{}, nil
}

func mapErr(err error) error {
	switch err {
	case ErrRedisDown:
		return status.Error(codes.Unavailable, err.Error())
	case ErrRecovering, ErrDraining:
		return status.Error(codes.FailedPrecondition, err.Error())
	case ErrEmptyQueue, ErrEmptyTaskID:
		return status.Error(codes.InvalidArgument, err.Error())
	default:
		return status.Errorf(codes.Internal, "%v", err)
	}
}

func protoToTask(t *gatewayv1.Task) *redisqueue.Task {
	if t == nil {
		return &redisqueue.Task{}
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
