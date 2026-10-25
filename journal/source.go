package journal

import (
	"context"
	"time"

	redisqueue "github.com/Rinsvent/smart-redis-queue"
)

type ResumeMode int

const (
	ResumeAuto ResumeMode = iota
	ResumeManual
)

type Record struct {
	Task   *redisqueue.Task
	Cursor Cursor
}

type AckEvent struct {
	TaskID string
	Cursor Cursor
	At     time.Time
}

type Source interface {
	Init(ctx context.Context) error
	ReadBatch(ctx context.Context, after Cursor, n int) ([]Record, error)
	ReadByOffset(ctx context.Context, c Cursor) (Record, error)
	Ack(ctx context.Context, taskID string, meta map[string]string) error
	ReadAcks(ctx context.Context, from Cursor, n int) ([]AckEvent, error)
}
