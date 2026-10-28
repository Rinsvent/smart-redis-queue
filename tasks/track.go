package tasks

import (
	"context"

	"github.com/redis/go-redis/v9"
)

// TrackStep — скопировать queue:{q}:step:{taskID} в run HASH (после Advance/Flush).
func (r *Registry) TrackStep(ctx context.Context, queue, baseName, taskID string) error {
	if r == nil {
		return nil
	}
	raw, err := r.rdb.Get(ctx, "queue:"+queue+":step:"+taskID).Result()
	if err == redis.Nil {
		return nil
	}
	if err != nil {
		return err
	}
	return r.MirrorProgress(ctx, baseName, raw)
}
