package redisqueue

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	errJournalCursorRewindLua = "ERR_JOURNAL_CURSOR_REWIND"
	journalARGVMarker         = "jn"
)

// ResumeAfterKey — stamp воркеров после recovery (unix ms).
const ResumeAfterKey = "srq:resume_after"

// JournalCursorKey — курсор bridge для имени журнала.
func JournalCursorKey(name string) string {
	return "srq:journal:" + name + ":cursor"
}

// ErrJournalCursorRewind — новый cursor не больше текущего в Redis.
var ErrJournalCursorRewind = fmt.Errorf("journal: cursor rewind forbidden")

// PublishJournalBatch пишет задачи в очередь и атомарно ставит cursor, если он строго больше текущего.
func PublishJournalBatch(ctx context.Context, rdb *redis.Client, queueName, journalName, newCursor string, tasks []*Task) error {
	if rdb == nil || queueName == "" {
		return fmt.Errorf("journal publish: redis and queue required")
	}
	if len(tasks) == 0 {
		return nil
	}
	args := make([]interface{}, 0, 6+len(tasks)*7)
	args = append(args, journalARGVMarker, JournalCursorKey(journalName), newCursor, queueName, len(tasks), 0)
	now := time.Now()
	for _, task := range tasks {
		if task.ID == "" {
			return fmt.Errorf("task ID is required")
		}
		scheduled := task.Scheduled.UnixMilli()
		if scheduled == 0 {
			scheduled = now.UnixMilli()
		}
		tags, err := encodeTags(task.Tags)
		if err != nil {
			return err
		}
		groups, err := encodeGroups(task.Groups)
		if err != nil {
			return err
		}
		payload, err := encodePayloadForRedis(task.Payload, CompressionNone, 0)
		if err != nil {
			return err
		}
		args = append(args, task.ID, task.Partition, task.Priority, scheduled, payload, tags, groups)
	}
	result, err := getAddScript().Run(ctx, rdb, []string{}, args...).Result()
	if err != nil {
		if isJournalCursorRewind(err) {
			return ErrJournalCursorRewind
		}
		return fmt.Errorf("journal publish: %w", err)
	}
	notAdded, ok := result.([]interface{})
	if !ok {
		return nil
	}
	if len(notAdded) == 0 {
		return nil
	}
	if s, _ := notAdded[0].(string); s == "!" {
		return parseBackpressureResult(notAdded)
	}
	return nil
}

func isJournalCursorRewind(err error) bool {
	return err != nil && strings.Contains(err.Error(), errJournalCursorRewindLua)
}

// SetResumeAfter пишет stamp (unix ms). Воркеры с faultAt < stamp продолжают работу.
func SetResumeAfter(ctx context.Context, rdb *redis.Client, at time.Time) error {
	if rdb == nil {
		return fmt.Errorf("resume: redis required")
	}
	return rdb.Set(ctx, ResumeAfterKey, at.UnixMilli(), 0).Err()
}

// GetResumeAfter читает stamp; ok=false если ключа нет.
func GetResumeAfter(ctx context.Context, rdb *redis.Client) (time.Time, bool, error) {
	s, err := rdb.Get(ctx, ResumeAfterKey).Result()
	if err == redis.Nil {
		return time.Time{}, false, nil
	}
	if err != nil {
		return time.Time{}, false, err
	}
	var ms int64
	if _, err := fmt.Sscan(s, &ms); err != nil {
		return time.Time{}, false, err
	}
	return time.UnixMilli(ms), true, nil
}
