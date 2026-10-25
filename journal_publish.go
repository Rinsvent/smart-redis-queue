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
