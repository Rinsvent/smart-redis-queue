package redisqueue

import (
	"context"
	"fmt"

	"github.com/redis/go-redis/v9"
)

// QueuesIndexKey — глобальный ZSET на инстансе Redis: member = имя очереди, score = total
// (ready + in-progress). Publish/Ack/Remove обновляют score в том же Lua.
// Каталог имён без SCAN queue:*:partitions; dest для Exchange (см. internal/docs/exchange-design.md).
const QueuesIndexKey = "srq:queues"

// QueueIndexEntry — очередь в глобальном индексе.
type QueueIndexEntry struct {
	Name  string
	Total int64
}

// EnsureQueue регистрирует очередь в индексе (ZADD NX score 0). Идемпотентно.
func EnsureQueue(ctx context.Context, rdb *redis.Client, queueName string) error {
	if rdb == nil || queueName == "" {
		return nil
	}
	if err := rdb.ZAddNX(ctx, QueuesIndexKey, redis.Z{Score: 0, Member: queueName}).Err(); err != nil {
		return fmt.Errorf("ensure queue %q: %w", queueName, err)
	}
	return nil
}
