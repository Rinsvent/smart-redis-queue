package redisqueue

import (
	"context"
	"strconv"

	"github.com/redis/go-redis/v9"
)

// HotPriorityKey — ключ порога hot-приоритета очереди.
func HotPriorityKey(queueName string) string {
	return "queue:" + queueName + ":hot_priority"
}

// SetHotPriority задаёт порог hot-приоритета очереди; priority <= 0 выключает.
//
// Приоритет Task.Priority упорядочивает задачи внутри партиции, а партиции группы
// Get обходит по кругу с random start. При тысячах партиций в группе задача с
// высоким приоритетом ждёт, пока обход дойдёт до её партиции. С порогом партиция,
// где есть задача с Priority >= порога, попадает в group:{g}:hot, и Get в каждой
// группе сначала берёт задачи из hot-партиций.
//
// Порог хранится в Redis: его читают скрипты Publish, Get, Reject и ping, поэтому
// продюсерам и консьюмерам отдельная настройка не нужна. Задачи, поставленные до
// включения порога, в hot не попадают — до первого Reject или новой задачи партиции.
func SetHotPriority(ctx context.Context, rdb redis.Cmdable, queueName string, priority int) error {
	if priority <= 0 {
		return rdb.Del(ctx, HotPriorityKey(queueName)).Err()
	}
	return rdb.Set(ctx, HotPriorityKey(queueName), strconv.Itoa(priority), 0).Err()
}
