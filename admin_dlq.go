package redisqueue

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

// MaxListDLQ — лимит Admin.ListDLQ.
const MaxListDLQ = 100

// DLQEntry — задача в dead letter queue (meta жива; не в pending/in-progress).
type DLQEntry struct {
	ID            string
	Partition     string
	RejectCount   int64
	DeadLetterAt  time.Time // ZSET score = время попадания в DLQ
	OriginalScore float64   // исходный score для RetryDLQ
}

func dlqKey(queueName string) string {
	return "queue:" + queueName + ":dlq"
}

func dlqScoreKey(queueName, taskID string) string {
	return "queue:" + queueName + ":dlq:score:" + taskID
}

// CountDLQ — число задач в queue:{name}:dlq.
func (a *Admin) CountDLQ(ctx context.Context, queueName string) (int64, error) {
	if queueName == "" {
		return 0, fmt.Errorf("admin: empty queue")
	}
	return a.redis.ZCard(ctx, dlqKey(queueName)).Result()
}

// ListDLQ — страница задач из DLQ (по возрастанию DeadLetterAt). offset/limit; limit≤0 → MaxListDLQ.
func (a *Admin) ListDLQ(ctx context.Context, queueName string, offset, limit int64) ([]DLQEntry, error) {
	if queueName == "" {
		return nil, fmt.Errorf("admin: empty queue")
	}
	if limit <= 0 || limit > MaxListDLQ {
		limit = MaxListDLQ
	}
	if offset < 0 {
		offset = 0
	}
	zs, err := a.redis.ZRangeWithScores(ctx, dlqKey(queueName), offset, offset+limit-1).Result()
	if err != nil {
		return nil, err
	}
	out := make([]DLQEntry, 0, len(zs))
	for _, z := range zs {
		id, _ := z.Member.(string)
		if id == "" {
			continue
		}
		e := DLQEntry{
			ID:           id,
			DeadLetterAt: time.UnixMilli(int64(z.Score)),
		}
		e.Partition, _ = a.redis.Get(ctx, "queue:"+queueName+":partition:"+id).Result()
		if rejStr, err := a.redis.Get(ctx, "queue:"+queueName+":reject_count:"+id).Result(); err == nil && rejStr != "" {
			e.RejectCount, _ = strconv.ParseInt(rejStr, 10, 64)
		}
		if s, err := a.redis.Get(ctx, dlqScoreKey(queueName, id)).Result(); err == nil && s != "" {
			e.OriginalScore, _ = strconv.ParseFloat(s, 64)
		}
		out = append(out, e)
	}
	return out, nil
}

// RetryDLQ возвращает задачи из DLQ в pending (те же partition/priority/score).
// Пустой taskIDs — все записи DLQ. srq:queues +N. Meta не трогаем.
func (a *Admin) RetryDLQ(ctx context.Context, queueName string, taskIDs ...string) (int, error) {
	if queueName == "" {
		return 0, fmt.Errorf("admin: empty queue")
	}
	ids, err := a.dlqTaskIDs(ctx, queueName, taskIDs)
	if err != nil {
		return 0, err
	}
	count := 0
	for _, id := range ids {
		ok, err := a.retryOneDLQ(ctx, queueName, id)
		if err != nil {
			return count, err
		}
		if ok {
			count++
		}
	}
	return count, nil
}

// PurgeDLQ удаляет задачи из DLQ вместе с meta (как Ack). Пустой taskIDs — всё.
// srq:queues не меняем (−1 уже на Reject→DLQ).
func (a *Admin) PurgeDLQ(ctx context.Context, queueName string, taskIDs ...string) (int, error) {
	if queueName == "" {
		return 0, fmt.Errorf("admin: empty queue")
	}
	ids, err := a.dlqTaskIDs(ctx, queueName, taskIDs)
	if err != nil {
		return 0, err
	}
	count := 0
	for _, id := range ids {
		ok, err := a.purgeOneDLQ(ctx, queueName, id)
		if err != nil {
			return count, err
		}
		if ok {
			count++
		}
	}
	return count, nil
}

func (a *Admin) dlqTaskIDs(ctx context.Context, queueName string, taskIDs []string) ([]string, error) {
	if len(taskIDs) > 0 {
		out := make([]string, 0, len(taskIDs))
		seen := map[string]struct{}{}
		for _, id := range taskIDs {
			if id == "" {
				continue
			}
			if _, ok := seen[id]; ok {
				continue
			}
			seen[id] = struct{}{}
			out = append(out, id)
		}
		return out, nil
	}
	return a.redis.ZRange(ctx, dlqKey(queueName), 0, -1).Result()
}

func (a *Admin) retryOneDLQ(ctx context.Context, queueName, taskID string) (bool, error) {
	n, err := a.redis.ZRem(ctx, dlqKey(queueName), taskID).Result()
	if err != nil {
		return false, err
	}
	if n == 0 {
		_ = a.redis.Del(ctx, dlqScoreKey(queueName, taskID)).Err()
		return false, nil
	}
	score := a.nowMs(ctx)
	if s, err := a.redis.Get(ctx, dlqScoreKey(queueName, taskID)).Result(); err == nil && s != "" {
		if v, err := strconv.ParseFloat(s, 64); err == nil {
			score = v
		}
	}
	_ = a.redis.Del(ctx, dlqScoreKey(queueName, taskID)).Err()
	if err := a.enqueuePending(ctx, queueName, taskID, score); err != nil {
		// вернуть в DLQ, чтобы не потерять
		_ = a.redis.ZAdd(ctx, dlqKey(queueName), redis.Z{Score: a.nowMs(ctx), Member: taskID}).Err()
		_ = a.redis.Set(ctx, dlqScoreKey(queueName, taskID), strconv.FormatFloat(score, 'f', -1, 64), 0).Err()
		return false, err
	}
	_ = a.redis.ZIncrBy(ctx, QueuesIndexKey, 1, queueName).Err()
	return true, nil
}

func (a *Admin) purgeOneDLQ(ctx context.Context, queueName, taskID string) (bool, error) {
	n, err := a.redis.ZRem(ctx, dlqKey(queueName), taskID).Result()
	if err != nil {
		return false, err
	}
	if n == 0 {
		_ = a.redis.Del(ctx, dlqScoreKey(queueName, taskID)).Err()
		return false, nil
	}
	_ = a.redis.Del(ctx, dlqScoreKey(queueName, taskID)).Err()

	tagsKey := "queue:" + queueName + ":tags:" + taskID
	tags, _ := a.redis.SMembers(ctx, tagsKey).Result()
	for _, tag := range tags {
		tagKey := "queue:" + queueName + ":tag:" + tag
		_ = a.redis.SRem(ctx, tagKey, taskID).Err()
		if a.redis.SCard(ctx, tagKey).Val() == 0 {
			_ = a.redis.Del(ctx, tagKey).Err()
		}
	}

	keys := []string{
		"queue:" + queueName + ":payload:" + taskID,
		"queue:" + queueName + ":partition:" + taskID,
		"queue:" + queueName + ":priority:" + taskID,
		"queue:" + queueName + ":qpriority:" + taskID,
		"queue:" + queueName + ":reject_count:" + taskID,
		"queue:" + queueName + ":step:" + taskID,
		"queue:" + queueName + ":headers:" + taskID,
		"queue:" + queueName + ":race:tags:" + taskID,
		"queue:" + queueName + ":debounce:" + taskID,
		tagsKey,
	}
	if err := a.redis.Del(ctx, keys...).Err(); err != nil {
		return false, err
	}
	return true, nil
}

// enqueuePending кладёт задачу обратно в ZSET партиции (meta уже есть).
func (a *Admin) enqueuePending(ctx context.Context, queueName, taskID string, score float64) error {
	partition, err := a.redis.Get(ctx, "queue:"+queueName+":partition:"+taskID).Result()
	if err == redis.Nil || partition == "" {
		return fmt.Errorf("task %s: partition not found", taskID)
	}
	if err != nil {
		return err
	}

	priorityStr, _ := a.redis.Get(ctx, "queue:"+queueName+":priority:"+taskID).Result()
	if priorityStr == "" {
		priorityStr = "0"
	}
	priorityNum, _ := strconv.ParseFloat(priorityStr, 64)

	pref := ""
	qpStr, _ := a.redis.Get(ctx, "queue:"+queueName+":qpriority:"+taskID).Result()
	if qpStr != "" && qpStr != "0" {
		if qpNum, err := strconv.ParseFloat(qpStr, 64); err == nil && qpNum > 0 {
			pref = "qp" + qpStr + ":"
			a.redis.ZAdd(ctx, "queue:"+queueName+":priorities", redis.Z{Score: qpNum, Member: qpStr})
		}
	}

	partitionQueueKey := "queue:" + queueName + ":" + pref + "partition:" + partition + ":" + priorityStr
	a.redis.ZAdd(ctx, partitionQueueKey, redis.Z{Score: score, Member: taskID})
	prioritiesKey := "queue:" + queueName + ":" + pref + "partition:" + partition + ":priorities"
	a.redis.ZAdd(ctx, prioritiesKey, redis.Z{Score: priorityNum, Member: priorityStr})
	a.redis.SAdd(ctx, "queue:"+queueName+":"+pref+"partitions", partition)

	groupsKey := "queue:" + queueName + ":groups"
	pgroupsKey := "queue:" + queueName + ":partition:" + partition + ":groups"
	pgroups, _ := a.redis.SMembers(ctx, pgroupsKey).Result()
	if len(pgroups) == 0 {
		pgroups = []string{DefaultGroup}
		a.redis.SAdd(ctx, pgroupsKey, DefaultGroup)
	}
	for _, g := range pgroups {
		if _, err := a.redis.ZScore(ctx, groupsKey, g).Result(); err == redis.Nil {
			a.redis.ZAdd(ctx, groupsKey, redis.Z{Score: 0, Member: g})
		}
		a.redis.ZAdd(ctx, "queue:"+queueName+":"+pref+"group:"+g+":ready", redis.Z{Score: 0, Member: partition})
	}
	return nil
}
