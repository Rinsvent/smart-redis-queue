package redisqueue

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

// Admin предоставляет операции для обслуживания очередей
type Admin struct {
	redis *redis.Client
}

// NewAdmin создаёт админ для управления очередями
func NewAdmin(redisClient *redis.Client) *Admin {
	return &Admin{redis: redisClient}
}

// PartitionStats статистика по партиции
type PartitionStats struct {
	Partition  string
	Pending    int64
	InProgress int64
	Priorities []string
	Locked     bool
	Blocked    bool
}

// QueueStats статистика по очереди
type QueueStats struct {
	QueueName       string
	Partitions      []PartitionStats
	TotalPending    int64
	TotalInProgress int64
	Consumers       []string
}

// ListQueues возвращает имена всех очередей (обнаруживает по ключам queue:*:partitions)
func (a *Admin) ListQueues(ctx context.Context) ([]string, error) {
	var queues []string
	iter := a.redis.Scan(ctx, 0, "queue:*:partitions", 0).Iterator()
	for iter.Next(ctx) {
		key := iter.Val()
		// queue:NAME:partitions -> NAME
		if strings.HasPrefix(key, "queue:") && strings.HasSuffix(key, ":partitions") {
			name := strings.TrimPrefix(strings.TrimSuffix(key, ":partitions"), "queue:")
			if name != "" {
				queues = append(queues, name)
			}
		}
	}
	if err := iter.Err(); err != nil {
		return nil, err
	}
	return queues, nil
}

// Inspect возвращает статистику по очередям с фильтрами
func (a *Admin) Inspect(ctx context.Context, queueFilter, partitionFilter string) ([]*QueueStats, error) {
	queues, err := a.ListQueues(ctx)
	if err != nil {
		return nil, err
	}

	var result []*QueueStats
	for _, q := range queues {
		if queueFilter != "" && q != queueFilter {
			continue
		}
		stats, err := a.queueStats(ctx, q, partitionFilter)
		if err != nil {
			return nil, fmt.Errorf("queue %s: %w", q, err)
		}
		if stats != nil {
			result = append(result, stats)
		}
	}
	return result, nil
}

func (a *Admin) queueStats(ctx context.Context, queueName, partitionFilter string) (*QueueStats, error) {
	partitionsKey := "queue:" + queueName + ":partitions"
	partitions, err := a.redis.SMembers(ctx, partitionsKey).Result()
	if err != nil {
		if err == redis.Nil {
			return nil, nil
		}
		return nil, err
	}

	stats := &QueueStats{QueueName: queueName}

	consumersKey := "queue:" + queueName + ":consumers"
	consumers, _ := a.redis.SMembers(ctx, consumersKey).Result()
	stats.Consumers = consumers

	consumerInProgress := make(map[string]int64)
	for _, cid := range consumers {
		tasksKey := "queue:" + queueName + ":consumer:" + cid + ":tasks"
		n, _ := a.redis.HLen(ctx, tasksKey).Result()
		consumerInProgress[cid] = n
		stats.TotalInProgress += n
	}

	for _, part := range partitions {
		if partitionFilter != "" && part != partitionFilter {
			continue
		}
		ps := PartitionStats{Partition: part}

		prioritiesKey := "queue:" + queueName + ":partition:" + part + ":priorities"
		priorities, err := a.redis.ZRevRange(ctx, prioritiesKey, 0, -1).Result()
		if err != nil && err != redis.Nil {
			continue
		}
		ps.Priorities = priorities

		for _, prio := range priorities {
			queueKey := "queue:" + queueName + ":partition:" + part + ":" + prio
			n, _ := a.redis.ZCard(ctx, queueKey).Result()
			ps.Pending += n
		}
		stats.TotalPending += ps.Pending

		lockKey := "queue:" + queueName + ":partition:" + part + ":lock"
		ps.Locked = a.redis.Exists(ctx, lockKey).Val() > 0

		ps.Blocked = a.isPartitionBlocked(ctx, queueName, part)

		for _, cid := range consumers {
			tasksKey := "queue:" + queueName + ":consumer:" + cid + ":tasks"
			taskIds, _ := a.redis.HKeys(ctx, tasksKey).Result()
			for _, tid := range taskIds {
				partKey := "queue:" + queueName + ":partition:" + tid
				taskPart, _ := a.redis.Get(ctx, partKey).Result()
				if taskPart == part {
					ps.InProgress++
				}
			}
		}

		stats.Partitions = append(stats.Partitions, ps)
	}

	return stats, nil
}

// isPartitionBlocked: blocked (partition) или groups score > now.
func (a *Admin) isPartitionBlocked(ctx context.Context, queueName, partition string) bool {
	nowMs := float64(time.Now().UnixMilli())
	if t, err := a.redis.Time(ctx).Result(); err == nil {
		nowMs = float64(t.UnixMilli())
	}
	blockedKey := "queue:" + queueName + ":blocked"
	if score, err := a.redis.ZScore(ctx, blockedKey, partition).Result(); err == nil && nowMs < score {
		return true
	}
	groupsKey := "queue:" + queueName + ":groups"
	groups, _ := a.redis.SMembers(ctx, "queue:"+queueName+":partition:"+partition+":groups").Result()
	if len(groups) == 0 {
		groups = []string{DefaultGroup}
	}
	for _, g := range groups {
		if score, err := a.redis.ZScore(ctx, groupsKey, g).Result(); err == nil && nowMs < score {
			return true
		}
	}
	return false
}

// BlockGroups паркует группы на waitTime секунд (см. Consumer.BlockGroups).
func (a *Admin) BlockGroups(ctx context.Context, queueName string, waitTime float64, groups ...string) error {
	return blockGroups(ctx, a.redis, queueName, waitTime, groups...)
}

// TaskRemoval — результат удаления одной задачи (Remove / RemoveByTag).
type TaskRemoval struct {
	TaskID  string
	Payload []byte
	Status  string // RemovalStatusMissing | RemovalStatusInProgress | RemovalStatusRemoved
}

// RemoveByTagOptions настройки массового удаления по тегу.
type RemoveByTagOptions struct {
	// Limit — сколько задач обработать за вызов (default 100, max 1000).
	Limit int
	// ReturnPayload — возвращать ли payload для removed (и missing, если ещё был).
	ReturnPayload bool
}

// Remove атомарно снимает ожидающую задачу из очереди по ID (без consumer).
// missing — задачи нет; inprogress — взята консьюмером (не трогаем); removed — снята.
func (a *Admin) Remove(ctx context.Context, queueName, taskID string) (*TaskRemoval, error) {
	if taskID == "" {
		return nil, fmt.Errorf("task ID is required")
	}

	result, err := getRemoveScript().Run(ctx, a.redis, []string{}, queueName, taskID).Result()
	if err != nil {
		return nil, fmt.Errorf("failed to remove task: %w", err)
	}

	arr, ok := result.([]interface{})
	if !ok || len(arr) == 0 {
		return nil, fmt.Errorf("unexpected remove script response: %v", result)
	}

	status, _ := arr[0].(string)
	payload := ""
	if status == RemovalStatusRemoved && len(arr) > 1 && arr[1] != nil {
		payload, _ = arr[1].(string)
	}

	return &TaskRemoval{
		TaskID:  taskID,
		Payload: []byte(payload),
		Status:  status,
	}, nil
}

// RemoveByTag атомарно удаляет до Limit ожидающих задач с тегом.
// Предпочитает removed: missing/inprogress вытесняются из ответа, пока не наберётся Limit removed
// или не закончится индекс тега. Если в ответе только inprogress — снимать больше нечего
// (остались задачи в обработке у воркеров).
func (a *Admin) RemoveByTag(ctx context.Context, queueName, tag string, opts RemoveByTagOptions) ([]TaskRemoval, error) {
	if tag == "" {
		return nil, fmt.Errorf("tag is required")
	}
	if strings.Contains(tag, TagSeparator) {
		return nil, fmt.Errorf("tag %q contains forbidden separator %q", tag, TagSeparator)
	}

	limit := opts.Limit
	if limit <= 0 {
		limit = DefaultRemoveByTagLimit
	}
	if limit > MaxRemoveByTagLimit {
		limit = MaxRemoveByTagLimit
	}

	returnPayload := "0"
	if opts.ReturnPayload {
		returnPayload = "1"
	}

	result, err := getRemoveByTagScript().Run(ctx, a.redis, []string{},
		queueName, tag, limit, returnPayload,
	).Result()
	if err != nil {
		return nil, fmt.Errorf("failed to remove by tag: %w", err)
	}

	arr, ok := result.([]interface{})
	if !ok {
		return nil, fmt.Errorf("unexpected remove-by-tag response: %v", result)
	}

	out := make([]TaskRemoval, 0, len(arr)/3)
	for i := 0; i+2 < len(arr); i += 3 {
		taskID, _ := arr[i].(string)
		status, _ := arr[i+1].(string)
		payload := ""
		if arr[i+2] != nil {
			payload, _ = arr[i+2].(string)
		}
		out = append(out, TaskRemoval{
			TaskID:  taskID,
			Payload: []byte(payload),
			Status:  status,
		})
	}
	return out, nil
}

// CountByTag возвращает число taskId в индексе тега (pending + inprogress; removed уже не считаются).
func (a *Admin) CountByTag(ctx context.Context, queueName, tag string) (int64, error) {
	if tag == "" {
		return 0, fmt.Errorf("tag is required")
	}
	if strings.Contains(tag, TagSeparator) {
		return 0, fmt.Errorf("tag %q contains forbidden separator %q", tag, TagSeparator)
	}
	return a.redis.SCard(ctx, "queue:"+queueName+":tag:"+tag).Result()
}

// Purge удаляет все задачи из очереди. Если partition != "", удаляет только указанную партицию.
func (a *Admin) Purge(ctx context.Context, queueName, partition string) (int, error) {
	if partition != "" {
		return a.purgePartition(ctx, queueName, partition)
	}
	return a.purgeQueue(ctx, queueName)
}

func (a *Admin) purgeQueue(ctx context.Context, queueName string) (int, error) {
	prefix := "queue:" + queueName + ":"
	var keys []string
	iter := a.redis.Scan(ctx, 0, prefix+"*", 0).Iterator()
	for iter.Next(ctx) {
		keys = append(keys, iter.Val())
	}
	if err := iter.Err(); err != nil {
		return 0, err
	}
	if len(keys) == 0 {
		return 0, nil
	}
	if err := a.redis.Del(ctx, keys...).Err(); err != nil {
		return 0, err
	}
	return len(keys), nil
}

func (a *Admin) purgePartition(ctx context.Context, queueName, partition string) (int, error) {
	var keysToDel []string
	prioritiesKey := "queue:" + queueName + ":partition:" + partition + ":priorities"
	priorities, err := a.redis.ZRevRange(ctx, prioritiesKey, 0, -1).Result()
	if err != nil && err != redis.Nil {
		return 0, err
	}

	for _, prio := range priorities {
		queueKey := "queue:" + queueName + ":partition:" + partition + ":" + prio
		taskIds, _ := a.redis.ZRange(ctx, queueKey, 0, -1).Result()
		for _, tid := range taskIds {
			tagsKey := "queue:" + queueName + ":tags:" + tid
			tags, _ := a.redis.SMembers(ctx, tagsKey).Result()
			for _, tag := range tags {
				tagKey := "queue:" + queueName + ":tag:" + tag
				a.redis.SRem(ctx, tagKey, tid)
				if a.redis.SCard(ctx, tagKey).Val() == 0 {
					a.redis.Del(ctx, tagKey)
				}
			}
			keysToDel = append(keysToDel,
				"queue:"+queueName+":payload:"+tid,
				"queue:"+queueName+":partition:"+tid,
				"queue:"+queueName+":priority:"+tid,
				"queue:"+queueName+":reject_count:"+tid,
				tagsKey,
			)
		}
		keysToDel = append(keysToDel, queueKey)
	}
	keysToDel = append(keysToDel, prioritiesKey)
	keysToDel = append(keysToDel, "queue:"+queueName+":partition:"+partition+":lock", "queue:"+queueName+":partition:"+partition+":block")

	partitionsKey := "queue:" + queueName + ":partitions"
	a.redis.SRem(ctx, partitionsKey, partition)

	if len(keysToDel) > 0 {
		if err := a.redis.Del(ctx, keysToDel...).Err(); err != nil {
			return 0, err
		}
	}
	return len(keysToDel), nil
}

// Retry возвращает все in-progress задачи обратно в очередь (для "зависших" после падения консьюмера)
func (a *Admin) Retry(ctx context.Context, queueName string) (int, error) {
	consumersKey := "queue:" + queueName + ":consumers"
	consumers, err := a.redis.SMembers(ctx, consumersKey).Result()
	if err != nil || len(consumers) == 0 {
		return 0, err
	}

	count := 0
	for _, cid := range consumers {
		tasksKey := "queue:" + queueName + ":consumer:" + cid + ":tasks"
		taskIds, err := a.redis.HKeys(ctx, tasksKey).Result()
		if err != nil || len(taskIds) == 0 {
			continue
		}
		for _, tid := range taskIds {
			if err := a.returnTaskToQueue(ctx, queueName, tid, cid); err != nil {
				continue
			}
			a.redis.HDel(ctx, tasksKey, tid)
			count++
		}
	}
	return count, nil
}

func (a *Admin) returnTaskToQueue(ctx context.Context, queueName, taskID, consumerID string) error {
	partitionKey := "queue:" + queueName + ":partition:" + taskID
	partition, err := a.redis.Get(ctx, partitionKey).Result()
	if err == redis.Nil {
		return fmt.Errorf("task %s: partition not found", taskID)
	}
	if err != nil {
		return err
	}

	priorityKey := "queue:" + queueName + ":priority:" + taskID
	priorityStr, _ := a.redis.Get(ctx, priorityKey).Result()
	if priorityStr == "" {
		priorityStr = "0"
	}
	priorityNum, _ := strconv.ParseFloat(priorityStr, 64)

	now, _ := a.redis.Time(ctx).Result()
	score := float64(now.UnixMilli())

	partitionQueueKey := "queue:" + queueName + ":partition:" + partition + ":" + priorityStr
	a.redis.ZAdd(ctx, partitionQueueKey, redis.Z{Score: score, Member: taskID})
	prioritiesKey := "queue:" + queueName + ":partition:" + partition + ":priorities"
	a.redis.ZAdd(ctx, prioritiesKey, redis.Z{Score: priorityNum, Member: priorityStr})
	a.redis.SAdd(ctx, "queue:"+queueName+":partitions", partition)

	// Get мог снять пустую партицию с groups/ready — восстанавливаем.
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
		a.redis.ZAdd(ctx, "queue:"+queueName+":group:"+g+":ready", redis.Z{Score: 0, Member: partition})
	}

	consumerPartitionCountKey := "queue:" + queueName + ":consumer:" + consumerID + ":partition:" + partition + ":count"
	a.redis.Decr(ctx, consumerPartitionCountKey)

	if len(partition) > 0 && partition[0] == '!' {
		lockKey := "queue:" + queueName + ":partition:" + partition + ":lock"
		a.redis.Del(ctx, lockKey)
	}
	return nil
}
