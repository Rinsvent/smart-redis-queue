package redisqueue

import (
	"context"
	"fmt"
	"sort"
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

// MaxListPartitions — лимит выдачи ListPartitions / ListGroups / ListTags.
const MaxListPartitions = 100

// MaxListFilterLen — макс. длина подстроки/glob-фильтра (защита от гигантского MATCH).
const MaxListFilterLen = 256

// scanMatchCount — COUNT hint для SSCAN/ZSCAN/SCAN (батч, не лимит выборки).
const scanMatchCount = 200

// PartitionStats статистика по партиции
type PartitionStats struct {
	Partition  string
	Pending    int64
	InProgress int64
	Priorities []string
	Groups     []string
	Locked     bool
	Blocked    bool
}

// GroupStats — группа очереди (ZSET groups + ready).
type GroupStats struct {
	Name    string
	Ready   int64   // ZCARD group:g:ready
	Score   float64 // unlockAt ms; ≤ now → свободна
	Blocked bool
}

// QueueStats статистика по очереди
type QueueStats struct {
	QueueName       string
	Partitions      []PartitionStats
	Groups          []GroupStats
	TotalPending    int64
	TotalInProgress int64
	Consumers       []string
	PartitionTotal  int // сколько партиций найдено до limit (для ListPartitions)
}

// ListQueues возвращает имена очередей (union SCAN queue:*:partitions и queue:*:consumers).
// Так видны и «осиротевшие» очереди, у которых остались только consumers.
func (a *Admin) ListQueues(ctx context.Context) ([]string, error) {
	seen := make(map[string]struct{})
	for _, pattern := range []string{"queue:*:partitions", "queue:*:consumers"} {
		suffix := ":partitions"
		if strings.HasSuffix(pattern, ":consumers") {
			suffix = ":consumers"
		}
		iter := a.redis.Scan(ctx, 0, pattern, 200).Iterator()
		for iter.Next(ctx) {
			key := iter.Val()
			if !strings.HasPrefix(key, "queue:") || !strings.HasSuffix(key, suffix) {
				continue
			}
			name := strings.TrimPrefix(strings.TrimSuffix(key, suffix), "queue:")
			if name != "" {
				seen[name] = struct{}{}
			}
		}
		if err := iter.Err(); err != nil {
			return nil, err
		}
	}
	queues := make([]string, 0, len(seen))
	for name := range seen {
		queues = append(queues, name)
	}
	sort.Strings(queues)
	return queues, nil
}

// Inspect возвращает статистику по очередям с фильтрами.
// Если задан queueFilter — ListQueues не вызывается (без лишнего SCAN).
func (a *Admin) Inspect(ctx context.Context, queueFilter, partitionFilter string) ([]*QueueStats, error) {
	var queues []string
	if queueFilter != "" {
		queues = []string{queueFilter}
	} else {
		var err error
		queues, err = a.ListQueues(ctx)
		if err != nil {
			return nil, err
		}
	}

	var result []*QueueStats
	for _, q := range queues {
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
	parts, total, err := a.ListPartitions(ctx, queueName, partitionFilter, MaxListPartitions)
	if err != nil {
		return nil, err
	}
	groups, _, err := a.ListGroups(ctx, queueName, "", MaxListPartitions)
	if err != nil {
		return nil, err
	}

	stats := &QueueStats{
		QueueName:      queueName,
		PartitionTotal: total,
		Partitions:     parts,
		Groups:         groups,
	}
	consumersKey := "queue:" + queueName + ":consumers"
	consumers, _ := a.redis.SMembers(ctx, consumersKey).Result()
	stats.Consumers = consumers

	for _, cid := range consumers {
		tasksKey := "queue:" + queueName + ":consumer:" + cid + ":tasks"
		n, _ := a.redis.HLen(ctx, tasksKey).Result()
		stats.TotalInProgress += n
	}
	for _, ps := range parts {
		stats.TotalPending += ps.Pending
	}
	return stats, nil
}

// QueueOverview — сводка без полного обхода партиций: SCARD + consumers + до 100 групп.
// TotalPending не суммируется по всем партициям (дорого на больших очередях) — смотрите ListPartitions.
func (a *Admin) QueueOverview(ctx context.Context, queueName string) (*QueueStats, error) {
	total, err := a.redis.SCard(ctx, "queue:"+queueName+":partitions").Result()
	if err != nil && err != redis.Nil {
		return nil, err
	}
	groups, _, err := a.ListGroups(ctx, queueName, "", MaxListPartitions)
	if err != nil {
		return nil, err
	}
	stats := &QueueStats{
		QueueName:      queueName,
		PartitionTotal: int(total),
		Groups:         groups,
	}
	consumers, _ := a.redis.SMembers(ctx, "queue:"+queueName+":consumers").Result()
	stats.Consumers = consumers
	for _, cid := range consumers {
		n, _ := a.redis.HLen(ctx, "queue:"+queueName+":consumer:"+cid+":tasks").Result()
		stats.TotalInProgress += n
	}
	return stats, nil
}

// ListPartitions — SET partitions: total=SCARD, выборка ≤limit через SSCAN MATCH *substr*.
func (a *Admin) ListPartitions(ctx context.Context, queueName, substr string, limit int) (parts []PartitionStats, total int, err error) {
	if limit <= 0 || limit > MaxListPartitions {
		limit = MaxListPartitions
	}
	key := "queue:" + queueName + ":partitions"
	n, err := a.redis.SCard(ctx, key).Result()
	if err != nil && err != redis.Nil {
		return nil, 0, err
	}
	total = int(n)

	ids, err := a.sscanMembers(ctx, key, substr, limit)
	if err != nil {
		return nil, 0, err
	}
	sort.Strings(ids)

	consumers, _ := a.redis.SMembers(ctx, "queue:"+queueName+":consumers").Result()
	parts = make([]PartitionStats, 0, len(ids))
	for _, part := range ids {
		parts = append(parts, a.partitionStats(ctx, queueName, part, consumers))
	}
	return parts, total, nil
}

// sscanMembers собирает до limit members SET через SSCAN MATCH.
// substr — фрагмент Redis glob (можно * ? [abc]); итоговый паттерн *{substr}*.
// Останавливается как только набрано limit — весь SET не читает.
func (a *Admin) sscanMembers(ctx context.Context, key, substr string, limit int) ([]string, error) {
	match := listMatchGlob(substr)
	seen := make(map[string]struct{}, limit)
	out := make([]string, 0, limit)
	var cursor uint64
	for {
		keys, next, err := a.redis.SScan(ctx, key, cursor, match, scanMatchCount).Result()
		if err != nil {
			return nil, err
		}
		for _, m := range keys {
			if m == "" {
				continue
			}
			if _, ok := seen[m]; ok {
				continue
			}
			seen[m] = struct{}{}
			out = append(out, m)
			if len(out) >= limit {
				return out, nil
			}
		}
		cursor = next
		if cursor == 0 {
			break
		}
	}
	return out, nil
}

func (a *Admin) partitionStats(ctx context.Context, queueName, part string, consumers []string) PartitionStats {
	ps := PartitionStats{Partition: part}

	prioritiesKey := "queue:" + queueName + ":partition:" + part + ":priorities"
	priorities, err := a.redis.ZRevRange(ctx, prioritiesKey, 0, -1).Result()
	if err == nil || err == redis.Nil {
		ps.Priorities = priorities
	}
	for _, prio := range ps.Priorities {
		queueKey := "queue:" + queueName + ":partition:" + part + ":" + prio
		n, _ := a.redis.ZCard(ctx, queueKey).Result()
		ps.Pending += n
	}

	ps.Locked = a.redis.Exists(ctx, "queue:"+queueName+":partition:"+part+":lock").Val() > 0
	ps.Blocked = a.isPartitionBlocked(ctx, queueName, part)

	pgroups, _ := a.redis.SMembers(ctx, "queue:"+queueName+":partition:"+part+":groups").Result()
	if len(pgroups) == 0 {
		pgroups = []string{DefaultGroup}
	}
	sort.Strings(pgroups)
	ps.Groups = pgroups

	for _, cid := range consumers {
		tasksKey := "queue:" + queueName + ":consumer:" + cid + ":tasks"
		taskIds, _ := a.redis.HKeys(ctx, tasksKey).Result()
		for _, tid := range taskIds {
			taskPart, _ := a.redis.Get(ctx, "queue:"+queueName+":partition:"+tid).Result()
			if taskPart == part {
				ps.InProgress++
			}
		}
	}
	return ps
}

func (a *Admin) nowMs(ctx context.Context) float64 {
	nowMs := float64(time.Now().UnixMilli())
	if t, err := a.redis.Time(ctx).Result(); err == nil {
		nowMs = float64(t.UnixMilli())
	}
	return nowMs
}

func (a *Admin) groupStatsFromZ(ctx context.Context, queueName string, z redis.Z, nowMs float64) GroupStats {
	name, _ := z.Member.(string)
	ready, _ := a.redis.ZCard(ctx, "queue:"+queueName+":group:"+name+":ready").Result()
	return GroupStats{
		Name:    name,
		Ready:   ready,
		Score:   z.Score,
		Blocked: z.Score > nowMs,
	}
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

// TagStats — индекс тега (SCARD queue:…:tag:…).
type TagStats struct {
	Tag   string
	Count int64
}

// BlockPartition паркует партицию в ZSET blocked на waitTime секунд (как Reject rate-limit).
// Get не берёт задачи из этой партиции, пока now < unlockAt. Consumer :lock не трогаем.
func (a *Admin) BlockPartition(ctx context.Context, queueName, partition string, waitTime float64) error {
	if partition == "" {
		return fmt.Errorf("partition is required")
	}
	if waitTime <= 0 {
		return fmt.Errorf("waitTime must be > 0")
	}
	nowMs := float64(time.Now().UnixMilli())
	if t, err := a.redis.Time(ctx).Result(); err == nil {
		nowMs = float64(t.UnixMilli())
	}
	unlockAt := nowMs + waitTime*1000
	return a.redis.ZAdd(ctx, "queue:"+queueName+":blocked", redis.Z{
		Score:  unlockAt,
		Member: partition,
	}).Err()
}

// UnblockPartition снимает партицию из blocked — воркеры снова могут брать задачи.
func (a *Admin) UnblockPartition(ctx context.Context, queueName, partition string) error {
	if partition == "" {
		return fmt.Errorf("partition is required")
	}
	return a.redis.ZRem(ctx, "queue:"+queueName+":blocked", partition).Err()
}

// ListGroups — ZSET groups: total=ZCARD; без фильтра ZRANGE 0..limit-1, с фильтром ZSCAN MATCH до limit.
func (a *Admin) ListGroups(ctx context.Context, queueName, substr string, limit int) ([]GroupStats, int, error) {
	if limit <= 0 || limit > MaxListPartitions {
		limit = MaxListPartitions
	}
	groupsKey := "queue:" + queueName + ":groups"
	n, err := a.redis.ZCard(ctx, groupsKey).Result()
	if err != nil && err != redis.Nil {
		return nil, 0, err
	}
	total := int(n)
	nowMs := a.nowMs(ctx)

	var out []GroupStats
	if substr == "" {
		if total == 0 {
			ready, _ := a.redis.ZCard(ctx, "queue:"+queueName+":group:"+DefaultGroup+":ready").Result()
			if ready > 0 {
				return []GroupStats{{Name: DefaultGroup, Ready: ready}}, 1, nil
			}
			return nil, 0, nil
		}
		zs, err := a.redis.ZRangeWithScores(ctx, groupsKey, 0, int64(limit-1)).Result()
		if err != nil && err != redis.Nil {
			return nil, 0, err
		}
		out = make([]GroupStats, 0, len(zs))
		for _, z := range zs {
			name, _ := z.Member.(string)
			if name == "" {
				continue
			}
			out = append(out, a.groupStatsFromZ(ctx, queueName, z, nowMs))
		}
	} else {
		match := listMatchGlob(substr)
		seen := make(map[string]struct{}, limit)
		out = make([]GroupStats, 0, limit)
		var cursor uint64
		for len(out) < limit {
			keys, next, err := a.redis.ZScan(ctx, groupsKey, cursor, match, scanMatchCount).Result()
			if err != nil {
				return nil, 0, err
			}
			// ZScan returns [member, score, member, score, ...]
			for i := 0; i+1 < len(keys) && len(out) < limit; i += 2 {
				name := keys[i]
				if name == "" {
					continue
				}
				if _, ok := seen[name]; ok {
					continue
				}
				seen[name] = struct{}{}
				score, _ := strconv.ParseFloat(keys[i+1], 64)
				out = append(out, a.groupStatsFromZ(ctx, queueName, redis.Z{Member: name, Score: score}, nowMs))
			}
			cursor = next
			if cursor == 0 {
				break
			}
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, total, nil
}

// ListTags — страница ≤limit через SCAN MATCH; total не считается (всегда 0). См. CountTags.
func (a *Admin) ListTags(ctx context.Context, queueName, substr string, limit int) ([]TagStats, int, error) {
	if limit <= 0 || limit > MaxListPartitions {
		limit = MaxListPartitions
	}
	prefix := "queue:" + queueName + ":tag:"
	// substr — glob-фрагмент; ключ = prefix + tag → MATCH prefix+*{substr}*
	pattern := prefix + listMatchGlob(substr)
	seen := make(map[string]struct{}, limit)
	tags := make([]string, 0, limit)
	var cursor uint64
	for len(tags) < limit {
		keys, next, err := a.redis.Scan(ctx, cursor, pattern, scanMatchCount).Result()
		if err != nil {
			return nil, 0, err
		}
		for _, key := range keys {
			if !strings.HasPrefix(key, prefix) {
				continue
			}
			tag := strings.TrimPrefix(key, prefix)
			if tag == "" {
				continue
			}
			if _, ok := seen[tag]; ok {
				continue
			}
			seen[tag] = struct{}{}
			tags = append(tags, tag)
			if len(tags) >= limit {
				break
			}
		}
		cursor = next
		if cursor == 0 {
			break
		}
	}
	sort.Strings(tags)
	out := make([]TagStats, 0, len(tags))
	for _, tag := range tags {
		n, _ := a.redis.SCard(ctx, prefix+tag).Result()
		out = append(out, TagStats{Tag: tag, Count: n})
	}
	return out, 0, nil
}

// CountTags — полное число тегов (итеративный SCAN, маленький COUNT). Тяжёлый; только по запросу UI.
func (a *Admin) CountTags(ctx context.Context, queueName string) (int, error) {
	prefix := "queue:" + queueName + ":tag:"
	seen := make(map[string]struct{})
	var cursor uint64
	for {
		keys, next, err := a.redis.Scan(ctx, cursor, prefix+"*", scanMatchCount).Result()
		if err != nil {
			return 0, err
		}
		for _, key := range keys {
			if !strings.HasPrefix(key, prefix) {
				continue
			}
			tag := strings.TrimPrefix(key, prefix)
			if tag == "" {
				continue
			}
			seen[tag] = struct{}{}
		}
		cursor = next
		if cursor == 0 {
			break
		}
	}
	return len(seen), nil
}

// listMatchGlob строит паттерн Redis MATCH из пользовательского фильтра.
// Пусто → "*"; иначе "*{filter}*" (filter — glob: * ? [abc] допустимы).
// Это не инъекция команд: MATCH — аргумент SSCAN/ZSCAN/SCAN через go-redis.
func listMatchGlob(filter string) string {
	if filter == "" {
		return "*"
	}
	if len(filter) > MaxListFilterLen {
		filter = filter[:MaxListFilterLen]
	}
	return "*" + filter + "*"
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

	raw := []byte(payload)
	decoded, err := decodePayloadFromRedis(raw)
	if err != nil {
		return nil, fmt.Errorf("decode payload: %w", err)
	}
	return &TaskRemoval{
		TaskID:  taskID,
		Payload: decoded,
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
		raw := []byte(payload)
		decoded, err := decodePayloadFromRedis(raw)
		if err != nil {
			return nil, fmt.Errorf("decode payload %s: %w", taskID, err)
		}
		out = append(out, TaskRemoval{
			TaskID:  taskID,
			Payload: decoded,
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
	// meta = original ZSET score (после Get); иначе now
	if meta, err := a.redis.HGet(ctx, "queue:"+queueName+":consumer:"+consumerID+":tasks", taskID).Result(); err == nil {
		if s, err := strconv.ParseFloat(meta, 64); err == nil {
			score = s
		}
	}

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

	if len(partition) > 0 && partition[0] == '!' {
		countKey := "queue:" + queueName + ":consumer:" + consumerID + ":partition:" + partition + ":count"
		a.redis.Del(ctx, countKey)
		lockKey := "queue:" + queueName + ":partition:" + partition + ":lock"
		a.redis.Del(ctx, lockKey)
	}
	return nil
}

// QueueStatPoint — секундный bucket метрик воркера (queue:{q}:stats:{sec}).
type QueueStatPoint struct {
	Sec    int64
	Get    int64
	Ack    int64
	Reject int64
	Tags   map[string]int64 // tag → SCARD snapshot (поля t:{tag})
}

// ListQueueStats читает точки за [fromSec, toSec] по stats:index.
func (a *Admin) ListQueueStats(ctx context.Context, queueName string, fromSec, toSec int64) ([]QueueStatPoint, error) {
	if toSec < fromSec {
		fromSec, toSec = toSec, fromSec
	}
	indexKey := "queue:" + queueName + ":stats:index"
	members, err := a.redis.ZRangeByScore(ctx, indexKey, &redis.ZRangeBy{
		Min: strconv.FormatInt(fromSec, 10),
		Max: strconv.FormatInt(toSec, 10),
	}).Result()
	if err != nil {
		return nil, err
	}
	out := make([]QueueStatPoint, 0, len(members))
	for _, m := range members {
		sec, err := strconv.ParseInt(m, 10, 64)
		if err != nil {
			continue
		}
		key := "queue:" + queueName + ":stats:" + m
		fields, err := a.redis.HGetAll(ctx, key).Result()
		if err != nil {
			continue
		}
		pt := QueueStatPoint{Sec: sec, Tags: map[string]int64{}}
		for k, v := range fields {
			n, _ := strconv.ParseInt(v, 10, 64)
			switch {
			case k == "g":
				pt.Get = n
			case k == "a":
				pt.Ack = n
			case k == "r":
				pt.Reject = n
			case strings.HasPrefix(k, "t:"):
				pt.Tags[strings.TrimPrefix(k, "t:")] = n
			}
		}
		out = append(out, pt)
	}
	return out, nil
}
