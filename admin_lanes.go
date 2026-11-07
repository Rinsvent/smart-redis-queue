package redisqueue

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/redis/go-redis/v9"
)

// qpKeyPrefix — "" для legacy (QueuePriority=0), иначе "qpN:".
func qpKeyPrefix(qp int) string {
	if qp <= 0 {
		return ""
	}
	return "qp" + strconv.Itoa(qp) + ":"
}

// queueLanePriorities — активные QueuePriority > 0 (desc) + всегда 0 (legacy lane).
func (a *Admin) queueLanePriorities(ctx context.Context, queueName string) ([]int, error) {
	zs, err := a.redis.ZRevRange(ctx, "queue:"+queueName+":priorities", 0, -1).Result()
	if err != nil && err != redis.Nil {
		return nil, err
	}
	out := make([]int, 0, len(zs)+1)
	seen := map[int]struct{}{}
	for _, s := range zs {
		qp, err := strconv.Atoi(s)
		if err != nil || qp <= 0 {
			continue
		}
		if _, ok := seen[qp]; ok {
			continue
		}
		seen[qp] = struct{}{}
		out = append(out, qp)
	}
	out = append(out, 0)
	return out, nil
}

// unionPartitionIDs — уникальные партиции по всем lanes (legacy + qpN).
// total — полный unique count без фильтра; ids — после MATCH substr + limit.
func (a *Admin) unionPartitionIDs(ctx context.Context, queueName, substr string, limit int) (ids []string, total int, err error) {
	lanes, err := a.queueLanePriorities(ctx, queueName)
	if err != nil {
		return nil, 0, err
	}
	allSeen := make(map[string]struct{})
	for _, qp := range lanes {
		key := "queue:" + queueName + ":" + qpKeyPrefix(qp) + "partitions"
		var cursor uint64
		for {
			batch, next, err := a.redis.SScan(ctx, key, cursor, "*", scanMatchCount).Result()
			if err != nil {
				return nil, 0, err
			}
			for _, m := range batch {
				if m != "" {
					allSeen[m] = struct{}{}
				}
			}
			cursor = next
			if cursor == 0 {
				break
			}
		}
	}
	total = len(allSeen)

	match := "*"
	if substr != "" {
		match = listMatchGlob(substr)
	}
	filtered := make(map[string]struct{})
	if substr == "" {
		filtered = allSeen
	} else {
		for _, qp := range lanes {
			key := "queue:" + queueName + ":" + qpKeyPrefix(qp) + "partitions"
			var cursor uint64
			for {
				batch, next, err := a.redis.SScan(ctx, key, cursor, match, scanMatchCount).Result()
				if err != nil {
					return nil, 0, err
				}
				for _, m := range batch {
					if m != "" {
						filtered[m] = struct{}{}
					}
				}
				cursor = next
				if cursor == 0 {
					break
				}
			}
		}
	}
	ids = make([]string, 0, len(filtered))
	for id := range filtered {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	if limit > 0 && len(ids) > limit {
		ids = ids[:limit]
	}
	return ids, total, nil
}

func (a *Admin) groupReadyTotal(ctx context.Context, queueName, group string, lanes []int) int64 {
	var ready int64
	for _, qp := range lanes {
		n, _ := a.redis.ZCard(ctx, "queue:"+queueName+":"+qpKeyPrefix(qp)+"group:"+group+":ready").Result()
		ready += n
	}
	return ready
}

func (a *Admin) partitionPendingAcrossLanes(ctx context.Context, queueName, part string, lanes []int) (pending int64, priorities []string) {
	prioSeen := map[string]struct{}{}
	for _, qp := range lanes {
		pref := qpKeyPrefix(qp)
		prioritiesKey := "queue:" + queueName + ":" + pref + "partition:" + part + ":priorities"
		prios, err := a.redis.ZRevRange(ctx, prioritiesKey, 0, -1).Result()
		if err != nil {
			continue
		}
		for _, prio := range prios {
			if _, ok := prioSeen[prio]; !ok {
				prioSeen[prio] = struct{}{}
				priorities = append(priorities, prio)
			}
			n, _ := a.redis.ZCard(ctx, "queue:"+queueName+":"+pref+"partition:"+part+":"+prio).Result()
			pending += n
		}
	}
	return pending, priorities
}

// PartitionSnapshot — чат/партиция в QueueSnapshot.
type PartitionSnapshot struct {
	Partition    string
	Pending      int64
	InProgress   int64
	Locked       bool
	Blocked      bool // ZSET blocked (partition pause)
	GroupBlocked bool // pause группы (коннектор и т.п.)
	Paused       bool // Blocked || GroupBlocked
	Groups       []string
	Priorities   []string
}

// GroupSnapshot — группа с ready по всем lanes.
type GroupSnapshot struct {
	Name    string
	Ready   int64 // сумма ZCARD ready по lanes
	Score   float64
	Blocked bool
	HasWork bool // Ready > 0
}

// OldestReadyTask — готовая к выдаче задача с учётом приоритетов (как порядок Get):
// больший QueuePriority, больший PartitionPriority, меньший score (старше).
type OldestReadyTask struct {
	ID                string
	Partition         string
	QueuePriority     int
	PartitionPriority int
	Score             float64
	Groups            []string
}

// QueueSnapshot — снимок очереди для gauges/ops с учётом qp lanes.
type QueueSnapshot struct {
	QueueName       string
	NowMs           float64
	Lanes           []int // QueuePriority lanes (desc, 0 = legacy в конце)
	Partitions      []PartitionSnapshot
	PartitionTotal  int
	Groups          []GroupSnapshot
	OldestReady     *OldestReadyTask
	TotalPending    int64
	TotalInProgress int64
	Consumers       []string
	PausedCount     int // Partitions с Paused
	ActiveChats     int // Partitions с Pending>0 && !Paused
}

// QueueSnapshot строит снимок: чаты, паузы (partition + group), группы с работой,
// самая приоритетная готовая задача (oldest score наверху приоритета).
func (a *Admin) QueueSnapshot(ctx context.Context, queueName string) (*QueueSnapshot, error) {
	if queueName == "" {
		return nil, fmt.Errorf("queue name is required")
	}
	lanes, err := a.queueLanePriorities(ctx, queueName)
	if err != nil {
		return nil, err
	}
	nowMs := a.nowMs(ctx)
	ids, total, err := a.unionPartitionIDs(ctx, queueName, "", 0)
	if err != nil {
		return nil, err
	}
	consumers, _ := a.redis.SMembers(ctx, "queue:"+queueName+":consumers").Result()

	snap := &QueueSnapshot{
		QueueName:      queueName,
		NowMs:          nowMs,
		Lanes:          append([]int(nil), lanes...),
		PartitionTotal: total,
		Consumers:      consumers,
		Partitions:     make([]PartitionSnapshot, 0, len(ids)),
	}
	for _, cid := range consumers {
		n, _ := a.redis.HLen(ctx, "queue:"+queueName+":consumer:"+cid+":tasks").Result()
		snap.TotalInProgress += n
	}

	inProgByPart := a.inProgressByPartition(ctx, queueName, consumers)

	for _, part := range ids {
		ps := a.partitionSnapshot(ctx, queueName, part, lanes, nowMs, inProgByPart[part])
		snap.Partitions = append(snap.Partitions, ps)
		snap.TotalPending += ps.Pending
		if ps.Paused {
			snap.PausedCount++
		}
		if ps.Pending > 0 && !ps.Paused {
			snap.ActiveChats++
		}
	}

	groups, _, err := a.ListGroups(ctx, queueName, "", MaxListPartitions)
	if err != nil {
		return nil, err
	}
	snap.Groups = make([]GroupSnapshot, 0, len(groups))
	for _, g := range groups {
		gs := GroupSnapshot{
			Name:    g.Name,
			Ready:   g.Ready,
			Score:   g.Score,
			Blocked: g.Blocked,
			HasWork: g.Ready > 0,
		}
		snap.Groups = append(snap.Groups, gs)
	}

	oldest, err := a.findOldestReadyTask(ctx, queueName, lanes, nowMs)
	if err != nil {
		return nil, err
	}
	snap.OldestReady = oldest
	return snap, nil
}

func (a *Admin) inProgressByPartition(ctx context.Context, queueName string, consumers []string) map[string]int64 {
	out := map[string]int64{}
	for _, cid := range consumers {
		tasksKey := "queue:" + queueName + ":consumer:" + cid + ":tasks"
		taskIDs, _ := a.redis.HKeys(ctx, tasksKey).Result()
		if len(taskIDs) == 0 {
			continue
		}
		pipe := a.redis.Pipeline()
		cmds := make([]*redis.StringCmd, len(taskIDs))
		for i, tid := range taskIDs {
			cmds[i] = pipe.Get(ctx, "queue:"+queueName+":partition:"+tid)
		}
		_, _ = pipe.Exec(ctx)
		for _, c := range cmds {
			part, err := c.Result()
			if err != nil || part == "" {
				continue
			}
			out[part]++
		}
	}
	return out
}

func (a *Admin) partitionSnapshot(ctx context.Context, queueName, part string, lanes []int, nowMs float64, inProg int64) PartitionSnapshot {
	pending, priorities := a.partitionPendingAcrossLanes(ctx, queueName, part, lanes)
	ps := PartitionSnapshot{
		Partition:  part,
		Pending:    pending,
		InProgress: inProg,
		Priorities: priorities,
		Locked:     a.redis.Exists(ctx, "queue:"+queueName+":partition:"+part+":lock").Val() > 0,
	}
	if score, err := a.redis.ZScore(ctx, "queue:"+queueName+":blocked", part).Result(); err == nil && nowMs < score {
		ps.Blocked = true
	}
	pgroups, _ := a.redis.SMembers(ctx, "queue:"+queueName+":partition:"+part+":groups").Result()
	if len(pgroups) == 0 {
		pgroups = []string{DefaultGroup}
	}
	sort.Strings(pgroups)
	ps.Groups = pgroups
	groupsKey := "queue:" + queueName + ":groups"
	for _, g := range pgroups {
		if score, err := a.redis.ZScore(ctx, groupsKey, g).Result(); err == nil && nowMs < score {
			ps.GroupBlocked = true
			break
		}
	}
	ps.Paused = ps.Blocked || ps.GroupBlocked
	return ps
}

// findOldestReadyTask — следующий кандидат в духе Get: qp desc, free groups,
// ready partitions, partition-prio desc, минимальный score среди голов.
func (a *Admin) findOldestReadyTask(ctx context.Context, queueName string, lanes []int, nowMs float64) (*OldestReadyTask, error) {
	groupsKey := "queue:" + queueName + ":groups"
	blockedKey := "queue:" + queueName + ":blocked"

	var best *OldestReadyTask
	better := func(cand *OldestReadyTask) {
		if cand == nil || cand.ID == "" {
			return
		}
		if best == nil {
			best = cand
			return
		}
		if cand.QueuePriority != best.QueuePriority {
			if cand.QueuePriority > best.QueuePriority {
				best = cand
			}
			return
		}
		if cand.PartitionPriority != best.PartitionPriority {
			if cand.PartitionPriority > best.PartitionPriority {
				best = cand
			}
			return
		}
		if cand.Score < best.Score {
			best = cand
		}
	}

	for _, qp := range lanes {
		pref := qpKeyPrefix(qp)
		// свободные группы: score ≤ now
		zs, err := a.redis.ZRangeByScoreWithScores(ctx, groupsKey, &redis.ZRangeBy{
			Min: "-inf",
			Max: strconv.FormatFloat(nowMs, 'f', -1, 64),
		}).Result()
		if err != nil && err != redis.Nil {
			return nil, err
		}
		// если ZSET groups пуст, но есть legacy ready — DefaultGroup
		if len(zs) == 0 {
			readyKey := "queue:" + queueName + ":" + pref + "group:" + DefaultGroup + ":ready"
			if n, _ := a.redis.ZCard(ctx, readyKey).Result(); n > 0 {
				zs = []redis.Z{{Member: DefaultGroup, Score: 0}}
			}
		}
		for _, z := range zs {
			g, _ := z.Member.(string)
			if g == "" {
				continue
			}
			readyKey := "queue:" + queueName + ":" + pref + "group:" + g + ":ready"
			parts, err := a.redis.ZRange(ctx, readyKey, 0, -1).Result()
			if err != nil {
				continue
			}
			for _, part := range parts {
				if score, err := a.redis.ZScore(ctx, blockedKey, part).Result(); err == nil && nowMs < score {
					continue
				}
				priosKey := "queue:" + queueName + ":" + pref + "partition:" + part + ":priorities"
				prios, err := a.redis.ZRevRange(ctx, priosKey, 0, -1).Result()
				if err != nil || len(prios) == 0 {
					continue
				}
				for _, prioStr := range prios {
					prio, _ := strconv.Atoi(prioStr)
					qKey := "queue:" + queueName + ":" + pref + "partition:" + part + ":" + prioStr
					head, err := a.redis.ZRangeWithScores(ctx, qKey, 0, 0).Result()
					if err != nil || len(head) == 0 {
						continue
					}
					id, _ := head[0].Member.(string)
					if id == "" || head[0].Score > nowMs {
						continue // future scheduled
					}
					pgroups, _ := a.redis.SMembers(ctx, "queue:"+queueName+":partition:"+part+":groups").Result()
					better(&OldestReadyTask{
						ID:                id,
						Partition:         part,
						QueuePriority:     qp,
						PartitionPriority: prio,
						Score:             head[0].Score,
						Groups:            pgroups,
					})
					break // next partition — head of highest prio only
				}
			}
		}
	}
	return best, nil
}

// isPartitionKeyMeta — ключи partition:* не являющиеся ZSET задач.
func isPartitionKeyMeta(key, prefix string) bool {
	if !strings.HasPrefix(key, prefix) {
		return true
	}
	switch {
	case strings.HasSuffix(key, ":priorities"),
		strings.HasSuffix(key, ":groups"),
		strings.HasSuffix(key, ":lock"),
		strings.HasSuffix(key, ":count"):
		return true
	}
	rest := strings.TrimPrefix(key, prefix)
	// taskID mapping — без ':'
	return !strings.Contains(rest, ":")
}
