package redisqueue

import (
	"container/heap"
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/redis/go-redis/v9"
)

// RaceLockEntry — активный concurrency lock (race!).
type RaceLockEntry struct {
	Tag    string
	TaskID string
}

// PoisonTask — задача с высоким reject_count (ещё в системе).
type PoisonTask struct {
	ID          string
	Partition   string
	RejectCount int
}

// ListLockedPartitions — ordered-партиции с существующим :lock (SCAN *:lock).
func (a *Admin) ListLockedPartitions(ctx context.Context, queueName string) ([]PartitionStats, error) {
	prefix := "queue:" + queueName + ":partition:"
	suffix := ":lock"
	var ids []string
	iter := a.redis.Scan(ctx, 0, prefix+"*"+suffix, scanMatchCount).Iterator()
	for iter.Next(ctx) {
		key := iter.Val()
		if !strings.HasPrefix(key, prefix) || !strings.HasSuffix(key, suffix) {
			continue
		}
		part := strings.TrimSuffix(strings.TrimPrefix(key, prefix), suffix)
		if part == "" || strings.HasSuffix(part, ":count") {
			continue
		}
		// consumer count keys: …:partition:{p}:count — pattern *:lock не матчит :count
		ids = append(ids, part)
	}
	if err := iter.Err(); err != nil {
		return nil, err
	}
	sort.Strings(ids)
	consumers, _ := a.redis.SMembers(ctx, "queue:"+queueName+":consumers").Result()
	out := make([]PartitionStats, 0, len(ids))
	for _, part := range ids {
		ps := a.partitionStats(ctx, queueName, part, consumers)
		ps.Locked = true
		out = append(out, ps)
	}
	return out, nil
}

// ListRaceLocks — HGETALL queue:{q}:race:locks.
func (a *Admin) ListRaceLocks(ctx context.Context, queueName string) ([]RaceLockEntry, error) {
	m, err := a.redis.HGetAll(ctx, "queue:"+queueName+":race:locks").Result()
	if err == redis.Nil {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	out := make([]RaceLockEntry, 0, len(m))
	for tag, taskID := range m {
		if tag == "" {
			continue
		}
		out = append(out, RaceLockEntry{Tag: tag, TaskID: taskID})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Tag < out[j].Tag })
	return out, nil
}

type pendingHeap []PartitionStats

func (h pendingHeap) Len() int           { return len(h) }
func (h pendingHeap) Less(i, j int) bool { return h[i].Pending < h[j].Pending } // min-heap
func (h pendingHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *pendingHeap) Push(x any)        { *h = append(*h, x.(PartitionStats)) }
func (h *pendingHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[:n-1]
	return x
}

// ScanTopPending — SSCAN partitions + ZCARD; возвращает topN по Pending (фон, не hot path).
func (a *Admin) ScanTopPending(ctx context.Context, queueName string, topN int) ([]PartitionStats, int, error) {
	if topN <= 0 {
		topN = 20
	}
	if topN > MaxListPartitions {
		topN = MaxListPartitions
	}
	ids, total, err := a.unionPartitionIDs(ctx, queueName, "", 0)
	if err != nil {
		return nil, 0, err
	}
	consumers, _ := a.redis.SMembers(ctx, "queue:"+queueName+":consumers").Result()
	h := &pendingHeap{}
	heap.Init(h)

	for _, part := range ids {
		if ctx.Err() != nil {
			return nil, total, ctx.Err()
		}
		ps := a.partitionStats(ctx, queueName, part, consumers)
		if h.Len() < topN {
			heap.Push(h, ps)
		} else if ps.Pending > (*h)[0].Pending {
			heap.Pop(h)
			heap.Push(h, ps)
		}
	}

	out := make([]PartitionStats, h.Len())
	for i := len(out) - 1; i >= 0; i-- {
		out[i] = heap.Pop(h).(PartitionStats)
	}
	return out, total, nil
}

type poisonHeap []PoisonTask

func (h poisonHeap) Len() int           { return len(h) }
func (h poisonHeap) Less(i, j int) bool { return h[i].RejectCount < h[j].RejectCount }
func (h poisonHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *poisonHeap) Push(x any)        { *h = append(*h, x.(PoisonTask)) }
func (h *poisonHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[:n-1]
	return x
}

// ScanHighReject — SCAN reject_count:*; topN задач с RejectCount >= minReject.
func (a *Admin) ScanHighReject(ctx context.Context, queueName string, minReject, topN int) ([]PoisonTask, error) {
	if minReject < 1 {
		minReject = 1
	}
	if topN <= 0 {
		topN = 50
	}
	if topN > 200 {
		topN = 200
	}
	prefix := "queue:" + queueName + ":reject_count:"
	h := &poisonHeap{}
	heap.Init(h)

	iter := a.redis.Scan(ctx, 0, prefix+"*", scanMatchCount).Iterator()
	for iter.Next(ctx) {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		key := iter.Val()
		if !strings.HasPrefix(key, prefix) {
			continue
		}
		taskID := strings.TrimPrefix(key, prefix)
		if taskID == "" {
			continue
		}
		raw, err := a.redis.Get(ctx, key).Result()
		if err == redis.Nil {
			continue
		}
		if err != nil {
			return nil, err
		}
		n, err := strconv.Atoi(raw)
		if err != nil || n < minReject {
			continue
		}
		part, _ := a.redis.Get(ctx, "queue:"+queueName+":partition:"+taskID).Result()
		pt := PoisonTask{ID: taskID, Partition: part, RejectCount: n}
		if h.Len() < topN {
			heap.Push(h, pt)
		} else if n > (*h)[0].RejectCount {
			heap.Pop(h)
			heap.Push(h, pt)
		}
	}
	if err := iter.Err(); err != nil {
		return nil, err
	}

	out := make([]PoisonTask, h.Len())
	for i := len(out) - 1; i >= 0; i-- {
		out[i] = heap.Pop(h).(PoisonTask)
	}
	return out, nil
}

// LockOwner — значение :lock (consumer id) или "".
func (a *Admin) LockOwner(ctx context.Context, queueName, partition string) (string, error) {
	if partition == "" {
		return "", fmt.Errorf("partition required")
	}
	v, err := a.redis.Get(ctx, "queue:"+queueName+":partition:"+partition+":lock").Result()
	if err == redis.Nil {
		return "", nil
	}
	return v, err
}
