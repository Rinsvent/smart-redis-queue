package redisqueue

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/redis/go-redis/v9"
)

const MaxListMessages = 50

// ListMessagesOptions — фильтры Admin.ListMessages (peek, без consume).
type ListMessagesOptions struct {
	Tag   string // SSCAN tag:{tag}
	Query string // exact id или подстрока
	Limit int    // default/max 50
}

// MessageView — снимок задачи без изъятия из очереди.
type MessageView struct {
	ID          string
	Partition   string
	State       string // pending | in_progress | unknown
	RejectCount int64
	Step        string
	Progress    *StepProgressView
}

// StepProgressView — разобранный progress из step-ключа.
type StepProgressView struct {
	Name         string
	Curr         int
	Total        int
	UnknownTotal bool
	StartedMs    int64
	Control      string
}

// TaskInspect — полный peek по id (payload/headers/step/reject).
type TaskInspect struct {
	ID          string
	Exists      bool
	Payload     string
	Headers     map[string]string
	Partition   string
	Priority    string
	RejectCount int64
	Step        string
	Progress    *StepProgressView
	State       string
}

// ListMessages — до Limit задач: exact id / tag / SCAN payload:*. Без consumer Get.
func (a *Admin) ListMessages(ctx context.Context, queueName string, opts ListMessagesOptions) ([]MessageView, error) {
	if queueName == "" {
		return nil, fmt.Errorf("admin: empty queue")
	}
	limit := opts.Limit
	if limit <= 0 || limit > MaxListMessages {
		limit = MaxListMessages
	}
	query := strings.TrimSpace(opts.Query)
	tag := strings.TrimSpace(opts.Tag)

	if query != "" {
		if exists, _ := a.redis.Exists(ctx, "queue:"+queueName+":payload:"+query).Result(); exists > 0 {
			mv, err := a.messageView(ctx, queueName, query)
			if err != nil {
				return nil, err
			}
			return []MessageView{mv}, nil
		}
	}

	ids := make([]string, 0, limit)
	seen := map[string]struct{}{}
	add := func(id string) bool {
		if id == "" {
			return false
		}
		if query != "" && id != query && !strings.Contains(id, query) {
			return false
		}
		if _, ok := seen[id]; ok {
			return false
		}
		seen[id] = struct{}{}
		ids = append(ids, id)
		return len(ids) >= limit
	}

	if tag != "" {
		key := "queue:" + queueName + ":tag:" + tag
		var cursor uint64
		for {
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			keys, next, err := a.redis.SScan(ctx, key, cursor, "*", scanMatchCount).Result()
			if err != nil {
				return nil, err
			}
			for _, id := range keys {
				if add(id) {
					return a.messageViews(ctx, queueName, ids)
				}
			}
			cursor = next
			if cursor == 0 {
				break
			}
		}
		return a.messageViews(ctx, queueName, ids)
	}

	prefix := "queue:" + queueName + ":payload:"
	iter := a.redis.Scan(ctx, 0, prefix+"*", scanMatchCount).Iterator()
	for iter.Next(ctx) {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		key := iter.Val()
		if !strings.HasPrefix(key, prefix) {
			continue
		}
		id := strings.TrimPrefix(key, prefix)
		if id == "" {
			continue
		}
		if add(id) {
			break
		}
	}
	if err := iter.Err(); err != nil {
		return nil, err
	}
	return a.messageViews(ctx, queueName, ids)
}

func (a *Admin) messageViews(ctx context.Context, queueName string, ids []string) ([]MessageView, error) {
	out := make([]MessageView, 0, len(ids))
	for _, id := range ids {
		mv, err := a.messageView(ctx, queueName, id)
		if err != nil {
			return nil, err
		}
		out = append(out, mv)
	}
	return out, nil
}

func (a *Admin) messageView(ctx context.Context, queueName, id string) (MessageView, error) {
	part, _ := a.redis.Get(ctx, "queue:"+queueName+":partition:"+id).Result()
	step, _ := a.redis.Get(ctx, "queue:"+queueName+":step:"+id).Result()
	rejStr, _ := a.redis.Get(ctx, "queue:"+queueName+":reject_count:"+id).Result()
	var rej int64
	if rejStr != "" {
		rej, _ = strconv.ParseInt(rejStr, 10, 64)
	}
	state := a.taskState(ctx, queueName, id)
	mv := MessageView{
		ID: id, Partition: part, State: state, RejectCount: rej, Step: step,
		Progress: stepProgressView(step),
	}
	return mv, nil
}

func (a *Admin) taskState(ctx context.Context, queueName, id string) string {
	consumers, _ := a.redis.SMembers(ctx, "queue:"+queueName+":consumers").Result()
	for _, cid := range consumers {
		if cid == "" {
			continue
		}
		n, err := a.redis.HExists(ctx, "queue:"+queueName+":consumer:"+cid+":tasks", id).Result()
		if err == nil && n {
			return "in_progress"
		}
	}
	if part, err := a.redis.Get(ctx, "queue:"+queueName+":partition:"+id).Result(); err == nil && part != "" {
		return "pending"
	}
	if n, _ := a.redis.Exists(ctx, "queue:"+queueName+":payload:"+id).Result(); n > 0 {
		return "unknown"
	}
	return "unknown"
}

func stepProgressView(step string) *StepProgressView {
	cp := ParseStepCheckpoint(step)
	if !cp.HasProgress {
		return nil
	}
	v := &StepProgressView{
		Name: cp.Name, Curr: cp.Curr, Total: cp.Total, UnknownTotal: cp.UnknownTotal, Control: cp.Control,
	}
	if !cp.Started.IsZero() {
		v.StartedMs = cp.Started.UnixMilli()
	}
	return v
}

// InspectTask — peek одной задачи по id.
func (a *Admin) InspectTask(ctx context.Context, queueName, taskID string) (*TaskInspect, error) {
	if queueName == "" || taskID == "" {
		return nil, fmt.Errorf("admin: queue and task id required")
	}
	ti := &TaskInspect{ID: taskID}
	payload, err := a.redis.Get(ctx, "queue:"+queueName+":payload:"+taskID).Result()
	if err == redis.Nil {
		return ti, nil
	}
	if err != nil {
		return nil, err
	}
	ti.Exists = true
	ti.Payload = payload
	ti.Partition, _ = a.redis.Get(ctx, "queue:"+queueName+":partition:"+taskID).Result()
	ti.Priority, _ = a.redis.Get(ctx, "queue:"+queueName+":priority:"+taskID).Result()
	ti.Headers, _ = a.redis.HGetAll(ctx, "queue:"+queueName+":headers:"+taskID).Result()
	if len(ti.Headers) == 0 {
		ti.Headers = nil
	}
	ti.Step, _ = a.redis.Get(ctx, "queue:"+queueName+":step:"+taskID).Result()
	if rejStr, e := a.redis.Get(ctx, "queue:"+queueName+":reject_count:"+taskID).Result(); e == nil && rejStr != "" {
		ti.RejectCount, _ = strconv.ParseInt(rejStr, 10, 64)
	}
	ti.Progress = stepProgressView(ti.Step)
	ti.State = a.taskState(ctx, queueName, taskID)
	return ti, nil
}

// SetTaskStepControl — pause|stop|"" на step-ключе задачи.
func (a *Admin) SetTaskStepControl(ctx context.Context, queueName, taskID, control string) error {
	return SetStepControl(ctx, a.redis, queueName, taskID, control)
}
