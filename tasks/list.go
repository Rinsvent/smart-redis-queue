package tasks

import (
	"context"
	"encoding/json"
	"strings"

	"github.com/redis/go-redis/v9"
)

// TaskView — строка UI mode tasks: catalog + last-run.
type TaskView struct {
	Namespace   string            `json:"namespace"`
	Name        string            `json:"name"`
	Queue       string            `json:"queue"`
	Cron        string            `json:"cron,omitempty"`
	Concurrency string            `json:"concurrency,omitempty"`
	Args        []ArgDef          `json:"args,omitempty"`
	Run         map[string]string `json:"run,omitempty"`
}

// ListForQueue — все catalog entries с Queue==queue по всем namespace.
func ListForQueue(ctx context.Context, rdb *redis.Client, queue string) ([]TaskView, error) {
	nsList, err := ListNamespaces(ctx, rdb)
	if err != nil {
		return nil, err
	}
	var out []TaskView
	for _, ns := range nsList {
		raw, err := rdb.HGetAll(ctx, catalogKey(ns)).Result()
		if err != nil {
			return nil, err
		}
		for name, v := range raw {
			var e CatalogEntry
			if json.Unmarshal([]byte(v), &e) != nil {
				continue
			}
			if e.Name == "" {
				e.Name = name
			}
			if queue != "" && e.Queue != queue {
				continue
			}
			run, err := rdb.HGetAll(ctx, runKey(ns, e.Name)).Result()
			if err != nil {
				return nil, err
			}
			if len(run) == 0 {
				run = nil
			}
			out = append(out, TaskView{
				Namespace: ns, Name: e.Name, Queue: e.Queue,
				Cron: e.Cron, Concurrency: e.Concurrency,
				Args: e.Args, Run: run,
			})
		}
	}
	return out, nil
}

// FilterQuery — подстрока по name.
func FilterQuery(views []TaskView, query string) []TaskView {
	query = strings.TrimSpace(query)
	if query == "" {
		return views
	}
	out := make([]TaskView, 0, len(views))
	for _, v := range views {
		if v.Name == query || strings.Contains(v.Name, query) {
			out = append(out, v)
		}
	}
	return out
}
