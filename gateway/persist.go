package gateway

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

const maxAllowedLossWindow = time.Second

// PersistInfo — окно потери Redis по CONFIG.
type PersistInfo struct {
	AppendOnly     bool
	AppendFsync    string // always | everysec | no | unknown
	LossWindow     time.Duration
	RawAppendOnly  string
	RawAppendFsync string
}

// DetectLossWindow читает appendonly/appendfsync у Redis.
// always → 0; everysec → 1s; иначе ErrPersist (в т.ч. appendonly no / appendfsync no).
func DetectLossWindow(ctx context.Context, rdb redis.Cmdable) (PersistInfo, error) {
	ao, err := rdb.ConfigGet(ctx, "appendonly").Result()
	if err != nil {
		return PersistInfo{}, fmt.Errorf("gateway: CONFIG GET appendonly: %w", err)
	}
	af, err := rdb.ConfigGet(ctx, "appendfsync").Result()
	if err != nil {
		return PersistInfo{}, fmt.Errorf("gateway: CONFIG GET appendfsync: %w", err)
	}
	return lossWindowFromConfig(ao, af)
}

func lossWindowFromConfig(appendOnly, appendFsync map[string]string) (PersistInfo, error) {
	info := PersistInfo{
		RawAppendOnly:  configVal(appendOnly, "appendonly"),
		RawAppendFsync: strings.ToLower(strings.TrimSpace(configVal(appendFsync, "appendfsync"))),
	}
	info.AppendOnly = isRedisYes(info.RawAppendOnly)
	info.AppendFsync = info.RawAppendFsync
	if info.AppendFsync == "" {
		info.AppendFsync = "unknown"
	}

	if !info.AppendOnly {
		return info, fmt.Errorf("%w: appendonly=%q (need yes)", ErrPersist, info.RawAppendOnly)
	}
	switch info.AppendFsync {
	case "always":
		info.LossWindow = 0
	case "everysec":
		info.LossWindow = time.Second
	case "no":
		return info, fmt.Errorf("%w: appendfsync=no (unbounded)", ErrPersist)
	default:
		return info, fmt.Errorf("%w: appendfsync=%q", ErrPersist, info.AppendFsync)
	}
	if info.LossWindow > maxAllowedLossWindow {
		return info, fmt.Errorf("%w: loss window %s > %s", ErrPersist, info.LossWindow, maxAllowedLossWindow)
	}
	return info, nil
}

func configVal(m map[string]string, key string) string {
	if m == nil {
		return ""
	}
	if v, ok := m[key]; ok {
		return v
	}
	for k, v := range m {
		if strings.EqualFold(k, key) {
			return v
		}
	}
	return ""
}

func isRedisYes(v string) bool {
	switch strings.ToLower(strings.TrimSpace(v)) {
	case "yes", "true", "1":
		return true
	default:
		return false
	}
}
