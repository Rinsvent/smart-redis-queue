package gateway

import (
	"strings"

	redisqueue "github.com/Rinsvent/smart-redis-queue"
)

// PublishOpts — опции Publish (HTTP / gRPC / внутренний вызов).
type PublishOpts struct {
	TagLimits        map[string]int64
	ShardKey         string
	ShardName        string
	Compression      string // "" | none | zstd
	CompressMinBytes int
}

func (o PublishOpts) toOptions() []redisqueue.PublishOption {
	var opts []redisqueue.PublishOption
	if len(o.TagLimits) > 0 {
		opts = append(opts, redisqueue.WithTagLimits(o.TagLimits))
	}
	if o.ShardKey != "" {
		opts = append(opts, redisqueue.WithShardKey(o.ShardKey))
	}
	if o.ShardName != "" {
		opts = append(opts, redisqueue.WithShardName(o.ShardName))
	}
	switch strings.ToLower(strings.TrimSpace(o.Compression)) {
	case "zstd":
		opts = append(opts, redisqueue.WithCompression(redisqueue.CompressionZstd))
	case "none":
		opts = append(opts, redisqueue.WithCompression(redisqueue.CompressionNone))
	}
	if o.CompressMinBytes > 0 {
		opts = append(opts, redisqueue.WithCompressMinBytes(o.CompressMinBytes))
	}
	return opts
}
