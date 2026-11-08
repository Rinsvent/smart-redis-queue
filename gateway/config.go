package gateway

import (
	"fmt"
	"os"
	"time"

	redisqueue "github.com/Rinsvent/smart-redis-queue"
	"github.com/redis/go-redis/v9"
	"gopkg.in/yaml.v3"
)

// FileConfig — YAML-конфиг процесса gateway (порты, Redis, шарды, dump).
type FileConfig struct {
	Listen           string      `yaml:"listen"`
	HTTP             string      `yaml:"http"`
	Dump             string      `yaml:"dump"`
	SkipPersistCheck bool        `yaml:"skip_persist_check"`
	StatsRetention   string      `yaml:"stats_retention"` // e.g. "1h"; empty = off
	Redis            RedisFile   `yaml:"redis"`
	Shards           *ShardsFile `yaml:"shards"`
}

type RedisFile struct {
	Addr     string `yaml:"addr"`
	Password string `yaml:"password"`
	DB       int    `yaml:"db"`
}

type ShardsFile struct {
	Strategy          string            `yaml:"strategy"` // by_group | by_queue | by_partition | by_manual
	StrictSingleGroup bool              `yaml:"strict_single_group"`
	KeyMap            map[string]string `yaml:"key_map"`
	Shards            []ShardFile       `yaml:"list"`
}

type ShardFile struct {
	Name     string `yaml:"name"`
	Addr     string `yaml:"addr"`
	Password string `yaml:"password"`
	DB       int    `yaml:"db"`
	Weight   int    `yaml:"weight"`
	ReadOnly bool   `yaml:"read_only"`
}

// LoadFileConfig читает YAML. Пустые поля заполняются дефолтами.
func LoadFileConfig(path string) (FileConfig, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return FileConfig{}, err
	}
	var fc FileConfig
	if err := yaml.Unmarshal(raw, &fc); err != nil {
		return FileConfig{}, fmt.Errorf("gateway config: %w", err)
	}
	fc.applyDefaults()
	return fc, nil
}

func (fc *FileConfig) applyDefaults() {
	if fc.Listen == "" {
		fc.Listen = "127.0.0.1:50051"
	}
	if fc.HTTP == "" {
		fc.HTTP = "127.0.0.1:50052"
	}
	if fc.Redis.Addr == "" {
		fc.Redis.Addr = "localhost:6379"
	}
}

// RedisClient строит клиент из секции redis.
func (fc FileConfig) RedisClient() *redis.Client {
	return redis.NewClient(&redis.Options{
		Addr:     fc.Redis.Addr,
		Password: fc.Redis.Password,
		DB:       fc.Redis.DB,
	})
}

// ShardConfig — nil, если секция shards отсутствует.
func (fc FileConfig) ShardConfig() (*redisqueue.ShardConfig, error) {
	if fc.Shards == nil || len(fc.Shards.Shards) == 0 {
		return nil, nil
	}
	strategy, err := parseShardStrategy(fc.Shards.Strategy)
	if err != nil {
		return nil, err
	}
	shards := make([]redisqueue.Shard, 0, len(fc.Shards.Shards))
	for _, s := range fc.Shards.Shards {
		shards = append(shards, redisqueue.Shard{
			Name:     s.Name,
			Addr:     s.Addr,
			Password: s.Password,
			DB:       s.DB,
			Weight:   s.Weight,
			ReadOnly: s.ReadOnly,
		})
	}
	return &redisqueue.ShardConfig{
		Strategy:          strategy,
		Shards:            shards,
		KeyMap:            fc.Shards.KeyMap,
		StrictSingleGroup: fc.Shards.StrictSingleGroup,
	}, nil
}

func parseShardStrategy(s string) (redisqueue.ShardStrategy, error) {
	switch s {
	case "", "by_group", "group":
		return redisqueue.ShardByGroup, nil
	case "by_queue", "queue":
		return redisqueue.ShardByQueue, nil
	case "by_partition", "partition":
		return redisqueue.ShardByPartition, nil
	case "by_manual", "manual":
		return redisqueue.ShardByManual, nil
	default:
		return 0, fmt.Errorf("unknown shard strategy %q", s)
	}
}

// StatsRetentionDuration парсит stats_retention (например "1h"); 0 если пусто/невалидно.
func (fc FileConfig) StatsRetentionDuration() time.Duration {
	if fc.StatsRetention == "" {
		return 0
	}
	d, err := time.ParseDuration(fc.StatsRetention)
	if err != nil {
		return 0
	}
	return d
}
