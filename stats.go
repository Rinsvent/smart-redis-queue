package redisqueue

import (
	"context"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
)

// StatsSink копит get/ack/reject по unix-секунде и флашит в Redis.
// Сэмпл тегов (SCARD) — отдельный тик ~1s. Default: выключен (retention=0).
type StatsSink struct {
	redis *redis.Client
	queue string

	mu        sync.Mutex
	retention time.Duration
	tags      []string
	buckets   map[int64]*secCounters

	stopOnce sync.Once
	stopCh   chan struct{}
	wg       sync.WaitGroup
	loopOn   atomic.Bool
}

type secCounters struct {
	get    atomic.Int64
	ack    atomic.Int64
	reject atomic.Int64
}

func newStatsSink(rdb *redis.Client, queue string) *StatsSink {
	return &StatsSink{
		redis:   rdb,
		queue:   queue,
		buckets: map[int64]*secCounters{},
		stopCh:  make(chan struct{}),
	}
}

func (s *StatsSink) enabled() bool {
	if s == nil {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.retention > 0
}

func (s *StatsSink) setRetention(d time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if d < 0 {
		d = 0
	}
	s.retention = d
}

func (s *StatsSink) setTags(tags []string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]string, 0, len(tags))
	seen := map[string]struct{}{}
	for _, t := range tags {
		if t == "" {
			continue
		}
		if _, ok := seen[t]; ok {
			continue
		}
		seen[t] = struct{}{}
		out = append(out, t)
	}
	s.tags = out
}

func (s *StatsSink) retentionSec() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.retention <= 0 {
		return 0
	}
	sec := int64(s.retention / time.Second)
	if sec < 1 {
		sec = 1
	}
	return sec
}

func (s *StatsSink) tagsCopy() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.tags) == 0 {
		return nil
	}
	out := make([]string, len(s.tags))
	copy(out, s.tags)
	return out
}

func (s *StatsSink) bucket(sec int64) *secCounters {
	s.mu.Lock()
	defer s.mu.Unlock()
	b := s.buckets[sec]
	if b == nil {
		b = &secCounters{}
		s.buckets[sec] = b
	}
	return b
}

func (s *StatsSink) addGet(n int64) {
	if n <= 0 || !s.enabled() {
		return
	}
	s.bucket(time.Now().Unix()).get.Add(n)
}

func (s *StatsSink) addAck(n int64) {
	if n <= 0 || !s.enabled() {
		return
	}
	s.bucket(time.Now().Unix()).ack.Add(n)
}

func (s *StatsSink) addReject(n int64) {
	if n <= 0 || !s.enabled() {
		return
	}
	s.bucket(time.Now().Unix()).reject.Add(n)
}

func (s *StatsSink) ensureLoop() {
	if s == nil || !s.enabled() {
		return
	}
	if s.loopOn.Swap(true) {
		return
	}
	s.wg.Add(1)
	go s.loop()
}

func (s *StatsSink) Stop() {
	if s == nil || !s.loopOn.Load() {
		return
	}
	s.stopOnce.Do(func() { close(s.stopCh) })
	s.wg.Wait()
}

func (s *StatsSink) loop() {
	defer s.wg.Done()
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	ctx := context.Background()
	for {
		select {
		case <-s.stopCh:
			_ = s.flush(ctx, true)
			return
		case <-ticker.C:
			_ = s.flush(ctx, false)
			_ = s.sampleTags(ctx)
		}
	}
}

func (s *StatsSink) flush(ctx context.Context, all bool) error {
	if !s.enabled() {
		return nil
	}
	now := time.Now().Unix()
	type snap struct {
		sec              int64
		get, ack, reject int64
	}
	var pending []snap

	s.mu.Lock()
	for sec, b := range s.buckets {
		if !all && sec >= now {
			continue
		}
		pending = append(pending, snap{
			sec:    sec,
			get:    b.get.Swap(0),
			ack:    b.ack.Swap(0),
			reject: b.reject.Swap(0),
		})
		delete(s.buckets, sec)
	}
	ret := s.retention
	s.mu.Unlock()

	if len(pending) == 0 {
		return nil
	}
	retSec := int64(ret / time.Second)
	if retSec < 1 {
		retSec = 1
	}
	indexKey := "queue:" + s.queue + ":stats:index"
	pipe := s.redis.Pipeline()
	for _, p := range pending {
		if p.get == 0 && p.ack == 0 && p.reject == 0 {
			continue
		}
		key := "queue:" + s.queue + ":stats:" + strconv.FormatInt(p.sec, 10)
		if p.get != 0 {
			pipe.HIncrBy(ctx, key, "g", p.get)
		}
		if p.ack != 0 {
			pipe.HIncrBy(ctx, key, "a", p.ack)
		}
		if p.reject != 0 {
			pipe.HIncrBy(ctx, key, "r", p.reject)
		}
		pipe.Expire(ctx, key, time.Duration(retSec)*time.Second)
		pipe.ZAdd(ctx, indexKey, redis.Z{Score: float64(p.sec), Member: strconv.FormatInt(p.sec, 10)})
	}
	pipe.ZRemRangeByScore(ctx, indexKey, "-inf", strconv.FormatInt(now-retSec, 10))
	_, err := pipe.Exec(ctx)
	return err
}

func (s *StatsSink) sampleTags(ctx context.Context) error {
	if !s.enabled() {
		return nil
	}
	tags := s.tagsCopy()
	if len(tags) == 0 {
		return nil
	}
	ret := s.retentionSec()
	args := make([]interface{}, 0, 2+len(tags))
	args = append(args, s.queue, ret)
	for _, t := range tags {
		args = append(args, t)
	}
	_, err := getSampleTagsStatsScript().Run(ctx, s.redis, []string{}, args...).Result()
	return err
}
