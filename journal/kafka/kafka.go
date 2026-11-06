package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/IBM/sarama"
	redisqueue "github.com/Rinsvent/smart-redis-queue"
	"github.com/Rinsvent/smart-redis-queue/journal"
	"github.com/redis/go-redis/v9"
)

type Config struct {
	Brokers  []string
	InTopic  string
	AckTopic string
	// ScheduleTopic — far-future (> DueSkew). Пусто = всё в InTopic (Redis будет ждать Scheduled).
	ScheduleTopic string
	// CursorRDB + JournalName — персистентный offset schedule-mover (тот же Redis, что у Bridge).
	CursorRDB   *redis.Client
	JournalName string
}

type Source struct {
	cfg      Config
	client   sarama.Client
	producer sarama.SyncProducer

	schedMu  sync.Mutex
	schedOff int64 // последний обработанный offset ScheduleTopic; -1 = ещё не читали
}

type envelope struct {
	ID        string   `json:"id"`
	Partition string   `json:"partition"`
	Priority  int      `json:"priority"`
	Payload   []byte   `json:"payload"`
	Tags      []string `json:"tags"`
	Groups    []string `json:"groups"`
	Scheduled int64    `json:"scheduled_ms"`
}

func New(cfg Config) (*Source, error) {
	sc := sarama.NewConfig()
	sc.Version = sarama.V2_6_0_0
	sc.Producer.Return.Successes = true
	sc.Producer.Partitioner = sarama.NewManualPartitioner // Publish всегда в partition 0
	cl, err := sarama.NewClient(cfg.Brokers, sc)
	if err != nil {
		return nil, err
	}
	prod, err := sarama.NewSyncProducerFromClient(cl)
	if err != nil {
		_ = cl.Close()
		return nil, err
	}
	s := &Source{cfg: cfg, client: cl, producer: prod, schedOff: -1}
	if cfg.CursorRDB != nil && cfg.JournalName != "" {
		if v, err := cfg.CursorRDB.Get(context.Background(), s.schedCursorKey()).Result(); err == nil {
			if off, err := strconv.ParseInt(v, 10, 64); err == nil {
				s.schedOff = off
			}
		}
	}
	return s, nil
}

func (s *Source) schedCursorKey() string {
	return "srq:journal:" + s.cfg.JournalName + ":kafka_sched"
}

func (s *Source) Close() error {
	_ = s.producer.Close()
	return s.client.Close()
}

func (s *Source) Init(context.Context) error { return nil }

// Publish — due → InTopic; far → ScheduleTopic (если задан), иначе InTopic.
func (s *Source) Publish(ctx context.Context, tasks ...*redisqueue.Task) error {
	now := time.Now()
	for _, t := range tasks {
		sch := journal.ScheduledOrNow(t.Scheduled)
		env := envelope{
			ID: t.ID, Partition: t.Partition, Priority: t.PartitionPriority,
			Payload: t.Payload, Tags: t.Tags, Groups: t.Groups,
			Scheduled: sch.UnixMilli(),
		}
		body, err := json.Marshal(env)
		if err != nil {
			return err
		}
		topic := s.cfg.InTopic
		if s.cfg.ScheduleTopic != "" && !journal.IsDue(sch, now) {
			topic = s.cfg.ScheduleTopic
		}
		// Контракт v1: одна партиция (0). Иначе ReadBatch/PromoteDue читают только partition 0.
		_, _, err = s.producer.SendMessage(&sarama.ProducerMessage{
			Topic:     topic,
			Partition: 0,
			Key:       sarama.StringEncoder(t.ID),
			Value:     sarama.ByteEncoder(body),
		})
		if err != nil {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
	}
	return nil
}

func (s *Source) ReadBatch(ctx context.Context, after journal.Cursor, n int) ([]journal.Record, error) {
	if n < 1 {
		n = 32
	}
	var off int64
	if after.Offset < 0 {
		off = sarama.OffsetOldest
	} else {
		off = after.Offset + 1
	}
	cons, err := sarama.NewConsumerFromClient(s.client)
	if err != nil {
		return nil, err
	}
	defer cons.Close()
	pc, err := cons.ConsumePartition(s.cfg.InTopic, after.Partition, off)
	if err != nil {
		return nil, err
	}
	defer pc.Close()

	var out []journal.Record
	timer := time.NewTimer(800 * time.Millisecond)
	defer timer.Stop()
	for len(out) < n {
		select {
		case <-ctx.Done():
			return out, ctx.Err()
		case <-timer.C:
			return out, nil
		case msg, ok := <-pc.Messages():
			if !ok {
				return out, nil
			}
			if after.Offset >= 0 && msg.Offset <= after.Offset {
				continue
			}
			rec, err := recordFromMsg(s.cfg.InTopic, msg)
			if err != nil {
				return nil, err
			}
			out = append(out, rec)
		}
	}
	return out, nil
}

func (s *Source) ReadByOffset(ctx context.Context, c journal.Cursor) (journal.Record, error) {
	cons, err := sarama.NewConsumerFromClient(s.client)
	if err != nil {
		return journal.Record{}, err
	}
	defer cons.Close()
	pc, err := cons.ConsumePartition(s.cfg.InTopic, c.Partition, c.Offset)
	if err != nil {
		return journal.Record{}, err
	}
	defer pc.Close()
	select {
	case <-ctx.Done():
		return journal.Record{}, ctx.Err()
	case msg, ok := <-pc.Messages():
		if !ok {
			return journal.Record{}, fmt.Errorf("kafka: no message")
		}
		if msg.Offset != c.Offset {
			return journal.Record{}, fmt.Errorf("kafka: want offset %d got %d", c.Offset, msg.Offset)
		}
		return recordFromMsg(s.cfg.InTopic, msg)
	case <-time.After(2 * time.Second):
		return journal.Record{}, fmt.Errorf("kafka: fetch timeout")
	}
}

func recordFromMsg(topic string, msg *sarama.ConsumerMessage) (journal.Record, error) {
	var env envelope
	if err := json.Unmarshal(msg.Value, &env); err != nil {
		return journal.Record{}, err
	}
	sch := time.UnixMilli(env.Scheduled)
	if env.Scheduled == 0 {
		sch = msg.Timestamp
	}
	id := env.ID
	if id == "" {
		id = fmt.Sprintf("%s-%d-%d", topic, msg.Partition, msg.Offset)
	}
	return journal.Record{
		Task: &redisqueue.Task{
			ID: id, Partition: env.Partition, PartitionPriority: env.Priority,
			Payload: env.Payload, Tags: env.Tags, Groups: env.Groups, Scheduled: sch,
		},
		Cursor: journal.KafkaCursor(msg.Partition, msg.Offset),
	}, nil
}

func (s *Source) Ack(_ context.Context, taskID string, _ map[string]string) error {
	_, _, err := s.producer.SendMessage(&sarama.ProducerMessage{
		Topic:     s.cfg.AckTopic,
		Partition: 0,
		Key:       sarama.StringEncoder(taskID),
		Value:     sarama.StringEncoder(taskID),
	})
	return err
}

func (s *Source) ReadAcks(ctx context.Context, from journal.Cursor, n int) ([]journal.AckEvent, error) {
	if s.cfg.AckTopic == "" {
		return nil, nil
	}
	if n < 1 {
		n = 32
	}
	var off int64
	if from.Offset < 0 {
		off = sarama.OffsetOldest
	} else {
		off = from.Offset + 1
	}
	cons, err := sarama.NewConsumerFromClient(s.client)
	if err != nil {
		return nil, err
	}
	defer cons.Close()
	pc, err := cons.ConsumePartition(s.cfg.AckTopic, from.Partition, off)
	if err != nil {
		return nil, err
	}
	defer pc.Close()
	var out []journal.AckEvent
	timer := time.NewTimer(800 * time.Millisecond)
	defer timer.Stop()
	for len(out) < n {
		select {
		case <-ctx.Done():
			return out, ctx.Err()
		case <-timer.C:
			return out, nil
		case msg, ok := <-pc.Messages():
			if !ok {
				return out, nil
			}
			out = append(out, journal.AckEvent{
				TaskID: string(msg.Value),
				Cursor: journal.KafkaCursor(msg.Partition, msg.Offset),
				At:     msg.Timestamp,
			})
		}
	}
	return out, nil
}

// PromoteDue — due из ScheduleTopic → InTopic (новый offset в main stream).
func (s *Source) PromoteDue(ctx context.Context, limit int) (int, error) {
	if s.cfg.ScheduleTopic == "" {
		return 0, nil
	}
	if limit < 1 {
		limit = 64
	}
	s.schedMu.Lock()
	defer s.schedMu.Unlock()

	start := sarama.OffsetOldest
	if s.schedOff >= 0 {
		start = s.schedOff + 1
	}
	cons, err := sarama.NewConsumerFromClient(s.client)
	if err != nil {
		return 0, err
	}
	defer cons.Close()
	pc, err := cons.ConsumePartition(s.cfg.ScheduleTopic, 0, start)
	if err != nil {
		return 0, err
	}
	defer pc.Close()

	now := time.Now()
	moved := 0
	timer := time.NewTimer(600 * time.Millisecond)
	defer timer.Stop()

	for moved < limit {
		select {
		case <-ctx.Done():
			return moved, s.persistSchedOff(ctx, s.schedOff)
		case <-timer.C:
			return moved, s.persistSchedOff(ctx, s.schedOff)
		case msg, ok := <-pc.Messages():
			if !ok {
				return moved, s.persistSchedOff(ctx, s.schedOff)
			}
			rec, err := recordFromMsg(s.cfg.ScheduleTopic, msg)
			if err != nil {
				return moved, err
			}
			if !journal.IsDue(rec.Task.Scheduled, now) {
				// стоп на первом not-due (schedule topic пишите примерно в порядке due)
				return moved, s.persistSchedOff(ctx, s.schedOff)
			}
			body, _ := json.Marshal(envelope{
				ID: rec.Task.ID, Partition: rec.Task.Partition, Priority: rec.Task.PartitionPriority,
				Payload: rec.Task.Payload, Tags: rec.Task.Tags, Groups: rec.Task.Groups,
				Scheduled: rec.Task.Scheduled.UnixMilli(),
			})
			_, _, err = s.producer.SendMessage(&sarama.ProducerMessage{
				Topic:     s.cfg.InTopic,
				Partition: 0,
				Key:       sarama.StringEncoder(rec.Task.ID),
				Value:     sarama.ByteEncoder(body),
			})
			if err != nil {
				return moved, err
			}
			moved++
			s.schedOff = msg.Offset
		}
	}
	return moved, s.persistSchedOff(ctx, s.schedOff)
}

func (s *Source) persistSchedOff(ctx context.Context, off int64) error {
	if off < 0 {
		return nil
	}
	s.schedOff = off
	if s.cfg.CursorRDB == nil || s.cfg.JournalName == "" {
		return nil
	}
	return s.cfg.CursorRDB.Set(ctx, s.schedCursorKey(), strconv.FormatInt(off, 10), 0).Err()
}

var _ journal.DuePromoter = (*Source)(nil)
var _ journal.Source = (*Source)(nil)
