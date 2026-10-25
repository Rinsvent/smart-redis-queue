package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/IBM/sarama"
	redisqueue "github.com/Rinsvent/smart-redis-queue"
	"github.com/Rinsvent/smart-redis-queue/journal"
)

type Config struct {
	Brokers  []string
	InTopic  string
	AckTopic string
}

type Source struct {
	cfg      Config
	client   sarama.Client
	producer sarama.SyncProducer
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
	cl, err := sarama.NewClient(cfg.Brokers, sc)
	if err != nil {
		return nil, err
	}
	prod, err := sarama.NewSyncProducerFromClient(cl)
	if err != nil {
		_ = cl.Close()
		return nil, err
	}
	return &Source{cfg: cfg, client: cl, producer: prod}, nil
}

func (s *Source) Close() error {
	_ = s.producer.Close()
	return s.client.Close()
}

func (s *Source) Init(context.Context) error { return nil }

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
			if after.Offset > 0 && msg.Offset <= after.Offset {
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
			ID: id, Partition: env.Partition, Priority: env.Priority,
			Payload: env.Payload, Tags: env.Tags, Groups: env.Groups, Scheduled: sch,
		},
		Cursor: journal.KafkaCursor(msg.Partition, msg.Offset),
	}, nil
}

func (s *Source) Ack(_ context.Context, taskID string, _ map[string]string) error {
	_, _, err := s.producer.SendMessage(&sarama.ProducerMessage{
		Topic: s.cfg.AckTopic,
		Key:   sarama.StringEncoder(taskID),
		Value: sarama.StringEncoder(taskID),
	})
	return err
}

func (s *Source) ReadAcks(ctx context.Context, from journal.Cursor, n int) ([]journal.AckEvent, error) {
	cons, err := sarama.NewConsumerFromClient(s.client)
	if err != nil {
		return nil, err
	}
	defer cons.Close()
	off := from.Offset
	if off < 0 {
		off = 0
	}
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
