package kafka

import (
	"context"
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/IBM/sarama"
	redisqueue "github.com/Rinsvent/smart-redis-queue"
	"github.com/Rinsvent/smart-redis-queue/journal"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

func kafkaBrokers() []string {
	if v := os.Getenv("SRQ_KAFKA"); v != "" {
		return []string{v}
	}
	return []string{"localhost:9092"}
}

func TestKafka_ReadBatchAndByOffset(t *testing.T) {
	cfg := Config{
		Brokers:  kafkaBrokers(),
		InTopic:  "srq-journal-in-test",
		AckTopic: "srq-journal-ack-test",
	}
	src, err := New(cfg)
	if err != nil {
		t.Skipf("kafka: %v", err)
	}
	defer src.Close()

	ctx := context.Background()
	id := "k1-" + time.Now().Format("150405.000")
	require.NoError(t, src.Publish(ctx, &redisqueue.Task{
		ID: id, Payload: []byte("hi"), Scheduled: time.Now(),
	}))

	recs, err := src.ReadBatch(ctx, journal.Cursor{Offset: -1}, 50)
	require.NoError(t, err)
	require.NotEmpty(t, recs)
	found := false
	for _, r := range recs {
		if r.Task.ID == id {
			found = true
			by, err := src.ReadByOffset(ctx, r.Cursor)
			require.NoError(t, err)
			require.Equal(t, id, by.Task.ID)
		}
	}
	require.True(t, found, "published message not read")
}

func TestKafka_SchedulePromoteOrdered(t *testing.T) {
	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379", DB: 8})
	if err := rdb.Ping(context.Background()).Err(); err != nil {
		t.Skipf("redis: %v", err)
	}
	defer rdb.Close()
	_ = rdb.FlushDB(context.Background())

	suffix := time.Now().Format("150405")
	cfg := Config{
		Brokers:       kafkaBrokers(),
		InTopic:       "srq-in-" + suffix,
		AckTopic:      "srq-ack-" + suffix,
		ScheduleTopic: "srq-sched-" + suffix,
		CursorRDB:     rdb,
		JournalName:   "kprom",
	}
	src, err := New(cfg)
	if err != nil {
		t.Skipf("kafka: %v", err)
	}
	defer src.Close()

	ctx := context.Background()
	id := "mov-" + time.Now().Format("150405.000")
	body, _ := json.Marshal(envelope{ID: id, Payload: []byte("p"), Scheduled: time.Now().Add(-time.Minute).UnixMilli()})
	_, _, err = src.producer.SendMessage(&sarama.ProducerMessage{Topic: cfg.ScheduleTopic, Value: sarama.ByteEncoder(body)})
	require.NoError(t, err)

	n, err := src.PromoteDue(ctx, 10)
	require.NoError(t, err)
	require.GreaterOrEqual(t, n, 1)

	recs, err := src.ReadBatch(ctx, journal.Cursor{Offset: -1}, 20)
	require.NoError(t, err)
	found := false
	for _, r := range recs {
		if r.Task.ID == id {
			found = true
		}
	}
	require.True(t, found, "promoted message should appear in InTopic")
}

func TestKafka_RecoverLockAcks(t *testing.T) {
	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379", DB: 9})
	if err := rdb.Ping(context.Background()).Err(); err != nil {
		t.Skipf("redis: %v", err)
	}
	defer rdb.Close()
	_ = rdb.FlushDB(context.Background())

	suffix := time.Now().Format("150405.000")
	cfg := Config{
		Brokers:  kafkaBrokers(),
		InTopic:  "srq-rec-in-" + suffix,
		AckTopic: "srq-rec-ack-" + suffix,
	}
	src, err := New(cfg)
	if err != nil {
		t.Skipf("kafka: %v", err)
	}
	defer src.Close()

	ctx := context.Background()
	id1, id2 := "a-"+suffix, "b-"+suffix
	require.NoError(t, src.Publish(ctx,
		&redisqueue.Task{ID: id1, Payload: []byte("1"), Scheduled: time.Now()},
		&redisqueue.Task{ID: id2, Payload: []byte("2"), Scheduled: time.Now()},
	))
	require.NoError(t, src.Ack(ctx, id1, nil))
	time.Sleep(500 * time.Millisecond) // ack topic visible

	q := "q-rec-" + suffix
	br := journal.NewBridge(rdb, "krec-"+suffix, q, src)
	require.NoError(t, br.RecoverAfterRedis(ctx, nil))

	require.Equal(t, int64(1), rdb.Exists(ctx, "queue:"+q+":payload:"+id1).Val(), "acked id must be Locked")
}
