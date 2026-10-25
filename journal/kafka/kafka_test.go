package kafka

import (
	"context"
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/Rinsvent/smart-redis-queue/journal"
	"github.com/stretchr/testify/require"
)

func TestKafka_ReadBatchAndByOffset(t *testing.T) {
	brokers := []string{"localhost:9092"}
	if v := os.Getenv("SRQ_KAFKA"); v != "" {
		brokers = []string{v}
	}
	cfg := Config{Brokers: brokers, InTopic: "srq-journal-in-test", AckTopic: "srq-journal-ack-test"}
	src, err := New(cfg)
	if err != nil {
		t.Skipf("kafka: %v", err)
	}
	defer src.Close()

	sc := sarama.NewConfig()
	sc.Producer.Return.Successes = true
	prod, err := sarama.NewSyncProducer(brokers, sc)
	if err != nil {
		t.Skipf("kafka producer: %v", err)
	}
	defer prod.Close()

	body, _ := json.Marshal(envelope{ID: "k1", Payload: []byte("hi"), Scheduled: time.Now().UnixMilli()})
	_, offset, err := prod.SendMessage(&sarama.ProducerMessage{Topic: cfg.InTopic, Value: sarama.ByteEncoder(body)})
	require.NoError(t, err)

	ctx := context.Background()
	recs, err := src.ReadBatch(ctx, journal.Cursor{Offset: -1}, 10)
	require.NoError(t, err)
	require.NotEmpty(t, recs)
	found := false
	for _, r := range recs {
		if r.Task.ID == "k1" {
			found = true
			by, err := src.ReadByOffset(ctx, r.Cursor)
			require.NoError(t, err)
			require.Equal(t, "k1", by.Task.ID)
			_ = offset
		}
	}
	require.True(t, found, "published message not read")
}
