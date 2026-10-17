package redisqueue

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEncodeDecode_ZstdRoundTrip(t *testing.T) {
	raw := bytes.Repeat([]byte("hello-world-"), 200)
	enc, err := encodePayloadForRedis(raw, CompressionZstd, 1)
	require.NoError(t, err)
	require.True(t, bytes.HasPrefix(enc, payloadCodecMagic))
	assert.Less(t, len(enc), len(raw))

	dec, err := decodePayloadFromRedis(enc)
	require.NoError(t, err)
	assert.Equal(t, raw, dec)
}

func TestEncode_SmallSkipped(t *testing.T) {
	raw := []byte("tiny")
	enc, err := encodePayloadForRedis(raw, CompressionZstd, 1024)
	require.NoError(t, err)
	assert.Equal(t, raw, enc)
	assert.False(t, bytes.HasPrefix(enc, payloadCodecMagic))
}

func TestEncode_NonePassthrough(t *testing.T) {
	raw := bytes.Repeat([]byte("x"), 5000)
	enc, err := encodePayloadForRedis(raw, CompressionNone, 1)
	require.NoError(t, err)
	assert.Equal(t, raw, enc)
}

func TestDecode_LegacyRaw(t *testing.T) {
	raw := []byte(`{"a":1}`)
	dec, err := decodePayloadFromRedis(raw)
	require.NoError(t, err)
	assert.Equal(t, raw, dec)
}

func TestCompression_PublishGet(t *testing.T) {
	producer, consumer, rdb := setupTestQueue(t)
	producer.SetCompression(CompressionZstd)
	producer.SetCompressMinBytes(1)
	consumer.SetPrefetchCount(1)
	ctx := context.Background()

	body := bytes.Repeat([]byte("payload-chunk-"), 100)
	require.NoError(t, producer.Publish(ctx, &Task{
		ID: "c1", Payload: body, Scheduled: time.Now().Add(-time.Second),
	}))

	stored, err := rdb.Get(ctx, "queue:test-queue:payload:c1").Bytes()
	require.NoError(t, err)
	assert.True(t, bytes.HasPrefix(stored, payloadCodecMagic))
	assert.Less(t, len(stored), len(body))

	got, err := consumer.Get(ctx)
	require.NoError(t, err)
	require.Len(t, got, 1)
	assert.Equal(t, body, got[0].Payload)
	require.NoError(t, consumer.Ack(ctx, got[0].ID, 0))
}

func TestCompression_WithOptionOverridesProducer(t *testing.T) {
	producer, consumer, rdb := setupTestQueue(t)
	consumer.SetPrefetchCount(1)
	ctx := context.Background()
	body := bytes.Repeat([]byte("zzzz"), 500)

	require.NoError(t, producer.PublishWith(ctx, []PublishOption{
		WithCompression(CompressionZstd),
		WithCompressMinBytes(1),
	}, &Task{ID: "o1", Payload: body, Scheduled: time.Now().Add(-time.Second)}))

	stored, err := rdb.Get(ctx, "queue:test-queue:payload:o1").Bytes()
	require.NoError(t, err)
	assert.True(t, bytes.HasPrefix(stored, payloadCodecMagic))

	got, err := consumer.Get(ctx)
	require.NoError(t, err)
	require.Len(t, got, 1)
	assert.Equal(t, body, got[0].Payload)
	require.NoError(t, consumer.Ack(ctx, got[0].ID, 0))
}

func TestCompression_Disabled_NoMagic(t *testing.T) {
	producer, consumer, rdb := setupTestQueue(t)
	consumer.SetPrefetchCount(1)
	ctx := context.Background()
	body := bytes.Repeat([]byte("n"), 2000)
	require.NoError(t, producer.Publish(ctx, &Task{
		ID: "n1", Payload: body, Scheduled: time.Now().Add(-time.Second),
	}))
	stored, err := rdb.Get(ctx, "queue:test-queue:payload:n1").Bytes()
	require.NoError(t, err)
	assert.Equal(t, body, stored)
	got, err := consumer.Get(ctx)
	require.NoError(t, err)
	require.Len(t, got, 1)
	assert.Equal(t, body, got[0].Payload)
	require.NoError(t, consumer.Ack(ctx, got[0].ID, 0))
}
