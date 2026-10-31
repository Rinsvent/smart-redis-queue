package redisqueue

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestJournal_ResumeAfterStamp(t *testing.T) {
	_, _, rdb := setupTestQueue(t)
	ctx := context.Background()

	require.Error(t, SetResumeAfter(ctx, nil, time.Now()))

	at, ok, err := GetResumeAfter(ctx, rdb)
	require.NoError(t, err)
	assert.False(t, ok)
	assert.True(t, at.IsZero())

	stamp := time.Now().UTC().Truncate(time.Millisecond)
	require.NoError(t, SetResumeAfter(ctx, rdb, stamp))
	got, ok, err := GetResumeAfter(ctx, rdb)
	require.NoError(t, err)
	assert.True(t, ok)
	assert.Equal(t, stamp.UnixMilli(), got.UnixMilli())

	assert.True(t, isJournalCursorRewind(errors.New("x "+errJournalCursorRewindLua)))
	assert.False(t, isJournalCursorRewind(nil))
}

func TestConsumer_JournalAckAndEmergencyResumeSetters(t *testing.T) {
	_, consumer, rdb := setupTestQueue(t)
	consumer.SetJournalAck(func(context.Context, string) error { return nil })
	consumer.SetEmergencyResume(true)

	pool := NewConsumerPool(rdb, "test-queue")
	pool.SetJournalAck(func(context.Context, string) error { return nil })
	pool.SetEmergencyResume(true)
	pool.SetSamePartitionBatch(true)
	pool.SetExtendGroupBlocksOnly(true)
}
