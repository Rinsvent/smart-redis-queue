package gateway

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestOpLog_TruncateOlderThanKeepsPending(t *testing.T) {
	l := newOpLog()
	now := time.Now()
	okOld := l.append(Op{Kind: OpAck, TaskID: "a", RedisOK: true, At: now.Add(-2 * time.Second)})
	pending := l.append(Op{Kind: OpAck, TaskID: "b", RedisOK: false, At: now.Add(-2 * time.Second)})
	okNew := l.append(Op{Kind: OpAck, TaskID: "c", RedisOK: true, At: now})

	l.truncateOlderThan(now.Add(-time.Second))
	snap := l.snapshot()
	require.Len(t, snap, 2)
	assert.Equal(t, pending.Offset, snap[0].Offset)
	assert.Equal(t, okNew.Offset, snap[1].Offset)
	assert.NotEqual(t, okOld.Offset, snap[0].Offset)
}

func TestOpLog_MarkRedisOK(t *testing.T) {
	l := newOpLog()
	op := l.append(Op{Kind: OpReject, TaskID: "r1", RedisOK: false})
	require.False(t, l.snapshot()[0].RedisOK)
	l.markRedisOK(op.Offset)
	require.True(t, l.snapshot()[0].RedisOK)
}
