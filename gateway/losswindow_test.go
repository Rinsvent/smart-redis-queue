package gateway

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestLossWindow_AckDropsRejectRetains(t *testing.T) {
	w := newLossWindow(time.Second)
	now := time.Now()
	rec := PublishRec{ID: "t1", Payload: []byte("x")}
	w.onPublishOK("q", rec, now)
	assert.Equal(t, 1, w.len())

	w.onAckOK("t1")
	assert.Equal(t, 0, w.len())

	w.onPublishOK("q", rec, now)
	w.onRejectOK("t1", now.Add(500*time.Millisecond))
	assert.Equal(t, 1, w.len())
	snap := w.snapshot()
	assert.True(t, snap[0].RetainUntil.After(now.Add(time.Second)))

	w.truncate(snap[0].RetainUntil.Add(time.Millisecond))
	assert.Equal(t, 0, w.len())
}
