package gateway

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestLossWindow_AckDropsRejectKeepsUntilWindow(t *testing.T) {
	w := newLossWindow(time.Second)
	now := time.Now()
	rec := PublishRec{ID: "t1", Payload: []byte("x")}
	w.onPublishOK("q", rec, now)
	assert.Equal(t, 1, w.len())

	w.onAckOK("t1")
	assert.Equal(t, 0, w.len())

	w.onPublishOK("q", rec, now)
	until := w.snapshot()[0].RetainUntil
	w.onRejectOK("t1", now.Add(500*time.Millisecond))
	assert.Equal(t, 1, w.len(), "Reject must keep publish for AOF loss recover")
	assert.Equal(t, until, w.snapshot()[0].RetainUntil, "Reject must not extend publish retention")

	w.truncate(until.Add(time.Millisecond))
	assert.Equal(t, 0, w.len())
}
