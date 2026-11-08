package gateway

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestLossWindow_AckAndRejectDropPublish(t *testing.T) {
	w := newLossWindow(time.Second)
	now := time.Now()
	rec := PublishRec{ID: "t1", Payload: []byte("x")}
	w.onPublishOK("q", rec, now)
	assert.Equal(t, 1, w.len())

	w.onAckOK("t1")
	assert.Equal(t, 0, w.len())

	w.onPublishOK("q", rec, now)
	w.onRejectOK("t1", now.Add(500*time.Millisecond))
	assert.Equal(t, 0, w.len(), "Reject must drop publish so recover does not re-publish")
}

func TestLossWindow_TruncateByAge(t *testing.T) {
	w := newLossWindow(time.Second)
	now := time.Now()
	w.onPublishOK("q", PublishRec{ID: "t2", Payload: []byte("x")}, now)
	assert.Equal(t, 1, w.len())
	w.truncate(now.Add(time.Second + time.Millisecond))
	assert.Equal(t, 0, w.len())
}
