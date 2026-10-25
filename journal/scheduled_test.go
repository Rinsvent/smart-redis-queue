package journal

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestScheduledOrNow(t *testing.T) {
	past := time.Now().Add(-time.Hour)
	assert.Equal(t, past, ScheduledOrNow(past))

	future := time.Now().Add(time.Hour)
	assert.Equal(t, future, ScheduledOrNow(future))

	got := ScheduledOrNow(time.Time{})
	assert.False(t, got.IsZero())
	assert.WithinDuration(t, time.Now(), got, time.Second)
}

func TestIsDue(t *testing.T) {
	now := time.Now()
	assert.True(t, IsDue(now, now))
	assert.True(t, IsDue(now.Add(DueSkew), now))
	assert.False(t, IsDue(now.Add(DueSkew+time.Second), now))
	assert.True(t, IsDue(now.Add(-time.Hour), now))
}
