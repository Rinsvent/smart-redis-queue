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
