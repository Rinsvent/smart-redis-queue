package redisqueue

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStatsSink_FlushAndTagsSample(t *testing.T) {
	admin, rdb := setupAdminTest(t)
	defer rdb.Close()
	ctx := context.Background()
	q := "stats-sink-q"

	require.NoError(t, rdb.SAdd(ctx, "queue:"+q+":tag:all", "t1", "t2", "t3").Err())
	require.NoError(t, rdb.SAdd(ctx, "queue:"+q+":tag:camp", "t1").Err())

	sink := newStatsSink(rdb, q)
	sink.setRetention(time.Hour)
	sink.setTags([]string{"all", "camp"})

	sec := time.Now().Unix() - 1
	b := sink.bucket(sec)
	b.get.Add(5)
	b.ack.Add(3)
	b.reject.Add(1)

	require.NoError(t, sink.flush(ctx, true))
	require.NoError(t, sink.sampleTags(ctx))

	points, err := admin.ListQueueStats(ctx, q, sec-5, time.Now().Unix()+1)
	require.NoError(t, err)
	require.NotEmpty(t, points)

	var foundRates, foundTags bool
	for _, p := range points {
		if p.Sec == sec {
			assert.Equal(t, int64(5), p.Get)
			assert.Equal(t, int64(3), p.Ack)
			assert.Equal(t, int64(1), p.Reject)
			foundRates = true
		}
		if p.Tags["all"] == 3 && p.Tags["camp"] == 1 {
			foundTags = true
		}
	}
	assert.True(t, foundRates)
	assert.True(t, foundTags)
}

func TestStatsSink_DisabledNoOp(t *testing.T) {
	_, rdb := setupAdminTest(t)
	defer rdb.Close()
	sink := newStatsSink(rdb, "stats-off")
	sink.addGet(10)
	sink.addAck(1)
	assert.False(t, sink.enabled())
	require.NoError(t, sink.flush(context.Background(), true))
	n, err := rdb.Exists(context.Background(), "queue:stats-off:stats:index").Result()
	require.NoError(t, err)
	assert.Equal(t, int64(0), n)
}

func TestStatsSink_SampleTagsOncePerSec(t *testing.T) {
	_, rdb := setupAdminTest(t)
	defer rdb.Close()
	ctx := context.Background()
	q := "stats-nx-tag"
	require.NoError(t, rdb.SAdd(ctx, "queue:"+q+":tag:x", "a").Err())

	s1 := newStatsSink(rdb, q)
	s1.setRetention(time.Hour)
	s1.setTags([]string{"x"})
	s2 := newStatsSink(rdb, q)
	s2.setRetention(time.Hour)
	s2.setTags([]string{"x"})

	require.NoError(t, s1.sampleTags(ctx))
	require.NoError(t, s2.sampleTags(ctx))

	sec := time.Now().Unix()
	key := "queue:" + q + ":stats:" + strconv.FormatInt(sec, 10)
	n, err := rdb.HGet(ctx, key, "t:x").Int64()
	require.NoError(t, err)
	assert.Equal(t, int64(1), n)
}
