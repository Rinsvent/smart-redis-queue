package journal

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCursor_OffsetMonotonic(t *testing.T) {
	a := OffsetCursor(1)
	b := OffsetCursor(2)
	assert.True(t, b.After(a))
	d, err := DecodeCursor(b.Encode())
	require.NoError(t, err)
	assert.Equal(t, int64(2), d.Offset)
}

func TestCursor_Kafka(t *testing.T) {
	a := KafkaCursor(0, 1)
	b := KafkaCursor(0, 2)
	assert.True(t, b.After(a))
	d, err := DecodeCursor(b.Encode())
	require.NoError(t, err)
	assert.Equal(t, int64(2), d.Offset)
	assert.Equal(t, int32(0), d.Partition)
}

func TestCursor_KeyObjectID(t *testing.T) {
	a := KeyCursor("000000000000000000000001")
	b := KeyCursor("000000000000000000000002")
	assert.True(t, b.After(a))
	d, err := DecodeCursor(b.Encode())
	require.NoError(t, err)
	assert.Equal(t, "000000000000000000000002", d.Key)
	assert.Equal(t, int64(0), d.Offset)
}
