package gateway

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLossWindowFromConfig(t *testing.T) {
	info, err := lossWindowFromConfig(
		map[string]string{"appendonly": "yes"},
		map[string]string{"appendfsync": "everysec"},
	)
	require.NoError(t, err)
	assert.Equal(t, time.Second, info.LossWindow)

	info, err = lossWindowFromConfig(
		map[string]string{"appendonly": "yes"},
		map[string]string{"appendfsync": "always"},
	)
	require.NoError(t, err)
	assert.Equal(t, time.Duration(0), info.LossWindow)

	_, err = lossWindowFromConfig(
		map[string]string{"appendonly": "no"},
		map[string]string{"appendfsync": "everysec"},
	)
	require.ErrorIs(t, err, ErrPersist)

	_, err = lossWindowFromConfig(
		map[string]string{"appendonly": "yes"},
		map[string]string{"appendfsync": "no"},
	)
	require.ErrorIs(t, err, ErrPersist)
}
