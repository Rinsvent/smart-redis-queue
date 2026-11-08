package gateway

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLoadFileConfig(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "gw.yaml")
	require.NoError(t, os.WriteFile(path, []byte(`
listen: "0.0.0.0:50051"
http: "0.0.0.0:50052"
dump: "/tmp/gw.dump"
redis:
  addr: "127.0.0.1:6379"
  db: 2
stats_retention: "30m"
`), 0o600))

	fc, err := LoadFileConfig(path)
	require.NoError(t, err)
	assert.Equal(t, "0.0.0.0:50051", fc.Listen)
	assert.Equal(t, "0.0.0.0:50052", fc.HTTP)
	assert.Equal(t, 2, fc.Redis.DB)
	assert.Equal(t, 30*time.Minute, fc.StatsRetentionDuration())
}
