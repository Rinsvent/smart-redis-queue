package desktopui

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStore_CRUD(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "connections.json")
	s, err := NewStore(path)
	require.NoError(t, err)
	// NewStore всегда добавляет Local Redis
	require.Len(t, s.List(), 1)
	assert.Equal(t, "Local Redis", s.List()[0].Name)
	assert.Equal(t, "localhost:6379", s.List()[0].Addr)
	assert.Equal(t, s.List()[0].ID, s.ActiveID())

	local := s.List()[0]
	c2, err := s.Add("other", "10.0.0.1:6379", "secret")
	require.NoError(t, err)
	assert.Equal(t, c2.ID, s.ActiveID())
	assert.Len(t, s.List(), 2)

	require.NoError(t, s.Select(local.ID))
	assert.Equal(t, local.ID, s.ActiveID())
	active, err := s.Active()
	require.NoError(t, err)
	assert.Equal(t, "Local Redis", active.Name)

	// reload — Local не дублируется
	s2, err := NewStore(path)
	require.NoError(t, err)
	assert.Len(t, s2.List(), 2)
	assert.Equal(t, local.ID, s2.ActiveID())

	require.NoError(t, s2.Remove(local.ID))
	assert.Len(t, s2.List(), 1)
	assert.Equal(t, c2.ID, s2.ActiveID())

	// после удаления Local — EnsureLocalRedis при следующем NewStore вернёт его
	s3, err := NewStore(path)
	require.NoError(t, err)
	assert.Len(t, s3.List(), 2)
	var hasLocal bool
	for _, c := range s3.List() {
		if c.Addr == "localhost:6379" {
			hasLocal = true
		}
	}
	assert.True(t, hasLocal)
	// active остаётся other (не переключаем)
	assert.Equal(t, c2.ID, s3.ActiveID())
}

func TestStore_EnsureLocal_DoesNotOverrideActive(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "connections.json")
	require.NoError(t, os.WriteFile(path, []byte(`{
  "connections": [{"id":"prod1","name":"Prod","addr":"10.100.0.25:6379","password":""}],
  "activeId": "prod1"
}`), 0o600))
	s, err := NewStore(path)
	require.NoError(t, err)
	assert.Equal(t, "prod1", s.ActiveID())
	assert.Len(t, s.List(), 2)
}

func TestStore_Validation(t *testing.T) {
	s, err := NewStore(filepath.Join(t.TempDir(), "c.json"))
	require.NoError(t, err)
	_, err = s.Add("", "", "")
	require.Error(t, err)
	require.Error(t, s.Select("missing"))
	require.Error(t, s.Remove("missing"))
}

func TestStore_IgnoresLegacyDBField(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "connections.json")
	require.NoError(t, os.WriteFile(path, []byte(`{
  "connections": [{"id":"a1","name":"legacy","addr":"localhost:6379","password":"","db":3}],
  "activeId": "a1"
}`), 0o600))

	s, err := NewStore(path)
	require.NoError(t, err)
	list := s.List()
	// legacy уже localhost — EnsureLocalRedis не дублирует
	require.Len(t, list, 1)
	assert.Equal(t, "legacy", list[0].Name)
	assert.Equal(t, "localhost:6379", list[0].Addr)
}
