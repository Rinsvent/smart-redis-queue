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
	assert.Empty(t, s.List())
	assert.Empty(t, s.ActiveID())

	c, err := s.Add("local", "localhost:6379", "")
	require.NoError(t, err)
	assert.Equal(t, "local", c.Name)
	assert.Equal(t, c.ID, s.ActiveID())
	assert.Len(t, s.List(), 1)

	c2, err := s.Add("other", "127.0.0.1:6380", "secret")
	require.NoError(t, err)
	assert.Equal(t, c2.ID, s.ActiveID())

	require.NoError(t, s.Select(c.ID))
	assert.Equal(t, c.ID, s.ActiveID())
	active, err := s.Active()
	require.NoError(t, err)
	assert.Equal(t, "local", active.Name)

	// reload
	s2, err := NewStore(path)
	require.NoError(t, err)
	assert.Len(t, s2.List(), 2)
	assert.Equal(t, c.ID, s2.ActiveID())

	require.NoError(t, s2.Remove(c.ID))
	assert.Len(t, s2.List(), 1)
	assert.Equal(t, c2.ID, s2.ActiveID())

	require.NoError(t, s2.Remove(c2.ID))
	assert.Empty(t, s2.List())
	assert.Empty(t, s2.ActiveID())
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
	require.Len(t, list, 1)
	assert.Equal(t, "legacy", list[0].Name)
	assert.Equal(t, "localhost:6379", list[0].Addr)
}
