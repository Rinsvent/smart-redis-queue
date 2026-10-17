package desktopui

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// Connection — профиль Redis в GUI (хост + пароль, без выбора DB).
type Connection struct {
	ID       string `json:"id"`
	Name     string `json:"name"`
	Addr     string `json:"addr"`
	Password string `json:"password"`
}

type storeFile struct {
	Connections []Connection `json:"connections"`
	ActiveID    string       `json:"activeId"`
}

// Store хранит профили подключений в UserConfigDir.
type Store struct {
	mu   sync.Mutex
	path string
	data storeFile
}

func defaultStorePath() (string, error) {
	dir, err := os.UserConfigDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(dir, "smart-redis-queue", "connections.json"), nil
}

// NewStore загружает (или создаёт) store по пути. Пустой path → default.
// При первом/каждом открытии гарантирует профиль Local Redis (localhost:6379),
// не переключая activeId, если пользователь уже выбрал другой хост.
func NewStore(path string) (*Store, error) {
	if path == "" {
		var err error
		path, err = defaultStorePath()
		if err != nil {
			return nil, err
		}
	}
	s := &Store{path: path, data: storeFile{Connections: []Connection{}}}
	if err := s.load(); err != nil && !os.IsNotExist(err) {
		return nil, err
	}
	if _, err := s.EnsureLocalRedis(); err != nil {
		return nil, err
	}
	return s, nil
}

const (
	localRedisName = "Local Redis"
	localRedisAddr = "localhost:6379"
)

func isLocalRedisAddr(addr string) bool {
	switch addr {
	case "localhost:6379", "127.0.0.1:6379":
		return true
	default:
		return false
	}
}

// EnsureLocalRedis добавляет localhost:6379, если такого профиля ещё нет.
// Если activeId пуст — делает Local активным. Иначе не трогает выбор пользователя.
// Возвращает профиль Local (существующий или новый).
func (s *Store) EnsureLocalRedis() (Connection, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for i := range s.data.Connections {
		if isLocalRedisAddr(s.data.Connections[i].Addr) {
			c := s.data.Connections[i]
			if s.data.ActiveID == "" {
				s.data.ActiveID = c.ID
				if err := s.save(); err != nil {
					return Connection{}, err
				}
			}
			return c, nil
		}
	}
	c := Connection{
		ID:   newID(),
		Name: localRedisName,
		Addr: localRedisAddr,
	}
	s.data.Connections = append(s.data.Connections, c)
	if s.data.ActiveID == "" {
		s.data.ActiveID = c.ID
	}
	if err := s.save(); err != nil {
		return Connection{}, err
	}
	return c, nil
}

func (s *Store) load() error {
	b, err := os.ReadFile(s.path)
	if err != nil {
		return err
	}
	var data storeFile
	if err := json.Unmarshal(b, &data); err != nil {
		return err
	}
	if data.Connections == nil {
		data.Connections = []Connection{}
	}
	s.data = data
	return nil
}

func (s *Store) save() error {
	if err := os.MkdirAll(filepath.Dir(s.path), 0o755); err != nil {
		return err
	}
	b, err := json.MarshalIndent(s.data, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(s.path, b, 0o600)
}

func (s *Store) List() []Connection {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]Connection, len(s.data.Connections))
	copy(out, s.data.Connections)
	return out
}

func (s *Store) ActiveID() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.data.ActiveID
}

func (s *Store) Active() (*Connection, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.data.ActiveID == "" {
		return nil, fmt.Errorf("no active connection")
	}
	for i := range s.data.Connections {
		if s.data.Connections[i].ID == s.data.ActiveID {
			c := s.data.Connections[i]
			return &c, nil
		}
	}
	return nil, fmt.Errorf("active connection not found")
}

func (s *Store) Add(name, addr, password string) (Connection, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if name == "" {
		name = addr
	}
	if addr == "" {
		return Connection{}, fmt.Errorf("addr is required")
	}
	c := Connection{
		ID:       newID(),
		Name:     name,
		Addr:     addr,
		Password: password,
	}
	s.data.Connections = append(s.data.Connections, c)
	s.data.ActiveID = c.ID
	if err := s.save(); err != nil {
		return Connection{}, err
	}
	return c, nil
}

func (s *Store) Remove(id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	idx := -1
	for i := range s.data.Connections {
		if s.data.Connections[i].ID == id {
			idx = i
			break
		}
	}
	if idx < 0 {
		return fmt.Errorf("connection not found")
	}
	s.data.Connections = append(s.data.Connections[:idx], s.data.Connections[idx+1:]...)
	if s.data.ActiveID == id {
		s.data.ActiveID = ""
		if len(s.data.Connections) > 0 {
			s.data.ActiveID = s.data.Connections[0].ID
		}
	}
	return s.save()
}

func (s *Store) Select(id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for i := range s.data.Connections {
		if s.data.Connections[i].ID == id {
			s.data.ActiveID = id
			return s.save()
		}
	}
	return fmt.Errorf("connection not found")
}

func newID() string {
	b := make([]byte, 8)
	if _, err := rand.Read(b); err != nil {
		return fmt.Sprintf("%d", os.Getpid())
	}
	return hex.EncodeToString(b)
}
