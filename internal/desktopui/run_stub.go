//go:build !wails && !android && !ios && !server

package desktopui

import "fmt"

// Run запускает GUI/server UI. Без -tags wails|android|ios|server недоступен.
func Run() error {
	return RunWithConfig(RunConfig{})
}

// RunConfig — см. run_wails.go (реальная реализация за build-тегами).
type RunConfig struct {
	Host                     string
	Port                     int
	WebSocketAllowAllOrigins bool
}

// RunWithConfig — stub без Wails.
func RunWithConfig(RunConfig) error {
	return fmt.Errorf("UI requires build tags, e.g.:\n" +
		"  desktop: go run -tags wails,production,gtk3 ./cmd/desktop\n" +
		"  server:  go run -tags server ./cmd/manager server --host 0.0.0.0 --port 8080")
}
