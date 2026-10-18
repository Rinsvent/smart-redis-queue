//go:build !wails && !android && !ios

package desktopui

import "fmt"

// Run запускает GUI. Без -tags wails / android / ios недоступен.
func Run() error {
	return fmt.Errorf("desktop UI requires: go run -tags wails,production,gtk3 ./cmd/desktop")
}
