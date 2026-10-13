//go:build !wails

package desktopui

import "fmt"

// Run запускает GUI. Без -tags wails недоступен.
func Run() error {
	return fmt.Errorf("desktop UI requires: go run -tags wails,production,webkit2_41 ./cmd/manager desktop")
}
