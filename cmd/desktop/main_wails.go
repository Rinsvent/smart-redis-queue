//go:build wails || android || ios

package main

import (
	"log"

	"github.com/Rinsvent/smart-redis-queue/internal/desktopui"
)

// Отдельный entrypoint для релизного desktop/mobile-бинаря:
// двойной клик / ./binary → сразу UI (Wails v3).
func main() {
	if err := desktopui.Run(); err != nil {
		log.Fatal(err)
	}
}
