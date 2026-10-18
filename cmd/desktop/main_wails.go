//go:build wails

package main

import (
	"log"

	"github.com/Rinsvent/smart-redis-queue/internal/desktopui"
)

// Отдельный entrypoint для релизного desktop-бинаря: двойной клик / ./binary → сразу UI.
func main() {
	if err := desktopui.Run(); err != nil {
		log.Fatal(err)
	}
}
