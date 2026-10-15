//go:build wails

package main

import (
	"context"

	"github.com/Rinsvent/smart-redis-queue/internal/desktopui"
	"github.com/urfave/cli/v3"
)

func runDesktop(ctx context.Context, c *cli.Command) error {
	return desktopui.Run()
}
