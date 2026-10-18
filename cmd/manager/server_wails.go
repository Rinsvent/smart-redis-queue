//go:build server

package main

import (
	"context"
	"fmt"

	"github.com/Rinsvent/smart-redis-queue/internal/desktopui"
	"github.com/urfave/cli/v3"
)

func runServer(ctx context.Context, c *cli.Command) error {
	host := c.String("host")
	port := c.Int("port")
	if host == "" {
		host = "127.0.0.1"
	}
	if port <= 0 {
		return fmt.Errorf("invalid --port %d", port)
	}
	return desktopui.RunWithConfig(desktopui.RunConfig{
		Host:                     host,
		Port:                     port,
		WebSocketAllowAllOrigins: c.Bool("allow-any-origin"),
	})
}
