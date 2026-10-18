//go:build !wails && !android && !ios

package main

import (
	"context"
	"fmt"

	"github.com/urfave/cli/v3"
)

func runDesktop(ctx context.Context, c *cli.Command) error {
	return fmt.Errorf("desktop UI requires: go run -tags wails,production,gtk3 ./cmd/desktop\n(see README: Linux needs libgtk-3-dev libwebkit2gtk-4.1-dev + -tags gtk3 for Wails v3 legacy GTK3)")
}
