//go:build !wails

package main

import (
	"context"
	"fmt"

	"github.com/urfave/cli/v3"
)

func runDesktop(ctx context.Context, c *cli.Command) error {
	return fmt.Errorf("desktop UI requires: go run -tags wails,production,webkit2_41 ./cmd/manager desktop\n(see README: Ubuntu 24.04 needs libwebkit2gtk-4.1-dev)")
}
