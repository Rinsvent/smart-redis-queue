//go:build !server

package main

import (
	"context"
	"fmt"

	"github.com/urfave/cli/v3"
)

func runServer(ctx context.Context, c *cli.Command) error {
	return fmt.Errorf("web UI server requires: go build -tags server -o bin/manager-server ./cmd/manager\n" +
		"then: ./bin/manager-server server --host 0.0.0.0 --port 8080\n" +
		"(or: make server && ./bin/manager-server server --host 0.0.0.0)")
}
