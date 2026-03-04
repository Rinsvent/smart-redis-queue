package main

import (
	"context"
	"log"
	"os"

	"github.com/urfave/cli/v3"
)

func main() {
	cmd := &cli.Command{
		Name:  "requester",
		Usage: "HTTP request service with rate limiting, retry, and caching",
		Commands: []*cli.Command{
			{
				Name:  "ps",
				Usage: "Start the requester server",
				Action: func(ctx context.Context, cmd *cli.Command) error {
					return nil
				},
			},
		},
	}

	if err := cmd.Run(context.Background(), os.Args); err != nil {
		log.Fatal(err)
	}
}
