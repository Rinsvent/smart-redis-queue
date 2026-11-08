package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/Rinsvent/smart-redis-queue/gateway"
	"github.com/urfave/cli/v3"
)

func runServe(ctx context.Context, c *cli.Command) error {
	rdb := redisClient(c)
	defer rdb.Close()

	gw, err := gateway.New(gateway.Config{
		Redis:            rdb,
		ListenAddr:       c.String("listen"),
		DumpPath:         c.String("dump"),
		SkipPersistCheck: c.Bool("skip-persist-check"),
	})
	if err != nil {
		return err
	}

	if err := gw.Boot(ctx); err != nil {
		return fmt.Errorf("gateway boot: %w", err)
	}

	runCtx, stop := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
	defer stop()

	errCh := make(chan error, 1)
	go func() {
		errCh <- gw.ListenAndServeGRPC(runCtx, c.String("listen"))
	}()

	select {
	case <-runCtx.Done():
		_ = gw.Drain(context.Background())
		return nil
	case err := <-errCh:
		_ = gw.Drain(context.Background())
		if err != nil && runCtx.Err() == nil {
			return err
		}
		return nil
	}
}
