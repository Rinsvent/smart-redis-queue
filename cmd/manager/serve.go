package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Rinsvent/smart-redis-queue/gateway"
	"github.com/urfave/cli/v3"
)

func gatewayHTTPFlag() cli.Flag {
	return &cli.StringFlag{
		Name:    "http",
		Value:   "127.0.0.1:50052",
		Usage:   "HTTP ops base URL or listen addr (status/stop / server bind)",
		Sources: cli.EnvVars("SRQ_GATEWAY_HTTP"),
	}
}

func runGateway(ctx context.Context, c *cli.Command) error {
	rdb := redisClient(c)
	defer rdb.Close()

	listen := c.String("listen")
	httpAddr := c.String("http")

	gw, err := gateway.New(gateway.Config{
		Redis:            rdb,
		ListenAddr:       listen,
		HTTPAddr:         httpAddr,
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
		errCh <- gw.ListenAndServe(runCtx, listen, httpAddr)
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

func runGatewayStatus(ctx context.Context, c *cli.Command) error {
	base := c.String("http")
	if base == "" {
		return fmt.Errorf("--http required (e.g. http://127.0.0.1:50052)")
	}
	if len(base) > 0 && base[0] != 'h' {
		base = "http://" + base
	}
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	ov, err := gateway.FetchOverview(ctx, base)
	if err != nil {
		return err
	}
	fmt.Printf("state:          %s\n", ov.State)
	fmt.Printf("appendfsync:    %s\n", ov.AppendFsync)
	fmt.Printf("loss_window:    %s\n", time.Duration(ov.LossWindowNs))
	fmt.Printf("op_log:         %d\n", ov.OpLogLen)
	fmt.Printf("loss_window_n:  %d\n", ov.LossWindowLen)
	if ov.ListenGRPC != "" {
		fmt.Printf("grpc:           %s\n", ov.ListenGRPC)
	}
	if ov.ListenHTTP != "" {
		fmt.Printf("http:           %s\n", ov.ListenHTTP)
	}
	return nil
}

func runGatewayStop(ctx context.Context, c *cli.Command) error {
	base := c.String("http")
	if base == "" {
		return fmt.Errorf("--http required (e.g. http://127.0.0.1:50052)")
	}
	if len(base) > 0 && base[0] != 'h' {
		base = "http://" + base
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := gateway.PostShutdown(ctx, base); err != nil {
		return err
	}
	fmt.Println("shutdown requested")
	return nil
}
