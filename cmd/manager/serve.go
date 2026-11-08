package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Rinsvent/smart-redis-queue/gateway"
	"github.com/redis/go-redis/v9"
	"github.com/urfave/cli/v3"
)

func gatewayHTTPFlag() cli.Flag {
	return &cli.StringFlag{
		Name:    "http",
		Value:   "127.0.0.1:50052",
		Usage:   "HTTP ops base URL (status/stop)",
		Sources: cli.EnvVars("SRQ_GATEWAY_HTTP"),
	}
}

func runGateway(ctx context.Context, c *cli.Command) error {
	cfg := gateway.Config{}
	var rdb *redis.Client

	if path := c.String("config"); path != "" {
		fc, err := gateway.LoadFileConfig(path)
		if err != nil {
			return err
		}
		root := c.Root()
		if root.IsSet("addr") {
			fc.Redis.Addr = root.String("addr")
		}
		if root.IsSet("password") {
			fc.Redis.Password = root.String("password")
		}
		if root.IsSet("db") {
			fc.Redis.DB = root.Int("db")
		}
		rdb = fc.RedisClient()
		cfg.ListenAddr = fc.Listen
		cfg.HTTPAddr = fc.HTTP
		cfg.DumpPath = fc.Dump
		cfg.SkipPersistCheck = fc.SkipPersistCheck
		cfg.StatsRetention = fc.StatsRetentionDuration()
		sc, err := fc.ShardConfig()
		if err != nil {
			_ = rdb.Close()
			return err
		}
		cfg.Shards = sc
	} else {
		rdb = redisClient(c)
		cfg.ListenAddr = "127.0.0.1:50051"
		cfg.HTTPAddr = "127.0.0.1:50052"
	}
	defer rdb.Close()
	cfg.Redis = rdb

	if v := c.String("listen"); v != "" {
		cfg.ListenAddr = v
	}
	if v := c.String("http"); v != "" {
		cfg.HTTPAddr = v
	}
	if v := c.String("dump"); v != "" {
		cfg.DumpPath = v
	}
	if c.Bool("skip-persist-check") {
		cfg.SkipPersistCheck = true
	}
	if cfg.ListenAddr == "" {
		cfg.ListenAddr = "127.0.0.1:50051"
	}
	if cfg.HTTPAddr == "" {
		cfg.HTTPAddr = "127.0.0.1:50052"
	}

	gw, err := gateway.New(cfg)
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
		errCh <- gw.ListenAndServe(runCtx, cfg.ListenAddr, cfg.HTTPAddr)
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
	if base[0] != 'h' {
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
	if base[0] != 'h' {
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
