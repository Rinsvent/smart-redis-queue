package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/Rinsvent/smart-redis-queue"
	"github.com/redis/go-redis/v9"
	"github.com/urfave/cli/v3"
)

func main() {
	cmd := &cli.Command{
		Name:  "manager",
		Usage: "CLI для обслуживания очередей Smart Redis Queue",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:    "addr",
				Aliases: []string{"a"},
				Value:   "localhost:6379",
				Usage:   "Redis address",
				Sources: cli.EnvVars("REDIS_ADDR"),
			},
			&cli.StringFlag{
				Name:    "password",
				Aliases: []string{"P"},
				Value:   "",
				Usage:   "Redis password",
				Sources: cli.EnvVars("REDIS_PASSWORD"),
			},
			&cli.IntFlag{
				Name:  "db",
				Value: 0,
				Usage: "Redis DB",
			},
		},
		Commands: []*cli.Command{
			{
				Name:    "info",
				Aliases: []string{"i", "list", "ls"},
				Usage:   "Информация об очередях: партиции, pending, in-progress",
				Flags: []cli.Flag{
					&cli.StringFlag{Name: "queue", Aliases: []string{"q"}, Value: "", Usage: "фильтр по имени очереди"},
					&cli.StringFlag{Name: "partition", Aliases: []string{"p"}, Value: "", Usage: "фильтр по партиции"},
				},
				Action: runInfo,
			},
			{
				Name:  "purge",
				Usage: "Очистить очередь (удалить все задачи)",
				Flags: []cli.Flag{
					&cli.StringFlag{Name: "queue", Aliases: []string{"q"}, Required: true, Usage: "имя очереди"},
					&cli.StringFlag{Name: "partition", Aliases: []string{"p"}, Value: "", Usage: "очистить только эту партицию (пусто = вся очередь)"},
				},
				Action: runPurge,
			},
			{
				Name:  "retry",
				Usage: "Вернуть in-progress задачи обратно в очередь (для зависших после падения консьюмера)",
				Flags: []cli.Flag{
					&cli.StringFlag{Name: "queue", Aliases: []string{"q"}, Required: true, Usage: "имя очереди"},
				},
				Action: runRetry,
			},
		},
	}

	if err := cmd.Run(context.Background(), os.Args); err != nil {
		log.Fatal(err)
	}
}

func redisClient(c *cli.Command) *redis.Client {
	root := c.Root()
	return redis.NewClient(&redis.Options{
		Addr:     root.String("addr"),
		Password: root.String("password"),
		DB:       root.Int("db"),
	})
}

func runInfo(ctx context.Context, c *cli.Command) error {
	rdb := redisClient(c)
	defer rdb.Close()

	admin := redisqueue.NewAdmin(rdb)
	statsList, err := admin.Inspect(ctx, c.String("queue"), c.String("partition"))
	if err != nil {
		return err
	}

	if len(statsList) == 0 {
		fmt.Println("Очереди не найдены.")
		return nil
	}

	for _, stats := range statsList {
		fmt.Printf("\n📦 Queue: %s\n", stats.QueueName)
		fmt.Printf("   Pending: %d  |  In-progress: %d  |  Consumers: %d\n",
			stats.TotalPending, stats.TotalInProgress, len(stats.Consumers))
		if len(stats.Consumers) > 0 {
			fmt.Printf("   Consumer IDs: %s\n", strings.Join(stats.Consumers, ", "))
		}

		for _, ps := range stats.Partitions {
			flags := ""
			if ps.Locked {
				flags += " [locked]"
			}
			if ps.Blocked {
				flags += " [blocked]"
			}
			fmt.Printf("   └── partition %s: pending=%d in-progress=%d%s\n",
				ps.Partition, ps.Pending, ps.InProgress, flags)
		}
	}
	fmt.Println()
	return nil
}

func runPurge(ctx context.Context, c *cli.Command) error {
	rdb := redisClient(c)
	defer rdb.Close()

	queue := c.String("queue")
	partition := c.String("partition")

	admin := redisqueue.NewAdmin(rdb)
	n, err := admin.Purge(ctx, queue, partition)
	if err != nil {
		return err
	}

	if partition != "" {
		fmt.Printf("Purged partition %q in queue %q: %d keys removed\n", partition, queue, n)
	} else {
		fmt.Printf("Purged queue %q: %d keys removed\n", queue, n)
	}
	return nil
}

func runRetry(ctx context.Context, c *cli.Command) error {
	rdb := redisClient(c)
	defer rdb.Close()

	admin := redisqueue.NewAdmin(rdb)
	n, err := admin.Retry(ctx, c.String("queue"))
	if err != nil {
		return err
	}
	fmt.Printf("Retried %d tasks (returned to queue)\n", n)
	return nil
}
