package journal

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	pumpLockTTL     = 60 * time.Second
	pumpLockRefresh = 10 * time.Second
)

func pumpLockKey(name string) string {
	return "srq:journal:" + name + ":pump"
}

// acquirePumpLock берёт NX-лок; refresh в фоне пока жив ctx. unlock отменяет refresh и DEL.
func acquirePumpLock(ctx context.Context, rdb *redis.Client, name, holder string) (unlock func(), err error) {
	key := pumpLockKey(name)
	ok, err := rdb.SetNX(ctx, key, holder, pumpLockTTL).Result()
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, fmt.Errorf("journal: pump lock busy for %q", name)
	}
	stop := make(chan struct{})
	go func() {
		t := time.NewTicker(pumpLockRefresh)
		defer t.Stop()
		for {
			select {
			case <-stop:
				return
			case <-ctx.Done():
				return
			case <-t.C:
				cur, err := rdb.Get(context.Background(), key).Result()
				if err != nil || cur != holder {
					return
				}
				_ = rdb.Expire(context.Background(), key, pumpLockTTL).Err()
			}
		}
	}()
	return func() {
		close(stop)
		cur, err := rdb.Get(context.Background(), key).Result()
		if err == nil && cur == holder {
			_ = rdb.Del(context.Background(), key).Err()
		}
	}, nil
}
