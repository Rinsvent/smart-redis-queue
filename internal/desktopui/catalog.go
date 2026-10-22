package desktopui

import (
	"context"
	"sort"
	"sync"
	"time"
)

// RefreshCatalog обновляет каталог очередей по всем DB.
// Если уже идёт предыдущий refresh — сразу выходит (single-flight).
// Есть srq:queues → быстрый путь (параллельный ZRANGE по DB).
// Иначе → legacy SCAN по одной DB (один поток).
func (a *App) RefreshCatalog() error {
	if err := a.ensureConnected(); err != nil {
		return err
	}
	if !a.catalogRefreshing.CompareAndSwap(false, true) {
		return nil
	}
	defer a.catalogRefreshing.Store(false)

	ctx, cancel := context.WithTimeout(a.ctxOrBackground(), catalogMaxTimeout)
	defer cancel()

	n := a.databaseCount(ctx)
	useIndex := a.anyDBHasQueueIndex(ctx, n)

	var hits []catalogHit
	if useIndex {
		hits = a.catalogFromIndex(ctx, n)
	} else {
		hits = a.catalogFromScan(ctx, n)
	}

	next := map[string][]int{}
	totals := map[string]int64{}
	for _, h := range hits {
		next[h.name] = append(next[h.name], h.db)
		totals[h.name] += h.total
	}
	for name := range next {
		sort.Ints(next[name])
		next[name] = uniqueInts(next[name])
	}

	a.catalogMu.Lock()
	a.catalog = next
	a.catalogTotals = totals
	a.catalogMu.Unlock()
	return nil
}

func (a *App) anyDBHasQueueIndex(ctx context.Context, n int) bool {
	type result struct{ ok bool }
	ch := make(chan result, n)
	var wg sync.WaitGroup
	for db := 0; db < n; db++ {
		wg.Add(1)
		go func(db int) {
			defer wg.Done()
			dbCtx, cancel := context.WithTimeout(ctx, dbDialTimeout)
			defer cancel()
			admin, err := a.adminForDB(db)
			if err != nil {
				ch <- result{}
				return
			}
			ok, err := admin.HasQueueIndex(dbCtx)
			ch <- result{ok: err == nil && ok}
		}(db)
	}
	go func() {
		wg.Wait()
		close(ch)
	}()
	for r := range ch {
		if r.ok {
			return true
		}
	}
	return false
}

type catalogHit struct {
	name  string
	db    int
	total int64
}

func (a *App) catalogFromIndex(ctx context.Context, n int) []catalogHit {
	ch := make(chan catalogHit, n*4)
	var wg sync.WaitGroup
	for db := 0; db < n; db++ {
		wg.Add(1)
		go func(db int) {
			defer wg.Done()
			dbCtx, cancel := context.WithTimeout(ctx, dbScanTimeout)
			defer cancel()
			admin, err := a.adminForDB(db)
			if err != nil {
				return
			}
			idx, err := admin.ListQueueIndex(dbCtx, false)
			if err != nil {
				return
			}
			for _, e := range idx {
				select {
				case ch <- catalogHit{name: e.Name, db: db, total: e.Total}:
				case <-dbCtx.Done():
					return
				}
			}
		}(db)
	}
	go func() {
		wg.Wait()
		close(ch)
	}()
	var out []catalogHit
	for h := range ch {
		out = append(out, h)
	}
	return out
}

func (a *App) catalogFromScan(ctx context.Context, n int) []catalogHit {
	var out []catalogHit
	for db := 0; db < n; db++ {
		if ctx.Err() != nil {
			break
		}
		dbCtx, cancel := context.WithTimeout(ctx, dbScanTimeout)
		admin, err := a.adminForDB(db)
		if err != nil {
			cancel()
			continue
		}
		names, err := admin.ListQueuesScan(dbCtx)
		if err != nil {
			cancel()
			continue
		}
		for _, name := range names {
			total, _ := admin.QueueDepth(dbCtx, name)
			out = append(out, catalogHit{name: name, db: db, total: total})
		}
		cancel()
		// лёгкая пауза между DB на legacy-пути, чтобы не долбить Redis
		select {
		case <-ctx.Done():
		case <-time.After(20 * time.Millisecond):
		}
	}
	return out
}
