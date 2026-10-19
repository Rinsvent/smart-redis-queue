package desktopui

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	redisqueue "github.com/Rinsvent/smart-redis-queue"
	"github.com/redis/go-redis/v9"
)

const (
	catalogInterval   = 5 * time.Minute
	defaultDBCount    = 16
	maxDBCount        = 16
	dbDialTimeout     = 2 * time.Second
	dbScanTimeout     = 5 * time.Second
	catalogMaxTimeout = 20 * time.Second
)

// PartitionView — партиция для UI.
type PartitionView struct {
	ID         string   `json:"id"`
	Pending    int64    `json:"pending"`
	InProgress int64    `json:"inProgress"`
	Locked     bool     `json:"locked"`
	Blocked    bool     `json:"blocked"`
	Groups     []string `json:"groups"`
}

// GroupView — группа очереди.
type GroupView struct {
	Name    string  `json:"name"`
	Ready   int64   `json:"ready"`
	Blocked bool    `json:"blocked"`
	Score   float64 `json:"score"`
}

// RateView — клиентская оценка скорости.
type RateView struct {
	ProcessedPerSec    float64 `json:"processedPerSec"`
	BacklogDeltaPerSec float64 `json:"backlogDeltaPerSec"`
}

// QueueListItem — схлопнутая очередь (только имя + DB для сайдбара).
type QueueListItem struct {
	Name string `json:"name"`
	DBs  []int  `json:"dbs"`
}

// QueueSnapshot — сводка очереди на DB (без полного списка партиций).
type QueueSnapshot struct {
	Name           string      `json:"name"`
	DB             int         `json:"db"`
	Pending        int64       `json:"pending"`
	InProgress     int64       `json:"inProgress"`
	Consumers      int         `json:"consumers"`
	PartitionTotal int         `json:"partitionTotal"`
	Groups         []GroupView `json:"groups"`
	Rate           RateView    `json:"rate"`
}

// PartitionListResult — отфильтрованные партиции.
type PartitionListResult struct {
	Partitions []PartitionView `json:"partitions"`
	Total      int             `json:"total"`
	Limit      int             `json:"limit"`
	Truncated  bool            `json:"truncated"`
}

// GroupListResult — отфильтрованные группы.
type GroupListResult struct {
	Groups    []GroupView `json:"groups"`
	Total     int         `json:"total"`
	Limit     int         `json:"limit"`
	Truncated bool        `json:"truncated"`
}

// TagView — тег для UI.
type TagView struct {
	Tag   string `json:"tag"`
	Count int64  `json:"count"`
}

// TagListResult — страница тегов (total не считается; см. CountTags).
type TagListResult struct {
	Tags      []TagView `json:"tags"`
	Limit     int       `json:"limit"`
	Truncated bool      `json:"truncated"` // true если набрали полный limit — возможно есть ещё
}

// TaskView — payload и мета по id.
type TaskView struct {
	TaskID    string `json:"taskId"`
	Payload   string `json:"payload"`
	Partition string `json:"partition"`
	Priority  string `json:"priority"`
	Group     string `json:"group"`
	Exists    bool   `json:"exists"`
}

// TagRemoveResult — агрегат RemoveByTag для UI.
type TagRemoveResult struct {
	Removed  int      `json:"removed"`
	Payloads []string `json:"payloads,omitempty"`
}

// StatPointView — секунда метрик для графика.
type StatPointView struct {
	Sec    int64            `json:"sec"`
	Get    int64            `json:"get"`
	Ack    int64            `json:"ack"`
	Reject int64            `json:"reject"`
	Tags   map[string]int64 `json:"tags,omitempty"`
}

// App — биндинги Wails / логика desktop manager.
type App struct {
	ctx   context.Context
	store *Store

	mu       sync.Mutex
	addr     string
	password string
	connID   string
	clients  map[int]*redis.Client

	catalogMu sync.RWMutex
	catalog   map[string][]int // queue name → DBs

	loopMu     sync.Mutex
	loopCancel context.CancelFunc

	// Отмена устаревших UI-запросов при смене очереди / новом list|inspect.
	queryMu       sync.Mutex
	listCancel    context.CancelFunc
	inspectCancel context.CancelFunc

	prevMu sync.Mutex
	prev   map[string]sample // key: name@db

	// Кольцо сэмплов Redis INFO/SLOWLOG для графика «Redis host».
	redisMu      sync.Mutex
	redisSamples []RedisMetricPoint
	redisLastCPU float64
	redisLastAt  time.Time
	redisHasLast bool
}

type sample struct {
	pending    int64
	inProgress int64
	at         time.Time
}

const redisMetricsKeep = 3600

func NewApp(store *Store) *App {
	return &App{
		store:   store,
		clients: map[int]*redis.Client{},
		catalog: map[string][]int{},
		prev:    map[string]sample{},
	}
}

func (a *App) ctxOrBackground() context.Context {
	if a.ctx != nil {
		return a.ctx
	}
	return context.Background()
}

func (a *App) beginList(timeout time.Duration) (context.Context, context.CancelFunc) {
	a.queryMu.Lock()
	defer a.queryMu.Unlock()
	if a.listCancel != nil {
		a.listCancel()
	}
	ctx, cancel := context.WithTimeout(a.ctxOrBackground(), timeout)
	a.listCancel = cancel
	return ctx, cancel
}

func (a *App) beginInspect(timeout time.Duration) (context.Context, context.CancelFunc) {
	a.queryMu.Lock()
	defer a.queryMu.Unlock()
	if a.inspectCancel != nil {
		a.inspectCancel()
	}
	ctx, cancel := context.WithTimeout(a.ctxOrBackground(), timeout)
	a.inspectCancel = cancel
	return ctx, cancel
}

// CancelQueries отменяет in-flight List*/Inspect (смена очереди в UI).
func (a *App) CancelQueries() {
	a.queryMu.Lock()
	defer a.queryMu.Unlock()
	if a.listCancel != nil {
		a.listCancel()
		a.listCancel = nil
	}
	if a.inspectCancel != nil {
		a.inspectCancel()
		a.inspectCancel = nil
	}
}

func sampleKey(name string, db int) string {
	return name + "@" + strconv.Itoa(db)
}

// --- Connections ---

func (a *App) ListConnections() []Connection {
	return a.store.List()
}

func (a *App) ActiveConnectionID() string {
	return a.store.ActiveID()
}

func (a *App) AddConnection(name, addr, password string) (Connection, error) {
	if err := a.ping(addr, password); err != nil {
		return Connection{}, fmt.Errorf("connection failed: %w", err)
	}
	c, err := a.store.Add(name, addr, password)
	if err != nil {
		return Connection{}, err
	}
	if err := a.connect(c); err != nil {
		return Connection{}, err
	}
	return c, nil
}

func (a *App) RemoveConnection(id string) error {
	a.mu.Lock()
	same := a.connID == id
	a.mu.Unlock()
	if same {
		a.disconnect()
	}
	return a.store.Remove(id)
}

func (a *App) SelectConnection(id string) error {
	if err := a.store.Select(id); err != nil {
		return err
	}
	c, err := a.store.Active()
	if err != nil {
		return err
	}
	return a.connect(*c)
}

func (a *App) TestConnection(addr, password string) error {
	return a.ping(addr, password)
}

func (a *App) ping(addr, password string) error {
	rdb := redis.NewClient(&redis.Options{Addr: addr, Password: password, DB: 0})
	defer rdb.Close()
	ctx, cancel := context.WithTimeout(context.Background(), dbDialTimeout)
	defer cancel()
	return rdb.Ping(ctx).Err()
}

func (a *App) connect(c Connection) error {
	a.disconnect()

	rdb := redis.NewClient(&redis.Options{Addr: c.Addr, Password: c.Password, DB: 0})
	ctx, cancel := context.WithTimeout(context.Background(), dbDialTimeout)
	defer cancel()
	if err := rdb.Ping(ctx).Err(); err != nil {
		_ = rdb.Close()
		return err
	}

	a.mu.Lock()
	a.addr = c.Addr
	a.password = c.Password
	a.connID = c.ID
	a.clients = map[int]*redis.Client{0: rdb}
	a.mu.Unlock()

	a.prevMu.Lock()
	a.prev = map[string]sample{}
	a.prevMu.Unlock()

	if err := a.RefreshCatalog(); err != nil {
		return err
	}
	a.startCatalogLoop()
	return nil
}

func (a *App) clearRedisMetrics() {
	a.redisMu.Lock()
	a.redisSamples = nil
	a.redisLastCPU = 0
	a.redisLastAt = time.Time{}
	a.redisHasLast = false
	a.redisMu.Unlock()
}

func (a *App) disconnect() {
	a.stopCatalogLoop()
	a.clearRedisMetrics()

	a.mu.Lock()
	for _, c := range a.clients {
		_ = c.Close()
	}
	a.clients = map[int]*redis.Client{}
	a.addr = ""
	a.password = ""
	a.connID = ""
	a.mu.Unlock()

	a.catalogMu.Lock()
	a.catalog = map[string][]int{}
	a.catalogMu.Unlock()
}

func (a *App) clientForDB(db int) (*redis.Client, error) {
	a.mu.Lock()
	if a.addr == "" {
		a.mu.Unlock()
		return nil, fmt.Errorf("no active connection")
	}
	if c, ok := a.clients[db]; ok {
		a.mu.Unlock()
		return c, nil
	}
	addr, password := a.addr, a.password
	a.mu.Unlock()

	c := redis.NewClient(&redis.Options{
		Addr:         addr,
		Password:     password,
		DB:           db,
		DialTimeout:  dbDialTimeout,
		ReadTimeout:  dbScanTimeout,
		WriteTimeout: dbDialTimeout,
	})
	ctx, cancel := context.WithTimeout(context.Background(), dbDialTimeout)
	defer cancel()
	if err := c.Ping(ctx).Err(); err != nil {
		_ = c.Close()
		return nil, fmt.Errorf("db %d: %w", db, err)
	}

	a.mu.Lock()
	defer a.mu.Unlock()
	if existing, ok := a.clients[db]; ok {
		_ = c.Close()
		return existing, nil
	}
	if a.addr == "" {
		_ = c.Close()
		return nil, fmt.Errorf("no active connection")
	}
	a.clients[db] = c
	return c, nil
}

func (a *App) adminForDB(db int) (*redisqueue.Admin, error) {
	rdb, err := a.clientForDB(db)
	if err != nil {
		return nil, err
	}
	return redisqueue.NewAdmin(rdb), nil
}

func (a *App) ensureConnected() error {
	a.mu.Lock()
	ok := a.addr != ""
	a.mu.Unlock()
	if ok {
		return nil
	}
	c, err := a.store.Active()
	if err != nil {
		return err
	}
	return a.connect(*c)
}

// EnsureActive подключает активный профиль только если ещё не подключён к нему.
func (a *App) EnsureActive() error {
	id := a.store.ActiveID()
	if id == "" {
		return nil
	}
	a.mu.Lock()
	already := a.connID == id && a.addr != ""
	a.mu.Unlock()
	if already {
		return nil
	}
	c, err := a.store.Active()
	if err != nil {
		return err
	}
	return a.connect(*c)
}

func (a *App) startCatalogLoop() {
	a.stopCatalogLoop()
	ctx, cancel := context.WithCancel(a.ctxOrBackground())
	a.loopMu.Lock()
	a.loopCancel = cancel
	a.loopMu.Unlock()
	go func() {
		t := time.NewTicker(catalogInterval)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				_ = a.RefreshCatalog()
			}
		}
	}()
}

func (a *App) stopCatalogLoop() {
	a.loopMu.Lock()
	defer a.loopMu.Unlock()
	if a.loopCancel != nil {
		a.loopCancel()
		a.loopCancel = nil
	}
}

func (a *App) databaseCount(ctx context.Context) int {
	rdb, err := a.clientForDB(0)
	if err != nil {
		return defaultDBCount
	}
	res, err := rdb.ConfigGet(ctx, "databases").Result()
	if err != nil {
		return defaultDBCount
	}
	raw, ok := res["databases"]
	if !ok || raw == "" {
		return defaultDBCount
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n <= 0 {
		return defaultDBCount
	}
	if n > maxDBCount {
		return maxDBCount
	}
	return n
}

// RefreshCatalog сканирует Redis DB (partitions + consumers) с таймаутами.
func (a *App) RefreshCatalog() error {
	if err := a.ensureConnected(); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(a.ctxOrBackground(), catalogMaxTimeout)
	defer cancel()

	n := a.databaseCount(ctx)
	type hit struct {
		name string
		db   int
	}
	hits := make(chan hit, n*4)
	var wg sync.WaitGroup

	for db := 0; db < n; db++ {
		wg.Add(1)
		go func(db int) {
			defer wg.Done()
			dbCtx, dbCancel := context.WithTimeout(ctx, dbScanTimeout)
			defer dbCancel()

			admin, err := a.adminForDB(db)
			if err != nil {
				return
			}
			names, err := admin.ListQueues(dbCtx)
			if err != nil {
				return
			}
			for _, name := range names {
				select {
				case hits <- hit{name: name, db: db}:
				case <-dbCtx.Done():
					return
				}
			}
		}(db)
	}

	go func() {
		wg.Wait()
		close(hits)
	}()

	next := map[string][]int{}
	for h := range hits {
		next[h.name] = append(next[h.name], h.db)
	}
	for name := range next {
		sort.Ints(next[name])
		// dedupe if any
		next[name] = uniqueInts(next[name])
	}

	a.catalogMu.Lock()
	a.catalog = next
	a.catalogMu.Unlock()
	return nil
}

func uniqueInts(in []int) []int {
	if len(in) < 2 {
		return in
	}
	out := in[:1]
	for _, v := range in[1:] {
		if v != out[len(out)-1] {
			out = append(out, v)
		}
	}
	return out
}

func (a *App) catalogSnapshot() map[string][]int {
	a.catalogMu.RLock()
	defer a.catalogMu.RUnlock()
	out := make(map[string][]int, len(a.catalog))
	for k, v := range a.catalog {
		cp := make([]int, len(v))
		copy(cp, v)
		out[k] = cp
	}
	return out
}

// --- Queues ---

// ListQueues возвращает схлопнутый каталог (только имена + DB).
func (a *App) ListQueues() ([]QueueListItem, error) {
	if err := a.ensureConnected(); err != nil {
		return nil, err
	}
	cat := a.catalogSnapshot()
	if len(cat) == 0 {
		_ = a.RefreshCatalog()
		cat = a.catalogSnapshot()
	}

	names := make([]string, 0, len(cat))
	for name := range cat {
		names = append(names, name)
	}
	sort.Strings(names)

	out := make([]QueueListItem, 0, len(names))
	for _, name := range names {
		out = append(out, QueueListItem{Name: name, DBs: cat[name]})
	}
	return out, nil
}

// InspectQueue — сводка очереди на DB (без таблицы партиций).
func (a *App) InspectQueue(name string, db int) (QueueSnapshot, error) {
	if err := a.ensureConnected(); err != nil {
		return QueueSnapshot{}, err
	}
	return a.inspectOne(name, db)
}

// ListPartitions — партиции с фильтром подстроки, max MaxListPartitions.
func (a *App) ListPartitions(name string, db int, filter string) (PartitionListResult, error) {
	admin, err := a.adminForDB(db)
	if err != nil {
		return PartitionListResult{}, err
	}
	ctx, cancel := a.beginList(15 * time.Second)
	defer cancel()
	parts, total, err := admin.ListPartitions(ctx, name, filter, redisqueue.MaxListPartitions)
	if err != nil {
		return PartitionListResult{}, err
	}
	out := make([]PartitionView, 0, len(parts))
	for _, p := range parts {
		groups := p.Groups
		if groups == nil {
			groups = []string{}
		}
		out = append(out, PartitionView{
			ID:         p.Partition,
			Pending:    p.Pending,
			InProgress: p.InProgress,
			Locked:     p.Locked,
			Blocked:    p.Blocked,
			Groups:     groups,
		})
	}
	return PartitionListResult{
		Partitions: out,
		Total:      total,
		Limit:      redisqueue.MaxListPartitions,
		Truncated:  total > len(out),
	}, nil
}

// ListGroups — группы с фильтром подстроки.
func (a *App) ListGroups(name string, db int, filter string) (GroupListResult, error) {
	admin, err := a.adminForDB(db)
	if err != nil {
		return GroupListResult{}, err
	}
	ctx, cancel := a.beginList(15 * time.Second)
	defer cancel()
	items, total, err := admin.ListGroups(ctx, name, filter, redisqueue.MaxListPartitions)
	if err != nil {
		return GroupListResult{}, err
	}
	out := make([]GroupView, 0, len(items))
	for _, g := range items {
		out = append(out, GroupView{
			Name:    g.Name,
			Ready:   g.Ready,
			Blocked: g.Blocked,
			Score:   g.Score,
		})
	}
	return GroupListResult{
		Groups:    out,
		Total:     total,
		Limit:     redisqueue.MaxListPartitions,
		Truncated: total > len(out),
	}, nil
}

// ListTags — теги с фильтром подстроки (без total; CountTags по клику).
func (a *App) ListTags(name string, db int, filter string) (TagListResult, error) {
	admin, err := a.adminForDB(db)
	if err != nil {
		return TagListResult{}, err
	}
	ctx, cancel := a.beginList(15 * time.Second)
	defer cancel()
	items, _, err := admin.ListTags(ctx, name, filter, redisqueue.MaxListPartitions)
	if err != nil {
		return TagListResult{}, err
	}
	out := make([]TagView, 0, len(items))
	for _, t := range items {
		out = append(out, TagView{Tag: t.Tag, Count: t.Count})
	}
	return TagListResult{
		Tags:      out,
		Limit:     redisqueue.MaxListPartitions,
		Truncated: len(out) >= redisqueue.MaxListPartitions,
	}, nil
}

// CountTags — полное число тегов (итеративный SCAN).
func (a *App) CountTags(name string, db int) (int, error) {
	admin, err := a.adminForDB(db)
	if err != nil {
		return 0, err
	}
	ctx, cancel := a.beginList(5 * time.Minute)
	defer cancel()
	return admin.CountTags(ctx, name)
}

// StatsBoundsView — диапазон доступных stats в Redis.
type StatsBoundsView struct {
	From int64 `json:"from"` // 0 если пусто
	To   int64 `json:"to"`
}

// ListQueueStats — секундные bucket'ы за lastSec (0 = весь stats:index; max 24h).
func (a *App) ListQueueStats(name string, db int, lastSec int) ([]StatPointView, error) {
	admin, err := a.adminForDB(db)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(a.ctxOrBackground(), 15*time.Second)
	defer cancel()
	to := time.Now().Unix()
	var from int64
	if lastSec <= 0 {
		bFrom, bTo, err := admin.QueueStatsBounds(ctx, name)
		if err != nil {
			return nil, err
		}
		if bFrom == 0 && bTo == 0 {
			return nil, nil
		}
		from, to = bFrom, bTo
		if to < time.Now().Unix() {
			to = time.Now().Unix()
		}
	} else {
		if lastSec > 86400 {
			lastSec = 86400
		}
		from = to - int64(lastSec)
	}
	pts, err := admin.ListQueueStats(ctx, name, from, to)
	if err != nil {
		return nil, err
	}
	out := make([]StatPointView, 0, len(pts))
	for _, p := range pts {
		tags := p.Tags
		if tags == nil {
			tags = map[string]int64{}
		}
		out = append(out, StatPointView{
			Sec:    p.Sec,
			Get:    p.Get,
			Ack:    p.Ack,
			Reject: p.Reject,
			Tags:   tags,
		})
	}
	return out, nil
}

// QueueStatsBounds — min/max sec в stats:index.
func (a *App) QueueStatsBounds(name string, db int) (StatsBoundsView, error) {
	admin, err := a.adminForDB(db)
	if err != nil {
		return StatsBoundsView{}, err
	}
	ctx, cancel := context.WithTimeout(a.ctxOrBackground(), 5*time.Second)
	defer cancel()
	from, to, err := admin.QueueStatsBounds(ctx, name)
	if err != nil {
		return StatsBoundsView{}, err
	}
	return StatsBoundsView{From: from, To: to}, nil
}

// RedisMetricPoint — точка для графика Redis host (уже с дельтами CPU).
type RedisMetricPoint struct {
	Sec          int64   `json:"sec"`
	Ops          int64   `json:"ops"`
	CpuUsPerSec  float64 `json:"cpuUsPerSec"`
	MemMB        float64 `json:"memMB"`
	SlowlogLen   int64   `json:"slowlogLen"`
	SlowlogMaxUs int64   `json:"slowlogMaxUs"`
}

// SampleRedisMetrics снимает INFO+SLOWLOG, кладёт в кольцо и возвращает последнюю точку.
func (a *App) SampleRedisMetrics(db int) (RedisMetricPoint, error) {
	rdb, err := a.clientForDB(db)
	if err != nil {
		return RedisMetricPoint{}, err
	}
	ctx, cancel := context.WithTimeout(a.ctxOrBackground(), 3*time.Second)
	defer cancel()

	now := time.Now()
	// Полный INFO: несколько секций в одном вызове на Redis 6 могут ломать команду.
	info, err := rdb.Info(ctx).Result()
	if err != nil {
		return RedisMetricPoint{}, err
	}

	var ops, usedMem int64
	var usedCPU float64
	for _, line := range strings.Split(info, "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		k, v, ok := strings.Cut(line, ":")
		if !ok {
			continue
		}
		switch k {
		case "instantaneous_ops_per_sec":
			ops, _ = strconv.ParseInt(v, 10, 64)
		case "used_cpu_sys", "used_cpu_user":
			if f, e := strconv.ParseFloat(v, 64); e == nil {
				usedCPU += f
			}
		case "used_memory":
			usedMem, _ = strconv.ParseInt(v, 10, 64)
		}
	}

	var slowLen, slowMax int64
	if n, err := rdb.SlowLogLen(ctx).Result(); err == nil {
		slowLen = n
	}
	if entries, err := rdb.SlowLogGet(ctx, 32).Result(); err == nil {
		for _, e := range entries {
			us := e.Duration.Microseconds()
			if us > slowMax {
				slowMax = us
			}
		}
	}

	pt := RedisMetricPoint{
		Sec:          now.Unix(),
		Ops:          ops,
		MemMB:        float64(usedMem) / (1024 * 1024),
		SlowlogLen:   slowLen,
		SlowlogMaxUs: slowMax,
	}

	a.redisMu.Lock()
	defer a.redisMu.Unlock()
	if a.redisHasLast && now.After(a.redisLastAt) {
		dt := now.Sub(a.redisLastAt).Seconds()
		if dt > 0 {
			dCPU := usedCPU - a.redisLastCPU
			if dCPU < 0 {
				dCPU = 0
			}
			pt.CpuUsPerSec = (dCPU / dt) * 1e6
		}
	}
	a.redisLastCPU = usedCPU
	a.redisLastAt = now
	a.redisHasLast = true

	a.redisSamples = append(a.redisSamples, pt)
	if len(a.redisSamples) > redisMetricsKeep {
		a.redisSamples = append([]RedisMetricPoint(nil), a.redisSamples[len(a.redisSamples)-redisMetricsKeep:]...)
	}
	return pt, nil
}

// ListRedisMetrics — точки за lastSec (0 = всё кольцо, max 1h). Перед отдачей делает Sample.
func (a *App) ListRedisMetrics(db int, lastSec int) ([]RedisMetricPoint, error) {
	if _, err := a.SampleRedisMetrics(db); err != nil {
		return nil, err
	}
	if lastSec <= 0 || lastSec > redisMetricsKeep {
		lastSec = redisMetricsKeep
	}
	cutoff := time.Now().Unix() - int64(lastSec)

	a.redisMu.Lock()
	defer a.redisMu.Unlock()
	out := make([]RedisMetricPoint, 0, len(a.redisSamples))
	for _, p := range a.redisSamples {
		if p.Sec >= cutoff {
			out = append(out, p)
		}
	}
	return out, nil
}

// BlockPartition — rate-limit block на waitTimeSec секунд.
func (a *App) BlockPartition(queue, partition string, waitTimeSec float64, db int) error {
	admin, err := a.adminForDB(db)
	if err != nil {
		return err
	}
	return admin.BlockPartition(a.ctxOrBackground(), queue, partition, waitTimeSec)
}

// UnblockPartition — снять из blocked.
func (a *App) UnblockPartition(queue, partition string, db int) error {
	admin, err := a.adminForDB(db)
	if err != nil {
		return err
	}
	return admin.UnblockPartition(a.ctxOrBackground(), queue, partition)
}

func (a *App) inspectOne(name string, db int) (QueueSnapshot, error) {
	admin, err := a.adminForDB(db)
	if err != nil {
		return QueueSnapshot{}, err
	}
	ctx, cancel := a.beginInspect(dbScanTimeout)
	defer cancel()
	s, err := admin.QueueOverview(ctx, name)
	if err != nil {
		return QueueSnapshot{}, err
	}
	snap := QueueSnapshot{
		Name:           name,
		DB:             db,
		Pending:        s.TotalPending,
		InProgress:     s.TotalInProgress,
		Consumers:      len(s.Consumers),
		PartitionTotal: s.PartitionTotal,
		Groups:         []GroupView{},
	}
	for _, g := range s.Groups {
		snap.Groups = append(snap.Groups, GroupView{
			Name:    g.Name,
			Ready:   g.Ready,
			Blocked: g.Blocked,
			Score:   g.Score,
		})
	}

	key := sampleKey(name, db)
	now := time.Now()
	cur := sample{pending: snap.Pending, inProgress: snap.InProgress, at: now}
	a.prevMu.Lock()
	prev, hasPrev := a.prev[key]
	if a.prev == nil {
		a.prev = map[string]sample{}
	}
	a.prev[key] = cur
	a.prevMu.Unlock()

	if hasPrev {
		dt := now.Sub(prev.at).Seconds()
		if dt > 0.2 {
			prevBacklog := prev.pending + prev.inProgress
			curBacklog := cur.pending + cur.inProgress
			snap.Rate.BacklogDeltaPerSec = float64(curBacklog-prevBacklog) / dt
			// processed: по ack из stats (pending-delta врал, пока overview не считал pending)
			fromSec := now.Add(-time.Duration(dt*float64(time.Second)) - time.Second).Unix()
			toSec := now.Unix()
			if pts, err := admin.ListQueueStats(ctx, name, fromSec, toSec); err == nil {
				var acks int64
				for _, p := range pts {
					acks += p.Ack
				}
				snap.Rate.ProcessedPerSec = float64(acks) / dt
			}
		}
	}
	return snap, nil
}

func (a *App) Purge(queue, partition string, db int) (int, error) {
	admin, err := a.adminForDB(db)
	if err != nil {
		return 0, err
	}
	n, err := admin.Purge(a.ctxOrBackground(), queue, partition)
	if err != nil {
		return 0, err
	}
	_ = a.RefreshCatalog()
	return n, nil
}

func (a *App) Retry(queue string, db int) (int, error) {
	admin, err := a.adminForDB(db)
	if err != nil {
		return 0, err
	}
	return admin.Retry(a.ctxOrBackground(), queue)
}

func (a *App) GetTaskPayload(queue, taskID string, db int) (TaskView, error) {
	rdb, err := a.clientForDB(db)
	if err != nil {
		return TaskView{}, err
	}
	ctx, cancel := context.WithTimeout(a.ctxOrBackground(), dbScanTimeout)
	defer cancel()
	payload, err := rdb.Get(ctx, "queue:"+queue+":payload:"+taskID).Result()
	if err == redis.Nil {
		return TaskView{TaskID: taskID, Exists: false}, nil
	}
	if err != nil {
		return TaskView{}, err
	}
	part, _ := rdb.Get(ctx, "queue:"+queue+":partition:"+taskID).Result()
	prio, _ := rdb.Get(ctx, "queue:"+queue+":priority:"+taskID).Result()
	return TaskView{
		TaskID:    taskID,
		Payload:   payload,
		Partition: part,
		Priority:  prio,
		Exists:    true,
	}, nil
}

func (a *App) CountByTag(queue, tag string, db int) (int64, error) {
	admin, err := a.adminForDB(db)
	if err != nil {
		return 0, err
	}
	return admin.CountByTag(a.ctxOrBackground(), queue, tag)
}

func (a *App) RemoveByTag(queue, tag string, limit int, returnPayload bool, db int) (TagRemoveResult, error) {
	admin, err := a.adminForDB(db)
	if err != nil {
		return TagRemoveResult{}, err
	}
	res, err := admin.RemoveByTag(a.ctxOrBackground(), queue, tag, redisqueue.RemoveByTagOptions{
		Limit:         limit,
		ReturnPayload: returnPayload,
	})
	if err != nil {
		return TagRemoveResult{}, err
	}
	out := TagRemoveResult{}
	for _, r := range res {
		if r.Status == redisqueue.RemovalStatusRemoved {
			out.Removed++
			if returnPayload && len(r.Payload) > 0 {
				out.Payloads = append(out.Payloads, string(r.Payload))
			}
		}
	}
	return out, nil
}
