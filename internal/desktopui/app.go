package desktopui

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	redisqueue "github.com/Rinsvent/smart-redis-queue"
	"github.com/Rinsvent/smart-redis-queue/tasks"
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

// QueueListItem — схлопнутая очередь (имя + DB + total из srq:queues).
type QueueListItem struct {
	Name  string `json:"name"`
	DBs   []int  `json:"dbs"`
	Total int64  `json:"total"` // сумма depth по DB
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

// TaskView — payload, headers и мета по id.
type TaskView struct {
	TaskID        string            `json:"taskId"`
	Payload       string            `json:"payload"`
	Headers       map[string]string `json:"headers,omitempty"`
	Partition     string            `json:"partition"`
	Priority      string            `json:"priority"`
	QueuePriority string            `json:"queuePriority,omitempty"`
	Tags          []string          `json:"tags,omitempty"`
	Group         string            `json:"group"`
	Exists        bool              `json:"exists"`
	RejectCount   int64             `json:"rejectCount,omitempty"`
	Step          string            `json:"step,omitempty"`
	State         string            `json:"state,omitempty"`
	Progress      *StepProgressUI   `json:"progress,omitempty"`
}

// DLQListResult — Admin.ListDLQ для UI.
type DLQListResult struct {
	Entries   []DLQRow `json:"entries"`
	Count     int64    `json:"count"`
	Truncated bool     `json:"truncated"`
}

// DLQRow — строка режима DLQ.
type DLQRow struct {
	ID           string `json:"id"`
	Partition    string `json:"partition"`
	RejectCount  int64  `json:"rejectCount"`
	DeadLetterAt int64  `json:"deadLetterAtMs"`
}

// StepProgressUI — progress из step-ключа.
type StepProgressUI struct {
	Name         string `json:"name"`
	Curr         int    `json:"curr"`
	Total        int    `json:"total"`
	UnknownTotal bool   `json:"unknownTotal,omitempty"`
	Cursor       string `json:"cursor,omitempty"`
	StartedMs    int64  `json:"startedMs,omitempty"`
	Control      string `json:"control,omitempty"`
}

// MessageListResult — Admin.ListMessages для UI.
type MessageListResult struct {
	Messages  []MessageRow `json:"messages"`
	Limit     int          `json:"limit"`
	Truncated bool         `json:"truncated"`
}

// MessageRow — строка режима messages.
type MessageRow struct {
	ID          string          `json:"id"`
	Partition   string          `json:"partition"`
	State       string          `json:"state"`
	RejectCount int64           `json:"rejectCount"`
	Step        string          `json:"step,omitempty"`
	Progress    *StepProgressUI `json:"progress,omitempty"`
}

// RegistryTaskRow — режим tasks (catalog + run).
type RegistryTaskRow struct {
	Namespace   string            `json:"namespace"`
	Name        string            `json:"name"`
	Queue       string            `json:"queue"`
	Cron        string            `json:"cron,omitempty"`
	Concurrency string            `json:"concurrency,omitempty"`
	Args        []tasksArgDef     `json:"args,omitempty"`
	Run         map[string]string `json:"run,omitempty"`
}

type tasksArgDef struct {
	Name    string `json:"name"`
	Type    string `json:"type,omitempty"`
	Default string `json:"default,omitempty"`
	Help    string `json:"help,omitempty"`
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

// QueueDepthView — live total из srq:queues (Ok=false если реестра/очереди ещё нет).
type QueueDepthView struct {
	Total int64 `json:"total"`
	Ok    bool  `json:"ok"`
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

	catalogMu         sync.RWMutex
	catalog           map[string][]int // queue name → DBs
	catalogTotals     map[string]int64 // queue name → sum depth across DBs
	catalogRefreshing atomic.Bool      // single-flight RefreshCatalog

	loopMu     sync.Mutex
	loopCancel context.CancelFunc

	// Отмена UI-запросов только через CancelQueries (смена очереди).
	// Параллельные List* больше не отменяют друг друга (раньше один beginList
	// убивал ListRegistryTasks / ListMessages при одновременном ListTags и т.п.).
	queryMu        sync.Mutex
	listCancels    []context.CancelFunc
	inspectCancels []context.CancelFunc

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
	ctx, cancel := context.WithTimeout(a.ctxOrBackground(), timeout)
	a.queryMu.Lock()
	a.listCancels = append(a.listCancels, cancel)
	a.queryMu.Unlock()
	return ctx, cancel
}

func (a *App) beginInspect(timeout time.Duration) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithTimeout(a.ctxOrBackground(), timeout)
	a.queryMu.Lock()
	a.inspectCancels = append(a.inspectCancels, cancel)
	a.queryMu.Unlock()
	return ctx, cancel
}

// CancelQueries отменяет in-flight List*/Inspect (смена очереди в UI).
func (a *App) CancelQueries() {
	a.queryMu.Lock()
	defer a.queryMu.Unlock()
	for _, c := range a.listCancels {
		c()
	}
	a.listCancels = nil
	for _, c := range a.inspectCancels {
		c()
	}
	a.inspectCancels = nil
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
	a.catalogTotals = map[string]int64{}
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

// ListQueues возвращает схлопнутый каталог (имя + DB + total). Сортировку alpha/total делает UI.
func (a *App) ListQueues() ([]QueueListItem, error) {
	if err := a.ensureConnected(); err != nil {
		return nil, err
	}
	cat := a.catalogSnapshot()
	if len(cat) == 0 {
		_ = a.RefreshCatalog()
		cat = a.catalogSnapshot()
	}
	a.catalogMu.RLock()
	totals := a.catalogTotals
	if totals == nil {
		totals = map[string]int64{}
	}
	a.catalogMu.RUnlock()

	names := make([]string, 0, len(cat))
	for name := range cat {
		names = append(names, name)
	}
	sort.Strings(names)

	out := make([]QueueListItem, 0, len(names))
	for _, name := range names {
		out = append(out, QueueListItem{Name: name, DBs: cat[name], Total: totals[name]})
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

// ListBlockedPartitions — только партиции из ZSET blocked (unlockAt > now).
func (a *App) ListBlockedPartitions(name string, db int) (PartitionListResult, error) {
	admin, err := a.adminForDB(db)
	if err != nil {
		return PartitionListResult{}, err
	}
	ctx, cancel := a.beginList(15 * time.Second)
	defer cancel()
	parts, err := admin.ListBlockedPartitions(ctx, name)
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
			Blocked:    true,
			Groups:     groups,
		})
	}
	return PartitionListResult{
		Partitions: out,
		Total:      len(out),
		Limit:      len(out),
		Truncated:  false,
	}, nil
}

// ListLockedPartitions — ordered-партиции с :lock.
func (a *App) ListLockedPartitions(name string, db int) (PartitionListResult, error) {
	admin, err := a.adminForDB(db)
	if err != nil {
		return PartitionListResult{}, err
	}
	ctx, cancel := a.beginList(30 * time.Second)
	defer cancel()
	parts, err := admin.ListLockedPartitions(ctx, name)
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
			Locked:     true,
			Blocked:    p.Blocked,
			Groups:     groups,
		})
	}
	return PartitionListResult{
		Partitions: out,
		Total:      len(out),
		Limit:      len(out),
		Truncated:  false,
	}, nil
}

// RaceLockView — для UI.
type RaceLockView struct {
	Tag    string `json:"tag"`
	TaskID string `json:"taskId"`
}

// ListRaceLocks — активные race! locks.
func (a *App) ListRaceLocks(name string, db int) ([]RaceLockView, error) {
	admin, err := a.adminForDB(db)
	if err != nil {
		return nil, err
	}
	ctx, cancel := a.beginList(10 * time.Second)
	defer cancel()
	items, err := admin.ListRaceLocks(ctx, name)
	if err != nil {
		return nil, err
	}
	out := make([]RaceLockView, 0, len(items))
	for _, e := range items {
		out = append(out, RaceLockView{Tag: e.Tag, TaskID: e.TaskID})
	}
	return out, nil
}

// ScanTopPending — топ партиций по pending (медленный SCAN, кнопка в UI).
func (a *App) ScanTopPending(name string, db int, topN int) (PartitionListResult, error) {
	admin, err := a.adminForDB(db)
	if err != nil {
		return PartitionListResult{}, err
	}
	ctx, cancel := a.beginList(120 * time.Second)
	defer cancel()
	parts, total, err := admin.ScanTopPending(ctx, name, topN)
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
		Limit:      len(out),
		Truncated:  total > len(out),
	}, nil
}

// PoisonTaskView — задача с высоким reject_count.
type PoisonTaskView struct {
	ID          string `json:"id"`
	Partition   string `json:"partition"`
	RejectCount int    `json:"rejectCount"`
}

// ScanHighReject — топ poison-задач (SCAN reject_count).
func (a *App) ScanHighReject(name string, db int, minReject, topN int) ([]PoisonTaskView, error) {
	admin, err := a.adminForDB(db)
	if err != nil {
		return nil, err
	}
	ctx, cancel := a.beginList(120 * time.Second)
	defer cancel()
	items, err := admin.ScanHighReject(ctx, name, minReject, topN)
	if err != nil {
		return nil, err
	}
	out := make([]PoisonTaskView, 0, len(items))
	for _, t := range items {
		out = append(out, PoisonTaskView{ID: t.ID, Partition: t.Partition, RejectCount: t.RejectCount})
	}
	return out, nil
}

// ListBlockedGroups — только группы на паузе (score > now).
func (a *App) ListBlockedGroups(name string, db int) (GroupListResult, error) {
	admin, err := a.adminForDB(db)
	if err != nil {
		return GroupListResult{}, err
	}
	ctx, cancel := a.beginList(15 * time.Second)
	defer cancel()
	items, err := admin.ListBlockedGroups(ctx, name)
	if err != nil {
		return GroupListResult{}, err
	}
	out := make([]GroupView, 0, len(items))
	for _, g := range items {
		out = append(out, GroupView{
			Name:    g.Name,
			Ready:   g.Ready,
			Blocked: true,
			Score:   g.Score,
		})
	}
	return GroupListResult{
		Groups:    out,
		Total:     len(out),
		Limit:     len(out),
		Truncated: false,
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

// QueueDepth — live total из srq:queues; Ok=false → на графике пропускаем.
func (a *App) QueueDepth(name string, db int) (QueueDepthView, error) {
	admin, err := a.adminForDB(db)
	if err != nil {
		return QueueDepthView{}, err
	}
	ctx, cancel := context.WithTimeout(a.ctxOrBackground(), 3*time.Second)
	defer cancel()
	total, ok, err := admin.QueueDepthIfIndexed(ctx, name)
	if err != nil {
		return QueueDepthView{}, err
	}
	return QueueDepthView{Total: total, Ok: ok}, nil
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

// BlockGroups — парковка групп на waitTimeSec (Admin: перезаписывает unlockAt).
func (a *App) BlockGroups(queue string, waitTimeSec float64, groups []string, db int) error {
	admin, err := a.adminForDB(db)
	if err != nil {
		return err
	}
	return admin.BlockGroups(a.ctxOrBackground(), queue, waitTimeSec, groups...)
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
	admin, err := a.adminForDB(db)
	if err != nil {
		return TaskView{}, err
	}
	ctx, cancel := context.WithTimeout(a.ctxOrBackground(), dbScanTimeout)
	defer cancel()
	ti, err := admin.InspectTask(ctx, queue, taskID)
	if err != nil {
		return TaskView{}, err
	}
	tv := TaskView{
		TaskID: taskID, Exists: ti.Exists, Payload: ti.Payload, Headers: ti.Headers,
		Partition: ti.Partition, Priority: ti.Priority, QueuePriority: ti.QueuePriority,
		Tags: ti.Tags, RejectCount: ti.RejectCount, Step: ti.Step, State: ti.State,
	}
	if ti.Progress != nil {
		tv.Progress = &StepProgressUI{
			Name: ti.Progress.Name, Curr: ti.Progress.Curr, Total: ti.Progress.Total,
			UnknownTotal: ti.Progress.UnknownTotal, Cursor: ti.Progress.Cursor,
			StartedMs: ti.Progress.StartedMs, Control: ti.Progress.Control,
		}
	}
	return tv, nil
}

// ListDLQ — страница dead letter + total count.
func (a *App) ListDLQ(queue string, db int) (DLQListResult, error) {
	admin, err := a.adminForDB(db)
	if err != nil {
		return DLQListResult{}, err
	}
	ctx, cancel := a.beginList(30 * time.Second)
	defer cancel()
	n, err := admin.CountDLQ(ctx, queue)
	if err != nil {
		return DLQListResult{}, err
	}
	entries, err := admin.ListDLQ(ctx, queue, 0, int64(redisqueue.MaxListDLQ))
	if err != nil {
		return DLQListResult{}, err
	}
	out := make([]DLQRow, 0, len(entries))
	for _, e := range entries {
		out = append(out, DLQRow{
			ID: e.ID, Partition: e.Partition, RejectCount: e.RejectCount,
			DeadLetterAt: e.DeadLetterAt.UnixMilli(),
		})
	}
	return DLQListResult{
		Entries: out, Count: n,
		Truncated: n > int64(len(out)),
	}, nil
}

// RetryDLQ — вернуть ids (пусто = все) из DLQ.
func (a *App) RetryDLQ(queue string, db int, ids []string) (int, error) {
	admin, err := a.adminForDB(db)
	if err != nil {
		return 0, err
	}
	return admin.RetryDLQ(a.ctxOrBackground(), queue, ids...)
}

// PurgeDLQ — удалить ids (пусто = все) из DLQ.
func (a *App) PurgeDLQ(queue string, db int, ids []string) (int, error) {
	admin, err := a.adminForDB(db)
	if err != nil {
		return 0, err
	}
	return admin.PurgeDLQ(a.ctxOrBackground(), queue, ids...)
}

// ListMessages — peek ≤50 (tag / query / SCAN).
func (a *App) ListMessages(queue string, db int, tag, query string) (MessageListResult, error) {
	admin, err := a.adminForDB(db)
	if err != nil {
		return MessageListResult{}, err
	}
	ctx, cancel := a.beginList(30 * time.Second)
	defer cancel()
	items, err := admin.ListMessages(ctx, queue, redisqueue.ListMessagesOptions{
		Tag: tag, Query: query, Limit: redisqueue.MaxListMessages,
	})
	if err != nil {
		return MessageListResult{}, err
	}
	out := make([]MessageRow, 0, len(items))
	for _, m := range items {
		row := MessageRow{
			ID: m.ID, Partition: m.Partition, State: m.State,
			RejectCount: m.RejectCount, Step: m.Step,
		}
		if m.Progress != nil {
			row.Progress = &StepProgressUI{
				Name: m.Progress.Name, Curr: m.Progress.Curr, Total: m.Progress.Total,
				UnknownTotal: m.Progress.UnknownTotal, Cursor: m.Progress.Cursor,
				StartedMs: m.Progress.StartedMs, Control: m.Progress.Control,
			}
		}
		out = append(out, row)
	}
	return MessageListResult{
		Messages: out, Limit: redisqueue.MaxListMessages,
		Truncated: len(out) >= redisqueue.MaxListMessages,
	}, nil
}

// ListRegistryTasks — catalog+run для очереди (все namespace).
func (a *App) ListRegistryTasks(queue string, db int, query string) ([]RegistryTaskRow, error) {
	rdb, err := a.clientForDB(db)
	if err != nil {
		return nil, err
	}
	ctx, cancel := a.beginList(30 * time.Second)
	defer cancel()
	views, err := tasks.ListForQueue(ctx, rdb, queue)
	if err != nil {
		return nil, err
	}
	views = tasks.FilterQuery(views, query)
	out := make([]RegistryTaskRow, 0, len(views))
	for _, v := range views {
		row := RegistryTaskRow{
			Namespace: v.Namespace, Name: v.Name, Queue: v.Queue,
			Cron: v.Cron, Concurrency: v.Concurrency, Run: v.Run,
		}
		for _, arg := range v.Args {
			row.Args = append(row.Args, tasksArgDef{
				Name: arg.Name, Type: arg.Type, Default: arg.Default, Help: arg.Help,
			})
		}
		out = append(out, row)
	}
	return out, nil
}

// EnqueueRegistryTask — Publish Name / ~o{hash} / ~r{rand}.
func (a *App) EnqueueRegistryTask(queue, name, payloadJSON string, bypassLock, byOptions bool, db int) (string, error) {
	rdb, err := a.clientForDB(db)
	if err != nil {
		return "", err
	}
	ctx, cancel := context.WithTimeout(a.ctxOrBackground(), dbScanTimeout)
	defer cancel()
	payload := []byte(payloadJSON)
	if len(payload) == 0 {
		payload = []byte("{}")
	}
	args := map[string]string{}
	_ = json.Unmarshal(payload, &args)
	id, err := tasks.BuildTaskID(name, args, bypassLock, byOptions)
	if err != nil {
		return "", err
	}
	p := redisqueue.NewProducer(rdb, queue)
	if err := p.Publish(ctx, &redisqueue.Task{ID: id, Payload: payload}); err != nil {
		return "", err
	}
	return id, nil
}

// SetTaskStepControl — pause|stop|"" на step.
func (a *App) SetTaskStepControl(queue, taskID, control string, db int) error {
	admin, err := a.adminForDB(db)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(a.ctxOrBackground(), dbScanTimeout)
	defer cancel()
	return admin.SetTaskStepControl(ctx, queue, taskID, control)
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
