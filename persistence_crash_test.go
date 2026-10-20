package redisqueue

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Persistence crash / durability.
// Запуск (нужен docker):
//
//	SRQ_PERSIST_DOCKER=1 go test -run='TestPersist_' -count=1 -timeout=15m -v .
//
// Бенчи throughput (контейнеры поднимает TestPersist_ThroughputBenches через тот же helper,
// либо вручную scripts/redis-persist/run.sh bench):
//
//	SRQ_PERSIST_DOCKER=1 go test -run='^$' -bench='BenchmarkPersistPublish' -benchtime=2s -count=3 -timeout=20m .

const persistDockerEnv = "SRQ_PERSIST_DOCKER"

type persistProfile struct {
	name   string
	conf   string // filename under scripts/redis-persist/
	port   int
	expect struct {
		publishSurvivesKill bool // Publish OK → kill -9 → restart → payload есть
		ackStaysGone        bool // Ack OK → kill -9 → restart → задачи нет (не redeliver)
	}
}

func persistProfiles() []persistProfile {
	return []persistProfile{
		{
			name: "no-persist",
			conf: "no-persist.conf",
			port: 6391,
			expect: struct {
				publishSurvivesKill bool
				ackStaysGone        bool
			}{false, false}, // после kill всё пусто — «ack gone» тоже true по сути, но сценарий бессмысленен
		},
		{
			name: "rdb-default",
			conf: "rdb-default.conf",
			port: 6392,
			expect: struct {
				publishSurvivesKill bool
				ackStaysGone        bool
			}{false, false},
		},
		{
			name: "aof-everysec",
			conf: "aof-everysec.conf",
			port: 6393,
			expect: struct {
				publishSurvivesKill bool
				ackStaysGone        bool
			}{false, false}, // окно ~1s: сразу после Publish kill часто теряет
		},
		{
			name: "aof-always",
			conf: "aof-always.conf",
			port: 6394,
			expect: struct {
				publishSurvivesKill bool
				ackStaysGone        bool
			}{true, true},
		},
	}
}

func requirePersistDocker(t testing.TB) {
	t.Helper()
	if os.Getenv(persistDockerEnv) != "1" {
		t.Skipf("set %s=1 (docker required)", persistDockerEnv)
	}
	if _, err := exec.LookPath("docker"); err != nil {
		t.Skip("docker not in PATH")
	}
}

func persistConfDir(t testing.TB) string {
	t.Helper()
	// test cwd = module root when go test .
	dir := filepath.Join("scripts", "redis-persist")
	if _, err := os.Stat(dir); err != nil {
		t.Fatalf("persist configs not found at %s: %v", dir, err)
	}
	abs, err := filepath.Abs(dir)
	require.NoError(t, err)
	return abs
}

func persistDataDir(t testing.TB, profile string) string {
	t.Helper()
	// Уникальный каталог: Redis в docker пишет файлы от root — RemoveAll с хоста часто EPERM.
	dir := filepath.Join(os.TempDir(), "srq-persist", fmt.Sprintf("%s-%d", profile, time.Now().UnixNano()))
	require.NoError(t, os.MkdirAll(dir, 0o755))
	t.Cleanup(func() {
		_ = exec.Command("docker", "run", "--rm", "-v", dir+":/data", "redis:6.2.6",
			"sh", "-c", "rm -rf /data/*").Run()
		_ = os.RemoveAll(dir)
	})
	return dir
}

func dockerName(profile string) string {
	return "srq_persist_" + profile
}

func startPersistRedis(t testing.TB, p persistProfile, dataDir string) {
	t.Helper()
	confDir := persistConfDir(t)
	_ = exec.Command("docker", "rm", "-f", dockerName(p.name)).Run()

	args := []string{
		"run", "-d",
		"--name", dockerName(p.name),
		"-p", fmt.Sprintf("%d:6379", p.port),
		"-v", fmt.Sprintf("%s:/data", dataDir),
		"-v", fmt.Sprintf("%s/%s:/usr/local/etc/redis/redis.conf:ro", confDir, p.conf),
		"redis:6.2.6",
		"redis-server", "/usr/local/etc/redis/redis.conf",
	}
	out, err := exec.Command("docker", args...).CombinedOutput()
	require.NoError(t, err, "docker run: %s", out)

	rdb := persistClient(p.port)
	defer rdb.Close()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		if rdb.Ping(context.Background()).Err() == nil {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("redis %s on :%d did not become ready", p.name, p.port)
}

func killPersistRedis9(t testing.TB, p persistProfile) {
	t.Helper()
	out, err := exec.Command("docker", "kill", "-s", "KILL", dockerName(p.name)).CombinedOutput()
	require.NoError(t, err, "docker kill: %s", out)
	// контейнер мёртв, но volume на хосте жив
	_ = exec.Command("docker", "rm", "-f", dockerName(p.name)).Run()
}

func restartPersistRedis(t testing.TB, p persistProfile, dataDir string) {
	t.Helper()
	startPersistRedis(t, p, dataDir)
}

func stopPersistRedis(t testing.TB, p persistProfile) {
	t.Helper()
	_ = exec.Command("docker", "rm", "-f", dockerName(p.name)).Run()
}

func persistClient(port int) *redis.Client {
	return redis.NewClient(&redis.Options{Addr: fmt.Sprintf("127.0.0.1:%d", port), DB: 0})
}

func payloadExists(ctx context.Context, rdb *redis.Client, queue, id string) bool {
	n, err := rdb.Exists(ctx, fmt.Sprintf("queue:%s:payload:%s", queue, id)).Result()
	return err == nil && n == 1
}

// TestPersist_PublishThenKill9 — Publish вернул OK → kill -9 → restart → есть ли задача.
func TestPersist_PublishThenKill9(t *testing.T) {
	requirePersistDocker(t)
	ctx := context.Background()

	for _, p := range persistProfiles() {
		p := p
		t.Run(p.name, func(t *testing.T) {
			data := persistDataDir(t, p.name+"-pub")
			startPersistRedis(t, p, data)
			defer stopPersistRedis(t, p)

			rdb := persistClient(p.port)
			defer rdb.Close()
			require.NoError(t, rdb.FlushAll(ctx).Err())

			queue := "persist-pub"
			producer := NewProducer(rdb, queue)
			id := "msg-after-publish"
			require.NoError(t, producer.Publish(ctx, &Task{
				ID: id, Payload: []byte("durable?"), Scheduled: time.Now(),
			}))
			require.True(t, payloadExists(ctx, rdb, queue, id), "pre-kill payload missing")

			killPersistRedis9(t, p)
			restartPersistRedis(t, p, data)

			rdb2 := persistClient(p.port)
			defer rdb2.Close()
			alive := payloadExists(ctx, rdb2, queue, id)
			t.Logf("%s: publish survives kill-9 = %v (expect %v)", p.name, alive, p.expect.publishSurvivesKill)
			if p.name == "aof-everysec" {
				// окно ~1s — сразу после Publish исход плавает; см. TestPersist_EverysecWindow
				return
			}
			assert.Equal(t, p.expect.publishSurvivesKill, alive)
		})
	}
}

// TestPersist_AckThenKill9 — Get+Ack OK → kill -9 → restart → задача не должна вернуться (при durable Ack).
func TestPersist_AckThenKill9(t *testing.T) {
	requirePersistDocker(t)
	ctx := context.Background()

	for _, p := range persistProfiles() {
		p := p
		t.Run(p.name, func(t *testing.T) {
			if p.name == "no-persist" || p.name == "rdb-default" {
				t.Skip("без durable store сценарий «ack остался» не отличим от полной потери БД")
			}
			data := persistDataDir(t, p.name+"-ack")
			startPersistRedis(t, p, data)
			defer stopPersistRedis(t, p)

			rdb := persistClient(p.port)
			defer rdb.Close()
			require.NoError(t, rdb.FlushAll(ctx).Err())

			queue := "persist-ack"
			producer := NewProducer(rdb, queue)
			consumer := NewConsumer(rdb, queue, "c-persist")
			defer consumer.Close()

			id := "msg-acked"
			require.NoError(t, producer.Publish(ctx, &Task{
				ID: id, Payload: []byte("ack-me"), Scheduled: time.Now().Add(-time.Second),
			}))

			var got *Task
			deadline := time.Now().Add(5 * time.Second)
			for time.Now().Before(deadline) {
				tasks, err := consumer.Get(ctx)
				require.NoError(t, err)
				if len(tasks) > 0 && tasks[0] != nil {
					got = tasks[0]
					break
				}
				time.Sleep(20 * time.Millisecond)
			}
			require.NotNil(t, got)
			require.Equal(t, id, got.ID)
			require.NoError(t, consumer.Ack(ctx, id, 0))
			require.False(t, payloadExists(ctx, rdb, queue, id), "payload should be gone after Ack")

			killPersistRedis9(t, p)
			restartPersistRedis(t, p, data)

			rdb2 := persistClient(p.port)
			defer rdb2.Close()
			consumer2 := NewConsumer(rdb2, queue, "c-persist-2")
			defer consumer2.Close()

			// ждём чуть-чуть: если Ack потерян, задача снова Get-ится
			redelivered := false
			deadline = time.Now().Add(2 * time.Second)
			for time.Now().Before(deadline) {
				tasks, _ := consumer2.Get(ctx)
				for _, task := range tasks {
					if task != nil && task.ID == id {
						redelivered = true
					}
				}
				if redelivered || payloadExists(ctx, rdb2, queue, id) {
					redelivered = true
					break
				}
				time.Sleep(50 * time.Millisecond)
			}
			survivesGone := !redelivered
			t.Logf("%s: ack stays gone after kill-9 = %v (expect %v)", p.name, survivesGone, p.expect.ackStaysGone)

			if p.name == "aof-everysec" {
				// flaky by design — логируем, не валим suite жёстко
				t.Logf("aof-everysec redelivered=%v (may lose last 1s of writes)", redelivered)
				return
			}
			assert.Equal(t, p.expect.ackStaysGone, survivesGone)
		})
	}
}

// TestPersist_GetNoAckThenKill9 — взяли задачу, не Ack → kill → после рестарта reclaim/повторная выдача.
func TestPersist_GetNoAckThenKill9(t *testing.T) {
	requirePersistDocker(t)
	ctx := context.Background()

	p := persistProfiles()[3] // aof-always
	data := persistDataDir(t, p.name+"-inflight")
	startPersistRedis(t, p, data)
	defer stopPersistRedis(t, p)

	rdb := persistClient(p.port)
	defer rdb.Close()
	require.NoError(t, rdb.FlushAll(ctx).Err())

	queue := "persist-inflight"
	producer := NewProducer(rdb, queue)
	consumer := NewConsumer(rdb, queue, "c-inflight")
	// короткий poll; lock TTL в SRQ ~120s — для теста форсим через kill+новый consumer после ожидания?
	// Проще: проверить что payload жив после kill (at-least-once: сообщение не потеряно).
	defer consumer.Close()

	id := "msg-inflight"
	require.NoError(t, producer.Publish(ctx, &Task{
		ID: id, Payload: []byte("in-flight"), Scheduled: time.Now().Add(-time.Second),
	}))
	var got *Task
	for i := 0; i < 50; i++ {
		tasks, _ := consumer.Get(ctx)
		if len(tasks) > 0 && tasks[0] != nil {
			got = tasks[0]
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	require.NotNil(t, got)

	killPersistRedis9(t, p)
	restartPersistRedis(t, p, data)

	rdb2 := persistClient(p.port)
	defer rdb2.Close()
	require.True(t, payloadExists(ctx, rdb2, queue, id), "in-flight payload must survive with aof-always")

	// Новый consumer: пока жив lock старого — Get может не отдать; payload при этом на месте.
	// Это at-least-once: не потеря, возможен повтор после expiry lock.
	t.Log("inflight: payload survived; redelivery after lock TTL (protocol at-least-once)")
}

// TestPersist_EverysecWindow — быстрый Publish+kill на everysec: документируем окно потери.
func TestPersist_EverysecWindow(t *testing.T) {
	requirePersistDocker(t)
	ctx := context.Background()
	p := persistProfiles()[2] // aof-everysec

	const rounds = 5
	lost := 0
	for i := 0; i < rounds; i++ {
		data := persistDataDir(t, fmt.Sprintf("%s-win-%d", p.name, i))
		startPersistRedis(t, p, data)

		rdb := persistClient(p.port)
		require.NoError(t, rdb.FlushAll(ctx).Err())
		producer := NewProducer(rdb, "win")
		id := fmt.Sprintf("w-%d", i)
		require.NoError(t, producer.Publish(ctx, &Task{
			ID: id, Payload: []byte("x"), Scheduled: time.Now(),
		}))
		_ = rdb.Close()
		killPersistRedis9(t, p)
		restartPersistRedis(t, p, data)

		rdb2 := persistClient(p.port)
		if !payloadExists(ctx, rdb2, "win", id) {
			lost++
		}
		_ = rdb2.Close()
		stopPersistRedis(t, p)
	}
	t.Logf("aof-everysec: lost %d/%d publishes on immediate process kill-9", lost, rounds)
	t.Log("note: process kill ≠ power loss — host page cache may keep unfsynced AOF bytes;")
	t.Log("      on machine crash / power loss Redis docs: everysec may lose ~1s of writes")
}

func BenchmarkPersistPublish(b *testing.B) {
	requirePersistDocker(b)
	ctx := context.Background()

	for _, p := range persistProfiles() {
		p := p
		b.Run(p.name, func(b *testing.B) {
			data := persistDataDir(b, p.name+"-bench")
			startPersistRedis(b, p, data)
			b.Cleanup(func() { stopPersistRedis(b, p) })

			rdb := persistClient(p.port)
			b.Cleanup(func() { _ = rdb.Close() })
			require.NoError(b, rdb.FlushAll(ctx).Err())
			producer := NewProducer(rdb, "persist-bench")

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_ = producer.Publish(ctx, &Task{
					ID:        fmt.Sprintf("b-%d", i),
					Payload:   []byte("bench"),
					Scheduled: time.Now(),
				})
			}
			b.StopTimer()
		})
	}
}
