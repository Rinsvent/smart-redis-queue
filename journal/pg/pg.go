package pg

import (
	"context"
	"fmt"
	"strings"
	"time"

	redisqueue "github.com/Rinsvent/smart-redis-queue"
	"github.com/Rinsvent/smart-redis-queue/journal"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Config struct {
	DSN   string
	Table string // default srq_journal_tasks
}

type Source struct {
	pool  *pgxpool.Pool
	table string
}

func New(ctx context.Context, cfg Config) (*Source, error) {
	if cfg.Table == "" {
		cfg.Table = "srq_journal_tasks"
	}
	pool, err := pgxpool.New(ctx, cfg.DSN)
	if err != nil {
		return nil, err
	}
	return &Source{pool: pool, table: cfg.Table}, nil
}

func (s *Source) Close() { s.pool.Close() }

func pgIdent(name string) string {
	if name == "" || strings.ContainsAny(name, `" ;`) {
		return "srq_journal_tasks"
	}
	return name
}

func (s *Source) Init(ctx context.Context) error {
	t := pgIdent(s.table)
	_, err := s.pool.Exec(ctx, fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS %s (
    journal_offset BIGSERIAL PRIMARY KEY,
    task_id    TEXT NOT NULL UNIQUE,
    partition  TEXT NOT NULL DEFAULT '',
    priority   INT NOT NULL DEFAULT 0,
    payload    BYTEA NOT NULL,
    tags       TEXT[] NOT NULL DEFAULT '{}',
    groups     TEXT[] NOT NULL DEFAULT '{}',
    scheduled  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    acked_at   TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS %s_due ON %s (scheduled) WHERE acked_at IS NULL;
`, t, t, t))
	return err
}

func clampScheduled(sch time.Time) time.Time {
	now := time.Now()
	if sch.IsZero() || sch.Before(now) {
		return now
	}
	return sch
}

func (s *Source) Insert(ctx context.Context, tasks ...*redisqueue.Task) error {
	if len(tasks) == 0 {
		return nil
	}
	b := &pgx.Batch{}
	q := fmt.Sprintf(`INSERT INTO %s (task_id, partition, priority, payload, tags, groups, scheduled)
		VALUES ($1,$2,$3,$4,$5,$6,$7) ON CONFLICT (task_id) DO NOTHING`, pgIdent(s.table))
	for _, t := range tasks {
		tags, groups := t.Tags, t.Groups
		if tags == nil {
			tags = []string{}
		}
		if groups == nil {
			groups = []string{}
		}
		b.Queue(q, t.ID, t.Partition, t.Priority, t.Payload, tags, groups, clampScheduled(t.Scheduled))
	}
	return s.pool.SendBatch(ctx, b).Close()
}

func (s *Source) ReadBatch(ctx context.Context, after journal.Cursor, n int) ([]journal.Record, error) {
	if n < 1 {
		n = 32
	}
	tbl := pgIdent(s.table)
	afterOff := after.Offset
	if afterOff < 0 {
		afterOff = 0
	}
	rows, err := s.pool.Query(ctx, fmt.Sprintf(`
		SELECT journal_offset, task_id, partition, priority, payload, tags, groups, scheduled
		FROM %s WHERE acked_at IS NULL AND journal_offset > $1
		ORDER BY journal_offset LIMIT $2`, tbl), afterOff, n)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanRows(rows)
}

func (s *Source) ReadByOffset(ctx context.Context, c journal.Cursor) (journal.Record, error) {
	tbl := pgIdent(s.table)
	row := s.pool.QueryRow(ctx, fmt.Sprintf(`
		SELECT journal_offset, task_id, partition, priority, payload, tags, groups, scheduled
		FROM %s WHERE journal_offset = $1`, tbl), c.Offset)
	recs, err := scanOne(row)
	return recs, err
}

func scanRows(rows pgx.Rows) ([]journal.Record, error) {
	var out []journal.Record
	for rows.Next() {
		rec, err := scanRow(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, rec)
	}
	return out, rows.Err()
}

type scannable interface {
	Scan(dest ...any) error
}

func scanOne(row scannable) (journal.Record, error) {
	return scanRow(row)
}

func scanRow(row scannable) (journal.Record, error) {
	var off int64
	var id, part string
	var prio int
	var payload []byte
	var tags, groups []string
	var sch time.Time
	if err := row.Scan(&off, &id, &part, &prio, &payload, &tags, &groups, &sch); err != nil {
		return journal.Record{}, err
	}
	return journal.Record{
		Task: &redisqueue.Task{
			ID: id, Partition: part, Priority: prio, Payload: payload,
			Tags: tags, Groups: groups, Scheduled: sch,
		},
		Cursor: journal.OffsetCursor(off),
	}, nil
}

func (s *Source) Ack(ctx context.Context, taskID string, _ map[string]string) error {
	_, err := s.pool.Exec(ctx, fmt.Sprintf(
		`UPDATE %s SET acked_at = NOW() WHERE task_id = $1 AND acked_at IS NULL`, pgIdent(s.table)), taskID)
	return err
}

func (s *Source) ReadAcks(context.Context, journal.Cursor, int) ([]journal.AckEvent, error) {
	return nil, nil
}
