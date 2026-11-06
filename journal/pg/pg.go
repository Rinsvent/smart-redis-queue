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

func (s *Source) seqName() string {
	return pgIdent(s.table) + "_offset_seq"
}

func (s *Source) Init(ctx context.Context) error {
	t := pgIdent(s.table)
	seq := s.seqName()
	_, err := s.pool.Exec(ctx, fmt.Sprintf(`
CREATE SEQUENCE IF NOT EXISTS %s;
CREATE TABLE IF NOT EXISTS %s (
    id             BIGSERIAL PRIMARY KEY,
    journal_offset BIGINT UNIQUE,
    state          TEXT NOT NULL DEFAULT '%s',
    task_id        TEXT NOT NULL UNIQUE,
    partition      TEXT NOT NULL DEFAULT '',
    priority       INT NOT NULL DEFAULT 0,
    payload        BYTEA NOT NULL,
    tags           TEXT[] NOT NULL DEFAULT '{}',
    groups         TEXT[] NOT NULL DEFAULT '{}',
    scheduled      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    acked_at       TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS %s_due ON %s (scheduled)
    WHERE state = '%s' AND acked_at IS NULL;
CREATE INDEX IF NOT EXISTS %s_ready ON %s (journal_offset)
    WHERE state = '%s' AND acked_at IS NULL AND journal_offset IS NOT NULL;
`, seq, t, journal.StateReady, t, t, journal.StateScheduled, t, t, journal.StateReady))
	return err
}

func (s *Source) reserveOffsets(ctx context.Context, n int) (start int64, err error) {
	if n < 1 {
		return 0, nil
	}
	// Contiguous nextval in one query.
	rows, err := s.pool.Query(ctx, fmt.Sprintf(
		`SELECT nextval('%s') FROM generate_series(1, $1)`, s.seqName()), n)
	if err != nil {
		return 0, err
	}
	defer rows.Close()
	var first int64
	i := 0
	for rows.Next() {
		var v int64
		if err := rows.Scan(&v); err != nil {
			return 0, err
		}
		if i == 0 {
			first = v
		}
		i++
	}
	return first, rows.Err()
}

func (s *Source) Insert(ctx context.Context, tasks ...*redisqueue.Task) error {
	if len(tasks) == 0 {
		return nil
	}
	now := time.Now()
	type item struct {
		t   *redisqueue.Task
		sch time.Time
		due bool
	}
	items := make([]item, len(tasks))
	dueN := 0
	for i, t := range tasks {
		sch := journal.ScheduledOrNow(t.Scheduled)
		due := journal.IsDue(sch, now)
		items[i] = item{t: t, sch: sch, due: due}
		if due {
			dueN++
		}
	}
	start, err := s.reserveOffsets(ctx, dueN)
	if err != nil {
		return err
	}
	b := &pgx.Batch{}
	q := fmt.Sprintf(`INSERT INTO %s (journal_offset, state, task_id, partition, priority, payload, tags, groups, scheduled)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9) ON CONFLICT (task_id) DO NOTHING`, pgIdent(s.table))
	var dueI int64
	for _, it := range items {
		tags, groups := it.t.Tags, it.t.Groups
		if tags == nil {
			tags = []string{}
		}
		if groups == nil {
			groups = []string{}
		}
		var off any
		state := journal.StateScheduled
		if it.due {
			v := start + dueI
			dueI++
			off = v
			state = journal.StateReady
		}
		b.Queue(q, off, state, it.t.ID, it.t.Partition, it.t.PartitionPriority, it.t.Payload, tags, groups, it.sch)
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
		FROM %s
		WHERE state = $1 AND acked_at IS NULL AND journal_offset IS NOT NULL AND journal_offset > $2
		ORDER BY journal_offset LIMIT $3`, tbl), journal.StateReady, afterOff, n)
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
	return scanOne(row)
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
			ID: id, Partition: part, PartitionPriority: prio, Payload: payload,
			Tags: tags, Groups: groups, Scheduled: sch,
		},
		Cursor: journal.OffsetCursor(off),
	}, nil
}

// PromoteDue — due scheduled → ready (тот же ряд, новый journal_offset).
func (s *Source) PromoteDue(ctx context.Context, limit int) (int, error) {
	if limit < 1 {
		limit = 64
	}
	tbl := pgIdent(s.table)
	dueAt := time.Now().Add(journal.DueSkew)
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback(ctx)

	rows, err := tx.Query(ctx, fmt.Sprintf(`
		SELECT id FROM %s
		WHERE state = $1 AND acked_at IS NULL AND scheduled <= $2
		ORDER BY scheduled
		FOR UPDATE SKIP LOCKED
		LIMIT $3`, tbl), journal.StateScheduled, dueAt, limit)
	if err != nil {
		return 0, err
	}
	var ids []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return 0, err
		}
		ids = append(ids, id)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return 0, err
	}
	if len(ids) == 0 {
		return 0, tx.Commit(ctx)
	}
	start, err := s.reserveOffsets(ctx, len(ids))
	if err != nil {
		return 0, err
	}
	// reserveOffsets uses pool, not tx — ok for sequence; updates in tx
	for i, id := range ids {
		off := start + int64(i)
		ct, err := tx.Exec(ctx, fmt.Sprintf(`
			UPDATE %s SET state = $1, journal_offset = $2
			WHERE id = $3 AND state = $4`, tbl),
			journal.StateReady, off, id, journal.StateScheduled)
		if err != nil {
			return 0, err
		}
		_ = ct
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, err
	}
	return len(ids), nil
}

func (s *Source) Ack(ctx context.Context, taskID string, _ map[string]string) error {
	_, err := s.pool.Exec(ctx, fmt.Sprintf(
		`UPDATE %s SET acked_at = NOW() WHERE task_id = $1 AND acked_at IS NULL`, pgIdent(s.table)), taskID)
	return err
}

func (s *Source) ReadAcks(context.Context, journal.Cursor, int) ([]journal.AckEvent, error) {
	return nil, nil
}

var _ journal.DuePromoter = (*Source)(nil)
