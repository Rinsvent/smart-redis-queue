package journal

import (
	"context"
	"fmt"
	"sync"
	"time"

	redisqueue "github.com/Rinsvent/smart-redis-queue"
)

// MemSource — in-memory журнал с монотонным offset (как PG BIGSERIAL).
type MemSource struct {
	mu    sync.Mutex
	next  int64
	byOff map[int64]memRow
	order []int64
}

type memRow struct {
	rec  Record
	done bool
}

func NewMemSource() *MemSource {
	return &MemSource{byOff: make(map[int64]memRow)}
}

func (m *MemSource) Init(context.Context) error { return nil }

func (m *MemSource) Insert(tasks ...*redisqueue.Task) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, t := range tasks {
		cp := *t
		if cp.Scheduled.IsZero() {
			cp.Scheduled = time.Now() // только отсутствие даты; прошлые значения не трогаем
		}
		m.next++
		off := m.next
		m.order = append(m.order, off)
		m.byOff[off] = memRow{rec: Record{Task: &cp, Cursor: OffsetCursor(off)}}
	}
}

func (m *MemSource) ReadBatch(_ context.Context, after Cursor, n int) ([]Record, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []Record
	afterOff := after.Offset
	if afterOff < 0 {
		afterOff = 0
	}
	for _, off := range m.order {
		if off <= afterOff {
			continue
		}
		row := m.byOff[off]
		if row.done {
			continue
		}
		out = append(out, row.rec)
		if len(out) >= n {
			break
		}
	}
	return out, nil
}

func (m *MemSource) ReadByOffset(_ context.Context, c Cursor) (Record, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	row, ok := m.byOff[c.Offset]
	if !ok {
		return Record{}, fmt.Errorf("journal: offset %d not found", c.Offset)
	}
	return row.rec, nil
}

func (m *MemSource) Ack(_ context.Context, taskID string, _ map[string]string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for off, row := range m.byOff {
		if row.rec.Task.ID == taskID {
			row.done = true
			m.byOff[off] = row
			return nil
		}
	}
	return nil
}

func (m *MemSource) ReadAcks(context.Context, Cursor, int) ([]AckEvent, error) {
	return nil, nil
}

func (m *MemSource) Pending() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	n := 0
	for _, r := range m.byOff {
		if !r.done {
			n++
		}
	}
	return n
}
