package gateway

import (
	"sync"
	"time"
)

// OpKind — mutating data-plane op.
type OpKind string

const (
	OpPublish OpKind = "publish"
	OpAck     OpKind = "ack"
	OpReject  OpKind = "reject"
)

// Op — запись единого хронологического лога.
type Op struct {
	Offset   uint64      `json:"offset"`
	Kind     OpKind      `json:"kind"`
	At       time.Time   `json:"at"`
	Queue    string      `json:"queue"`
	TaskID   string      `json:"task_id,omitempty"`
	Consumer string      `json:"consumer,omitempty"`
	RedisOK  bool        `json:"redis_ok"`
	Publish  *PublishRec `json:"publish,omitempty"`
	Ack      *AckRec     `json:"ack,omitempty"`
	Reject   *RejectRec  `json:"reject,omitempty"`
}

// PublishRec — снимок задачи для loss-window / replay.
type PublishRec struct {
	ID                string    `json:"id"`
	Partition         string    `json:"partition"`
	PartitionPriority int       `json:"partition_priority"`
	QueuePriority     int       `json:"queue_priority"`
	Payload           []byte    `json:"payload"`
	Tags              []string  `json:"tags,omitempty"`
	Groups            []string  `json:"groups,omitempty"`
	Scheduled         time.Time `json:"scheduled,omitempty"`
}

// AckRec — опции Ack.
type AckRec struct {
	IdempotencyTTLSec float64           `json:"idempotency_ttl_sec,omitempty"`
	Shared            map[string][]byte `json:"shared,omitempty"`
	SharedTTLSec      float64           `json:"shared_ttl_sec,omitempty"`
	OpenBlocks        []string          `json:"open_blocks,omitempty"`
}

// RejectRec — опции Reject.
type RejectRec struct {
	Delay         float64  `json:"delay,omitempty"`
	BlockGroups   []string `json:"block_groups,omitempty"`
	BlockTags     []string `json:"block_tags,omitempty"`
	DeadLetter    bool     `json:"dead_letter,omitempty"`
	DeadLetterMax int      `json:"dead_letter_max,omitempty"` // -1 = consumer default; 0 = unlimited
}

type opLog struct {
	mu      sync.Mutex
	next    uint64
	entries []Op
}

func newOpLog() *opLog {
	return &opLog{next: 1, entries: make([]Op, 0, 256)}
}

func (l *opLog) append(op Op) Op {
	l.mu.Lock()
	defer l.mu.Unlock()
	if op.At.IsZero() {
		op.At = time.Now()
	}
	op.Offset = l.next
	l.next++
	l.entries = append(l.entries, op)
	return op
}

func (l *opLog) snapshot() []Op {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := make([]Op, len(l.entries))
	copy(out, l.entries)
	return out
}

func (l *opLog) load(entries []Op, next uint64) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.entries = append([]Op(nil), entries...)
	if next == 0 {
		next = 1
		for _, e := range entries {
			if e.Offset >= next {
				next = e.Offset + 1
			}
		}
	}
	l.next = next
}

func (l *opLog) truncateBefore(offset uint64) {
	l.mu.Lock()
	defer l.mu.Unlock()
	i := 0
	for i < len(l.entries) && l.entries[i].Offset < offset {
		i++
	}
	if i == 0 {
		return
	}
	l.entries = append([]Op(nil), l.entries[i:]...)
}

// markRedisOK — после успешного replay: op больше не повторяется.
func (l *opLog) markRedisOK(offset uint64) {
	l.mu.Lock()
	defer l.mu.Unlock()
	for i := range l.entries {
		if l.entries[i].Offset == offset {
			l.entries[i].RedisOK = true
			return
		}
	}
}

// truncateOlderThan удаляет RedisOK-записи старше cutoff.
// Pending (RedisOK=false) всегда сохраняются до успешного replay.
func (l *opLog) truncateOlderThan(cutoff time.Time) {
	l.mu.Lock()
	defer l.mu.Unlock()
	kept := make([]Op, 0, len(l.entries))
	for _, e := range l.entries {
		if !e.RedisOK || e.At.After(cutoff) {
			kept = append(kept, e)
		}
	}
	l.entries = kept
}

func (l *opLog) len() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.entries)
}
