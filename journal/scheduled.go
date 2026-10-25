package journal

import (
	"context"
	"time"
)

// DueSkew — окно «уже можно в ready-stream» (scheduled <= now+DueSkew).
const DueSkew = 10 * time.Second

const (
	StateReady     = "ready"
	StateScheduled = "scheduled"
)

// ScheduledOrNow — zero → now; иначе исходное значение (в том числе в прошлом).
func ScheduledOrNow(t time.Time) time.Time {
	if t.IsZero() {
		return time.Now()
	}
	return t
}

// IsDue — пора ли ставить задачу в ready-stream (курсорный поток pump).
func IsDue(scheduled, now time.Time) bool {
	return !ScheduledOrNow(scheduled).After(now.Add(DueSkew))
}

// DuePromoter — pg/mongo: due scheduled → ready (выдать journal_offset в той же таблице).
// Kafka так не умеет — там отдельный топик + mover.
type DuePromoter interface {
	PromoteDue(ctx context.Context, limit int) (int, error)
}
