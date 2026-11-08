package gateway

import (
	"sync"
	"time"
)

// lossEntry — publish, который Redis мог ещё не зафиксировать на диске.
type lossEntry struct {
	Queue       string     `json:"queue"`
	Rec         PublishRec `json:"rec"`
	RetainUntil time.Time  `json:"retain_until"`
}

type lossWindow struct {
	mu     sync.Mutex
	window time.Duration
	byID   map[string]*lossEntry
}

func newLossWindow(window time.Duration) *lossWindow {
	return &lossWindow{
		window: window,
		byID:   make(map[string]*lossEntry),
	}
}

func (w *lossWindow) setWindow(d time.Duration) {
	w.mu.Lock()
	w.window = d
	w.mu.Unlock()
}

func (w *lossWindow) onPublishOK(queue string, rec PublishRec, at time.Time) {
	w.mu.Lock()
	defer w.mu.Unlock()
	until := at.Add(w.window)
	if w.window == 0 {
		until = at
	}
	w.byID[rec.ID] = &lossEntry{Queue: queue, Rec: rec, RetainUntil: until}
}

// onAckOK — задача обработана: из loss-window убираем (не воскрешать).
func (w *lossWindow) onAckOK(taskID string) {
	w.mu.Lock()
	delete(w.byID, taskID)
	w.mu.Unlock()
}

// onRejectOK — no-op для retention publish: окно от publishOK; после истечения
// Redis уже на диске, повторно грузить publish не нужно. Reject живёт в op-log
// до своего окна truncation (replay Reject, если Redis потерял только Reject).
func (w *lossWindow) onRejectOK(taskID string, at time.Time) {
	_ = taskID
	_ = at
}

func (w *lossWindow) truncate(now time.Time) {
	w.mu.Lock()
	defer w.mu.Unlock()
	for id, e := range w.byID {
		if !e.RetainUntil.After(now) {
			delete(w.byID, id)
		}
	}
}

func (w *lossWindow) snapshot() []lossEntry {
	w.mu.Lock()
	defer w.mu.Unlock()
	out := make([]lossEntry, 0, len(w.byID))
	for _, e := range w.byID {
		out = append(out, *e)
	}
	return out
}

func (w *lossWindow) load(entries []lossEntry) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.byID = make(map[string]*lossEntry, len(entries))
	for i := range entries {
		e := entries[i]
		cp := e
		w.byID[e.Rec.ID] = &cp
	}
}

func (w *lossWindow) len() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return len(w.byID)
}
