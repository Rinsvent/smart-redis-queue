package gateway

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"time"

	redisqueue "github.com/Rinsvent/smart-redis-queue"
)

// JSON data-plane — дубль gRPC для PHP/curl и клиентов без gRPC.
//
//	POST /v1/publish  POST /v1/get  POST /v1/ack  POST /v1/reject
//
// Payload: строка; encoding "utf8" (default) или "base64".

type httpTask struct {
	ID                string   `json:"id"`
	Partition         string   `json:"partition,omitempty"`
	PartitionPriority int      `json:"partition_priority,omitempty"`
	QueuePriority     int      `json:"queue_priority,omitempty"`
	Payload           string   `json:"payload"`
	PayloadEncoding   string   `json:"payload_encoding,omitempty"` // utf8 | base64
	Tags              []string `json:"tags,omitempty"`
	Groups            []string `json:"groups,omitempty"`
	ScheduledUnixMs   int64    `json:"scheduled_unix_ms,omitempty"`
	RejectCount       int      `json:"reject_count,omitempty"`
}

type publishHTTPRequest struct {
	Queue            string           `json:"queue"`
	Tasks            []httpTask       `json:"tasks"`
	TagLimits        map[string]int64 `json:"tag_limits,omitempty"`
	ShardKey         string           `json:"shard_key,omitempty"`
	ShardName        string           `json:"shard_name,omitempty"`
	Compression      string           `json:"compression,omitempty"` // none | zstd
	CompressMinBytes int              `json:"compress_min_bytes,omitempty"`
}

type getHTTPRequest struct {
	Queue      string `json:"queue"`
	ConsumerID string `json:"consumer_id"`
	Prefetch   int    `json:"prefetch"`
}

type getHTTPResponse struct {
	Tasks []httpTask `json:"tasks"`
}

type ackHTTPRequest struct {
	Queue             string            `json:"queue"`
	ConsumerID        string            `json:"consumer_id"`
	TaskID            string            `json:"task_id"`
	IdempotencyTTLSec float64           `json:"idempotency_ttl_sec,omitempty"`
	Shared            map[string]string `json:"shared,omitempty"` // values base64
	SharedTTLSec      float64           `json:"shared_ttl_sec,omitempty"`
	OpenBlocks        []string          `json:"open_blocks,omitempty"`
}

type rejectHTTPRequest struct {
	Queue         string   `json:"queue"`
	ConsumerID    string   `json:"consumer_id"`
	TaskID        string   `json:"task_id"`
	Delay         float64  `json:"delay,omitempty"`
	BlockGroups   []string `json:"block_groups,omitempty"`
	BlockTags     []string `json:"block_tags,omitempty"`
	DeadLetter    bool     `json:"dead_letter,omitempty"`
	DeadLetterMax *int     `json:"dead_letter_max,omitempty"` // nil → consumer default (-1); 0 → unlimited
}

func (h *httpOps) registerDataPlane(mux *http.ServeMux) {
	mux.HandleFunc("/v1/publish", h.handlePublish)
	mux.HandleFunc("/v1/get", h.handleGet)
	mux.HandleFunc("/v1/ack", h.handleAck)
	mux.HandleFunc("/v1/reject", h.handleReject)
}

func (h *httpOps) handlePublish(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeHTTPError(w, http.StatusMethodNotAllowed, errors.New("method not allowed"))
		return
	}
	var req publishHTTPRequest
	if err := decodeJSON(r, &req); err != nil {
		writeHTTPError(w, http.StatusBadRequest, err)
		return
	}
	tasks := make([]*redisqueue.Task, 0, len(req.Tasks))
	for _, t := range req.Tasks {
		task, err := httpTaskToQueue(t)
		if err != nil {
			writeHTTPError(w, http.StatusBadRequest, err)
			return
		}
		tasks = append(tasks, task)
	}
	po := PublishOpts{
		TagLimits:        req.TagLimits,
		ShardKey:         req.ShardKey,
		ShardName:        req.ShardName,
		Compression:      req.Compression,
		CompressMinBytes: req.CompressMinBytes,
	}
	if err := h.g.PublishTasksWith(r.Context(), req.Queue, tasks, po); err != nil {
		writeGatewayErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

func (h *httpOps) handleGet(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeHTTPError(w, http.StatusMethodNotAllowed, errors.New("method not allowed"))
		return
	}
	var req getHTTPRequest
	if err := decodeJSON(r, &req); err != nil {
		writeHTTPError(w, http.StatusBadRequest, err)
		return
	}
	tasks, err := h.g.GetTasks(r.Context(), req.Queue, req.ConsumerID, req.Prefetch)
	if err != nil {
		writeGatewayErr(w, err)
		return
	}
	out := make([]httpTask, 0, len(tasks))
	for _, t := range tasks {
		out = append(out, queueTaskToHTTP(t))
	}
	writeJSON(w, http.StatusOK, getHTTPResponse{Tasks: out})
}

func (h *httpOps) handleAck(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeHTTPError(w, http.StatusMethodNotAllowed, errors.New("method not allowed"))
		return
	}
	var req ackHTTPRequest
	if err := decodeJSON(r, &req); err != nil {
		writeHTTPError(w, http.StatusBadRequest, err)
		return
	}
	ack := &AckRec{
		IdempotencyTTLSec: req.IdempotencyTTLSec,
		SharedTTLSec:      req.SharedTTLSec,
		OpenBlocks:        req.OpenBlocks,
	}
	if len(req.Shared) > 0 {
		ack.Shared = make(map[string][]byte, len(req.Shared))
		for k, v := range req.Shared {
			b, err := base64.StdEncoding.DecodeString(v)
			if err != nil {
				writeHTTPError(w, http.StatusBadRequest, err)
				return
			}
			ack.Shared[k] = b
		}
	}
	if err := h.g.AckTask(r.Context(), req.Queue, req.ConsumerID, req.TaskID, ack); err != nil {
		writeGatewayErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

func (h *httpOps) handleReject(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeHTTPError(w, http.StatusMethodNotAllowed, errors.New("method not allowed"))
		return
	}
	var req rejectHTTPRequest
	if err := decodeJSON(r, &req); err != nil {
		writeHTTPError(w, http.StatusBadRequest, err)
		return
	}
	dlqMax := -1
	if req.DeadLetter {
		if req.DeadLetterMax != nil {
			dlqMax = *req.DeadLetterMax
		}
	}
	if err := h.g.RejectTask(r.Context(), req.Queue, req.ConsumerID, req.TaskID, &RejectRec{
		Delay:         req.Delay,
		BlockGroups:   req.BlockGroups,
		BlockTags:     req.BlockTags,
		DeadLetter:    req.DeadLetter,
		DeadLetterMax: dlqMax,
	}); err != nil {
		writeGatewayErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

func decodeJSON(r *http.Request, dst any) error {
	defer r.Body.Close()
	dec := json.NewDecoder(io.LimitReader(r.Body, 32<<20))
	return dec.Decode(dst)
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func writeHTTPError(w http.ResponseWriter, code int, err error) {
	writeJSON(w, code, map[string]any{"ok": false, "error": err.Error()})
}

func writeGatewayErr(w http.ResponseWriter, err error) {
	code := http.StatusInternalServerError
	var bp *redisqueue.ErrBackpressure
	switch {
	case errors.Is(err, ErrRedisDown):
		code = http.StatusServiceUnavailable
	case errors.Is(err, ErrRecovering), errors.Is(err, ErrDraining):
		code = http.StatusConflict
	case errors.Is(err, ErrEmptyQueue), errors.Is(err, ErrEmptyTaskID):
		code = http.StatusBadRequest
	case errors.As(err, &bp):
		code = http.StatusTooManyRequests
	}
	writeHTTPError(w, code, err)
}

func httpTaskToQueue(t httpTask) (*redisqueue.Task, error) {
	var payload []byte
	enc := t.PayloadEncoding
	if enc == "" {
		enc = "utf8"
	}
	switch enc {
	case "utf8", "raw":
		payload = []byte(t.Payload)
	case "base64":
		b, err := base64.StdEncoding.DecodeString(t.Payload)
		if err != nil {
			return nil, err
		}
		payload = b
	default:
		return nil, errors.New("payload_encoding must be utf8 or base64")
	}
	var sched time.Time
	if t.ScheduledUnixMs > 0 {
		sched = time.UnixMilli(t.ScheduledUnixMs)
	}
	return &redisqueue.Task{
		ID:                t.ID,
		Partition:         t.Partition,
		PartitionPriority: t.PartitionPriority,
		QueuePriority:     t.QueuePriority,
		Payload:           payload,
		Tags:              t.Tags,
		Groups:            t.Groups,
		Scheduled:         sched,
		RejectCount:       t.RejectCount,
	}, nil
}

func queueTaskToHTTP(t *redisqueue.Task) httpTask {
	if t == nil {
		return httpTask{}
	}
	var ms int64
	if !t.Scheduled.IsZero() {
		ms = t.Scheduled.UnixMilli()
	}
	return httpTask{
		ID:                t.ID,
		Partition:         t.Partition,
		PartitionPriority: t.PartitionPriority,
		QueuePriority:     t.QueuePriority,
		Payload:           base64.StdEncoding.EncodeToString(t.Payload),
		PayloadEncoding:   "base64",
		Tags:              t.Tags,
		Groups:            t.Groups,
		ScheduledUnixMs:   ms,
		RejectCount:       t.RejectCount,
	}
}
