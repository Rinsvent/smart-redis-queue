package gateway

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"sync"
	"time"
)

// Overview — снимок для HTTP /api/overview и CLI status.
type Overview struct {
	State         string `json:"state"`
	AppendFsync   string `json:"append_fsync"`
	LossWindowNs  int64  `json:"loss_window_ns"`
	OpLogLen      int    `json:"op_log_len"`
	LossWindowLen int    `json:"loss_window_len"`
	ListenGRPC    string `json:"listen_grpc,omitempty"`
	ListenHTTP    string `json:"listen_http,omitempty"`
}

// Overview возвращает текущий статус (без блокировки Redis).
func (g *Gateway) Overview() Overview {
	p := g.Persist()
	return Overview{
		State:         g.State().String(),
		AppendFsync:   p.AppendFsync,
		LossWindowNs:  int64(p.LossWindow),
		OpLogLen:      g.ops.len(),
		LossWindowLen: g.loss.len(),
		ListenGRPC:    g.cfg.ListenAddr,
		ListenHTTP:    g.cfg.HTTPAddr,
	}
}

type httpOps struct {
	g      *Gateway
	mu     sync.Mutex
	srv    *http.Server
	cancel context.CancelFunc // shutdown signal for whole process
}

func (h *httpOps) mux() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/health", h.handleHealth)
	mux.HandleFunc("/api/health", h.handleHealth)
	mux.HandleFunc("/api/overview", h.handleOverview)
	mux.HandleFunc("/api/shutdown", h.handleShutdown)
	h.registerDataPlane(mux)
	return mux
}

func (h *httpOps) handleHealth(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	ov := h.g.Overview()
	code := http.StatusOK
	if ov.State == StateFailed.String() || ov.State == StateRedisDown.String() {
		code = http.StatusServiceUnavailable
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"status": ov.State,
		"ok":     code == http.StatusOK,
	})
}

func (h *httpOps) handleOverview(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(h.g.Overview())
}

func (h *httpOps) handleShutdown(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost && r.Method != http.MethodDelete {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]string{"status": "shutting_down"})
	go func() {
		time.Sleep(50 * time.Millisecond)
		if h.cancel != nil {
			h.cancel()
		}
	}()
}

// ListenAndServe поднимает gRPC и HTTP.
// HTTP: data-plane /v1/publish|get|ack|reject (дубль gRPC для PHP) + ops /health /api/overview /api/shutdown.
// httpAddr пустой — только gRPC.
func (g *Gateway) ListenAndServe(ctx context.Context, grpcAddr, httpAddr string) error {
	g.cfg.ListenAddr = grpcAddr
	g.cfg.HTTPAddr = httpAddr

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	errCh := make(chan error, 2)

	go func() {
		errCh <- g.ListenAndServeGRPC(ctx, grpcAddr)
	}()

	if httpAddr != "" {
		h := &httpOps{g: g, cancel: cancel}
		h.srv = &http.Server{
			Addr:              httpAddr,
			Handler:           h.mux(),
			ReadHeaderTimeout: 5 * time.Second,
		}
		go func() {
			g.log.Printf("HTTP listen %s (/v1/publish|get|ack|reject, /health, /api/overview, /api/shutdown)", httpAddr)
			ln, err := net.Listen("tcp", httpAddr)
			if err != nil {
				errCh <- err
				return
			}
			err = h.srv.Serve(ln)
			if err == http.ErrServerClosed {
				err = nil
			}
			if err != nil {
				errCh <- err
			}
		}()
		go func() {
			<-ctx.Done()
			shutdownCtx, c := context.WithTimeout(context.Background(), 3*time.Second)
			defer c()
			_ = h.srv.Shutdown(shutdownCtx)
		}()
	}

	select {
	case <-ctx.Done():
		return nil
	case err := <-errCh:
		cancel()
		return err
	}
}

// FetchOverview — HTTP-клиент к /api/overview.
func FetchOverview(ctx context.Context, httpBase string) (Overview, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, trimSlash(httpBase)+"/api/overview", nil)
	if err != nil {
		return Overview{}, err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return Overview{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		return Overview{}, fmt.Errorf("gateway status: HTTP %d", resp.StatusCode)
	}
	var ov Overview
	if err := json.NewDecoder(resp.Body).Decode(&ov); err != nil {
		return Overview{}, err
	}
	return ov, nil
}

// PostShutdown — POST /api/shutdown.
func PostShutdown(ctx context.Context, httpBase string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, trimSlash(httpBase)+"/api/shutdown", nil)
	if err != nil {
		return err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		return fmt.Errorf("gateway stop: HTTP %d", resp.StatusCode)
	}
	return nil
}

func trimSlash(s string) string {
	for len(s) > 0 && s[len(s)-1] == '/' {
		s = s[:len(s)-1]
	}
	return s
}
