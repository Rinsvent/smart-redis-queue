package gateway

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHTTP_PublishGetAck(t *testing.T) {
	rdb := gatewayRedis(t)
	ctx := context.Background()

	gw, err := New(Config{Redis: rdb, SkipPersistCheck: true, LossWindowOverride: time.Second})
	require.NoError(t, err)
	require.NoError(t, gw.Boot(ctx))
	t.Cleanup(gw.Close)

	h := &httpOps{g: gw}
	mux := h.mux()

	q := "http-q"
	pubBody, _ := json.Marshal(publishHTTPRequest{
		Queue: q,
		Tasks: []httpTask{{
			ID:              "h1",
			Partition:       "p1",
			Payload:         "hello-php",
			ScheduledUnixMs: time.Now().Add(-time.Second).UnixMilli(),
		}},
	})
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, httptest.NewRequest(http.MethodPost, "/v1/publish", bytes.NewReader(pubBody)))
	require.Equal(t, http.StatusOK, rr.Code, rr.Body.String())

	getBody, _ := json.Marshal(getHTTPRequest{Queue: q, ConsumerID: "php-1", Prefetch: 5})
	rr = httptest.NewRecorder()
	mux.ServeHTTP(rr, httptest.NewRequest(http.MethodPost, "/v1/get", bytes.NewReader(getBody)))
	require.Equal(t, http.StatusOK, rr.Code, rr.Body.String())
	var got getHTTPResponse
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &got))
	require.Len(t, got.Tasks, 1)
	assert.Equal(t, "h1", got.Tasks[0].ID)
	assert.Equal(t, "base64", got.Tasks[0].PayloadEncoding)

	ackBody, _ := json.Marshal(ackHTTPRequest{Queue: q, ConsumerID: "php-1", TaskID: "h1"})
	rr = httptest.NewRecorder()
	mux.ServeHTTP(rr, httptest.NewRequest(http.MethodPost, "/v1/ack", bytes.NewReader(ackBody)))
	require.Equal(t, http.StatusOK, rr.Code, rr.Body.String())
}
