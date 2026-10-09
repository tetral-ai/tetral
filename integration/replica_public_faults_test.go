package integration

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

// The barrier pauses only after the selected real SQL statement has completed.
// For a queue insert this is inside the append transaction, before its commit.
type replicaPublicTransactionBarrier struct {
	mu       sync.Mutex
	contains string
	reached  chan struct{}
	release  chan struct{}
}

func (b *replicaPublicTransactionBarrier) arm(t *testing.T, contains string) (<-chan struct{}, func()) {
	t.Helper()
	b.mu.Lock()
	defer b.mu.Unlock()
	b.contains = contains
	b.reached = make(chan struct{})
	b.release = make(chan struct{})
	release := b.release
	var once sync.Once
	finish := func() { once.Do(func() { close(release) }) }
	t.Cleanup(finish)
	return b.reached, finish
}

type replicaPublicSQLKey struct{}

func (*replicaPublicTransactionBarrier) TraceQueryStart(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	return context.WithValue(ctx, replicaPublicSQLKey{}, strings.Join(strings.Fields(strings.ToLower(data.SQL)), " "))
}
func (b *replicaPublicTransactionBarrier) TraceQueryEnd(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryEndData) {
	if data.Err != nil {
		return
	}
	query, _ := ctx.Value(replicaPublicSQLKey{}).(string)
	b.mu.Lock()
	if b.contains == "" || !strings.Contains(query, b.contains) {
		b.mu.Unlock()
		return
	}
	reached, release := b.reached, b.release
	b.contains = ""
	b.mu.Unlock()
	close(reached)
	select {
	case <-release:
	case <-ctx.Done():
	}
}

func replicaPublicBarrierReached(t *testing.T, reached <-chan struct{}) {
	t.Helper()
	select {
	case <-reached:
	case <-time.After(5 * time.Second):
		t.Fatal("actual SQL barrier not reached")
	}
}

// The complete real edge response is recorded but its public connection is
// closed without writing any response bytes. POST is attempted exactly once.
func replicaPublicLoseResponse(ctx context.Context, t *testing.T, handler http.Handler, path, key, body string, headers map[string]string) (int, []byte) {
	t.Helper()
	type recorded struct {
		code int
		body []byte
		err  error
	}
	recordedResponse := make(chan recorded, 1)
	var requests atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		recorder := httptest.NewRecorder()
		handler.ServeHTTP(recorder, r)
		connection, _, err := w.(http.Hijacker).Hijack()
		if err == nil {
			err = connection.Close()
		}
		recordedResponse <- recorded{code: recorder.Code, body: append([]byte(nil), recorder.Body.Bytes()...), err: err}
	}))
	defer server.Close()
	transport := &http.Transport{DisableKeepAlives: true}
	defer transport.CloseIdleConnections()
	client := &http.Client{Transport: transport}
	r, err := http.NewRequestWithContext(ctx, "POST", server.URL+path, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	r.Header.Set("X-Api-Key", key)
	r.Header.Set("Content-Type", "application/json")
	for name, value := range headers {
		r.Header.Set(name, value)
	}
	started := time.Now()
	response, err := client.Do(r)
	if err == nil {
		_, _ = io.Copy(io.Discard, response.Body)
		_ = response.Body.Close()
		t.Fatal("lost response unexpectedly acknowledged")
	}
	replicaRecordCompletion(t, "public_authentication_api", "POST", server.URL, "response_lost", started)
	if !errors.Is(err, io.EOF) {
		t.Fatalf("unexpected lost response result: %v", err)
	}
	var captured recorded
	select {
	case captured = <-recordedResponse:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if captured.err != nil || requests.Load() != 1 {
		t.Fatalf("connection close/request count=%v/%d", captured.err, requests.Load())
	}
	return captured.code, captured.body
}
