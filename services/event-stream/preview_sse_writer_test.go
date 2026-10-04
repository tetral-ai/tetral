package eventstream

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"
)

// Fixture stores only the borrowed body slice while its actual Write is held.
// A formatter-created full-frame copy cannot satisfy this identity barrier.
type borrowedSSEBodyWriter struct {
	header                    http.Header
	borrowed                  []byte
	entered, release          chan struct{}
	output                    bytes.Buffer
	calls, deadlines, flushes int
	mu                        sync.Mutex
}

func (w *borrowedSSEBodyWriter) Header() http.Header { return w.header }
func (*borrowedSSEBodyWriter) WriteHeader(int)       {}
func (w *borrowedSSEBodyWriter) SetWriteDeadline(time.Time) error {
	w.mu.Lock()
	w.deadlines++
	w.mu.Unlock()
	return nil
}
func (w *borrowedSSEBodyWriter) FlushError() error {
	w.mu.Lock()
	w.flushes++
	w.mu.Unlock()
	return nil
}
func (w *borrowedSSEBodyWriter) Write(data []byte) (int, error) {
	w.mu.Lock()
	w.calls++
	call := w.calls
	if call == 2 {
		w.borrowed = data
		close(w.entered)
	}
	w.mu.Unlock()
	if call == 2 {
		<-w.release
	}
	return w.output.Write(data)
}
func TestSSEWriterBorrowsReservedEncodingAcrossHeldBodyWrite(t *testing.T) {
	body := []byte(`{"text":"` + strings.Repeat("a", 96*1024) + `"}`)
	fixture := &borrowedSSEBodyWriter{header: make(http.Header), entered: make(chan struct{}), release: make(chan struct{})}
	metrics := NewPreviewMetrics()
	writer := newSSEWriter(t.Context(), fixture, http.NewResponseController(fixture), time.Second, metrics)
	defer writer.close()
	var once sync.Once
	release := func() { once.Do(func() { close(fixture.release) }) }
	t.Cleanup(release)
	done := make(chan error, 1)
	joined := make(chan struct{})
	go func() { defer close(joined); done <- writer.event("event_delta", body) }()
	t.Cleanup(func() {
		release()
		select {
		case <-joined:
		case <-time.After(5 * time.Second):
			t.Error("held body fixture did not join")
		}
	})
	select {
	case <-fixture.entered:
	case err := <-done:
		t.Fatalf("no distinct borrowed body Write barrier: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("body Write did not enter")
	}
	fixture.mu.Lock()
	borrowed, deadlines, flushes := fixture.borrowed, fixture.deadlines, fixture.flushes
	fixture.mu.Unlock()
	if len(borrowed) != len(body) || &borrowed[0] != &body[0] {
		t.Fatal("blocked response Write holds an extra complete encoding instead of reserved original")
	}
	if deadlines != 1 || flushes != 0 {
		t.Fatalf("event uses separate part deadlines or premature flush: deadlines=%d flushes=%d", deadlines, flushes)
	}
	// Only the complete small header has returned so far; held payload bytes
	// are recorded after the body Write returns, with no fabricated sent count.
	if got := metrics.sentBytes.Load(); got != uint64(len("event: event_delta\ndata: ")) {
		t.Fatalf("held sent bytes=%d", got)
	}
	release()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("released event write not joined")
	}
	want := []byte("event: event_delta\ndata: " + string(body) + "\n\n")
	if !bytes.Equal(fixture.output.Bytes(), want) || metrics.sentBytes.Load() != uint64(len(want)) {
		t.Fatal("SSE exact framing/cumulative sent-byte accounting changed")
	}
	fixture.mu.Lock()
	flushes = fixture.flushes
	deadlines = fixture.deadlines
	fixture.mu.Unlock()
	if deadlines != 1 || flushes != 1 {
		t.Fatalf("event deadline/flush=%d/%d want1/1", deadlines, flushes)
	}
}

type partialSSEWriter struct {
	http.ResponseWriter
	failPart, keep            int
	writeError, flushError    error
	calls, deadlines, flushes int
	output                    bytes.Buffer
}

func (w *partialSSEWriter) SetWriteDeadline(time.Time) error { w.deadlines++; return nil }
func (w *partialSSEWriter) FlushError() error                { w.flushes++; return w.flushError }
func (w *partialSSEWriter) Write(data []byte) (int, error) {
	w.calls++
	if w.calls == w.failPart {
		n := min(w.keep, len(data))
		_, _ = w.output.Write(data[:n])
		return n, w.writeError
	}
	return w.output.Write(data)
}
func TestSSEWriterPartialWritesStopAndCountActualBytes(t *testing.T) {
	body := []byte(`{"text":"content"}`)
	header := "event: agent.message\ndata: "
	failure := errors.New("controlled response failure")
	for _, item := range []struct {
		name                   string
		part, keep             int
		err, flushErr, wantErr error
		want                   string
		flushes                int
	}{
		{"header", 1, 7, failure, nil, failure, header[:7], 0},
		{"body", 2, 5, failure, nil, failure, header + string(body[:5]), 0},
		{"short-body", 2, 5, nil, nil, io.ErrShortWrite, header + string(body[:5]), 0},
		{"terminator", 3, 1, nil, nil, io.ErrShortWrite, header + string(body) + "\n", 0},
		{"flush", 0, 0, nil, failure, failure, header + string(body) + "\n\n", 1},
	} {
		t.Run(item.name, func(t *testing.T) {
			fixture := &partialSSEWriter{failPart: item.part, keep: item.keep, writeError: item.err, flushError: item.flushErr}
			metrics := NewPreviewMetrics()
			writer := newSSEWriter(context.Background(), fixture, http.NewResponseController(fixture), time.Second, metrics)
			defer writer.close()
			if err := writer.event("agent.message", body); !errors.Is(err, item.wantErr) {
				t.Fatalf("write failure=%v want%v", err, item.wantErr)
			}
			if fixture.output.String() != item.want || metrics.sentBytes.Load() != uint64(len(item.want)) {
				t.Fatalf("partial bytes=%q sent=%d want%d", fixture.output.String(), metrics.sentBytes.Load(), len(item.want))
			}
			if fixture.deadlines != 1 || fixture.flushes != item.flushes {
				t.Fatal("part failure reset deadline or flushed incomplete SSE")
			}
		})
	}
}
