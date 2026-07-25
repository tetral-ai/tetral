package workload_test

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/workload"
)

// TestWorkloadRunServeErrorFlipsReadinessFalseDuringDrain proves the serve-error
// branch drains symmetrically with the signal branch: when server.Serve fails, the
// workload flips /ready false before draining so a load balancer stops routing while
// an in-flight request is still completing, and Run still surfaces the serve error.
func TestWorkloadRunServeErrorFlipsReadinessFalseDuringDrain(t *testing.T) {
	readiness := workload.NewReadiness()
	readiness.MarkReady()

	handlerStarted := make(chan struct{})
	releaseHandler := make(chan struct{})
	handler := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		close(handlerStarted)
		<-releaseHandler
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("drained"))
	})

	base := newWorkloadTestListener(t)
	listener := &serveErrorListener{Listener: base, fail: make(chan struct{})}

	runDone := make(chan error, 1)
	go func() {
		runDone <- workload.Run(context.Background(), workload.Config{
			ServiceName:     "api",
			Listener:        listener,
			Handler:         handler,
			Readiness:       readiness,
			ShutdownTimeout: 5 * time.Second,
		})
	}()

	// Start an in-flight request and wait until the handler is executing.
	responseBody := make(chan string, 1)
	go func() {
		resp, err := http.Get("http://" + base.Addr().String() + "/slow")
		if err != nil {
			return
		}
		defer func() { _ = resp.Body.Close() }()
		body, _ := io.ReadAll(resp.Body)
		responseBody <- string(body)
	}()

	select {
	case <-handlerStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("handler did not start")
	}

	// Force server.Serve to return a fatal accept error while the request is in flight.
	listener.triggerServeFailure()

	// Readiness must flip false during the drain window, before the in-flight request completes.
	deadline := time.Now().Add(2 * time.Second)
	for readiness.Ready() {
		if time.Now().After(deadline) {
			t.Fatal("readiness did not flip false during serve-error drain")
		}
		time.Sleep(5 * time.Millisecond)
	}

	close(releaseHandler)
	select {
	case body := <-responseBody:
		if body != "drained" {
			t.Fatalf("in-flight response body = %q; want drained", body)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("in-flight request did not complete during serve-error drain")
	}

	select {
	case err := <-runDone:
		if err == nil {
			t.Fatal("Run returned nil; want the surfaced serve error")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after serve-error drain")
	}
}

// serveErrorListener accepts real connections until triggerServeFailure is called,
// after which Accept returns a permanent error so http.Server.Serve exits with a
// non-ErrServerClosed error, exercising the serve-error branch deterministically.
type serveErrorListener struct {
	net.Listener
	fail     chan struct{}
	failOnce sync.Once
}

func (l *serveErrorListener) Accept() (net.Conn, error) {
	type result struct {
		conn net.Conn
		err  error
	}
	results := make(chan result, 1)
	go func() {
		conn, err := l.Listener.Accept()
		results <- result{conn: conn, err: err}
	}()
	select {
	case <-l.fail:
		return nil, errors.New("serve failure injected")
	case r := <-results:
		return r.conn, r.err
	}
}

func (l *serveErrorListener) triggerServeFailure() {
	l.failOnce.Do(func() { close(l.fail) })
}
