package workload_test

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/workload"
)

// TestWorkloadRunDrainsInFlightRequestBeforeReturning proves graceful shutdown:
// a request already executing when shutdown is triggered runs to completion and
// the client receives its full response before Run returns.
func TestWorkloadRunDrainsInFlightRequestBeforeReturning(t *testing.T) {
	handlerStarted := make(chan struct{})
	releaseHandler := make(chan struct{})
	handlerFinished := make(chan struct{})

	handler := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		close(handlerStarted)
		<-releaseHandler
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("drained"))
		close(handlerFinished)
	})

	listener := newWorkloadTestListener(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	metrics := workload.NewOperationMetrics("api")
	runDone := make(chan error, 1)
	go func() {
		runDone <- workload.Run(ctx, workload.Config{
			Metrics:         metrics,
			ServiceName:     "api",
			Listener:        listener,
			Handler:         handler,
			ShutdownTimeout: 5 * time.Second,
		})
	}()

	// Start an in-flight request and wait until the handler is executing.
	responseBody := make(chan string, 1)
	requestErr := make(chan error, 1)
	go func() {
		resp, err := http.Get("http://" + listener.Addr().String() + "/slow")
		if err != nil {
			requestErr <- err
			return
		}
		defer func() { _ = resp.Body.Close() }()
		body, readErr := io.ReadAll(resp.Body)
		if readErr != nil {
			requestErr <- readErr
			return
		}
		if resp.StatusCode != http.StatusOK {
			requestErr <- err
			return
		}
		responseBody <- string(body)
	}()

	select {
	case <-handlerStarted:
	case err := <-requestErr:
		t.Fatalf("request failed before handler started: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("handler did not start")
	}

	// Trigger shutdown while the request is in flight, then let the handler finish.
	cancel()
	// Give shutdown a beat to begin draining, then release the in-flight handler.
	close(releaseHandler)

	select {
	case <-handlerFinished:
	case <-time.After(5 * time.Second):
		t.Fatal("in-flight handler did not finish during graceful drain")
	}

	select {
	case body := <-responseBody:
		if body != "drained" {
			t.Fatalf("in-flight response body = %q; want \"drained\"", body)
		}
	case err := <-requestErr:
		t.Fatalf("in-flight request did not complete during drain: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("in-flight request did not return")
	}

	select {
	case err := <-runDone:
		if err != nil {
			t.Fatalf("Run returned error after clean drain: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after drain completed")
	}
	response := httptest.NewRecorder()
	workload.HealthRouter(workload.NewReadiness(), workload.WithMetricsCollector("operations", metrics.Collector())).ServeHTTP(response, httptest.NewRequest("GET", "/metrics", nil))
	if !strings.Contains(response.Body.String(), `tetral_operation_duration_seconds_count{operation="shutdown_http_drain",outcome="success",service="api"} 1`) {
		t.Fatalf("missing actual drain outcome: %s", response.Body.String())
	}
}

// TestWorkloadRunSurfacesDrainTimeoutError proves that when an in-flight request
// outlives ShutdownTimeout, Run reports the failed drain to its caller instead
// of returning nil.
func TestWorkloadRunSurfacesDrainTimeoutError(t *testing.T) {
	handlerStarted := make(chan struct{})
	releaseHandler := make(chan struct{})
	handlerCancelled := make(chan struct{})

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(handlerStarted)
		// Forced close must cancel this actual request, then still join its cleanup.
		<-r.Context().Done()
		close(handlerCancelled)
		<-releaseHandler
		w.WriteHeader(http.StatusOK)
	})

	listener := newWorkloadTestListener(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	metrics := workload.NewOperationMetrics("api")
	runDone := make(chan error, 1)
	go func() {
		runDone <- workload.Run(ctx, workload.Config{
			Metrics:         metrics,
			ServiceName:     "api",
			Listener:        listener,
			Handler:         handler,
			ShutdownTimeout: 100 * time.Millisecond,
		})
	}()

	go func() {
		resp, err := http.Get("http://" + listener.Addr().String() + "/slow")
		if err == nil {
			_, _ = io.Copy(io.Discard, resp.Body)
			_ = resp.Body.Close()
		}
	}()

	select {
	case <-handlerStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("handler did not start")
	}

	// Trigger shutdown; the in-flight handler stays blocked past ShutdownTimeout.
	cancel()
	select {
	case <-handlerCancelled:
	case <-time.After(5 * time.Second):
		close(releaseHandler)
		t.Fatal("request context did not cancel at forced shutdown")
	}
	select {
	case err := <-runDone:
		close(releaseHandler)
		t.Fatalf("Run returned before handler cleanup joined: %v", err)
	default:
	}
	close(releaseHandler)

	select {
	case err := <-runDone:
		if err == nil {
			t.Fatal("Run returned nil despite a drain that outlived ShutdownTimeout")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after drain timeout")
	}
	response := httptest.NewRecorder()
	workload.HealthRouter(workload.NewReadiness(), workload.WithMetricsCollector("operations", metrics.Collector())).ServeHTTP(response, httptest.NewRequest("GET", "/metrics", nil))
	if !strings.Contains(response.Body.String(), `tetral_operation_duration_seconds_count{operation="shutdown_http_drain",outcome="timeout",service="api"} 1`) {
		t.Fatalf("missing actual drain outcome: %s", response.Body.String())
	}
}

func TestReadinessShutdownCannotBeOverwrittenByLateStartup(t *testing.T) {
	ready := workload.NewReadiness()
	ready.BeginShutdown()
	ready.MarkReady()
	if ready.Ready() {
		t.Fatal("late listener startup reopened shutdown readiness")
	}
}
