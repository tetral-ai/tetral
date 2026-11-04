package httpapi_test

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/drive"
	"github.com/tetral-ai/tetral/internal/httpapi"
	"github.com/tetral-ai/tetral/internal/workspace"
)

func TestDriveChangeStreamHeartbeatAndListenerRecovery(t *testing.T) {
	api := newDriveAPI(t, drive.DefaultUploadBudgetBytesPerMinute, httpapi.WithDriveHeartbeatInterval(20*time.Millisecond))
	id := api.newDrive(`{}`)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	handlerDone := make(chan struct{})
	streamServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer close(handlerDone)
		api.handler.ServeHTTP(w, r)
	}))
	defer func() { cancel(); streamServer.Close() }()
	request, _ := http.NewRequestWithContext(ctx, http.MethodGet, streamServer.URL+"/v1/drives/"+id+"/changes/stream", nil)
	request.Header.Set("x-api-key", api.env.envKey)
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("stream status %d", response.StatusCode)
	}
	reader := bufio.NewReader(response.Body)
	next := func() string {
		t.Helper()
		var frame strings.Builder
		for {
			line, err := reader.ReadString('\n')
			if err != nil {
				t.Fatal(err)
			}
			if line == "\n" {
				return frame.String()
			}
			frame.WriteString(line)
		}
	}
	if got := next(); got != ": heartbeat\n" {
		t.Fatalf("quiet stream frame %q", got)
	}
	if count := httpapi.DriveChangeSubscriberCountForTest(api.changes, workspace.DefaultID, id); count != 1 {
		t.Fatalf("active stream subscribers = %d; want one", count)
	}
	// Interrupt the real LISTEN connection, not the HTTP client. The same
	// open stream must resume notifications after the listener reconnects.
	var pid int
	deadline := time.Now().Add(5 * time.Second)
	for {
		err = api.env.admin.QueryRow(`SELECT pid FROM pg_stat_activity WHERE datname=current_database() AND query ILIKE 'LISTEN %tetral_drive_changed%' LIMIT 1`).Scan(&pid)
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("listener did not connect: %v", err)
		}
		time.Sleep(10 * time.Millisecond)
	}
	var terminated bool
	if err := api.env.admin.QueryRow(`SELECT pg_terminate_backend($1)`, pid).Scan(&terminated); err != nil || !terminated {
		t.Fatalf("terminate listener: %v %v", terminated, err)
	}
	decodeDrive(t, api.upload(id, "after-reconnect.txt", "value"), http.StatusCreated, nil)
	want := fmt.Sprintf("event: drive.changed\ndata: {\"drive_id\":%q,\"seq\":1}\n", id)
	for {
		got := next()
		if got == want {
			break
		}
		if got != ": heartbeat\n" {
			t.Fatalf("unexpected frame: %q", got)
		}
	}
	cancel()
	// Drain bytes already flushed; the canceled request must then terminate.
	for {
		_, err := reader.ReadByte()
		if err != nil {
			break
		}
	}
	select {
	case <-handlerDone:
	case <-time.After(5 * time.Second):
		t.Fatal("canceled stream handler did not return")
	}
	if count := httpapi.DriveChangeSubscriberCountForTest(api.changes, workspace.DefaultID, id); count != 0 {
		t.Fatalf("subscribers after canceled handler returned = %d; want zero", count)
	}
	for _, path := range []string{"drive_missing"} {
		expectError(t, api.json(http.MethodGet, "/v1/drives/"+path+"/changes/stream", ""), http.StatusNotFound, "not_found_error", "drive not found")
	}
	api.env.seedWorkspace(t, "wksp_stream_other", "other")
	other, err := api.drives.Create(context.Background(), "wksp_stream_other", drive.Settings{})
	if err != nil {
		t.Fatal(err)
	}
	expectError(t, api.json(http.MethodGet, "/v1/drives/"+other.ID+"/changes/stream", ""), http.StatusNotFound, "not_found_error", "drive not found")
}

type failedHeartbeatWriter struct {
	header    http.Header
	attempted chan struct{}
	onWrite   func()
}

func (w *failedHeartbeatWriter) Header() http.Header { return w.header }
func (w *failedHeartbeatWriter) WriteHeader(int)     {}
func (w *failedHeartbeatWriter) Flush()              {}
func (w *failedHeartbeatWriter) Write([]byte) (int, error) {
	if w.onWrite != nil {
		w.onWrite()
	}
	close(w.attempted)
	return 0, errors.New("client disconnected")
}

func TestDriveChangeStreamEndsOnHeartbeatWriteFailure(t *testing.T) {
	api := newDriveAPI(t, drive.DefaultUploadBudgetBytesPerMinute, httpapi.WithDriveHeartbeatInterval(time.Millisecond))
	id := api.newDrive(`{}`)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	request, _ := http.NewRequestWithContext(ctx, http.MethodGet, "/v1/drives/"+id+"/changes/stream", nil)
	request.Header.Set("x-api-key", api.env.envKey)
	activeSubscribers := 0
	writer := &failedHeartbeatWriter{header: make(http.Header), attempted: make(chan struct{}), onWrite: func() {
		activeSubscribers = httpapi.DriveChangeSubscriberCountForTest(api.changes, workspace.DefaultID, id)
	}}
	done := make(chan struct{})
	go func() { defer close(done); api.handler.ServeHTTP(writer, request) }()
	select {
	case <-done:
	case <-ctx.Done():
		t.Fatal("write failure did not release handler")
	}
	select {
	case <-writer.attempted:
		if activeSubscribers != 1 {
			t.Fatalf("subscribers at heartbeat write = %d; want one", activeSubscribers)
		}
		if count := httpapi.DriveChangeSubscriberCountForTest(api.changes, workspace.DefaultID, id); count != 0 {
			t.Fatalf("subscribers after write-failure handler returned = %d; want zero", count)
		}
	default:
		t.Fatal("handler returned before heartbeat write")
	}
}
