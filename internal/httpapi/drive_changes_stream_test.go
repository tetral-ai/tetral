package httpapi_test

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/drive"
	"github.com/tetral-ai/tetral/internal/httpapi"
)

func TestDriveChangeStreamHeartbeatAndListenerRecovery(t *testing.T) {
	api := newDriveAPI(t, drive.DefaultUploadBudgetBytesPerMinute, httpapi.WithDriveHeartbeatInterval(20*time.Millisecond))
	id := api.newDrive(`{}`)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	request, _ := http.NewRequestWithContext(ctx, http.MethodGet, api.url+"/v1/drives/"+id+"/changes/stream", nil)
	request.Header.Set("x-api-key", api.env.envKey)
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
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
}

func (w *failedHeartbeatWriter) Header() http.Header { return w.header }
func (w *failedHeartbeatWriter) WriteHeader(int)     {}
func (w *failedHeartbeatWriter) Flush()              {}
func (w *failedHeartbeatWriter) Write([]byte) (int, error) {
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
	writer := &failedHeartbeatWriter{header: make(http.Header), attempted: make(chan struct{})}
	done := make(chan struct{})
	go func() { defer close(done); api.handler.ServeHTTP(writer, request) }()
	select {
	case <-done:
	case <-ctx.Done():
		t.Fatal("write failure did not release handler")
	}
	select {
	case <-writer.attempted:
	default:
		t.Fatal("handler returned before heartbeat write")
	}
}
