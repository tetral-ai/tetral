package computerhub

import (
	"bufio"
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/auth"
	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/instance"
	"github.com/tetral-ai/tetral/internal/workspace"
)

// TestInstanceStreamDeliversOnlyItsWorkspaceEvents opens one stream in each
// of two Workspaces, notices an event in the second Workspace and then one in
// the first, and reads each stream's first event.
func TestInstanceStreamDeliversOnlyItsWorkspaceEvents(t *testing.T) {
	f := newHubFixture(t, 720*time.Hour)
	other := workspace.ID("ws_hub_other")
	if _, err := workspace.NewSeeder(f.admin).Seed(f.ctx, other, "other"); err != nil {
		t.Fatal(err)
	}
	admin := dbconnect.NewClientForTesting(f.admin)
	createIn := func(ws workspace.ID) string {
		var id string
		if err := admin.WithWorkspaceTx(f.ctx, string(ws), "computer_hub.test_instance", func(tx *dbconnect.Tx) error {
			row, err := instance.CreateUserInstance(f.ctx, tx, ws, "laptop")
			id = row.ID
			return err
		}); err != nil {
			t.Fatal(err)
		}
		return id
	}
	notify := func(ws workspace.ID, instanceID, event string) {
		if err := admin.WithWorkspaceTx(f.ctx, string(ws), "computer_hub.test_notify", func(tx *dbconnect.Tx) error {
			return instance.NotifyInstanceEvent(f.ctx, tx, ws, instanceID, event)
		}); err != nil {
			t.Fatal(err)
		}
	}
	mine, theirs := createIn(f.ws), createIn(other)
	select {
	case <-f.events.listening:
	case <-f.ctx.Done():
		t.Fatal("the instance event listener never listened")
	}
	myStream := f.openStream(f.principal)
	otherStream := f.openStream(auth.IndependentKeyPrincipal(workspace.Workspace{ID: other}, "key_hub_other"))
	notify(other, theirs, instance.EventCredentialReused)
	notify(f.ws, mine, instance.EventUnused)
	for _, tc := range []struct {
		stream    *bufio.Reader
		event, id string
	}{{myStream, instance.EventUnused, mine}, {otherStream, instance.EventCredentialReused, theirs}} {
		event, data := readServerSentEvent(t, tc.stream)
		var object map[string]any
		if err := json.Unmarshal([]byte(data), &object); err != nil {
			t.Fatalf("event data %q: %v", data, err)
		}
		if event != tc.event || object["id"] != tc.id {
			t.Fatalf("first event %s %s; want %s for %s", event, data, tc.event, tc.id)
		}
	}
}

func (f *hubFixture) openStream(principal auth.Principal) *bufio.Reader {
	f.t.Helper()
	token, err := f.signer.Mint(principal, http.MethodGet, "/v1/instances/stream", "req_hub_stream", time.Minute)
	if err != nil {
		f.t.Fatal(err)
	}
	request, err := http.NewRequestWithContext(f.ctx, http.MethodGet, f.url+"/v1/instances/stream", nil)
	if err != nil {
		f.t.Fatal(err)
	}
	request.Header.Set("X-Tetral-Internal-Principal", token)
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		f.t.Fatal(err)
	}
	f.t.Cleanup(func() { _ = response.Body.Close() })
	if response.StatusCode != http.StatusOK || response.Header.Get("Content-Type") != "text/event-stream" {
		f.t.Fatalf("stream = %d %s", response.StatusCode, response.Header.Get("Content-Type"))
	}
	return bufio.NewReader(response.Body)
}

// readServerSentEvent returns the next event's name and data.
func readServerSentEvent(t *testing.T, stream *bufio.Reader) (string, string) {
	t.Helper()
	var event, data string
	for {
		line, err := stream.ReadString('\n')
		if err != nil {
			t.Fatalf("read event: %v", err)
		}
		line = strings.TrimSuffix(line, "\n")
		switch {
		case line == "" && event != "":
			return event, data
		case strings.HasPrefix(line, "event: "):
			event = strings.TrimPrefix(line, "event: ")
		case strings.HasPrefix(line, "data: "):
			data = strings.TrimPrefix(line, "data: ")
		}
	}
}
