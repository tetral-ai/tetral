package computerhub

import (
	"encoding/json"
	"net/http"
	neturl "net/url"
	"sync"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/integration/enginetest"
	"github.com/tetral-ai/tetral/internal/auth"
	"github.com/tetral-ai/tetral/internal/protocol"
	"github.com/tetral-ai/tetral/internal/workspace"
)

// manualClock is a StreamClock whose time moves only by Advance.
type manualClock struct {
	mu      sync.Mutex
	now     time.Duration
	pending []manualTimer
}

type manualTimer struct {
	at time.Duration
	ch chan time.Time
}

func newManualClock() *manualClock { return &manualClock{} }

func (c *manualClock) After(d time.Duration) <-chan time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	ch := make(chan time.Time, 1)
	c.pending = append(c.pending, manualTimer{at: c.now + d, ch: ch})
	return ch
}

// Advance moves the clock by d and fires every timer now due.
func (c *manualClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now += d
	kept := c.pending[:0]
	for _, timer := range c.pending {
		if timer.at <= c.now {
			timer.ch <- time.Time{}
			continue
		}
		kept = append(kept, timer)
	}
	c.pending = kept
}

func (c *manualClock) waiting() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.pending)
}

// waitTimers waits until n timers are pending.
func (c *manualClock) waitTimers(t *testing.T, n int) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for c.waiting() < n {
		if time.Now().After(deadline) {
			t.Fatalf("pending timers = %d; want %d", c.waiting(), n)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// cli returns a test CLI for the instance and credential whose requests
// carry a minted connection key principal, as the edge forwards them.
func (f *hubFixture) cli(instanceID, credentialID string) *enginetest.CLI {
	principal := auth.ConnectionKeyPrincipal(workspace.Workspace{ID: f.ws}, instanceID, credentialID)
	return &enginetest.CLI{BaseURL: f.url, InstanceID: instanceID, CredentialID: credentialID, Authorize: func(r *http.Request) error {
		token, err := f.signer.Mint(principal, r.Method, r.URL.Path, "req_hub_cli", time.Minute)
		r.Header.Set("X-Tetral-Internal-Principal", token)
		return err
	}}
}

// userInstance creates a user instance whose first credential expires at
// expiresAt (the maximum lifetime when zero) and returns its CLI.
func (f *hubFixture) userInstance(expiresAt time.Time) *enginetest.CLI {
	f.t.Helper()
	credential := `{}`
	if !expiresAt.IsZero() {
		credential = `{"expires_at":"` + expiresAt.UTC().Format(time.RFC3339Nano) + `"}`
	}
	status, created := f.createInstance("create-"+expiresAt.String()+f.t.Name()+time.Now().String(), `{"name":"laptop","credential":`+credential+`}`)
	if status != http.StatusCreated {
		f.t.Fatalf("create instance = %d", status)
	}
	return f.cli(created.Instance["id"].(string), created.Credential["id"].(string))
}

// secondCredential issues another active credential of cli's instance,
// expiring at expiresAt, and returns a CLI that uses it.
func (f *hubFixture) secondCredential(cli *enginetest.CLI, expiresAt time.Time) *enginetest.CLI {
	f.t.Helper()
	body := `{"delivery":"response"}`
	if !expiresAt.IsZero() {
		body = `{"delivery":"response","expires_at":"` + expiresAt.UTC().Format(time.RFC3339Nano) + `"}`
	}
	status, credential := f.issue(cli.InstanceID, "issue-"+f.t.Name()+time.Now().String(), body)
	if status != http.StatusCreated {
		f.t.Fatalf("issue credential = %d", status)
	}
	return f.cli(cli.InstanceID, credential["id"].(string))
}

// testStream is an open event stream with its events read as they come.
type testStream struct {
	t            *testing.T
	stream       *enginetest.Stream
	events       chan enginetest.Event
	connectionID string
}

// open opens cli's event stream as a new connection and reads connected.
func (f *hubFixture) open(cli *enginetest.CLI) *testStream {
	f.t.Helper()
	return f.openClientStream(f.url, cli, cli.EventsQuery("", nil), false)
}

// resume resumes connectionID on the hub server at url and reads connected.
func (f *hubFixture) resume(url string, cli *enginetest.CLI, connectionID string) *testStream {
	f.t.Helper()
	resume := true
	opened := f.openClientStream(url, cli, cli.EventsQuery(connectionID, &resume), true)
	if opened.connectionID != connectionID {
		f.t.Fatalf("resumed connection %s; want %s", opened.connectionID, connectionID)
	}
	return opened
}

// openClientStream opens an event stream with query on the hub server at url and
// reads connected, which must carry resumed.
func (f *hubFixture) openClientStream(url string, cli *enginetest.CLI, query neturl.Values, resumed bool) *testStream {
	f.t.Helper()
	at := *cli
	at.BaseURL = url
	stream, response, err := at.OpenEvents(f.ctx, query)
	if err != nil || stream == nil {
		f.t.Fatalf("open events = %d %s, %v", response.Status, response.Body, err)
	}
	if response.Header.Get("Content-Type") != "text/event-stream" || response.Header.Get("Cache-Control") != "no-store" {
		f.t.Fatalf("stream headers = %v", response.Header)
	}
	opened := &testStream{t: f.t, stream: stream, events: make(chan enginetest.Event, 64)}
	f.t.Cleanup(func() { _ = stream.Close() })
	go func() {
		defer close(opened.events)
		for {
			event, err := stream.Next()
			if err != nil {
				if len(event.Lines) > 0 {
					opened.events <- event
				}
				return
			}
			opened.events <- event
		}
	}()
	connected := opened.next()
	payload, ok := connected.Envelope.Payload.(protocol.ConnectedPayload)
	if !ok || payload.Resumed != resumed {
		f.t.Fatalf("first event = %s; want connected with resumed %v", connected.Data, resumed)
	}
	opened.connectionID = connected.Envelope.ConnectionID
	return opened
}

// next returns the stream's next event; a malformed event fails the test.
func (s *testStream) next() enginetest.Event {
	s.t.Helper()
	select {
	case event, ok := <-s.events:
		if !ok {
			s.t.Fatal("the stream ended")
		}
		if event.Envelope.Payload == nil {
			s.t.Fatalf("malformed event %q", event.Lines)
		}
		return event
	case <-time.After(10 * time.Second):
		s.t.Fatal("no event within 10 seconds")
	}
	return enginetest.Event{}
}

// ended waits for the stream to end and returns the events written before.
func (s *testStream) ended() []enginetest.Event {
	s.t.Helper()
	var events []enginetest.Event
	timeout := time.After(10 * time.Second)
	for {
		select {
		case event, ok := <-s.events:
			if !ok {
				return events
			}
			events = append(events, event)
		case <-timeout:
			s.t.Fatal("the stream did not end within 10 seconds")
		}
	}
}

// heartbeat reads the next event, which must be a heartbeat, and returns
// the CLI's report answering it.
func (s *testStream) heartbeat(messageID string) protocol.Heartbeat {
	s.t.Helper()
	event := s.next()
	payload, ok := event.Envelope.Payload.(protocol.HeartbeatPayload)
	if !ok {
		s.t.Fatalf("event = %s; want heartbeat", event.Data)
	}
	return protocol.Heartbeat{ConnectionID: s.connectionID, MessageID: messageID, RequestMessageID: event.Envelope.MessageID, HeartbeatSeq: payload.HeartbeatSeq}
}

func readyOf(connectionID, messageID, cliVersion string) protocol.Ready {
	return protocol.Ready{ConnectionID: connectionID, MessageID: messageID, OS: "linux", Arch: "x86_64", CLIVersion: cliVersion, Capabilities: protocol.Capabilities{Drives: protocol.DriveCapability{Mount: true}}}
}

// acceptReady sends a ready from CLI 1.0.0 and returns its grant.
func (f *hubFixture) acceptReady(cli *enginetest.CLI, stream *testStream, messageID string) protocol.GrantAcknowledgement {
	f.t.Helper()
	response, err := cli.Ready(f.ctx, readyOf(stream.connectionID, messageID, "1.0.0"))
	if err != nil {
		f.t.Fatal(err)
	}
	grant, err := response.Grant()
	if err != nil {
		f.t.Fatalf("ready: %v", err)
	}
	return grant
}

// requireClientError checks a refusal's status and code of the client error
// table.
func requireClientError(t *testing.T, response enginetest.Response, err error, code protocol.ErrorCode) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
	body, decodeErr := response.Error()
	if decodeErr != nil || body.Code != code || response.Status != protocol.ClientErrors[code] {
		t.Fatalf("answer = %d %s; want %d %s", response.Status, response.Body, protocol.ClientErrors[code], code)
	}
}

// connectionRow reads a connection row through the admin pool.
func (f *hubFixture) connectionRow(connectionID string) map[string]any {
	f.t.Helper()
	var data []byte
	if err := f.admin.QueryRowContext(f.ctx, `SELECT row_to_json(c) FROM instance_connections c WHERE id=$1`, connectionID).Scan(&data); err != nil {
		f.t.Fatal(err)
	}
	var row map[string]any
	decodeJSON(f.t, data, &row)
	return row
}

// instanceRead returns the instance object of GET /v1/instances/{id}.
func (f *hubFixture) instanceRead(instanceID string) map[string]any {
	f.t.Helper()
	status, data := f.do(http.MethodGet, "/v1/instances/"+instanceID, nil, "")
	if status != http.StatusOK {
		f.t.Fatalf("instance read = %d %s", status, data)
	}
	var object map[string]any
	decodeJSON(f.t, data, &object)
	return object
}

// connectionState returns the instance read's connection object.
func (f *hubFixture) connectionState(instanceID string) map[string]any {
	f.t.Helper()
	connection, _ := f.instanceRead(instanceID)["connection"].(map[string]any)
	if connection == nil {
		f.t.Fatalf("instance %s has no connection", instanceID)
	}
	return connection
}

func mustJSON(t *testing.T, value any) []byte {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return data
}
