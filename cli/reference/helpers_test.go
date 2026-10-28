package reference

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"
)

var testStart = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

// waitLimit only bounds how long a test waits for something that must
// happen; no test reads its passing as evidence that something did not.
const waitLimit = 10 * time.Second

type testServer struct {
	t       *testing.T
	server  *Server
	api     *httptest.Server
	control *httptest.Server
}

func startServer(t *testing.T, change ...func(*Config)) *testServer {
	t.Helper()
	config := Config{ClockStart: testStart, Credentials: []Credential{
		{InstanceID: "inst_a", CredentialID: "cred_a", ConnectionKey: "tetral_ck_a", ExpiresAt: testStart.Add(time.Hour), State: "active"},
		{InstanceID: "inst_b", CredentialID: "cred_b", ConnectionKey: "tetral_ck_b", ExpiresAt: testStart.Add(time.Hour), State: "active"},
		{InstanceID: "inst_a", CredentialID: "cred_revoked", ConnectionKey: "tetral_ck_revoked", ExpiresAt: testStart.Add(time.Hour), State: "revoked"},
		{InstanceID: "inst_a", CredentialID: "cred_expired", ConnectionKey: "tetral_ck_expired", ExpiresAt: testStart.Add(-time.Minute), State: "active"},
	}}
	for _, apply := range change {
		apply(&config)
	}
	server, err := New(config)
	if err != nil {
		t.Fatal(err)
	}
	ts := &testServer{t: t, server: server, api: httptest.NewServer(server.Handler()), control: httptest.NewServer(server.ControlHandler())}
	t.Cleanup(func() {
		ts.api.CloseClientConnections()
		ts.api.Close()
		ts.control.Close()
	})
	return ts
}

// call runs a control and decodes its result into result, when given.
func (ts *testServer) call(name string, input any, result any) {
	ts.t.Helper()
	status, answer := ts.callStatus(name, input)
	if status != http.StatusOK {
		ts.t.Fatalf("control %s: %d %s", name, status, answer)
	}
	if result != nil {
		if err := json.Unmarshal(answer, result); err != nil {
			ts.t.Fatal(err)
		}
	}
}

// callStatus runs a control and returns its status and body as they are.
func (ts *testServer) callStatus(name string, input any) (int, []byte) {
	ts.t.Helper()
	body, _ := json.Marshal(input)
	response, err := http.Post(ts.control.URL+"/control/"+name, "application/json", bytes.NewReader(body))
	if err != nil {
		ts.t.Fatal(err)
	}
	defer func() { _ = response.Body.Close() }()
	answer, _ := io.ReadAll(response.Body)
	return response.StatusCode, answer
}

// eventsOf is the types of the events the server wrote on a connection, as
// the state control reports them.
func (ts *testServer) eventsOf(connectionID string) []string {
	ts.t.Helper()
	var kinds []string
	for _, event := range ts.state().Events {
		if event.ConnectionID == connectionID {
			kinds = append(kinds, event.Type)
		}
	}
	return kinds
}

func (ts *testServer) state() Snapshot {
	ts.t.Helper()
	var state Snapshot
	ts.call("state", nil, &state)
	return state
}

func (ts *testServer) connection(connectionID string) ConnectionState {
	ts.t.Helper()
	for _, conn := range ts.state().Connections {
		if conn.ConnectionID == connectionID {
			return conn
		}
	}
	ts.t.Fatalf("no connection %s", connectionID)
	return ConnectionState{}
}

func (ts *testServer) advance(step time.Duration) {
	ts.t.Helper()
	ts.call("clock", map[string]any{"advance_ms": step.Milliseconds()}, nil)
}

// reply is an HTTP answer as a test reads it.
type reply struct {
	status int
	header http.Header
	body   []byte
	err    error
}

func (r reply) code() string {
	var body struct {
		Code string `json:"code"`
	}
	_ = json.Unmarshal(r.body, &body)
	return body.Code
}

func (r reply) member(name string) any {
	var members map[string]any
	_ = json.Unmarshal(r.body, &members)
	return members[name]
}

func (ts *testServer) post(endpoint, key string, body any) reply {
	return ts.postContext(context.Background(), endpoint, key, body)
}

func (ts *testServer) postContext(ctx context.Context, endpoint, key string, body any) reply {
	encoded, _ := json.Marshal(body)
	request, _ := http.NewRequestWithContext(ctx, http.MethodPost, ts.api.URL+"/v1/client/"+endpoint, bytes.NewReader(encoded))
	request.Header.Set("Authorization", "Bearer "+key)
	request.Header.Set("Content-Type", "application/json")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		return reply{err: err}
	}
	defer func() { _ = response.Body.Close() }()
	answer, err := io.ReadAll(response.Body)
	return reply{status: response.StatusCode, header: response.Header, body: answer, err: err}
}

// postAsync sends a request whose answer the test expects later.
func (ts *testServer) postAsync(endpoint, key string, body any) <-chan reply {
	done := make(chan reply, 1)
	go func() { done <- ts.post(endpoint, key, body) }()
	return done
}

// eventStream is an open events request.
type eventStream struct {
	t        *testing.T
	events   chan map[string]any
	comments chan string
	ended    chan struct{}
	cancel   context.CancelFunc
	// mu guards sizes, the data bytes of each event in arrival order.
	mu    sync.Mutex
	sizes []int
}

// open sends GET /v1/client/events and returns the stream, or the
// refusal.
func (ts *testServer) open(key, instanceID, credentialID string, extra url.Values) (*eventStream, reply) {
	ts.t.Helper()
	query := url.Values{"protocol_version": {"1"}, "instance_id": {instanceID}, "credential_id": {credentialID}}
	for name, values := range extra {
		query[name] = values
	}
	ctx, cancel := context.WithCancel(context.Background())
	request, _ := http.NewRequestWithContext(ctx, http.MethodGet, ts.api.URL+"/v1/client/events?"+query.Encode(), nil)
	request.Header.Set("Authorization", "Bearer "+key)
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		cancel()
		return nil, reply{err: err}
	}
	if response.StatusCode != http.StatusOK {
		defer cancel()
		defer func() { _ = response.Body.Close() }()
		body, _ := io.ReadAll(response.Body)
		return nil, reply{status: response.StatusCode, body: body}
	}
	stream := &eventStream{
		t: ts.t, events: make(chan map[string]any, 1024), comments: make(chan string, 1024),
		ended: make(chan struct{}), cancel: cancel,
	}
	go func() {
		defer close(stream.ended)
		defer func() { _ = response.Body.Close() }()
		lines := bufio.NewScanner(response.Body)
		lines.Buffer(make([]byte, 1<<20), 1<<24)
		for lines.Scan() {
			if strings.HasPrefix(lines.Text(), ":") {
				stream.comments <- lines.Text()
				continue
			}
			data, isData := strings.CutPrefix(lines.Text(), "data: ")
			if !isData {
				continue
			}
			stream.mu.Lock()
			stream.sizes = append(stream.sizes, len(data))
			stream.mu.Unlock()
			var event map[string]any
			if err := json.Unmarshal([]byte(data), &event); err != nil {
				event = map[string]any{"raw": data}
			}
			stream.events <- event
		}
	}()
	ts.t.Cleanup(cancel)
	return stream, reply{status: http.StatusOK}
}

func (ts *testServer) mustOpen(key, instanceID, credentialID string, extra url.Values) *eventStream {
	ts.t.Helper()
	stream, refused := ts.open(key, instanceID, credentialID, extra)
	if stream == nil {
		ts.t.Fatalf("events: %d %s %v", refused.status, refused.body, refused.err)
	}
	return stream
}

func (stream *eventStream) next() map[string]any {
	stream.t.Helper()
	select {
	case event := <-stream.events:
		return event
	case <-time.After(waitLimit):
		stream.t.Fatal("no event arrived")
		return nil
	}
}

// comment waits for the next SSE comment line.
func (stream *eventStream) comment() string {
	stream.t.Helper()
	select {
	case line := <-stream.comments:
		return line
	case <-time.After(waitLimit):
		stream.t.Fatal("no comment arrived")
		return ""
	}
}

// dataBytes is the size of each event's data received so far.
func (stream *eventStream) dataBytes() []int {
	stream.mu.Lock()
	defer stream.mu.Unlock()
	return slices.Clone(stream.sizes)
}

func (stream *eventStream) expect(kind string) map[string]any {
	stream.t.Helper()
	event := stream.next()
	if event["type"] != kind {
		stream.t.Fatalf("event %v; want type %s", event, kind)
	}
	return event
}

// waitEnded waits until the server closes the stream, and returns the
// events written before it closed.
func (stream *eventStream) waitEnded() []map[string]any {
	stream.t.Helper()
	select {
	case <-stream.ended:
	case <-time.After(waitLimit):
		stream.t.Fatal("the stream did not end")
	}
	var rest []map[string]any
	for {
		select {
		case event := <-stream.events:
			rest = append(rest, event)
		default:
			return rest
		}
	}
}

func payload(event map[string]any) map[string]any {
	members, _ := event["payload"].(map[string]any)
	return members
}

func readyBody(connectionID, messageID string) map[string]any {
	return map[string]any{
		"protocol_version": 1, "connection_id": connectionID, "message_id": messageID,
		"os": "linux", "arch": "x86_64", "cli_version": "1.0.0",
		"capabilities": map[string]any{"session_preparation": true, "drives": map[string]any{"mount": false}},
	}
}

func heartbeatBody(connectionID, messageID, requestMessageID string, seq int64) map[string]any {
	return map[string]any{
		"protocol_version": 1, "connection_id": connectionID, "message_id": messageID,
		"request_message_id": requestMessageID, "heartbeat_seq": seq,
	}
}

func preparedReport(connectionID, messageID, requestMessageID, sessionID, bindingID string) map[string]any {
	return map[string]any{
		"protocol_version": 1, "connection_id": connectionID, "message_id": messageID,
		"request_message_id": requestMessageID, "session_id": sessionID, "binding_id": bindingID,
		"status": "prepared", "cwd": "/home/user/project",
	}
}

func preparePayload(sessionID, bindingID string) map[string]any {
	return map[string]any{
		"session_id": sessionID, "binding_id": bindingID, "cwd": "/home/user/project",
		"drives": []any{}, "drive_exclude": []any{}, "repositories": []any{},
	}
}

// receive waits for the answer of a request sent with postAsync, failing the
// test past waitLimit.
func receive(t *testing.T, done <-chan reply) reply {
	t.Helper()
	select {
	case answer := <-done:
		return answer
	case <-time.After(waitLimit):
		t.Fatal("no answer arrived")
		return reply{}
	}
}

// online opens inst_a's stream and has its ready accepted.
func (ts *testServer) online() (*eventStream, string) {
	ts.t.Helper()
	stream := ts.mustOpen("tetral_ck_a", "inst_a", "cred_a", nil)
	connectionID := stream.expect("connected")["connection_id"].(string)
	if answer := ts.post("ready", "tetral_ck_a", readyBody(connectionID, "ready_1")); answer.status != http.StatusOK {
		ts.t.Fatalf("ready: %d %s", answer.status, answer.body)
	}
	return stream, connectionID
}

// prepare issues a session.prepare for inst_a and returns its message_id.
func (ts *testServer) prepare(sessionID, bindingID string) string {
	ts.t.Helper()
	var issued struct {
		MessageID string `json:"message_id"`
	}
	ts.call("prepare", map[string]any{"instance_id": "inst_a", "payload": preparePayload(sessionID, bindingID)}, &issued)
	return issued.MessageID
}
