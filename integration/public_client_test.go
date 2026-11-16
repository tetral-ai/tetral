package integration

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"maps"
	"mime"
	"mime/multipart"
	"net/http"
	"net/textproto"
	"net/url"
	"os"
	"slices"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/integration/enginetest"
)

// publicClient is client, http.DefaultClient when nil, with every request it
// sends to a public route, and that request's answer, held to Engine's
// OpenAPI file.
func publicClient(t testing.TB, client *http.Client) *http.Client {
	if client == nil {
		client = http.DefaultClient
	}
	held := *client
	base := held.Transport
	if base == nil {
		base = http.DefaultTransport
	}
	held.Transport = enginetest.NewDeveloper(t).Transport(base)
	return &held
}

// publicDeveloper is a developer at a public edge: an API key, and a client
// that holds every request, answer and stream event to Engine's OpenAPI
// file. Its viewers are the event streams it holds open, by name.
type publicDeveloper struct {
	client    *enginetest.Developer
	transport *http.Transport

	mu      sync.Mutex
	viewers map[string]*publicViewer
}

// newPublicDeveloper returns a developer of the edge at baseURL with key. A
// CA path makes it trust that authority alone.
func newPublicDeveloper(t *testing.T, baseURL, key string, caPaths ...string) *publicDeveloper {
	t.Helper()
	client := enginetest.NewDeveloper(t)
	client.BaseURL = baseURL
	client.Authorize = func(request *http.Request) error {
		request.Header.Set("X-Api-Key", key)
		return nil
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	if len(caPaths) != 0 && caPaths[0] != "" {
		authority, err := os.ReadFile(caPaths[0])
		roots := x509.NewCertPool()
		if err != nil || !roots.AppendCertsFromPEM(authority) {
			t.Fatalf("the edge's CA at %s: %v", caPaths[0], err)
		}
		transport.TLSClientConfig = &tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12}
	}
	client.HTTP = &http.Client{Transport: transport}
	d := &publicDeveloper{client: client, transport: transport, viewers: map[string]*publicViewer{}}
	t.Cleanup(d.closeIdleConnections)
	t.Cleanup(d.close)
	return d
}

// closeIdleConnections closes the connections no request of d is using.
func (d *publicDeveloper) closeIdleConnections() { d.transport.CloseIdleConnections() }

// request sends one JSON request and returns the body of its 2xx answer.
func (d *publicDeveloper) request(t *testing.T, method, path string, body any) json.RawMessage {
	t.Helper()
	status, answer := d.answer(t, method, path, body)
	if status/100 != 2 {
		t.Fatalf("%s %s = %d %s", method, path, status, answer)
	}
	return answer
}

// object is request with the answer decoded as a JSON object.
func (d *publicDeveloper) object(t *testing.T, method, path string, body any) map[string]any {
	t.Helper()
	var object map[string]any
	if err := json.Unmarshal(d.request(t, method, path, body), &object); err != nil {
		t.Fatalf("%s %s answered no JSON object: %v", method, path, err)
	}
	return object
}

// refusal sends one JSON request that Engine must refuse with status, and
// returns the error's type and message. outside marks a request the OpenAPI
// file does not allow.
func (d *publicDeveloper) refusal(t *testing.T, method, path string, body any, outside bool, status int) (errorType, message string) {
	t.Helper()
	client := d.client
	if outside {
		client = client.Invalid()
	}
	got, answer := d.do(t, client, method, path, body)
	var refusal struct {
		Error struct {
			Type    string `json:"type"`
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := json.Unmarshal(answer, &refusal); err != nil || got != status {
		t.Fatalf("%s %s = %d %s; want a refusal with %d", method, path, got, answer, status)
	}
	return refusal.Error.Type, refusal.Error.Message
}

// answer sends one JSON request and returns its answer's status and body.
func (d *publicDeveloper) answer(t *testing.T, method, path string, body any) (int, []byte) {
	t.Helper()
	return d.do(t, d.client, method, path, body)
}

func (d *publicDeveloper) do(t *testing.T, client *enginetest.Developer, method, path string, body any) (int, []byte) {
	t.Helper()
	var encoded []byte
	header := http.Header{}
	if body != nil {
		var err error
		if encoded, err = json.Marshal(body); err != nil {
			t.Fatal(err)
		}
		header.Set("Content-Type", "application/json")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	response, err := client.Do(ctx, method, path, header, encoded)
	if err != nil {
		t.Fatalf("%s %s: %v", method, path, err)
	}
	return response.Status, response.Body
}

// formPart is one part of a multipart request: a field, or a file when it
// has a filename.
type formPart struct {
	name, filename, contentType, content string
}

// form sends parts as one multipart request and returns the answer decoded
// as a JSON object.
func (d *publicDeveloper) form(t *testing.T, path string, parts ...formPart) map[string]any {
	t.Helper()
	var body bytes.Buffer
	form := multipart.NewWriter(&body)
	for _, part := range parts {
		header := textproto.MIMEHeader{"Content-Disposition": {mime.FormatMediaType("form-data", map[string]string{"name": part.name})}}
		if part.filename != "" {
			header.Set("Content-Disposition", mime.FormatMediaType("form-data", map[string]string{"name": part.name, "filename": part.filename}))
			header.Set("Content-Type", part.contentType)
		}
		writer, err := form.CreatePart(header)
		if err == nil {
			_, err = writer.Write([]byte(part.content))
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	if err := form.Close(); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	response, err := d.client.Do(ctx, http.MethodPost, path, http.Header{"Content-Type": {form.FormDataContentType()}}, body.Bytes())
	var object map[string]any
	if err != nil || response.Status/100 != 2 || json.Unmarshal(response.Body, &object) != nil {
		t.Fatalf("POST %s = %d %s, %v", path, response.Status, response.Body, err)
	}
	return object
}

// provision creates agent and a Session of it in the Environment, and
// returns both as {"agent", "session"}.
func (d *publicDeveloper) provision(t *testing.T, environmentID string, agent map[string]any, vaultIDs ...string) json.RawMessage {
	t.Helper()
	created := d.request(t, http.MethodPost, "/v1/agents?beta=true", agent)
	var identity struct {
		ID      string `json:"id"`
		Version int    `json:"version"`
	}
	if err := json.Unmarshal(created, &identity); err != nil {
		t.Fatal(err)
	}
	if vaultIDs == nil {
		vaultIDs = []string{}
	}
	session := d.request(t, http.MethodPost, "/v1/sessions?beta=true", map[string]any{
		"agent": map[string]any{"type": "agent", "id": identity.ID, "version": identity.Version}, "environment_id": environmentID, "vault_ids": vaultIDs})
	both, err := json.Marshal(map[string]json.RawMessage{"agent": created, "session": session})
	if err != nil {
		t.Fatal(err)
	}
	return both
}

func (d *publicDeveloper) sendEvent(t *testing.T, sessionID string, event map[string]any) json.RawMessage {
	t.Helper()
	return d.request(t, http.MethodPost, "/v1/sessions/"+sessionID+"/events?beta=true", map[string]any{"events": []any{event}})
}

// send sends a user message of text, with the image file when one is given.
func (d *publicDeveloper) send(t *testing.T, sessionID, text string, fileIDs ...string) json.RawMessage {
	t.Helper()
	content := []any{map[string]any{"type": "text", "text": text}}
	for _, fileID := range fileIDs {
		content = append(content, map[string]any{"type": "image", "source": map[string]any{"type": "file", "file_id": fileID}})
	}
	return d.sendEvent(t, sessionID, map[string]any{"type": "user.message", "content": content})
}

// confirm answers the tool call with allow or deny.
func (d *publicDeveloper) confirm(t *testing.T, sessionID, toolUseEventID, result string) json.RawMessage {
	t.Helper()
	return d.sendEvent(t, sessionID, map[string]any{"type": "user.tool_confirmation", "tool_use_id": toolUseEventID, "result": result})
}

func (d *publicDeveloper) interrupt(t *testing.T, sessionID string) json.RawMessage {
	t.Helper()
	return d.sendEvent(t, sessionID, map[string]any{"type": "user.interrupt"})
}

func (d *publicDeveloper) session(t *testing.T, sessionID string) json.RawMessage {
	t.Helper()
	return d.request(t, http.MethodGet, "/v1/sessions/"+sessionID+"?beta=true", nil)
}

// list reads one page of the Session's events, or of the thread's when
// threadID is set; order and page are sent when not empty.
func (d *publicDeveloper) list(t *testing.T, sessionID, threadID string, limit int, order, page string) publicProjectionPage {
	t.Helper()
	path := "/v1/sessions/" + sessionID
	if threadID != "" {
		path += "/threads/" + threadID
	}
	query := url.Values{"beta": {"true"}, "limit": {strconv.Itoa(limit)}}
	if order != "" {
		query.Set("order", order)
	}
	if page != "" {
		query.Set("page", page)
	}
	var result publicProjectionPage
	if err := json.Unmarshal(d.request(t, http.MethodGet, path+"/events?"+query.Encode(), nil), &result); err != nil {
		t.Fatal(err)
	}
	return result
}

// events reads every event of the Session, page by page, as one JSON array.
func (d *publicDeveloper) events(t *testing.T, sessionID string) json.RawMessage {
	t.Helper()
	events := []map[string]any{}
	for page := ""; ; {
		listed := d.list(t, sessionID, "", 100, "", page)
		if events = append(events, listed.Data...); len(events) > 1000 {
			t.Fatalf("Session %s lists more than 1000 events", sessionID)
		}
		if listed.Next == nil {
			break
		}
		page = *listed.Next
	}
	encoded, err := json.Marshal(events)
	if err != nil {
		t.Fatal(err)
	}
	return encoded
}

// uploadPNG uploads a 1x1 PNG as a file and returns the file object.
func (d *publicDeveloper) uploadPNG(t *testing.T) json.RawMessage {
	t.Helper()
	png, err := base64.StdEncoding.DecodeString("iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR4nGP4z8DwHwAFAAH/iZk9HQAAAABJRU5ErkJggg==")
	if err != nil {
		t.Fatal(err)
	}
	file, err := json.Marshal(d.form(t, "/v1/files?beta=true", formPart{"file", "content-lifecycle-pixel.png", "image/png", string(png)}))
	if err != nil {
		t.Fatal(err)
	}
	return file
}

// publicViewer is one open event stream and what it has carried.
type publicViewer struct {
	stream *enginetest.DeveloperStream
	cancel context.CancelFunc
	done   chan struct{}

	mu     sync.Mutex
	events []map[string]any
	ended  bool
	// failure is why the stream ended when neither Engine nor the test
	// closed it in order.
	failure error
}

// publicSnapshot is what a viewer has read so far: each event's data, the
// comment lines counted as heartbeats, and whether the stream has ended.
type publicSnapshot struct {
	Events     []map[string]any
	Heartbeats int
	Ended      bool
}

// open opens the Session's event stream as viewer name, with previews of
// deltas, or the thread's stream when threadID is set. It returns once the
// stream's answer has begun.
func (d *publicDeveloper) open(t *testing.T, name, sessionID string, deltas []string, threadID string) {
	t.Helper()
	path, query := "/v1/sessions/"+sessionID+"/events/stream", url.Values{"beta": {"true"}}
	if threadID != "" {
		path = "/v1/sessions/" + sessionID + "/threads/" + threadID + "/stream"
	}
	for _, delta := range deltas {
		query.Add("event_deltas[]", delta)
	}
	ctx, cancel := context.WithCancel(context.Background())
	stream, response, err := d.client.Stream(ctx, http.MethodGet, path+"?"+query.Encode(), nil)
	if err != nil || stream == nil {
		cancel()
		t.Fatalf("stream %s = %d %s, %v", path, response.Status, response.Body, err)
	}
	viewer := &publicViewer{stream: stream, cancel: cancel, done: make(chan struct{})}
	d.mu.Lock()
	_, reused := d.viewers[name]
	d.viewers[name] = viewer
	d.mu.Unlock()
	if reused {
		t.Fatalf("viewer %s is open already", name)
	}
	go viewer.read(ctx)
}

func (v *publicViewer) read(ctx context.Context) {
	defer close(v.done)
	for {
		event, err := v.stream.Next()
		v.mu.Lock()
		if err != nil {
			if v.ended = true; err != io.EOF && ctx.Err() == nil {
				v.failure = err
			}
			v.mu.Unlock()
			return
		}
		var data map[string]any
		if err := json.Unmarshal(event.Data, &data); err != nil {
			v.failure = fmt.Errorf("stream event %s holds no JSON object: %w", event.Name, err)
		}
		v.events = append(v.events, data)
		v.mu.Unlock()
	}
}

func (d *publicDeveloper) viewer(t *testing.T, name string) *publicViewer {
	t.Helper()
	d.mu.Lock()
	defer d.mu.Unlock()
	viewer := d.viewers[name]
	if viewer == nil {
		t.Fatalf("no viewer %s", name)
	}
	return viewer
}

// snapshot returns what viewer name has read so far.
func (d *publicDeveloper) snapshot(t *testing.T, name string) publicSnapshot {
	t.Helper()
	viewer := d.viewer(t, name)
	viewer.mu.Lock()
	defer viewer.mu.Unlock()
	if viewer.failure != nil {
		t.Fatalf("viewer %s: %v", name, viewer.failure)
	}
	return publicSnapshot{Events: slices.Clone(viewer.events), Heartbeats: viewer.stream.Comments(), Ended: viewer.ended}
}

// waitEvent waits until viewer name has read count events of eventType and
// returns what it has read by then.
func (d *publicDeveloper) waitEvent(t *testing.T, name, eventType string, count int) publicSnapshot {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for {
		snapshot := d.snapshot(t, name)
		switch {
		case countPublicEvents(snapshot, eventType) >= count:
			return snapshot
		case snapshot.Ended:
			t.Fatalf("viewer %s ended before %d %s events", name, count, eventType)
		case time.Now().After(deadline):
			t.Fatalf("deadline awaiting %d %s events on viewer %s", count, eventType, name)
		}
		time.Sleep(2 * time.Millisecond)
	}
}

// closeViewer closes viewer name's stream and returns all it read.
func (d *publicDeveloper) closeViewer(t *testing.T, name string) publicSnapshot {
	t.Helper()
	viewer := d.viewer(t, name)
	viewer.end()
	return d.snapshot(t, name)
}

func (v *publicViewer) end() {
	v.cancel()
	_ = v.stream.Close()
	<-v.done
}

// close closes every viewer.
func (d *publicDeveloper) close() {
	d.mu.Lock()
	viewers := slices.Collect(maps.Values(d.viewers))
	d.mu.Unlock()
	for _, viewer := range viewers {
		viewer.end()
	}
}
