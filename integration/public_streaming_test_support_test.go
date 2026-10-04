package integration

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/auth"
	"github.com/tetral-ai/tetral/internal/blob"
	"github.com/tetral-ai/tetral/internal/eventwire"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	"github.com/tetral-ai/tetral/internal/testinfra"
	"github.com/tetral-ai/tetral/internal/workspace"
	eventstream "github.com/tetral-ai/tetral/services/event-stream"
	tetralsandbox "github.com/tetral-ai/tetral/services/sandbox"
)

// Only this adapter controls frame delivery, immediately before the real hub.
// Its ledger distinguishes broker deliveries from application deliveries.
type publicFrameTap struct {
	transport  eventstream.PreviewTransport
	mu         sync.Mutex
	frames     []eventwire.PreviewFrame
	deliveries int
	transform  func(eventwire.PreviewFrame, string, []byte) [][]byte
}

func (p *publicFrameTap) Subscribe(ctx context.Context, subject string, receive func(string, []byte), lost func(string)) (eventstream.PreviewSubscription, error) {
	return p.transport.Subscribe(ctx, subject, func(subject string, data []byte) {
		frame, err := eventwire.DecodePreviewFrame(subject, data)
		p.mu.Lock()
		if err == nil && len(p.frames) < 8192 {
			p.frames = append(p.frames, frame)
		}
		batches := [][]byte{data}
		if p.transform != nil && err == nil {
			batches = p.transform(frame, subject, data)
		}
		p.deliveries += len(batches)
		p.mu.Unlock()
		for _, body := range batches {
			receive(subject, body)
		}
	}, lost)
}
func (p *publicFrameTap) snapshot() []eventwire.PreviewFrame {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]eventwire.PreviewFrame(nil), p.frames...)
}
func (p *publicFrameTap) count() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.frames)
}

type publicReadObserver struct {
	eventstream.Reader
	polls  atomic.Int64
	finals atomic.Int64
}

func (r *publicReadObserver) ListSessionEventChanges(ctx context.Context, ws workspace.ID, session string, after int64, limit int) ([]eventstream.StreamChange, error) {
	result, err := r.Reader.ListSessionEventChanges(ctx, ws, session, after, limit)
	r.polls.Add(1)
	return result, err
}
func (r *publicReadObserver) ListThreadEventChanges(ctx context.Context, ws workspace.ID, session, thread string, after int64, limit int) ([]eventstream.StreamChange, error) {
	result, err := r.Reader.ListThreadEventChanges(ctx, ws, session, thread, after, limit)
	r.polls.Add(1)
	return result, err
}
func (r *publicReadObserver) ListRequestFinalMessages(ctx context.Context, scope eventstream.ReadScope, end string, after int64, limit int) ([]eventstream.RequestFinalMessage, error) {
	r.finals.Add(1)
	// A page of one is the required production final-body bound.
	return r.Reader.ListRequestFinalMessages(ctx, scope, end, after, limit)
}

type publicStreamingHarness struct {
	*contentE2E
	client  *contentSDKChild
	broker  testinfra.NATSFixture
	tap     *publicFrameTap
	metrics *eventstream.PreviewMetrics
	reader  *publicReadObserver
	config  eventstream.StreamConfig
}
type publicStreamingOptions struct {
	provider          tetralsandbox.ProviderAdapter
	eventsFactory     func(*testing.T, *storagetest.WorkloadDB, eventstream.Reader, *auth.InternalPrincipalVerifier, string) http.Handler
	broker            *testinfra.NATSFixture
	subscriberServers []string
	gateway           map[string]any
	approval          string
	transform         func(eventwire.PreviewFrame, string, []byte) [][]byte
	wrapWriter        func(http.Handler) http.Handler
	config            func(*eventstream.StreamConfig)
}

func newPublicStreamingHarness(t *testing.T, scenario string, options publicStreamingOptions) *publicStreamingHarness {
	t.Helper()
	var broker testinfra.NATSFixture
	var err error
	if options.broker != nil {
		broker = *options.broker
	} else {
		broker, err = testinfra.LoadNATSFixture()
		if err != nil {
			t.Fatal(err)
		}
	}
	h := &publicStreamingHarness{broker: broker, metrics: eventstream.NewPreviewMetrics(), config: eventstream.DefaultStreamConfig()}
	h.config.PollInterval = 5 * time.Millisecond
	if options.config != nil {
		options.config(&h.config)
	}
	subscriberServers := broker.Servers
	if options.subscriberServers != nil {
		subscriberServers = options.subscriberServers
	}
	transport, err := eventstream.NewNATSPreviewTransport(context.Background(), eventstream.NATSConfig{Servers: subscriberServers, UserPath: broker.Subscriber.UserPath, PasswordPath: broker.Subscriber.PasswordPath, CAPath: broker.Subscriber.TLS.CAPath, CertPath: broker.Subscriber.TLS.CertPath, KeyPath: broker.Subscriber.TLS.KeyPath}, h.config, h.metrics, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(transport.Close)
	h.tap = &publicFrameTap{transport: transport, transform: options.transform}
	hub, err := eventstream.NewPreviewHub(h.tap, h.config, h.metrics)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(hub.Close)
	gateway := map[string]any{"holdFinish": true, "holdPreviewFragments": true, "followupScenario": "done", "previewNats": publicPublisherConfig(broker)}
	for key, value := range options.gateway {
		gateway[key] = value
	}
	runtime := map[string]any{}
	if options.approval != "" {
		runtime["approvalMode"] = options.approval
	}
	h.contentE2E = startContentE2EWithOptions(t, scenario, false, false, contentE2EOptions{Budget: 300 * time.Second, Gateway: gateway, Runtime: runtime, Provider: options.provider, ApprovalMode: options.approval, PublicEdge: func(t *testing.T, pools *storagetest.WorkloadDB, objects blob.BlobStore) (string, string) {
		return startContentSDKPublicEdgeWithEvents(t, pools, objects, func(reader eventstream.Reader, verifier *auth.InternalPrincipalVerifier, publicKey string) http.Handler {
			if options.eventsFactory != nil {
				return options.eventsFactory(t, pools, reader, verifier, publicKey)
			}
			h.reader = &publicReadObserver{Reader: reader}
			handler := eventstream.NewRouter(h.reader, verifier, eventstream.WithPreviewHub(hub), eventstream.WithPreviewMetrics(h.metrics), eventstream.WithStreamConfig(h.config))
			if options.wrapWriter != nil {
				handler = options.wrapWriter(handler)
			}
			return handler
		})
	}})
	h.client = startPublicStreamingSDK(t, h.baseURL, h.apiKey)
	// LIFO closes all active readers before httptest waits for active handlers.
	t.Cleanup(func() { h.client.control(t, "close", nil) })
	return h
}
func publicPublisherConfig(b testinfra.NATSFixture) map[string]any {
	result := map[string]any{"servers": b.Servers, "userPath": b.Publisher.UserPath, "passwordPath": b.Publisher.PasswordPath, "policy": map[string]any{"queueBytes": 4 * 1024 * 1024, "queueFrames": 1024, "batchBytes": 256 * 1024, "batchFrames": 64, "connectTimeoutMs": 1000, "flushTimeoutMs": 1000, "retryMaxMs": 5000, "credentialPollMs": 250, "pingIntervalMs": 1000, "maxPingOut": 1}}
	if b.Publisher.TLS.CAPath != "" {
		result["tls"] = map[string]any{"caPath": b.Publisher.TLS.CAPath, "certPath": b.Publisher.TLS.CertPath, "keyPath": b.Publisher.TLS.KeyPath}
	}
	return result
}
func startPublicStreamingSDK(t *testing.T, baseURL, key string) *contentSDKChild {
	t.Helper()
	sdk := os.Getenv("TETRAL_ENGINE_SDK_ROOT")
	if sdk == "" {
		t.Fatal("public streaming requires the declared immutable SDK checkout")
	}
	sdk, err := filepath.Abs(sdk)
	if err != nil {
		t.Fatal(err)
	}
	directory := t.TempDir()
	source, err := os.ReadFile("testdata/public-streaming-client.ts")
	if err != nil {
		t.Fatal(err)
	}
	// #nosec G703 -- fixed fixture basename under the test-owned private temporary directory.
	if err = os.WriteFile(filepath.Join(directory, "public-streaming-client.ts"), source, 0600); err != nil {
		t.Fatal(err)
	}
	if err = os.Symlink(sdk, filepath.Join(directory, "sdk")); err != nil {
		t.Fatal(err)
	}
	bootstrap, err := json.Marshal(map[string]string{"baseURL": baseURL, "apiKey": key})
	if err != nil {
		t.Fatal(err)
	}
	config := filepath.Join(directory, "bootstrap.json")
	if err = os.WriteFile(config, bootstrap, 0600); err != nil {
		t.Fatal(err)
	}
	child := &contentSDKChild{lines: make(chan []byte, 8), joined: make(chan error, 1)}
	child.command = exec.Command("bun", filepath.Join(directory, "public-streaming-client.ts"), config) //nolint:gosec // Run-owned fixture and private credential path.
	child.command.Dir = directory
	child.command.Stderr = &child.output
	child.input, err = child.command.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	stdout, err := child.command.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err = child.command.Start(); err != nil {
		t.Fatal(err)
	}
	go func() {
		scanner := bufio.NewScanner(stdout)
		scanner.Buffer(make([]byte, 4096), 72*1024*1024)
		for scanner.Scan() {
			child.lines <- append([]byte(nil), scanner.Bytes()...)
		}
		close(child.lines)
	}()
	go func() { child.joined <- child.command.Wait() }()
	t.Cleanup(func() { child.stop(t) })
	return child
}

type publicSDKSnapshot struct {
	Events     []map[string]any `json:"events"`
	Fields     []string         `json:"fieldNames"`
	Heartbeats int              `json:"heartbeats"`
	Ended      bool             `json:"ended"`
	Error      *string          `json:"error"`
}

func (h *publicStreamingHarness) open(t *testing.T, name string, deltas []string, thread string) {
	t.Helper()
	args := map[string]any{"viewer": name, "sessionId": h.session}
	if deltas != nil {
		args["eventDeltas"] = deltas
	}
	if thread != "" {
		args["threadId"] = thread
	}
	raw := h.client.control(t, "open", args)
	var opened struct {
		Requests []struct {
			Path  string      `json:"path"`
			Query [][2]string `json:"query"`
		} `json:"requests"`
	}
	if json.Unmarshal(raw, &opened) != nil || len(opened.Requests) != 1 {
		t.Fatal("SDK did not expose one exact public request")
	}
	var observed []string
	beta := false
	for _, pair := range opened.Requests[0].Query {
		if pair[0] == "event_deltas[]" {
			observed = append(observed, pair[1])
		}
		if pair[0] == "beta" && pair[1] == "true" {
			beta = true
		}
	}
	if !beta || strings.Join(observed, ",") != strings.Join(deltas, ",") {
		t.Fatalf("SDK query contract mismatch: %v", opened.Requests[0].Query)
	}
}
func (h *publicStreamingHarness) snapshot(t *testing.T, name string) publicSDKSnapshot {
	t.Helper()
	return decodePublicSnapshot(t, h.client.control(t, "snapshot", map[string]any{"viewer": name}))
}
func (h *publicStreamingHarness) waitEvent(t *testing.T, name, eventType string, count int) publicSDKSnapshot {
	t.Helper()
	return decodePublicSnapshot(t, h.client.control(t, "wait_event", map[string]any{"viewer": name, "eventType": eventType, "count": count}))
}
func decodePublicSnapshot(t *testing.T, raw []byte) publicSDKSnapshot {
	t.Helper()
	var result publicSDKSnapshot
	if json.Unmarshal(raw, &result) != nil || result.Error != nil {
		t.Fatal("SDK stream capture failed")
	}
	return result
}
func (h *publicStreamingHarness) send(t *testing.T) {
	t.Helper()
	h.client.control(t, "send", map[string]any{"sessionId": h.session, "text": "fixed public streaming input"})
}
func (h *publicStreamingHarness) releaseFragments(t *testing.T, count int) {
	t.Helper()
	for i := 0; i < count; i++ {
		h.gateway.control(t, map[string]any{"kind": "release_fragment"}, "released_fragment")
	}
}
func (h *publicStreamingHarness) finish(t *testing.T) {
	t.Helper()
	h.gateway.control(t, map[string]any{"kind": "release_finish"}, "released_finish")
}
func publicWait(t *testing.T, description string, predicate func() bool) {
	t.Helper()
	deadline := time.NewTimer(30 * time.Second)
	defer deadline.Stop()
	tick := time.NewTicker(5 * time.Millisecond)
	defer tick.Stop()
	for {
		if predicate() {
			return
		}
		select {
		case <-deadline.C:
			t.Fatalf("deadline awaiting %s", description)
		case <-tick.C:
		}
	}
}
func (h *publicStreamingHarness) waitFragments(t *testing.T) {
	t.Helper()
	publicWait(t, "external provider fragment barrier", func() bool {
		raw := h.gateway.control(t, map[string]any{"kind": "observe"}, "observation")
		var count int
		_ = json.Unmarshal(raw["fragmentWaiting"], &count)
		return count > 0
	})
}
func (h *publicStreamingHarness) thread(t *testing.T) string {
	t.Helper()
	var value string
	if err := h.db.QueryRow(`SELECT main_thread_id FROM sessions WHERE id=$1`, h.session).Scan(&value); err != nil {
		t.Fatal(err)
	}
	return value
}
func (h *publicStreamingHarness) metric(t *testing.T, name string) float64 {
	t.Helper()
	values, err := h.metrics.Collector()(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for _, value := range values {
		if value.Name == name {
			return value.Value
		}
	}
	t.Fatalf("missing instrument %s", name)
	return 0
}
func publicEventID(event map[string]any) string   { id, _ := event["id"].(string); return id }
func publicEventType(event map[string]any) string { kind, _ := event["type"].(string); return kind }
func publicText(event map[string]any) string {
	content, _ := event["content"].([]any)
	var text strings.Builder
	for _, raw := range content {
		part, _ := raw.(map[string]any)
		if part["type"] == "text" {
			value, _ := part["text"].(string)
			text.WriteString(value)
		}
	}
	return text.String()
}
func publicLogAssertion(t *testing.T, name string) {
	t.Helper()
	t.Logf("public_streaming_sdk_assertion=%s passed=true", name)
}
func (h *publicStreamingHarness) assertFormal(t *testing.T, snapshot publicSDKSnapshot, expected []string) {
	t.Helper()
	var texts []string
	var messageIDs []string
	endIndex := -1
	lastMessage := -1
	for index, event := range snapshot.Events {
		switch publicEventType(event) {
		case "agent.message":
			texts = append(texts, publicText(event))
			messageIDs = append(messageIDs, publicEventID(event))
			lastMessage = index
		case "span.model_request_end":
			endIndex = index
		}
	}
	if fmt.Sprint(texts) != fmt.Sprint(expected) || endIndex <= lastMessage || endIndex < 0 {
		t.Fatalf("formal message/End order differs: texts=%q expected=%q end=%d last=%d", texts, expected, endIndex, lastMessage)
	}
	for _, id := range messageIDs {
		var count int
		if err := h.db.QueryRow(`SELECT count(*) FROM session_events WHERE session_id=$1 AND event_id=$2 AND type='agent.message'`, h.session, id).Scan(&count); err != nil || count != 1 {
			t.Fatal("SDK formal identity lacks one original SQL event")
		}
	}
	for _, name := range snapshot.Fields {
		if name != "event" && name != "data" {
			t.Fatalf("unexpected SSE field %q", name)
		}
	}
	publicLogAssertion(t, "formal-original-identities-content-end-order")
}
