package integration

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/tetral-ai/tetral/internal/auth"
	"github.com/tetral-ai/tetral/internal/blob"
	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/environment"
	"github.com/tetral-ai/tetral/internal/eventwire"
	"github.com/tetral-ai/tetral/internal/id"
	"github.com/tetral-ai/tetral/internal/runtimecontrol"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	"github.com/tetral-ai/tetral/internal/testinfra"
	"github.com/tetral-ai/tetral/internal/workspace"
	bridge "github.com/tetral-ai/tetral/services/bridge"
	bridgev1 "github.com/tetral-ai/tetral/services/bridge/gen/tetral/bridge/v1"
	eventstream "github.com/tetral-ai/tetral/services/event-stream"
)

// Projection tests drive legal transactions through the actual authenticated
// Bridge, with no Runtime concurrently consuming their manually controlled turn.
// The only invented infrastructure fact is the discovered Runtime binding.
type publicProjectionFixture struct {
	*publicStreamingHarness
	pools      *storagetest.WorkloadDB
	bridge     bridgev1.AgentRuntimeBridgeServiceClient
	ctx        context.Context
	scope      *bridgev1.RuntimeScope
	baseReader eventstream.Reader
	publisher  *nats.Conn
	lostEnd    atomic.Bool
	vectors    publicStreamingVectors
}

type publicProjectionOptions struct {
	reader     func(eventstream.Reader) eventstream.Reader
	transport  func(eventstream.PreviewTransport) eventstream.PreviewTransport
	transform  func(eventwire.PreviewFrame, string, []byte) [][]byte
	config     func(*eventstream.StreamConfig)
	wrapWriter func(http.Handler) http.Handler
	publicEdge func(*testing.T, *storagetest.WorkloadDB, blob.BlobStore, func(eventstream.Reader, *auth.InternalPrincipalVerifier, string) http.Handler) (string, string, string)
}

type publicProjectionRequest struct {
	scope           *bridgev1.RuntimeScope
	id, start, kind string
	sequence        *int64
	tools           []string
}

// Go and Bun consume this one versioned vector source. Literal controls below
// remain independent of the encoder, producer and document under test.
type publicStreamingVectors struct {
	Version int `json:"version"`
	Content struct {
		FirstFragments     []string `json:"first_fragments"`
		FirstComplete      string   `json:"first_complete"`
		SecondFragments    []string `json:"second_fragments"`
		SecondComplete     string   `json:"second_complete"`
		Incomplete         string   `json:"incomplete"`
		ReasoningText      string   `json:"reasoning_text"`
		ReasoningSignature string   `json:"reasoning_signature"`
		ToolInputMarker    string   `json:"tool_input_marker"`
	} `json:"content"`
}

func loadPublicStreamingVectors(t *testing.T) publicStreamingVectors {
	t.Helper()
	raw, err := os.ReadFile("testdata/public-streaming.json")
	if err != nil {
		t.Fatal(err)
	}
	var vectors publicStreamingVectors
	if json.Unmarshal(raw, &vectors) != nil || vectors.Version != 1 {
		t.Fatal("invalid shared public-streaming vector version")
	}
	if strings.Join(vectors.Content.FirstFragments, "|") != "alpha |βeta |omega\n" || vectors.Content.FirstComplete != "alpha βeta omega\n" || strings.Join(vectors.Content.SecondFragments, "|") != "second\n" || vectors.Content.SecondComplete != "second\n" || vectors.Content.Incomplete != "unfinished must never become a final" || vectors.Content.ReasoningText != "private-reasoning-marker" || vectors.Content.ReasoningSignature != "private-provider-signature-marker" || vectors.Content.ToolInputMarker != "private-tool-input-marker" {
		t.Fatal("shared content vector differs from independent fixed contract")
	}
	return vectors
}

func newPublicProjectionFixture(t *testing.T, options publicProjectionOptions) *publicProjectionFixture {
	t.Helper()
	_, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	pools := storagetest.OpenWorkloadDB(t, admin, "bridge")
	broker, err := testinfra.LoadNATSFixture()
	if err != nil {
		t.Fatal(err)
	}
	h := &publicStreamingHarness{broker: broker, metrics: eventstream.NewPreviewMetrics(), config: eventstream.DefaultStreamConfig()}
	h.config.PollInterval = 5 * time.Millisecond
	if options.config != nil {
		options.config(&h.config)
	}
	f := &publicProjectionFixture{publicStreamingHarness: h, pools: pools, vectors: loadPublicStreamingVectors(t)}
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	t.Cleanup(cancel)
	f.ctx = metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer projection-runtime-token")
	transport, err := eventstream.NewNATSPreviewTransport(ctx, eventstream.NATSConfig{Servers: broker.Servers, UserPath: broker.Subscriber.UserPath, PasswordPath: broker.Subscriber.PasswordPath, CAPath: broker.Subscriber.TLS.CAPath, CertPath: broker.Subscriber.TLS.CertPath, KeyPath: broker.Subscriber.TLS.KeyPath}, h.config, h.metrics, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(transport.Close)
	var receiver eventstream.PreviewTransport = transport
	if options.transport != nil {
		receiver = options.transport(receiver)
	}
	h.tap = &publicFrameTap{transport: receiver, transform: options.transform}
	hub, err := eventstream.NewPreviewHub(h.tap, h.config, h.metrics)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(hub.Close)
	edgeFactory := options.publicEdge
	if edgeFactory == nil {
		edgeFactory = func(t *testing.T, pools *storagetest.WorkloadDB, objects blob.BlobStore, events func(eventstream.Reader, *auth.InternalPrincipalVerifier, string) http.Handler) (string, string, string) {
			base, key := startContentSDKPublicEdgeWithEvents(t, pools, objects, events)
			return base, key, ""
		}
	}
	base, key, caPath := edgeFactory(t, pools, nil, func(reader eventstream.Reader, verifier *auth.InternalPrincipalVerifier, _ string) http.Handler {
		f.baseReader = reader
		if options.reader != nil {
			reader = options.reader(reader)
		}
		h.reader = &publicReadObserver{Reader: reader}
		handler := eventstream.NewRouter(h.reader, verifier, eventstream.WithPreviewHub(hub), eventstream.WithPreviewMetrics(h.metrics), eventstream.WithStreamConfig(h.config))
		if options.wrapWriter != nil {
			handler = options.wrapWriter(handler)
		}
		return handler
	})
	h.contentE2E = &contentE2E{db: admin, baseURL: base, apiKey: key}
	provisioner := startContentSDKChildContext(ctx, t, base, key, caPath)
	env, err := environment.NewPostgreSQLEnvironmentStore(dbconnect.NewClientForTesting(pools.OpenWorkload(t, "api", nil)), environment.WithDefaultArtifactRef("artifact_projection")).Create(ctx, workspace.DefaultID, environment.CreateEnvironmentRequest{Name: "projection"})
	if err != nil {
		t.Fatal(err)
	}
	created := provisioner.control(t, "provision", map[string]any{"environmentId": env.ID, "agent": map[string]any{"name": "projection", "model": "anthropic/claude-opus-4-8", "approval_mode": "full_access", "tools": []any{map[string]any{"type": "tetral_agent_toolset", "family": "claude"}}, "skills": []any{}, "metadata": map[string]any{}}})
	h.sdk = provisioner
	h.environmentID = env.ID
	h.approvalMode = "full_access"
	var result struct {
		Session struct {
			ID string `json:"id"`
		} `json:"session"`
	}
	if json.Unmarshal(created, &result) != nil || result.Session.ID == "" {
		t.Fatal("actual SDK provision lacked Session")
	}
	h.session = result.Session.ID
	binding, pod := id.New("bind_"), id.New("pod_")
	seedBridgeAPIRuntimeBinding(t, admin, "default", h.session, binding, 1, pod)
	f.scope = bridgeAPIScope(h.session, h.thread(t), binding, 1, pod)
	store := bridge.NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(pools.DB))
	store.RuntimeBindingTokenHMACKey = []byte("projection-test-binding-hmac-key-32")
	endpoint := serveContentBridge(t, store, map[string]string{"projection-runtime-token": pod}, func(_ context.Context, method string, response any) error {
		if method == bridgev1.AgentRuntimeBridgeService_WriteRequestEnd_FullMethodName && f.lostEnd.CompareAndSwap(true, false) {
			if ended, ok := response.(*bridgev1.WriteRequestEndResponse); ok && ended.GetCommitted() != nil {
				return status.Error(codes.Unavailable, "controlled response loss after actual commit")
			}
		}
		return nil
	})
	f.bridge = endpoint.Client
	h.client = startPublicStreamingSDK(t, base, key, caPath)
	t.Cleanup(func() { h.client.control(t, "close", nil) })
	f.publisher = publicProjectionPublisher(t, broker)
	return f
}

func publicProjectionPublisher(t *testing.T, broker testinfra.NATSFixture) *nats.Conn {
	t.Helper()
	user, err := os.ReadFile(broker.Publisher.UserPath)
	if err != nil {
		t.Fatal(err)
	}
	password, err := os.ReadFile(broker.Publisher.PasswordPath)
	if err != nil {
		t.Fatal(err)
	}
	options := []nats.Option{nats.UserInfo(strings.TrimSpace(string(user)), strings.TrimSpace(string(password))), nats.NoReconnect(), nats.Timeout(time.Second), nats.Name("projection-controlled-external-frames")}
	if broker.Publisher.TLS.CAPath != "" {
		ca, err := os.ReadFile(broker.Publisher.TLS.CAPath)
		if err != nil {
			t.Fatal(err)
		}
		roots := x509.NewCertPool()
		if !roots.AppendCertsFromPEM(ca) {
			t.Fatal("invalid fixture CA")
		}
		cert, err := tls.LoadX509KeyPair(broker.Publisher.TLS.CertPath, broker.Publisher.TLS.KeyPath)
		if err != nil {
			t.Fatal(err)
		}
		options = append(options, nats.Secure(&tls.Config{MinVersion: tls.VersionTLS12, RootCAs: roots, Certificates: []tls.Certificate{cert}}), nats.TLSHandshakeFirst())
	}
	connection, err := nats.Connect(strings.Join(broker.Servers, ","), options...)
	if err != nil {
		t.Fatal("actual NATS controlled publisher failed")
	}
	t.Cleanup(connection.Close)
	return connection
}

func (f *publicProjectionFixture) start(t *testing.T, thread, kind string) *publicProjectionRequest {
	t.Helper()
	scope := proto.Clone(f.scope).(*bridgev1.RuntimeScope)
	if thread != "" {
		scope.SessionThreadId = thread
	}
	if kind == "" {
		kind = runtimecontrol.RequestKindAgentProviderRequest
	}
	request := &publicProjectionRequest{scope: scope, id: id.New("mreq_"), kind: kind}
	var boundary int64
	if err := f.db.QueryRow(`SELECT COALESCE(max(sequence),0) FROM session_messages WHERE workspace_id=$1 AND session_id=$2 AND session_thread_id=$3`, scope.WorkspaceId, scope.SessionId, scope.SessionThreadId).Scan(&boundary); err != nil {
		t.Fatal(err)
	}
	response, err := f.bridge.WriteEvent(f.ctx, &bridgev1.WriteEventRequest{Scope: scope, RuntimeWriteId: id.New("rwrite_"), ModelRequestId: request.id, EventType: "span.model_request_start", PayloadJson: fmt.Sprintf(`{"type":"span.model_request_start","model_request_id":%q}`, request.id), ContextThroughMessageSequence: &boundary, RequestKind: kind})
	if err != nil || response.GetCommitted() == nil {
		t.Fatalf("actual Bridge Start: %v", err)
	}
	request.start = response.GetCommitted().GetEventId()
	return request
}

func (f *publicProjectionFixture) text(t *testing.T, request *publicProjectionRequest, text string, reasoning bool) string {
	t.Helper()
	event := publicProjectionContentID()
	parts := []*bridgev1.RuntimeContextPart{}
	if reasoning {
		metadata, err := json.Marshal(map[string]any{"anthropic": map[string]string{"signature": f.vectors.Content.ReasoningSignature}})
		if err != nil {
			t.Fatal(err)
		}
		parts = append(parts, &bridgev1.RuntimeContextPart{Content: &bridgev1.RuntimeContextPart_Reasoning{Reasoning: &bridgev1.RuntimeContextReasoning{Text: f.vectors.Content.ReasoningText, ProviderMetadataJson: bridgeString(string(metadata))}}})
	}
	parts = append(parts, bridgeTextContextDeltaForTest(text).Parts...)
	return f.textWithID(t, request, event, text, parts)
}

// The producer case commits the actual Gateway allocation and assembled context
// through the same Bridge writer as the controlled legal-fact cases.
func (f *publicProjectionFixture) textWithID(t *testing.T, request *publicProjectionRequest, event, text string, parts []*bridgev1.RuntimeContextPart) string {
	t.Helper()
	payload, err := json.Marshal(map[string]any{"type": "agent.message", "content": []any{map[string]any{"type": "text", "text": text}}})
	if err != nil {
		t.Fatal(err)
	}
	response, err := f.bridge.WriteEvent(f.ctx, &bridgev1.WriteEventRequest{Scope: request.scope, RuntimeWriteId: id.New("rwrite_"), ModelRequestId: request.id, EventType: "agent.message", PayloadJson: string(payload), PreallocatedEventId: &event, AssistantContextDelta: &bridgev1.RuntimeContextDelta{Parts: parts}})
	if err != nil || response.GetCommitted().GetEventId() != event {
		t.Fatalf("actual Bridge complete text: %v", err)
	}
	sequence := response.GetCommitted().GetAssignedMessageSequence()
	request.sequence = &sequence
	return event
}

// Gateway-owned content IDs have a 128-bit suffix, unlike the Go durable-event
// allocator's 64-bit suffix. These legal writer facts use the production Go
// CSPRNG allocator twice; actual Gateway allocation is proved by Identity.
func publicProjectionContentID() string { return id.New("evt_") + id.New("") }

func (f *publicProjectionFixture) thinking(t *testing.T, request *publicProjectionRequest) string {
	t.Helper()
	return f.thinkingWithID(t, request, publicProjectionContentID())
}

func (f *publicProjectionFixture) thinkingWithID(t *testing.T, request *publicProjectionRequest, event string) string {
	t.Helper()
	response, err := f.bridge.WriteEvent(f.ctx, &bridgev1.WriteEventRequest{Scope: request.scope, RuntimeWriteId: id.New("rwrite_"), ModelRequestId: request.id, EventType: "agent.thinking", PayloadJson: `{"type":"agent.thinking"}`, PreallocatedEventId: &event})
	if err != nil || response.GetCommitted().GetEventId() != event {
		t.Fatalf("actual Bridge thinking: %v", err)
	}
	return event
}

func (f *publicProjectionFixture) tool(t *testing.T, request *publicProjectionRequest, name, input string) string {
	t.Helper()
	response, err := f.bridge.WriteEvent(f.ctx, &bridgev1.WriteEventRequest{Scope: request.scope, RuntimeWriteId: id.New("rwrite_"), ModelRequestId: request.id, ToolDeclaration: bridgeToolDeclarationWithRouteForTest(id.New("call_"), name, input, "allow")})
	if err != nil || response.GetCommitted() == nil {
		t.Fatalf("actual Bridge tool declaration: %v", err)
	}
	sequence := response.GetCommitted().GetAssignedMessageSequence()
	request.sequence = &sequence
	event := response.GetCommitted().GetEventId()
	request.tools = append(request.tools, event)
	return event
}

func (f *publicProjectionFixture) child(t *testing.T, request *publicProjectionRequest) string {
	t.Helper()
	task := id.New("projection-child-")
	source := f.tool(t, request, "spawn_agent", fmt.Sprintf(`{"task_name":%q,"agent_type":"worker","fork_turns":"none","prompt":"fixed child work"}`, task))
	response, err := f.bridge.CreateSubagentThread(f.ctx, &bridgev1.CreateSubagentThreadRequest{Scope: request.scope, SourceToolUseEventId: source, TaskName: task, AgentType: "worker", InitialPrompt: "fixed child work"})
	if err != nil || response.GetCommitted().GetChildThreadId() == "" {
		t.Fatalf("actual Bridge child lifecycle: %v", err)
	}
	return response.GetCommitted().GetChildThreadId()
}

func (f *publicProjectionFixture) reviewer(t *testing.T) string {
	t.Helper()
	response, err := f.bridge.EnsureApprovalReviewerTrunk(f.ctx, &bridgev1.EnsureApprovalReviewerTrunkRequest{Scope: f.scope, EnsureOperationId: id.New("ensure_")})
	if err != nil || response.GetCommitted().GetReviewerThreadId() == "" {
		t.Fatalf("actual Bridge reviewer lifecycle: %v", err)
	}
	return response.GetCommitted().GetReviewerThreadId()
}

func (f *publicProjectionFixture) endRequest(request *publicProjectionRequest) *bridgev1.WriteRequestEndRequest {
	return &bridgev1.WriteRequestEndRequest{Scope: request.scope, RuntimeWriteId: id.New("rwrite_"), ModelRequestId: request.id, FinishReason: "end_turn", UsageJson: `{}`, ProviderContextRetention: &bridgev1.ProviderContextRetention{Disposition: "completed", AssistantMessageSequence: request.sequence, ToolUseEventIds: request.tools}}
}
func (f *publicProjectionFixture) end(t *testing.T, request *publicProjectionRequest) string {
	t.Helper()
	response, err := f.bridge.WriteRequestEnd(f.ctx, f.endRequest(request))
	if err != nil || response.GetCommitted() == nil {
		t.Fatalf("actual Bridge End: %v", err)
	}
	return response.GetCommitted().GetRequestEndEventId()
}

func (f *publicProjectionFixture) compact(t *testing.T, request *publicProjectionRequest, summary string) string {
	t.Helper()
	end := f.endRequest(request)
	end.ProviderContextRetention.Disposition = "compacted"
	end.CompactionContext = bridgeTextContextDeltaForTest(summary)
	end.CompactionEventPayloadJson = `{"type":"agent.thread_context_compacted"}`
	var boundary int64
	if err := f.db.QueryRow(`SELECT COALESCE(max(sequence),0) FROM session_messages WHERE workspace_id=$1 AND session_id=$2 AND session_thread_id=$3`, request.scope.WorkspaceId, request.scope.SessionId, request.scope.SessionThreadId).Scan(&boundary); err != nil {
		t.Fatal(err)
	}
	end.CompactedThroughMessageSequence = &boundary
	var parentBoundary string
	err := f.db.QueryRow(`SELECT parent_boundary_event_id FROM session_thread_context_prefixes WHERE workspace_id=$1 AND session_id=$2 AND child_thread_id=$3 AND consumed_by_checkpoint_message_id IS NULL`, request.scope.WorkspaceId, request.scope.SessionId, request.scope.SessionThreadId).Scan(&parentBoundary)
	if err == nil {
		end.PrefixConsumption = &bridgev1.PrefixConsumptionDraft{ChildThreadId: request.scope.SessionThreadId, ParentBoundaryEventId: parentBoundary}
	} else if err != sql.ErrNoRows {
		t.Fatal(err)
	}
	response, err := f.bridge.WriteRequestEnd(f.ctx, end)
	if err != nil || response.GetCommitted() == nil {
		t.Fatalf("actual compaction End: %v", err)
	}
	return response.GetCommitted().GetRequestEndEventId()
}

func (f *publicProjectionFixture) frame(request *publicProjectionRequest, kind, event, eventType string, sequence int64, text string) eventwire.PreviewFrame {
	frame := eventwire.PreviewFrame{Version: 1, WorkspaceID: request.scope.WorkspaceId, SessionID: request.scope.SessionId, ThreadID: request.scope.SessionThreadId, ModelRequestID: request.id, ModelRequestStartEventID: request.start, RequestKind: runtimecontrol.RequestKindAgentProviderRequest, Kind: kind}
	if kind != "request_open" {
		frame.EventID = event
		frame.EventType = eventType
		frame.PreviewSequence = &sequence
	}
	if kind == "event_delta" {
		frame.Text = &text
	}
	return frame
}
func (f *publicProjectionFixture) publish(t *testing.T, frames ...eventwire.PreviewFrame) {
	t.Helper()
	before := f.tap.count()
	for _, frame := range frames {
		data, err := json.Marshal(frame)
		if err != nil {
			t.Fatal(err)
		}
		if err = f.publisher.Publish(eventwire.PreviewSubject(frame.WorkspaceID, frame.SessionID), data); err != nil {
			t.Fatal(err)
		}
	}
	if err := f.publisher.FlushTimeout(time.Second); err != nil {
		t.Fatal("native NATS flush failed")
	}
	publicWait(t, "controlled frame batch crosses real NATS", func() bool { return f.tap.count() >= before+len(frames) })
}

func (f *publicProjectionFixture) fence(t *testing.T, viewer string, request *publicProjectionRequest) publicSDKSnapshot {
	t.Helper()
	before := countPublicEvents(f.snapshot(t, viewer), "agent.thinking")
	f.thinking(t, request)
	f.waitEvent(t, viewer, "agent.thinking", before+1)
	polls := f.reader.polls.Load()
	publicWait(t, "completed formal poll and drained preview queue", func() bool {
		return f.reader.polls.Load() > polls+1 && f.metric(t, "event_stream_preview_pending_bytes") == 0
	})
	return f.snapshot(t, viewer)
}

type publicProjectionPage struct {
	Data []map[string]any `json:"data"`
	Next *string          `json:"next_page"`
}

func (f *publicProjectionFixture) list(t *testing.T, thread, order, page string) publicProjectionPage {
	t.Helper()
	args := map[string]any{"sessionId": f.session, "limit": 1}
	if thread != "" {
		args["threadId"] = thread
	} else {
		args["order"] = order
	}
	if page != "" {
		args["page"] = page
	}
	var result publicProjectionPage
	if json.Unmarshal(f.client.control(t, "list", args), &result) != nil {
		t.Fatal("SDK list decode")
	}
	return result
}
func (f *publicProjectionFixture) raw(t *testing.T, method, path, key string) (int, []byte) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	request, err := http.NewRequestWithContext(ctx, method, f.baseURL+path, nil)
	if err != nil {
		t.Fatal(err)
	}
	if key == "" {
		key = f.apiKey
	}
	request.Header.Set("X-Api-Key", key)
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = response.Body.Close() }()
	body, err := io.ReadAll(io.LimitReader(response.Body, 2*1024*1024))
	if err != nil {
		t.Fatal(err)
	}
	return response.StatusCode, body
}
func (f *publicProjectionFixture) rawList(t *testing.T, thread, order, page string) publicProjectionPage {
	t.Helper()
	path := "/v1/sessions/" + f.session
	if thread != "" {
		path += "/threads/" + thread
	}
	query := url.Values{"beta": {"true"}, "limit": {"1"}, "order": {order}}
	if page != "" {
		query.Set("page", page)
	}
	code, body := f.raw(t, http.MethodGet, path+"/events?"+query.Encode(), "")
	if code != 200 {
		t.Fatalf("raw list order control status %d", code)
	}
	var result publicProjectionPage
	if json.Unmarshal(body, &result) != nil {
		t.Fatal("raw list decode")
	}
	return result
}
func publicProjectionSQLIDs(t *testing.T, db *sql.DB, session, thread, order string) []string {
	t.Helper()
	ordering := "ASC"
	if order == "desc" {
		ordering = "DESC"
	}
	query := `SELECT e.event_id FROM session_events e JOIN session_threads t ON t.workspace_id=e.workspace_id AND t.session_id=e.session_id AND t.id=e.session_thread_id WHERE e.workspace_id='default' AND e.session_id=$1 AND e.visibility='public' AND t.visibility='public' AND t.role<>'approval_reviewer'`
	args := []any{session}
	if thread != "" {
		query += ` AND e.session_thread_id=$2 ORDER BY e.sequence ` + ordering
		args = append(args, thread)
	} else {
		query += ` AND e.session_visible=TRUE ORDER BY e.insert_stream_position ` + ordering + `, e.event_id ` + ordering
	}
	rows, err := db.Query(query, args...)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rows.Close() }()
	var result []string
	for rows.Next() {
		var value string
		if rows.Scan(&value) != nil {
			t.Fatal("SQL oracle scan")
		}
		result = append(result, value)
	}
	if rows.Err() != nil {
		t.Fatal(rows.Err())
	}
	return result
}
