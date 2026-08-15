package integration

import (
	"bufio"
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/tetral-ai/tetral/internal/auth"
	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/httpapi"
	"github.com/tetral-ai/tetral/internal/queue"
	"github.com/tetral-ai/tetral/internal/sessionevent"
	"github.com/tetral-ai/tetral/internal/sessionrpc"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	"github.com/tetral-ai/tetral/internal/workspace"
	agentruntimev1 "github.com/tetral-ai/tetral/services/agent-runtime/gen/tetral/agent_runtime/v1"
	agentruntimebridge "github.com/tetral-ai/tetral/services/bridge"
	bridgev1 "github.com/tetral-ai/tetral/services/bridge/gen/tetral/bridge/v1"
)

const transportAdmissionBodyBytes = 1 << 20

func TestMaximumAdmissionBodyTraversesRuntimeDeliveryAndRequestSettlement(t *testing.T) {
	runTransportAdmissionTraversal(t, "plain", func(bytes int) string {
		return strings.Repeat("x", bytes)
	})
}

func TestWorstCaseEscapingAdmissionBodyTraversesRuntimeDeliveryAndRequestSettlement(t *testing.T) {
	runTransportAdmissionTraversal(t, "escaping", worstCaseEscapingJSONText)
}

func runTransportAdmissionTraversal(t *testing.T, suffix string, bodyText func(int) string) {
	t.Helper()
	runtimePort := startTransportRuntimePodHarness(t)
	runtimeDB, adminDB := storagetest.NewPostgreSQLDBWithAdmin(t)
	client := dbconnect.NewClientForTesting(runtimeDB)
	sessionID := "sesn_transport_" + suffix
	threadID := "thr_transport_" + suffix
	bindingID := "bind_transport_" + suffix
	podUID := "uid-a"
	seedTransportSession(t, adminDB, sessionID, threadID, bindingID, podUID)

	eventService := sessionevent.NewService(
		sessionevent.NewPostgreSQLStore(client),
		sessionevent.WithClock(func() time.Time {
			return time.Date(2026, 1, 1, 0, 0, 10, 0, time.UTC)
		}),
	)
	router := httpapi.NewRouter(
		httpapi.NewSessionHandler(nil),
		"",
		httpapi.WithAuthenticator(auth.AuthenticatorFunc(func(context.Context, string) (auth.Principal, error) {
			return auth.Principal{Workspace: workspace.Workspace{ID: workspace.DefaultID}}, nil
		})),
		httpapi.WithSessionEventHandler(httpapi.NewSessionEventHandler(eventService)),
	)
	prefix := `{"events":[{"type":"user.message","content":[{"type":"text","text":"`
	suffixJSON := `"}]}]}`
	textJSON := bodyText(transportAdmissionBodyBytes - len(prefix) - len(suffixJSON))
	body := prefix + textJSON + suffixJSON
	if len(body) != transportAdmissionBodyBytes {
		t.Fatalf("admission body bytes = %d; want %d", len(body), transportAdmissionBodyBytes)
	}

	request := httptest.NewRequest(http.MethodPost, "/v1/sessions/"+sessionID+"/events?beta=true", strings.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("X-Api-Key", "integration-key")
	request.Header.Set("Idempotency-Key", "idem_transport_"+suffix)
	response := httptest.NewRecorder()
	router.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("admission status = %d body=%s; want 200", response.Code, response.Body.String())
	}

	job := readTransportRuntimeJob(t, adminDB, sessionID)
	bridgeStore := agentruntimebridge.NewPostgreSQLBridgeAPIStore(client)
	sender := &settlingTransportSender{
		transport: agentruntimebridge.NewRuntimePodCommandClient(fixedTransportTokenSource{}),
		bridge:    bridgeStore,
		threadID:  threadID,
		bindingID: bindingID,
		podUID:    podUID,
		suffix:    suffix,
	}
	deliveryStore := agentruntimebridge.NewPostgreSQLRuntimeDeliveryStore(client, runtimePort)
	deliveryStore.Clock = func() time.Time {
		return time.Date(2026, 1, 1, 0, 0, 20, 0, time.UTC)
	}
	result, err := (agentruntimebridge.RuntimePodDirectDeliverer{
		Store:  deliveryStore,
		Sender: sender,
	}).DeliverRuntimeJob(context.Background(), job)
	if err != nil {
		t.Fatalf("deliver runtime input: %v", err)
	}
	if result.Status != agentruntimebridge.RuntimeDeliveryAccepted || sender.request == nil {
		t.Fatalf("delivery result/request = %+v/%v; want accepted command", result, sender.request != nil)
	}
	if commandBytes := proto.Size(sender.request); commandBytes > sessionrpc.MaxRuntimeCommandGRPCMessageBytes {
		t.Fatalf("runtime command bytes = %d; exceeds %d-byte command fuse", commandBytes, sessionrpc.MaxRuntimeCommandGRPCMessageBytes)
	} else if commandBytes <= sessionrpc.MaxInboundGRPCMessageBytes {
		t.Fatalf("runtime command bytes = %d; proof must exceed the superseded %d-byte shared channel bound", commandBytes, sessionrpc.MaxInboundGRPCMessageBytes)
	}

	var messageData string
	if err := adminDB.QueryRowContext(context.Background(),
		`SELECT data_json
		   FROM session_messages
		  WHERE workspace_id = 'default'
		    AND session_id = $1
		    AND kind = 'user'`,
		sessionID,
	).Scan(&messageData); err != nil {
		t.Fatalf("read committed user message: %v", err)
	}
	var message struct {
		Parts []struct {
			Text string `json:"text"`
		} `json:"parts"`
	}
	if err := json.Unmarshal([]byte(messageData), &message); err != nil {
		t.Fatalf("decode committed user message: %v", err)
	}
	wantText, err := decodeTransportText(body)
	if err != nil {
		t.Fatalf("decode admitted request text: %v", err)
	}
	if len(message.Parts) != 1 || message.Parts[0].Text != wantText {
		t.Fatalf("committed text parts/bytes = %d/%d; want one exact %d-byte text part", len(message.Parts), len(firstTransportPartText(message.Parts)), len(wantText))
	}

	var requestEndCount int
	if err := adminDB.QueryRowContext(context.Background(),
		`SELECT count(*)
		   FROM session_events
		  WHERE workspace_id = 'default'
		    AND session_id = $1
		    AND type = 'span.model_request_end'`,
		sessionID,
	).Scan(&requestEndCount); err != nil {
		t.Fatalf("count provider request ends: %v", err)
	}
	if requestEndCount != 1 {
		t.Fatalf("provider request end count = %d; want 1", requestEndCount)
	}
}

type settlingTransportSender struct {
	agentruntimebridge.RuntimeCommandSender
	transport *agentruntimebridge.RuntimePodCommandClient
	bridge    *agentruntimebridge.PostgreSQLBridgeAPIStore
	threadID  string
	bindingID string
	podUID    string
	suffix    string
	request   *agentruntimev1.AcceptInputRequest
}

func (s *settlingTransportSender) AcceptInput(
	ctx context.Context,
	target agentruntimebridge.RuntimePodTarget,
	request *agentruntimev1.AcceptInputRequest,
) (*agentruntimev1.AcceptInputResponse, error) {
	s.request = request
	response, err := s.transport.AcceptInput(ctx, target, request)
	if err != nil {
		return nil, err
	}
	var payload struct {
		Messages []struct {
			Parts []struct {
				Type string `json:"type"`
				Text string `json:"text"`
			} `json:"parts"`
		} `json:"messages"`
	}
	if err := json.Unmarshal([]byte(request.GetMessagesJson()), &payload); err != nil {
		return nil, err
	}
	if len(payload.Messages) != 1 || len(payload.Messages[0].Parts) != 1 || payload.Messages[0].Parts[0].Type != "text" {
		return nil, fmt.Errorf("runtime input payload does not contain one text message")
	}
	scope := &bridgev1.RuntimeScope{
		WorkspaceId:     request.GetWorkspaceId(),
		SessionId:       request.GetSessionId(),
		SessionThreadId: s.threadID,
		Binding: &bridgev1.RuntimeBindingRef{
			BindingId:         s.bindingID,
			BindingGeneration: request.GetBindingGeneration(),
			TargetPodUid:      s.podUID,
		},
	}
	committed, err := s.bridge.CommitInputs(ctx, &bridgev1.CommitInputsRequest{
		Scope:          scope,
		RuntimeInputId: request.GetRuntimeInputId(),
		Disposition:    bridgev1.RuntimeInputDisposition_RUNTIME_INPUT_DISPOSITION_COMMIT,
	})
	if err != nil {
		return nil, err
	}
	assignedContextSequences := committed.GetCommitted().GetContext().GetAssignedContextSequences()
	if len(assignedContextSequences) != 1 {
		return nil, fmt.Errorf("runtime input commit did not return one assigned context sequence")
	}
	modelRequestID := "mreq_transport_" + s.suffix
	contextThroughMessageSequence := assignedContextSequences[0]
	if contextThroughMessageSequence <= 0 {
		return nil, fmt.Errorf("runtime input commit returned invalid message sequence")
	}
	start, err := s.bridge.WriteEvent(ctx, &bridgev1.WriteEventRequest{
		Scope:                         scope,
		RuntimeWriteId:                "rwrite_transport_start_" + s.suffix,
		ModelRequestId:                modelRequestID,
		EventType:                     "span.model_request_start",
		PayloadJson:                   fmt.Sprintf(`{"type":"span.model_request_start","model_request_id":%q}`, modelRequestID),
		RequestKind:                   "agent_provider_request",
		ContextThroughMessageSequence: &contextThroughMessageSequence,
	})
	if err != nil {
		return nil, err
	}
	if _, err := s.bridge.WriteRequestEnd(ctx, &bridgev1.WriteRequestEndRequest{
		Scope:                    scope,
		RuntimeWriteId:           "rwrite_transport_end_" + s.suffix,
		ModelRequestId:           modelRequestID,
		ModelRequestStartEventId: start.GetEventId(),
		FinishReason:             "stop",
		UsageJson:                `{"input_tokens":1,"output_tokens":1}`,
	}); err != nil {
		return nil, err
	}
	return response, nil
}

func seedTransportSession(t *testing.T, db *sql.DB, sessionID string, threadID string, bindingID string, podUID string) {
	t.Helper()
	agentID := "agent_" + sessionID
	environmentID := "env_" + sessionID
	statements := []struct {
		query string
		args  []any
	}{
		{`INSERT INTO workspaces (id, type, name, created_at) VALUES ('default', 'workspace', 'default', '2026-01-01T00:00:00Z') ON CONFLICT (id) DO NOTHING`, nil},
		{`INSERT INTO agents (workspace_id, id, name, version, created_at, updated_at) VALUES ('default', $1, $1, 1, '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z')`, []any{agentID}},
		{`INSERT INTO agent_versions (workspace_id, id, agent_id, version, config_json, config_hash, created_at) VALUES ('default', $1, $2, 1, '{"tools":[{"type":"tetral_agent_toolset","family":"claude"}]}', $3, '2026-01-01T00:00:00Z')`, []any{"agv_" + sessionID, agentID, "hash_" + sessionID}},
		{`INSERT INTO environments (workspace_id, id, name, config_json, current_generation, created_at, updated_at) VALUES ('default', $1, $1, '{}', 1, '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z')`, []any{environmentID}},
		{`INSERT INTO sessions (workspace_id, id, main_thread_id, type, status, lifecycle_state, agent_id, agent_version, environment_id, installed_tools_json, created_at, updated_at) VALUES ('default', $1, $2, 'session', 'idle', 'active', $3, 1, $4, '{"tools":[{"type":"tetral_agent_toolset","family":"claude"}]}', '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z')`, []any{sessionID, threadID, agentID, environmentID}},
		{`INSERT INTO session_threads (workspace_id, id, session_id, role, visibility, status, created_at, last_active_at, updated_at) VALUES ('default', $1, $2, 'main', 'public', 'idle', '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z')`, []any{threadID, sessionID}},
		{`INSERT INTO session_runtime_status (workspace_id, session_id, status, idle_since, created_at, updated_at) VALUES ('default', $1, 'idle', '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z')`, []any{sessionID}},
		{`INSERT INTO session_runtime_bindings (workspace_id, session_id, binding_id, binding_generation, agent_runtime_namespace, agent_runtime_pod_name, agent_runtime_pod_uid, agent_runtime_pod_ip, bound_at, updated_at) VALUES ('default', $1, $2, 1, 'engine', 'runtime-pod-a', $3, '127.0.0.1', '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z')`, []any{sessionID, bindingID, podUID}},
	}
	for _, statement := range statements {
		if _, err := db.ExecContext(context.Background(), statement.query, statement.args...); err != nil {
			t.Fatalf("seed transport session: %v", err)
		}
	}
}

func readTransportRuntimeJob(t *testing.T, db *sql.DB, sessionID string) agentruntimebridge.RuntimeJob {
	t.Helper()
	var jobID string
	var payloadJSON string
	if err := db.QueryRowContext(context.Background(),
		`SELECT id, payload_json
		   FROM queue_jobs
		  WHERE workspace_id = 'default'
		    AND partition_key = $1
		    AND kind = $2
		    AND status = 'pending'`,
		queue.FormatSessionPartitionKey(workspace.DefaultID, sessionID),
		queue.KindRuntimeInput,
	).Scan(&jobID, &payloadJSON); err != nil {
		t.Fatalf("read runtime input job: %v", err)
	}
	var payload struct {
		WorkspaceID     string   `json:"workspace_id"`
		SessionID       string   `json:"session_id"`
		SessionThreadID string   `json:"session_thread_id"`
		RuntimeInputID  string   `json:"runtime_input_id"`
		EventIDs        []string `json:"event_ids"`
		SequenceFrom    int64    `json:"sequence_from"`
		SequenceTo      int64    `json:"sequence_to"`
		InputKind       string   `json:"input_kind"`
	}
	if err := json.Unmarshal([]byte(payloadJSON), &payload); err != nil {
		t.Fatalf("decode runtime input job: %v", err)
	}
	return agentruntimebridge.RuntimeJob{
		JobID:           jobID,
		LeaseToken:      "lease_transport",
		Kind:            queue.KindRuntimeInput,
		WorkspaceID:     payload.WorkspaceID,
		SessionID:       payload.SessionID,
		SessionThreadID: payload.SessionThreadID,
		RuntimeInputID:  payload.RuntimeInputID,
		EventIDs:        payload.EventIDs,
		SequenceFrom:    payload.SequenceFrom,
		SequenceTo:      payload.SequenceTo,
		InputKind:       payload.InputKind,
		PayloadJSON:     payloadJSON,
	}
}

func worstCaseEscapingJSONText(bytes int) string {
	const pattern = `&<\"`
	var text strings.Builder
	text.Grow(bytes)
	for text.Len()+len(pattern) <= bytes {
		text.WriteString(pattern)
	}
	text.WriteString(strings.Repeat("&", bytes-text.Len()))
	return text.String()
}

func decodeTransportText(body string) (string, error) {
	var request struct {
		Events []struct {
			Content []struct {
				Text string `json:"text"`
			} `json:"content"`
		} `json:"events"`
	}
	if err := json.Unmarshal([]byte(body), &request); err != nil {
		return "", err
	}
	if len(request.Events) != 1 || len(request.Events[0].Content) != 1 {
		return "", fmt.Errorf("unexpected transport request shape")
	}
	return request.Events[0].Content[0].Text, nil
}

func firstTransportPartText(parts []struct {
	Text string `json:"text"`
}) string {
	if len(parts) == 0 {
		return ""
	}
	return parts[0].Text
}

type fixedTransportTokenSource struct{}

func (fixedTransportTokenSource) Token(context.Context) (string, error) {
	return "caller-token", nil
}

func startTransportRuntimePodHarness(t *testing.T) int {
	t.Helper()
	if _, err := exec.LookPath("bun"); err != nil {
		t.Skip("bun is required for the Runtime command transport integration")
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	runtimeRoot := filepath.Join("..", "services", "agent-runtime")
	command := exec.CommandContext(ctx, "bun", "run", "packages/runtime-pod/test/harness/grpc-harness.ts")
	command.Dir = runtimeRoot
	command.Env = append(os.Environ(), "TETRAL_TEST_RUNTIME_POD_IP=127.0.0.1")
	stdout, err := command.StdoutPipe()
	if err != nil {
		t.Fatalf("Runtime Pod harness stdout: %v", err)
	}
	var stderr syncBuffer
	command.Stderr = &stderr
	if err := command.Start(); err != nil {
		t.Fatalf("start Runtime Pod harness: %v", err)
	}
	t.Cleanup(func() {
		if command.Process != nil {
			_ = command.Process.Signal(os.Interrupt)
			_ = command.Process.Kill()
		}
		_ = command.Wait()
	})
	scanner := bufio.NewScanner(stdout)
	if !scanner.Scan() {
		t.Fatalf("Runtime Pod harness did not print an address: %v stderr=%s", scanner.Err(), stderr.String())
	}
	var started struct {
		Address string `json:"address"`
	}
	if err := json.Unmarshal(scanner.Bytes(), &started); err != nil {
		t.Fatalf("decode Runtime Pod harness address %q: %v stderr=%s", scanner.Text(), err, stderr.String())
	}
	host, rawPort, err := net.SplitHostPort(started.Address)
	if err != nil || host != "127.0.0.1" {
		t.Fatalf("Runtime Pod harness address = %q; want 127.0.0.1 with a port", started.Address)
	}
	port, err := strconv.Atoi(rawPort)
	if err != nil || port <= 0 {
		t.Fatalf("Runtime Pod harness port = %q; want positive integer", rawPort)
	}
	return port
}

// syncBuffer guards a child process's stderr. os/exec fills it from its own
// goroutine while this test reads it for failure messages, so the two need a
// lock between them.
type syncBuffer struct {
	mutex  sync.Mutex
	buffer bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mutex.Lock()
	defer b.mutex.Unlock()
	return b.buffer.Write(p)
}

func (b *syncBuffer) String() string {
	b.mutex.Lock()
	defer b.mutex.Unlock()
	return b.buffer.String()
}
