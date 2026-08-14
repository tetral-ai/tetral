package agentruntimebridge

import (
	"bufio"
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	enginekubernetes "github.com/tetral-ai/tetral/internal/kubernetes"
	"github.com/tetral-ai/tetral/internal/queue"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	"github.com/tetral-ai/tetral/internal/workspace"
	agentruntimev1 "github.com/tetral-ai/tetral/services/agent-runtime/gen/tetral/agent_runtime/v1"
	bridgev1 "github.com/tetral-ai/tetral/services/bridge/gen/tetral/bridge/v1"
	providergatewayv1 "github.com/tetral-ai/tetral/services/gateway/gen/tetral/provider_gateway/v1"
	tetralqueue "github.com/tetral-ai/tetral/services/queue"
	queuev1 "github.com/tetral-ai/tetral/services/queue/gen/tetral/queue/v1"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
)

// This file owns PostgreSQL runtime delivery-store behavior tests.

type recordingMCPConnectorServer struct {
	providergatewayv1.UnimplementedMcpConnectorServiceServer

	mu       sync.Mutex
	requests []*providergatewayv1.ListMcpToolsRequest
}

func (s *recordingMCPConnectorServer) ListMcpTools(_ context.Context, request *providergatewayv1.ListMcpToolsRequest) (*providergatewayv1.ListMcpToolsResponse, error) {
	s.mu.Lock()
	s.requests = append(s.requests, request)
	s.mu.Unlock()
	return &providergatewayv1.ListMcpToolsResponse{
		ManifestEtag: "etag_production_assembly",
		Tools: []*providergatewayv1.McpToolDefinition{{
			Name:            "github_search",
			Description:     "Search GitHub",
			InputSchemaJson: `{"type":"object"}`,
		}},
	}, nil
}

func (s *recordingMCPConnectorServer) recordedRequests() []*providergatewayv1.ListMcpToolsRequest {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]*providergatewayv1.ListMcpToolsRequest(nil), s.requests...)
}

type expiringMCPManifestLister struct {
	contexts []context.Context
}

type racingInitialMCPManifestLister struct {
	initialStarted chan struct{}
	releaseInitial chan struct{}
	blockInitial   bool
	once           sync.Once
}

func (l *racingInitialMCPManifestLister) ListMCPTools(ctx context.Context, request MCPManifestListRequest) (MCPManifestListResult, error) {
	if request.ManifestETag != "" {
		return mcpManifestResult(request.ManifestETag, "github_ready"), nil
	}
	if l.blockInitial {
		l.once.Do(func() { close(l.initialStarted) })
		select {
		case <-l.releaseInitial:
		case <-ctx.Done():
			return MCPManifestListResult{}, ctx.Err()
		}
	}
	return MCPManifestListResult{}, mcpManifestDiscoveryError{diagnostic: mcpManifestDiagnosticDiscoveryUnavailable}
}

func (l *expiringMCPManifestLister) ListMCPTools(ctx context.Context, request MCPManifestListRequest) (MCPManifestListResult, error) {
	if len(l.contexts) > 0 {
		select {
		case <-l.contexts[len(l.contexts)-1].Done():
		default:
			return MCPManifestListResult{}, errors.New("previous list context was not canceled before the next iteration")
		}
	}
	l.contexts = append(l.contexts, ctx)
	<-ctx.Done()
	return MCPManifestListResult{
		ManifestETag: "etag_" + request.MCPServerName,
		Tools: []MCPManifestTool{{
			Name:            request.MCPServerName + "_search",
			Description:     "Search " + request.MCPServerName,
			InputSchemaJSON: `{"type":"object"}`,
		}},
	}, nil
}

func TestPostgreSQLRuntimeDeliveryStoreInitialMCPManifestCaptureAdvancesInputAndRedrivesGeneration(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	seedBridgeAPISession(t, admin, "default", "sesn_bridge_initial_mcp", "thr_bridge_initial_mcp")
	seedBridgeAPIAgentConfig(t, admin, "default", "sesn_bridge_initial_mcp", `{"name":"agent","model":"anthropic/claude-opus-4-8","tools":[{"type":"mcp_toolset","mcp_server_name":"github","default_config":{"enabled":false,"permission_policy":{"type":"always_ask"}},"configs":[{"name":"github_search","enabled":true,"permission_policy":{"type":"always_allow"}}]}],"mcp_servers":[{"type":"url","name":"github","url":"https://api.githubcopilot.com/mcp/"}],"skills":[],"metadata":{}}`)
	if _, err := admin.ExecContext(context.Background(), `UPDATE sessions SET installed_tools_json = '{"tools":[{"type":"tetral_agent_toolset","family":"claude"},{"type":"mcp_toolset","mcp_server_name":"github","default_config":{"enabled":false,"permission_policy":{"type":"always_ask"}},"configs":[{"name":"github_search","enabled":true,"permission_policy":{"type":"always_allow"}}]}],"mcp_servers":[{"type":"url","name":"github","url":"https://api.githubcopilot.com/mcp/"}]}' WHERE workspace_id = 'default' AND id = 'sesn_bridge_initial_mcp'`); err != nil {
		t.Fatalf("seed durable initial MCP config: %v", err)
	}
	seedBridgeAPIRuntimeBinding(t, admin, "default", "sesn_bridge_initial_mcp", "bind_bridge_initial_mcp", 1, "pod_uid_initial_mcp")

	lister := &recordingMCPManifestLister{results: []MCPManifestListResult{{
		ManifestETag: "etag_initial",
		Tools:        []MCPManifestTool{{Name: "github_search", Description: "Search GitHub", InputSchemaJSON: `{"type":"object"}`}},
	}}}
	store := NewPostgreSQLRuntimeDeliveryStore(dbconnect.NewClientForTesting(runtime), 9090)
	store.Clock = func() time.Time { return time.Date(2026, 1, 1, 0, 0, 30, 0, time.UTC) }
	store.MCPManifestLister = lister
	sender := &recordingRuntimeCommandSender{response: &agentruntimev1.RuntimeInputCommandResponse{
		Status: agentruntimev1.RuntimeCommandStatus_RUNTIME_COMMAND_STATUS_ACCEPTED,
	}}
	job := RuntimeJob{
		JobID:           "qjob_bridge_initial_mcp",
		LeaseToken:      "lease_bridge_initial_mcp",
		Kind:            queue.KindRuntimeInput,
		WorkspaceID:     "default",
		SessionID:       "sesn_bridge_initial_mcp",
		SessionThreadID: "thr_bridge_initial_mcp",
		RuntimeInputID:  "rin_bridge_initial_mcp",
		EventIDs:        []string{"evt_bridge_initial_mcp"},
		SequenceFrom:    1,
		SequenceTo:      1,
		InputKind:       "messages",
		CommandKind:     agentruntimev1.RuntimeCommandKind_RUNTIME_COMMAND_KIND_MESSAGES,
		PayloadJSON:     `{"type":"messages"}`,
	}
	seedBridgeAPIEvent(t, admin, "default", job.SessionID, job.SessionThreadID, job.EventIDs[0], 1, "user.message", `{"content":[{"type":"text","text":"hello"}]}`)
	seedRuntimeInboxBirthForJob(t, admin, job)

	result, err := (RuntimePodDirectDeliverer{Store: store, Sender: sender}).DeliverRuntimeJob(context.Background(), job)
	if err != nil {
		t.Fatalf("DeliverRuntimeJob initial MCP manifest: %v", err)
	}
	if result.Status != RuntimeDeliveryAccepted {
		t.Fatalf("initial MCP manifest delivery result = %#v; want accepted", result)
	}
	if len(lister.requests) != 1 ||
		lister.requests[0].WorkspaceID != "default" ||
		lister.requests[0].SessionID != "sesn_bridge_initial_mcp" ||
		lister.requests[0].MCPServerName != "github" ||
		lister.requests[0].ManifestETag != "" {
		t.Fatalf("MCP manifest lister requests = %#v; want initial github list", lister.requests)
	}
	if len(sender.requests) != 1 || sender.requests[0].GetRuntimeInputId() != job.RuntimeInputID {
		t.Fatalf("runtime commands = %#v; want the original input in the capture attempt", sender.requests)
	}
	assertRuntimeMCPManifestQueueJob(t, admin, "default", "sesn_bridge_initial_mcp", "github", 1)
	var operationRuntimeInputID string
	if err := admin.QueryRowContext(context.Background(),
		`SELECT runtime_input_id
		   FROM session_bridge_operations
		  WHERE workspace_id = 'default'
		    AND session_id = 'sesn_bridge_initial_mcp'
		    AND operation = $1
		    AND idempotency_key = 'github:1'`,
		bridgeOpMcpManifestChanged,
	).Scan(&operationRuntimeInputID); err != nil {
		t.Fatalf("read initial MCP manifest bridge operation: %v", err)
	}
	if operationRuntimeInputID != "runtime_config_update:mcp_manifest:sesn_bridge_initial_mcp:github:1" {
		t.Fatalf("bridge operation runtime_input_id = %q; want manifest runtime config update id", operationRuntimeInputID)
	}

	// The independent hot-projection carrier remains durable even though the
	// original input no longer waits for that Queue job.
	var queuedJobID string
	var queuedPayload string
	if err := admin.QueryRowContext(context.Background(),
		`SELECT id, payload_json
		   FROM queue_jobs
		  WHERE workspace_id = 'default'
		    AND payload_json::jsonb ->> 'session_id' = 'sesn_bridge_initial_mcp'
		    AND kind = 'runtime_config_update'
		    AND payload_json::jsonb ->> 'mcp_server_name' = 'github'
		    AND payload_json::jsonb ->> 'manifest_generation' = '1'`,
	).Scan(&queuedJobID, &queuedPayload); err != nil {
		t.Fatalf("read committed MCP manifest delivery intent: %v", err)
	}
	redrivenJob, err := DecodeRuntimeJob(&queuev1.QueueJob{ //nolint:gosec // Test lease token fixture, not a secret.
		Id:          queuedJobID,
		WorkspaceId: "default",
		Kind:        queue.KindRuntimeConfigUpdate,
		LeaseToken:  "lease_redriven_manifest",
		PayloadJson: queuedPayload,
	})
	if err != nil {
		t.Fatalf("decode committed MCP manifest delivery intent: %v", err)
	}
	redriveSender := &recordingRuntimeCommandSender{response: &agentruntimev1.RuntimeInputCommandResponse{
		Status: agentruntimev1.RuntimeCommandStatus_RUNTIME_COMMAND_STATUS_ACCEPTED,
	}}
	redriveStore := NewPostgreSQLRuntimeDeliveryStore(dbconnect.NewClientForTesting(runtime), 9090)
	redriveResult, err := (RuntimePodDirectDeliverer{Store: redriveStore, Sender: redriveSender}).DeliverRuntimeJob(context.Background(), redrivenJob)
	if err != nil {
		t.Fatalf("redrive committed MCP manifest generation: %v", err)
	}
	if redriveResult.Status != RuntimeDeliveryAccepted || len(redriveSender.requests) != 1 ||
		redriveSender.requests[0].GetRuntimeInputId() != "runtime_config_update:mcp_manifest:sesn_bridge_initial_mcp:github:1" {
		t.Fatalf("redriven manifest delivery = %#v requests %#v; want one accepted generation-1 command", redriveResult, redriveSender.requests)
	}
	var delivered struct {
		MCPManifest map[string]json.RawMessage `json:"mcp_manifest"`
	}
	if err := json.Unmarshal([]byte(redriveSender.requests[0].GetPayloadJson()), &delivered); err != nil {
		t.Fatalf("decode rebuilt MCP command: %v", err)
	}
	var manifestETag string
	var tools []struct {
		Name string `json:"name"`
	}
	if err := json.Unmarshal(delivered.MCPManifest["manifest_etag"], &manifestETag); err != nil {
		t.Fatalf("decode rebuilt manifest etag: %v", err)
	}
	if err := json.Unmarshal(delivered.MCPManifest["tools"], &tools); err != nil {
		t.Fatalf("decode rebuilt manifest tools: %v", err)
	}
	if manifestETag != "etag_initial" || len(tools) != 1 || tools[0].Name != "github_search" {
		t.Fatalf("rebuilt MCP command = %#v; want durable manifest row content", delivered)
	}
	for _, forbidden := range []string{"default_config", "configs"} {
		if _, exists := delivered.MCPManifest[forbidden]; exists {
			t.Fatalf("rebuilt MCP command carries forbidden policy key %q: %#v", forbidden, delivered)
		}
	}
	if len(lister.requests) != 1 {
		t.Fatalf("connector list calls after durable redrive = %d; want original capture call only", len(lister.requests))
	}
}

func TestPostgreSQLRuntimeDeliveryPreparationBoundsStateDrivenReentry(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID = "sesn_runtime_prepare_reentry_bound"
		threadID  = "thr_runtime_prepare_reentry_bound"
	)
	seedMCPFamilySession(t, admin, sessionID, threadID, "claude")
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, "bind_runtime_prepare_reentry_bound", 1, "pod_runtime_prepare_reentry_bound")
	job := RuntimeJob{
		Kind: queue.KindRuntimeInput, WorkspaceID: "default", SessionID: sessionID, SessionThreadID: threadID,
		RuntimeInputID: "rin_runtime_prepare_reentry_bound", EventIDs: []string{"evt_runtime_prepare_reentry_bound"},
		SequenceFrom: 1, SequenceTo: 1, InputKind: "messages",
	}
	seedBridgeAPIEvent(t, admin, "default", sessionID, threadID, job.EventIDs[0], 1, "user.message", `{"content":[{"type":"text","text":"continue"}]}`)
	seedRuntimeInboxBirthForJob(t, admin, job)
	lister := &recordingMCPManifestLister{results: []MCPManifestListResult{mcpManifestResult("etag_reentry_bound", "github_search")}}
	store := NewPostgreSQLRuntimeDeliveryStore(dbconnect.NewClientForTesting(runtime), 9090)
	store.MCPManifestLister = lister

	_, err := store.prepareRuntimeCommand(context.Background(), job, maxRuntimePreparationReentries)
	var prepareErr runtimeDeliveryPrepareError
	if !errors.As(err, &prepareErr) || prepareErr.kind != "runtime_reconcile_invariant" || prepareErr.retryable {
		t.Fatalf("bounded preparation error = %#v; want terminal runtime_reconcile_invariant", err)
	}
	if len(lister.requests) != 0 {
		t.Fatalf("manifest calls after reentry bound = %d; want zero", len(lister.requests))
	}
}

func TestPostgreSQLRuntimeDeliveryStoreInitialMCPFailureAdvancesSingleAttemptInput(t *testing.T) {
	connector := startInitialManifestFailureConnector(t)
	tests := []struct {
		name       string
		suffix     string
		lister     func(*testing.T) MCPManifestLister
		diagnostic string
	}{
		{name: "credential unavailable", suffix: "credential", lister: func(*testing.T) MCPManifestLister {
			return NewGatewayMCPManifestLister(connector.address, &countingRuntimeCommandTokenSource{})
		}, diagnostic: mcpManifestDiagnosticCredentialUnavailable},
		{name: "server unavailable", suffix: "server", lister: func(*testing.T) MCPManifestLister {
			return NewGatewayMCPManifestLister(connector.address, &countingRuntimeCommandTokenSource{})
		}, diagnostic: mcpManifestDiagnosticDiscoveryUnavailable},
		{name: "discovery timeout", suffix: "timeout", lister: func(*testing.T) MCPManifestLister {
			return NewGatewayMCPManifestLister(connector.address, &countingRuntimeCommandTokenSource{})
		}, diagnostic: mcpManifestDiagnosticDiscoveryUnavailable},
		{name: "manifest invalid trailer", suffix: "invalid", lister: func(*testing.T) MCPManifestLister {
			return NewGatewayMCPManifestLister(connector.address, &countingRuntimeCommandTokenSource{})
		}, diagnostic: mcpManifestDiagnosticInvalid},
		{name: "untyped unavailable transport", suffix: "transport_unavailable", lister: func(t *testing.T) MCPManifestLister { return newUntypedFailureMCPManifestLister(t, codes.Unavailable) }, diagnostic: mcpManifestDiagnosticDiscoveryUnavailable},
		{name: "untyped deadline transport", suffix: "transport_deadline", lister: func(t *testing.T) MCPManifestLister {
			return newUntypedFailureMCPManifestLister(t, codes.DeadlineExceeded)
		}, diagnostic: mcpManifestDiagnosticDiscoveryUnavailable},
		{name: "manifest invalid locally", suffix: "local_invalid", lister: func(*testing.T) MCPManifestLister {
			return &recordingMCPManifestLister{results: []MCPManifestListResult{{ManifestETag: "etag_invalid", Tools: []MCPManifestTool{{Name: "github_search", InputSchemaJSON: `not-json`}}}}}
		}, diagnostic: mcpManifestDiagnosticInvalid},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
			sessionID := "sesn_initial_mcp_failure_" + test.suffix
			threadID := "thr_initial_mcp_failure_" + test.suffix
			seedMCPFamilySession(t, admin, sessionID, threadID, "claude")
			seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, "bind_initial_mcp_failure", 1, "pod_initial_mcp_failure")
			job := RuntimeJob{
				Kind: queue.KindRuntimeInput, WorkspaceID: "default", SessionID: sessionID, SessionThreadID: threadID,
				RuntimeInputID: "rin_initial_mcp_failure", EventIDs: []string{"evt_initial_mcp_failure"},
				SequenceFrom: 1, SequenceTo: 1, InputKind: "messages",
			}
			seedBridgeAPIEvent(t, admin, "default", sessionID, threadID, job.EventIDs[0], 1, "user.message", `{"content":[{"type":"text","text":"continue"}]}`)
			seedRuntimeInboxBirthForJob(t, admin, job)
			now := time.Date(2026, 1, 1, 0, 0, 30, 0, time.UTC)
			queueStore := queue.NewPostgreSQLStore(dbconnect.NewClientForTesting(runtime))
			enqueueExhaustionJob(t, queueStore, job, now.Add(-time.Second))

			deliveryStore := NewPostgreSQLRuntimeDeliveryStore(dbconnect.NewClientForTesting(runtime), 9090)
			deliveryStore.Clock = func() time.Time { return now }
			deliveryStore.MCPManifestLister = test.lister(t)
			sender := &recordingRuntimeCommandSender{response: &agentruntimev1.RuntimeInputCommandResponse{
				Status: agentruntimev1.RuntimeCommandStatus_RUNTIME_COMMAND_STATUS_ACCEPTED,
			}}
			runner := &JobRunner{
				Queue: tetralqueue.NewServer(queueStore, nil), Workspaces: staticWorkspaceLister{"default"},
				Deliverer: manifestCompositionDeliverer{direct: RuntimePodDirectDeliverer{Store: deliveryStore, Sender: sender}},
				Config:    JobRunnerConfig{MaxJobs: 1, LeaseDuration: time.Minute, HeartbeatInterval: time.Hour},
			}
			if err := runner.RunOnce(context.Background()); err != nil {
				t.Fatalf("run single-attempt input: %v", err)
			}
			if len(sender.requests) != 1 || sender.requests[0].GetRuntimeInputId() != job.RuntimeInputID {
				t.Fatalf("Runtime requests = %#v; want original input once", sender.requests)
			}
			var readiness, diagnostic, inboxStatus, inputQueueStatus string
			if err := admin.QueryRowContext(context.Background(), `SELECT readiness, diagnostic FROM session_mcp_manifests
				WHERE workspace_id='default' AND session_id=$1 AND mcp_server_name='github'`, sessionID).Scan(&readiness, &diagnostic); err != nil {
				t.Fatalf("read durable unready manifest: %v", err)
			}
			if err := admin.QueryRowContext(context.Background(), `SELECT status FROM session_runtime_inbox
				WHERE workspace_id='default' AND runtime_input_id=$1`, job.RuntimeInputID).Scan(&inboxStatus); err != nil {
				t.Fatalf("read original Inbox custody: %v", err)
			}
			if err := admin.QueryRowContext(context.Background(), `SELECT status FROM queue_jobs
				WHERE workspace_id='default' AND kind='runtime_input' AND payload_json::jsonb ->> 'runtime_input_id'=$1`, job.RuntimeInputID).Scan(&inputQueueStatus); err != nil {
				t.Fatalf("read original Queue custody: %v", err)
			}
			if readiness != "unready" || diagnostic != test.diagnostic || inboxStatus != "accepted" || inputQueueStatus != queue.StatusAcknowledged {
				t.Fatalf("manifest/Inbox/Queue = %s/%s %s/%s; want unready/%s accepted/succeeded", readiness, diagnostic, inboxStatus, inputQueueStatus, test.diagnostic)
			}
			assertRuntimeMCPManifestQueueJob(t, admin, "default", sessionID, "github", 1)
			var manifestJobs, sessionErrors int
			if err := admin.QueryRowContext(context.Background(), `SELECT count(*) FROM queue_jobs
				WHERE workspace_id='default' AND kind='runtime_config_update'
				AND payload_json::jsonb ->> 'session_id'=$1 AND payload_json::jsonb ->> 'mcp_server_name'='github'`, sessionID).Scan(&manifestJobs); err != nil {
				t.Fatalf("count manifest Queue custody: %v", err)
			}
			if err := admin.QueryRowContext(context.Background(), `SELECT count(*) FROM session_events
				WHERE workspace_id='default' AND session_id=$1 AND type='session.error'`, sessionID).Scan(&sessionErrors); err != nil {
				t.Fatalf("count Session errors: %v", err)
			}
			if manifestJobs != 1 || sessionErrors != 0 {
				t.Fatalf("manifest jobs/session errors = %d/%d; want 1/0", manifestJobs, sessionErrors)
			}
		})
	}
	stats := connector.finish(t)
	if stats.ListCalls != 4 || !stats.LogsRedacted {
		t.Fatalf("production Connector failure composition = %+v; want four typed calls with redacted logs", stats)
	}
}

func TestPostgreSQLRuntimeDeliveryStoreRejectsUnclassifiedMCPFailureWithoutDurableTransition(t *testing.T) {
	tests := []struct {
		name   string
		code   codes.Code
		values []string
	}{
		{name: "invalid request", code: codes.InvalidArgument},
		{name: "unauthenticated", code: codes.Unauthenticated},
		{name: "permission denied", code: codes.PermissionDenied},
		{name: "internal", code: codes.Internal},
		{name: "failed precondition without token", code: codes.FailedPrecondition},
		{name: "duplicate token", code: codes.Unavailable, values: []string{"server_unavailable", "server_unavailable"}},
		{name: "wrong status pair", code: codes.FailedPrecondition, values: []string{"server_unavailable"}},
		{name: "unknown token", code: codes.Unavailable, values: []string{"unknown"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
			const (
				sessionID = "sesn_initial_mcp_fail_closed"
				threadID  = "thr_initial_mcp_fail_closed"
			)
			seedMCPFamilySession(t, admin, sessionID, threadID, "claude")
			seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, "bind_initial_mcp_fail_closed", 1, "pod_initial_mcp_fail_closed")
			job := RuntimeJob{
				JobID: "qjob_initial_mcp_fail_closed", LeaseToken: "lease_initial_mcp_fail_closed", Kind: queue.KindRuntimeInput,
				WorkspaceID: "default", SessionID: sessionID, SessionThreadID: threadID,
				RuntimeInputID: "rin_initial_mcp_fail_closed", EventIDs: []string{"evt_initial_mcp_fail_closed"},
				SequenceFrom: 1, SequenceTo: 1, InputKind: "messages",
			}
			seedBridgeAPIEvent(t, admin, "default", sessionID, threadID, job.EventIDs[0], 1, "user.message", `{"content":[{"type":"text","text":"retain custody"}]}`)
			seedRuntimeInboxBirthForJob(t, admin, job)
			store := NewPostgreSQLRuntimeDeliveryStore(dbconnect.NewClientForTesting(runtime), 9090)
			store.MCPManifestLister = newExactFailureMCPManifestLister(t, test.code, test.values)
			sender := &recordingRuntimeCommandSender{response: &agentruntimev1.RuntimeInputCommandResponse{Status: agentruntimev1.RuntimeCommandStatus_RUNTIME_COMMAND_STATUS_ACCEPTED}}
			result, err := (RuntimePodDirectDeliverer{Store: store, Sender: sender}).DeliverRuntimeJob(context.Background(), job)
			if err != nil || result.Status == RuntimeDeliveryAccepted || len(sender.requests) != 0 {
				t.Fatalf("unclassified discovery delivery = %+v/%v requests:%d; want retained input without Runtime", result, err, len(sender.requests))
			}
			var inboxStatus string
			var manifests, manifestJobs, sessionErrors int
			if err := admin.QueryRowContext(context.Background(), `SELECT status FROM session_runtime_inbox
				WHERE workspace_id='default' AND runtime_input_id=$1`, job.RuntimeInputID).Scan(&inboxStatus); err != nil {
				t.Fatalf("read retained Inbox: %v", err)
			}
			if err := admin.QueryRowContext(context.Background(), `SELECT
				(SELECT count(*) FROM session_mcp_manifests WHERE workspace_id='default' AND session_id=$1),
				(SELECT count(*) FROM queue_jobs WHERE workspace_id='default' AND kind='runtime_config_update'
				 AND payload_json::jsonb->>'session_id'=$1),
				(SELECT count(*) FROM session_events WHERE workspace_id='default' AND session_id=$1 AND type='session.error')`, sessionID).Scan(&manifests, &manifestJobs, &sessionErrors); err != nil {
				t.Fatalf("count fail-closed discovery facts: %v", err)
			}
			if inboxStatus != "queued" || manifests != 0 || manifestJobs != 0 || sessionErrors != 0 {
				t.Fatalf("fail-closed discovery facts = Inbox:%s manifests:%d jobs:%d errors:%d; want queued/0/0/0",
					inboxStatus, manifests, manifestJobs, sessionErrors)
			}
		})
	}
}

func TestPostgreSQLRuntimeDeliveryStoreInitialMCPFailureRacesManifestNotification(t *testing.T) {
	setup := func(t *testing.T, lister MCPManifestLister) (*sql.DB, *PostgreSQLRuntimeDeliveryStore, RuntimeJob, *recordingRuntimeCommandSender) {
		t.Helper()
		runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
		const (
			sessionID = "sesn_initial_mcp_race"
			threadID  = "thr_initial_mcp_race"
		)
		seedMCPFamilySession(t, admin, sessionID, threadID, "claude")
		seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, "bind_initial_mcp_race", 1, "pod_initial_mcp_race")
		job := RuntimeJob{
			JobID: "qjob_initial_mcp_race", LeaseToken: "lease_initial_mcp_race", Kind: queue.KindRuntimeInput,
			WorkspaceID: "default", SessionID: sessionID, SessionThreadID: threadID,
			RuntimeInputID: "rin_initial_mcp_race", EventIDs: []string{"evt_initial_mcp_race"},
			SequenceFrom: 1, SequenceTo: 1, InputKind: "messages",
		}
		seedBridgeAPIEvent(t, admin, "default", sessionID, threadID, job.EventIDs[0], 1, "user.message", `{"content":[{"type":"text","text":"race"}]}`)
		seedRuntimeInboxBirthForJob(t, admin, job)
		store := NewPostgreSQLRuntimeDeliveryStore(dbconnect.NewClientForTesting(runtime), 9090)
		store.MCPManifestLister = lister
		sender := &recordingRuntimeCommandSender{response: &agentruntimev1.RuntimeInputCommandResponse{Status: agentruntimev1.RuntimeCommandStatus_RUNTIME_COMMAND_STATUS_ACCEPTED}}
		return admin, store, job, sender
	}
	readState := func(t *testing.T, admin *sql.DB, wantGeneration int64, wantReadiness string, wantJobs int) {
		t.Helper()
		var generation int64
		var readiness string
		if err := admin.QueryRowContext(context.Background(), `SELECT manifest_generation, readiness FROM session_mcp_manifests
			WHERE workspace_id='default' AND session_id='sesn_initial_mcp_race' AND mcp_server_name='github'`).Scan(&generation, &readiness); err != nil {
			t.Fatalf("read raced manifest: %v", err)
		}
		var jobs int
		if err := admin.QueryRowContext(context.Background(), `SELECT count(*) FROM queue_jobs
			WHERE workspace_id='default' AND kind='runtime_config_update'
			AND payload_json::jsonb ->> 'session_id'='sesn_initial_mcp_race'
			AND payload_json::jsonb ->> 'mcp_server_name'='github'`).Scan(&jobs); err != nil {
			t.Fatalf("count raced manifest Queue jobs: %v", err)
		}
		if generation != wantGeneration || readiness != wantReadiness || jobs != wantJobs {
			t.Fatalf("raced manifest = generation %d readiness %s jobs %d; want %d/%s/%d", generation, readiness, jobs, wantGeneration, wantReadiness, wantJobs)
		}
	}

	t.Run("notification wins expected absence", func(t *testing.T) {
		lister := &racingInitialMCPManifestLister{initialStarted: make(chan struct{}), releaseInitial: make(chan struct{}), blockInitial: true}
		admin, store, job, sender := setup(t, lister)
		resultCh := make(chan RuntimeDeliveryResult, 1)
		errorCh := make(chan error, 1)
		go func() {
			result, err := (RuntimePodDirectDeliverer{Store: store, Sender: sender}).DeliverRuntimeJob(context.Background(), job)
			resultCh <- result
			errorCh <- err
		}()
		select {
		case <-lister.initialStarted:
		case <-time.After(5 * time.Second):
			t.Fatal("initial discovery did not reach the staged list")
		}
		bridge := NewPostgreSQLBridgeAPIStore(store.Client)
		bridge.MCPManifestLister = lister
		mustAcceptMCPManifestChange(t, bridge, job.SessionID, "etag_notification")
		close(lister.releaseInitial)
		if err := <-errorCh; err != nil {
			t.Fatalf("deliver after notification winner: %v", err)
		}
		if result := <-resultCh; result.Status != RuntimeDeliveryAccepted || len(sender.requests) != 1 {
			t.Fatalf("delivery after notification winner = %#v requests %d; want accepted/1", result, len(sender.requests))
		}
		readState(t, admin, 1, "ready", 1)
	})

	t.Run("initial failure wins before notification", func(t *testing.T) {
		lister := &racingInitialMCPManifestLister{}
		admin, store, job, sender := setup(t, lister)
		result, err := (RuntimePodDirectDeliverer{Store: store, Sender: sender}).DeliverRuntimeJob(context.Background(), job)
		if err != nil || result.Status != RuntimeDeliveryAccepted || len(sender.requests) != 1 {
			t.Fatalf("delivery after initial failure = %#v/%v requests %d; want accepted/nil/1", result, err, len(sender.requests))
		}
		readState(t, admin, 1, "unready", 1)
		bridge := NewPostgreSQLBridgeAPIStore(store.Client)
		bridge.MCPManifestLister = lister
		mustAcceptMCPManifestChange(t, bridge, job.SessionID, "etag_notification")
		readState(t, admin, 2, "ready", 2)
	})
}

func TestPostgreSQLRuntimeDeliveryStoreInitialMCPTransitionRollbackRetainsInputCustody(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID = "sesn_initial_mcp_rollback"
		threadID  = "thr_initial_mcp_rollback"
	)
	seedMCPFamilySession(t, admin, sessionID, threadID, "claude")
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, "bind_initial_mcp_rollback", 1, "pod_initial_mcp_rollback")
	job := RuntimeJob{
		JobID: "qjob_initial_mcp_rollback", LeaseToken: "lease_initial_mcp_rollback", Kind: queue.KindRuntimeInput,
		WorkspaceID: "default", SessionID: sessionID, SessionThreadID: threadID,
		RuntimeInputID: "rin_initial_mcp_rollback", EventIDs: []string{"evt_initial_mcp_rollback"},
		SequenceFrom: 1, SequenceTo: 1, InputKind: "messages",
	}
	seedBridgeAPIEvent(t, admin, "default", sessionID, threadID, job.EventIDs[0], 1, "user.message", `{"content":[{"type":"text","text":"retain"}]}`)
	seedRuntimeInboxBirthForJob(t, admin, job)
	if _, err := admin.ExecContext(context.Background(), `DROP TABLE queue_jobs`); err != nil {
		t.Fatalf("remove manifest Queue persistence: %v", err)
	}
	store := NewPostgreSQLRuntimeDeliveryStore(dbconnect.NewClientForTesting(runtime), 9090)
	store.MCPManifestLister = &recordingMCPManifestLister{err: mcpManifestDiscoveryError{diagnostic: mcpManifestDiagnosticDiscoveryUnavailable}}
	sender := &recordingRuntimeCommandSender{response: &agentruntimev1.RuntimeInputCommandResponse{Status: agentruntimev1.RuntimeCommandStatus_RUNTIME_COMMAND_STATUS_ACCEPTED}}
	result, err := (RuntimePodDirectDeliverer{Store: store, Sender: sender}).DeliverRuntimeJob(context.Background(), job)
	if err != nil || result.Status != RuntimeDeliveryRejected || !result.Retryable || len(sender.requests) != 0 {
		t.Fatalf("delivery after manifest transaction rollback = %#v/%v requests %d; want retryable rejection/nil/0", result, err, len(sender.requests))
	}
	var manifests int
	var inboxStatus string
	if err := admin.QueryRowContext(context.Background(), `SELECT count(*) FROM session_mcp_manifests
		WHERE workspace_id='default' AND session_id=$1`, sessionID).Scan(&manifests); err != nil {
		t.Fatalf("count rolled-back manifests: %v", err)
	}
	if err := admin.QueryRowContext(context.Background(), `SELECT status FROM session_runtime_inbox
		WHERE workspace_id='default' AND runtime_input_id=$1`, job.RuntimeInputID).Scan(&inboxStatus); err != nil {
		t.Fatalf("read retained Inbox custody: %v", err)
	}
	if manifests != 0 || inboxStatus != "queued" {
		t.Fatalf("rolled-back manifests/Inbox = %d/%s; want 0/queued", manifests, inboxStatus)
	}
}

func TestMCPManifestProductionCompositionRemovesWarmAndColdToolCatalogEntry(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const sessionID = "sesn_manifest_composition"
	seedBridgeAPISession(t, admin, "default", sessionID, "thrd_"+sessionID)
	seedBridgeAPIAgentConfig(t, admin, "default", sessionID, `{"name":"agent","model":"anthropic/claude-opus-4-8","tools":[{"type":"mcp_toolset","mcp_server_name":"github"}],"mcp_servers":[{"type":"url","name":"github","url":"https://api.githubcopilot.com/mcp/"}],"skills":[],"metadata":{}}`)
	if _, err := admin.ExecContext(context.Background(), `UPDATE sessions
		SET installed_tools_json = '{"tools":[{"type":"tetral_agent_toolset","family":"claude"},{"type":"mcp_toolset","mcp_server_name":"github"}],"mcp_servers":[{"type":"url","name":"github","url":"https://api.githubcopilot.com/mcp/"}]}'
		WHERE workspace_id = 'default' AND id = $1`, sessionID); err != nil {
		t.Fatalf("seed manifest composition tools: %v", err)
	}
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, "bind_manifest_composition", 1, "pod_manifest_composition")

	exactTool := exactBoundMCPManifestTool(t)
	lister := &recordingMCPManifestLister{results: []MCPManifestListResult{
		mcpManifestResult("etag_ready", "github_search"),
		{ManifestETag: "etag_over", Tools: []MCPManifestTool{{
			Name: exactTool.Name, Description: exactTool.Description + "x", InputSchemaJSON: exactTool.InputSchemaJSON,
		}}},
		{ManifestETag: "etag_over", Tools: []MCPManifestTool{{
			Name: exactTool.Name, Description: exactTool.Description + "x", InputSchemaJSON: exactTool.InputSchemaJSON,
		}}},
	}}
	bridge := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	bridge.RuntimeBindingTokenHMACKey = []byte("manifest-composition-binding-token-key")
	bridge.MCPManifestLister = lister
	mustAcceptMCPManifestChange(t, bridge, sessionID, "etag_ready")
	readyPayload := deliverQueuedManifestPayload(t, runtime, admin, sessionID, 1)

	request := &bridgev1.McpManifestChangedRequest{
		WorkspaceId: "default", SessionId: sessionID, McpServerName: "github", ManifestEtag: "etag_over",
	}
	for attempt := 0; attempt < 2; attempt++ {
		response, err := bridge.McpManifestChanged(context.Background(), request)
		if err != nil || (attempt == 0 && response.GetCommitted() == nil) || (attempt == 1 && response.GetDuplicate() == nil) {
			t.Fatalf("over-cap manifest attempt %d = %+v err %v; want committed then duplicate", attempt+1, response, err)
		}
	}
	var manifestRows, queueJobs int
	if err := admin.QueryRowContext(context.Background(), `SELECT count(*) FROM session_mcp_manifests
		WHERE workspace_id = 'default' AND session_id = $1 AND manifest_generation = 2
		AND readiness = 'unready' AND diagnostic = 'manifest_too_large'`, sessionID).Scan(&manifestRows); err != nil {
		t.Fatalf("read unready manifest: %v", err)
	}
	if err := admin.QueryRowContext(context.Background(), `SELECT count(*) FROM queue_jobs
		WHERE workspace_id = 'default' AND kind = 'runtime_config_update'
		AND payload_json::jsonb ->> 'session_id' = $1
		AND payload_json::jsonb ->> 'mcp_server_name' = 'github'`, sessionID).Scan(&queueJobs); err != nil {
		t.Fatalf("read manifest queue custody: %v", err)
	}
	if manifestRows != 1 || queueJobs != 2 {
		t.Fatalf("manifest rows/jobs = %d/%d; want one generation-2 row and generations 1+2 queue custody", manifestRows, queueJobs)
	}

	var runtimeConfigPayload string
	client := dbconnect.NewClientForTesting(runtime)
	if err := client.WithWorkspaceTx(context.Background(), "default", "test.manifest_composition_runtime_config", func(tx *dbconnect.Tx) error {
		var err error
		runtimeConfigPayload, _, err = runtimeCommandPayloadForJobTx(context.Background(), tx, RuntimeJob{
			Kind: queue.KindRuntimeConfigUpdate, WorkspaceID: "default", SessionID: sessionID, ConfigGeneration: "1",
		})
		return err
	}); err != nil {
		t.Fatalf("rebuild runtime config: %v", err)
	}
	cold, err := bridge.LoadContext(context.Background(), &bridgev1.LoadContextRequest{
		Scope:          bridgeAPIScope(sessionID, "thrd_"+sessionID, "bind_manifest_composition", 1, "pod_manifest_composition"),
		RuntimeInputId: "rin_manifest_composition_cold",
	})
	if err != nil {
		t.Fatalf("load replacement Runtime context: %v", err)
	}
	sender := &bunRuntimeManifestCompositionSender{
		InputPath:                t.TempDir() + "/manifest-composition.json",
		RuntimeConfigPayloadJSON: runtimeConfigPayload,
		ReadyManifestPayloadJSON: readyPayload,
		ColdContextJSON:          cold.GetContextJson(),
		ColdRuntimeBindingToken:  cold.GetRuntimeBindingToken(),
		ReadyGeneration:          1,
		UnreadyGeneration:        2,
		ToolName:                 "github_search",
	}
	delivery := NewPostgreSQLRuntimeDeliveryStore(dbconnect.NewClientForTesting(runtime), 9090)
	deliveryResult, err := (RuntimePodDirectDeliverer{Store: delivery, Sender: sender}).DeliverRuntimeJob(
		context.Background(), queuedManifestJob(t, admin, sessionID, 2),
	)
	if err != nil {
		t.Fatalf("deliver unready manifest through Runtime command host: %v", err)
	}
	if deliveryResult.Status != RuntimeDeliveryAccepted {
		t.Fatalf("unready manifest delivery = %+v; want accepted Runtime command", deliveryResult)
	}
	if sender.Result.WarmCurrentGeneration != 2 || sender.Result.ColdCurrentGeneration != 2 ||
		sender.Result.WarmMCPConnectorCalls != 0 || sender.Result.WarmNextProviderRequests < 1 ||
		sender.Result.ColdMCPConnectorCalls != 0 || sender.Result.ColdNextProviderRequests < 1 {
		t.Fatalf("Runtime manifest composition = %+v; want warm/cold generation 2, completed next provider requests, and zero connector calls", sender.Result)
	}
}

func TestPostgreSQLMCPManifestExhaustionDefersAndRedrivesCurrentGenerationBeforePartitionFollower(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID = "sesn_manifest_exhaustion_composition"
		threadID  = "thrd_manifest_exhaustion_composition"
	)
	seedMCPFamilySession(t, admin, sessionID, threadID, "claude")
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, "bind_manifest_exhaustion", 1, "pod_manifest_exhaustion")
	bridge := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	bridge.MCPManifestLister = &constantMCPManifestLister{result: mcpManifestResult("etag_exhaustion", "github_search")}
	mustAcceptMCPManifestChange(t, bridge, sessionID, "etag_exhaustion")

	queueStore := queue.NewPostgreSQLStoreWithRetryPolicy(dbconnect.NewClientForTesting(runtime), queue.RetryPolicy{
		BaseDelay: time.Millisecond,
		MaxDelay:  time.Millisecond,
		RandomInt64: func(int64) int64 {
			return 0
		},
	})
	follower := RuntimeJob{
		Kind: queue.KindRuntimeInput, WorkspaceID: "default", SessionID: sessionID, SessionThreadID: threadID,
		RuntimeInputID: "rin_manifest_partition_follower", EventIDs: []string{"evt_manifest_partition_follower"},
		SequenceFrom: 1, SequenceTo: 1, InputKind: "messages",
	}
	seedBridgeAPIEvent(t, admin, "default", sessionID, threadID, follower.EventIDs[0], 1, "user.message", `{"content":[{"type":"text","text":"after manifest"}]}`)
	seedRuntimeInboxBirthForJob(t, admin, follower)
	followerPayload, err := json.Marshal(map[string]any{
		"workspace_id": "default", "session_id": sessionID, "session_thread_id": threadID,
		"runtime_input_id": follower.RuntimeInputID, "event_ids": follower.EventIDs,
		"sequence_from": 1, "sequence_to": 1, "input_kind": "messages",
	})
	if err != nil {
		t.Fatalf("marshal partition follower: %v", err)
	}
	if _, err := queueStore.Enqueue(context.Background(), queue.EnqueueRequest{
		WorkspaceID: workspace.DefaultID, Kind: queue.KindRuntimeInput,
		PartitionKey:   queue.FormatSessionPartitionKey(workspace.DefaultID, sessionID),
		DedupeKey:      queue.FormatRuntimeInputDedupeKey(workspace.DefaultID, sessionID, follower.RuntimeInputID),
		PayloadVersion: 1, PayloadJSON: followerPayload, MaxAttempts: queue.DefaultMaxAttempts,
		Now: time.Now().UTC().Add(-time.Minute),
	}); err != nil {
		t.Fatalf("enqueue partition follower: %v", err)
	}

	var manifestJobID string
	var manifestMaxAttempts int
	if err := admin.QueryRowContext(context.Background(), `SELECT id, max_attempts FROM queue_jobs
		WHERE workspace_id='default' AND kind='runtime_config_update'
		AND payload_json::jsonb ->> 'session_id'=$1
		AND payload_json::jsonb ->> 'mcp_server_name'='github'`, sessionID).Scan(&manifestJobID, &manifestMaxAttempts); err != nil {
		t.Fatalf("read manifest Queue job: %v", err)
	}
	rejected := &agentruntimev1.RuntimeInputCommandResponse{
		Status:    agentruntimev1.RuntimeCommandStatus_RUNTIME_COMMAND_STATUS_REJECTED,
		Retryable: true,
		ErrorCode: agentruntimev1.RuntimeInputErrorCode_RUNTIME_INPUT_ERROR_CODE_BINDING_IDENTITY_MISMATCH,
	}
	responses := make([]*agentruntimev1.RuntimeInputCommandResponse, 0, manifestMaxAttempts+1)
	for range manifestMaxAttempts {
		responses = append(responses, rejected)
	}
	responses = append(responses, &agentruntimev1.RuntimeInputCommandResponse{Status: agentruntimev1.RuntimeCommandStatus_RUNTIME_COMMAND_STATUS_ACCEPTED})
	sender := &recordingRuntimeCommandSender{responses: responses}
	queueServer := tetralqueue.NewServer(queueStore, nil)
	deliveryStore := NewPostgreSQLRuntimeDeliveryStore(dbconnect.NewClientForTesting(runtime), 9090)
	composedDeliverer := manifestCompositionDeliverer{direct: RuntimePodDirectDeliverer{Store: deliveryStore, Sender: sender}}
	runner := &JobRunner{
		Queue: queueServer, Workspaces: staticWorkspaceLister{"default"},
		Deliverer: composedDeliverer,
		Config:    JobRunnerConfig{MaxJobs: 1, LeaseDuration: time.Minute, HeartbeatInterval: time.Hour},
	}
	for attempt := 1; attempt <= manifestMaxAttempts; attempt++ {
		if _, err := admin.ExecContext(context.Background(), `UPDATE queue_jobs SET available_at=clock_timestamp()-interval '1 second' WHERE workspace_id='default' AND id=$1`, manifestJobID); err != nil {
			t.Fatalf("make manifest attempt %d available: %v", attempt, err)
		}
		if err := runner.RunOnce(context.Background()); err != nil {
			t.Fatalf("run manifest attempt %d: %v", attempt, err)
		}
	}

	var generation int64
	var readiness, diagnostic, queueStatus string
	var attemptCount int
	if err := admin.QueryRowContext(context.Background(), `SELECT manifest_generation, readiness, diagnostic FROM session_mcp_manifests
		WHERE workspace_id='default' AND session_id=$1 AND mcp_server_name='github'`, sessionID).Scan(&generation, &readiness, &diagnostic); err != nil {
		t.Fatalf("read exhausted manifest: %v", err)
	}
	if err := admin.QueryRowContext(context.Background(), `SELECT status, attempt_count FROM queue_jobs WHERE workspace_id='default' AND id=$1`, manifestJobID).Scan(&queueStatus, &attemptCount); err != nil {
		t.Fatalf("read deferred manifest custody: %v", err)
	}
	if generation != 2 || readiness != "unready" || diagnostic != "delivery_exhausted" || queueStatus != queue.StatusPending || attemptCount != manifestMaxAttempts-1 {
		t.Fatalf("exhausted manifest/Queue = generation %d %s/%s, %s attempt %d; want generation 2 unready/delivery_exhausted, pending attempt %d", generation, readiness, diagnostic, queueStatus, attemptCount, manifestMaxAttempts-1)
	}
	blocked, err := queueStore.Lease(context.Background(), queue.LeaseRequest{
		WorkspaceID: workspace.DefaultID, Kinds: []string{queue.KindRuntimeInput}, LeaseOwner: "partition-proof",
		MaxJobs: 1, LeaseDuration: time.Minute, Now: time.Now().UTC(),
	})
	if err != nil || len(blocked) != 0 {
		t.Fatalf("partition follower lease before manifest redrive = %d/%v; want blocked", len(blocked), err)
	}

	if _, err := admin.ExecContext(context.Background(), `UPDATE queue_jobs SET available_at=clock_timestamp()-interval '1 second' WHERE workspace_id='default' AND id=$1`, manifestJobID); err != nil {
		t.Fatalf("make deferred manifest available: %v", err)
	}
	if err := runner.RunOnce(context.Background()); err != nil {
		t.Fatalf("redrive deferred manifest: %v", err)
	}
	if len(sender.requests) != manifestMaxAttempts+1 {
		t.Fatalf("Runtime manifest requests = %d; want %d", len(sender.requests), manifestMaxAttempts+1)
	}
	redrive := sender.requests[len(sender.requests)-1]
	var rebuilt struct {
		MCPManifest struct {
			Generation int64  `json:"manifest_generation"`
			Readiness  string `json:"readiness"`
			Diagnostic string `json:"diagnostic"`
		} `json:"mcp_manifest"`
	}
	if err := json.Unmarshal([]byte(redrive.GetPayloadJson()), &rebuilt); err != nil {
		t.Fatalf("decode redriven manifest command: %v", err)
	}
	if redrive.GetRuntimeInputId() != runtimeMCPManifestInputID(sessionID, "github", 2) || rebuilt.MCPManifest.Generation != 2 || rebuilt.MCPManifest.Readiness != "unready" || rebuilt.MCPManifest.Diagnostic != "delivery_exhausted" {
		t.Fatalf("redriven command = input %q payload %+v; want durable generation 2 unready payload", redrive.GetRuntimeInputId(), rebuilt.MCPManifest)
	}

	followers, err := queueStore.Lease(context.Background(), queue.LeaseRequest{
		WorkspaceID: workspace.DefaultID, Kinds: []string{queue.KindRuntimeInput}, LeaseOwner: "partition-proof",
		MaxJobs: 1, LeaseDuration: time.Minute, Now: time.Now().UTC(),
	})
	if err != nil || len(followers) != 1 || followers[0].DedupeKey != queue.FormatRuntimeInputDedupeKey(workspace.DefaultID, sessionID, follower.RuntimeInputID) {
		t.Fatalf("partition follower after manifest ACK = %#v/%v; want one exact follower", followers, err)
	}
}

type manifestCompositionDeliverer struct {
	direct RuntimePodDirectDeliverer
}

func (d manifestCompositionDeliverer) DeliverRuntimeJob(ctx context.Context, job RuntimeJob) (RuntimeDeliveryResult, error) {
	return d.direct.DeliverRuntimeJob(ctx, job)
}

func (d manifestCompositionDeliverer) FinalizeRuntimeDelivery(ctx context.Context, job RuntimeJob, result RuntimeDeliveryResult) (RuntimeDeliveryResult, error) {
	return d.direct.FinalizeRuntimeDelivery(ctx, job, result)
}

func (d manifestCompositionDeliverer) ReplayRuntimeDeliveryFinalization(ctx context.Context, job RuntimeJob) (RuntimeDeliveryResult, bool, error) {
	return d.direct.ReplayRuntimeDeliveryFinalization(ctx, job)
}

func deliverQueuedManifestPayload(t *testing.T, runtime *sql.DB, admin *sql.DB, sessionID string, generation int64) string {
	t.Helper()
	job := queuedManifestJob(t, admin, sessionID, generation)
	sender := &recordingRuntimeCommandSender{response: &agentruntimev1.RuntimeInputCommandResponse{
		Status: agentruntimev1.RuntimeCommandStatus_RUNTIME_COMMAND_STATUS_ACCEPTED,
	}}
	delivery := NewPostgreSQLRuntimeDeliveryStore(dbconnect.NewClientForTesting(runtime), 9090)
	result, err := (RuntimePodDirectDeliverer{Store: delivery, Sender: sender}).DeliverRuntimeJob(context.Background(), job)
	if err != nil || result.Status != RuntimeDeliveryAccepted || len(sender.requests) != 1 {
		t.Fatalf("deliver manifest generation %d = %+v, %v, requests %d", generation, result, err, len(sender.requests))
	}
	return sender.requests[0].GetPayloadJson()
}

func queuedManifestJob(t *testing.T, admin *sql.DB, sessionID string, generation int64) RuntimeJob {
	t.Helper()
	var queuedJobID, queuedPayload string
	if err := admin.QueryRowContext(context.Background(), `SELECT id, payload_json FROM queue_jobs
		WHERE workspace_id = 'default' AND kind = 'runtime_config_update'
		AND payload_json::jsonb ->> 'session_id' = $1
		AND payload_json::jsonb ->> 'mcp_server_name' = 'github'
		AND payload_json::jsonb ->> 'manifest_generation' = $2
		ORDER BY created_at LIMIT 1`, sessionID, fmt.Sprint(generation)).Scan(&queuedJobID, &queuedPayload); err != nil {
		t.Fatalf("read queued manifest generation %d: %v", generation, err)
	}
	job, err := DecodeRuntimeJob(&queuev1.QueueJob{
		Id: queuedJobID, WorkspaceId: "default", Kind: queue.KindRuntimeConfigUpdate,
		LeaseToken: "lease_manifest_composition", PayloadJson: queuedPayload,
	})
	if err != nil {
		t.Fatalf("decode queued manifest generation %d: %v", generation, err)
	}
	return job
}

type runtimeManifestCompositionResult struct {
	CommandResponse struct {
		Status            int32  `json:"status"`
		Retryable         bool   `json:"retryable"`
		ErrorCode         int32  `json:"errorCode"`
		SessionID         string `json:"sessionId"`
		RuntimeInputID    string `json:"runtimeInputId"`
		BindingID         string `json:"bindingId"`
		BindingGeneration int64  `json:"bindingGeneration"`
	} `json:"commandResponse"`
	WarmCurrentGeneration    int `json:"warmCurrentGeneration"`
	ColdCurrentGeneration    int `json:"coldCurrentGeneration"`
	WarmMCPConnectorCalls    int `json:"warmMcpConnectorCalls"`
	WarmNextProviderRequests int `json:"warmNextProviderRequests"`
	ColdMCPConnectorCalls    int `json:"coldMcpConnectorCalls"`
	ColdNextProviderRequests int `json:"coldNextProviderRequests"`
}

type bunRuntimeManifestCompositionSender struct {
	InputPath                string
	RuntimeConfigPayloadJSON string
	ReadyManifestPayloadJSON string
	ColdContextJSON          string
	ColdRuntimeBindingToken  string
	ReadyGeneration          int
	UnreadyGeneration        int
	ToolName                 string
	Result                   runtimeManifestCompositionResult
}

func (s *bunRuntimeManifestCompositionSender) SendRuntimeCommand(ctx context.Context, _ RuntimePodTarget, request *agentruntimev1.RuntimeInputCommandRequest) (*agentruntimev1.RuntimeInputCommandResponse, error) {
	inputJSON, err := json.Marshal(map[string]any{
		"workspaceId":              request.GetWorkspaceId(),
		"sessionId":                request.GetSessionId(),
		"runtimeConfigPayloadJson": s.RuntimeConfigPayloadJSON,
		"readyManifestPayloadJson": s.ReadyManifestPayloadJSON,
		"runtimeCommandRequest": map[string]any{
			"requestId":          request.GetRequestId(),
			"workspaceId":        request.GetWorkspaceId(),
			"sessionId":          request.GetSessionId(),
			"sessionThreadId":    request.GetSessionThreadId(),
			"bindingId":          request.GetBindingId(),
			"bindingGeneration":  request.GetBindingGeneration(),
			"targetPodNamespace": request.GetTargetPodNamespace(),
			"targetPodName":      request.GetTargetPodName(),
			"targetPodUid":       request.GetTargetPodUid(),
			"targetPodIp":        request.GetTargetPodIp(),
			"runtimeInputId":     request.GetRuntimeInputId(),
			"eventIds":           append([]string{}, request.GetEventIds()...),
			"sequenceFrom":       request.GetSequenceFrom(),
			"sequenceTo":         request.GetSequenceTo(),
			"commandKind":        request.GetCommandKind(),
			"payloadJson":        request.GetPayloadJson(),
		},
		"coldContextJson":         s.ColdContextJSON,
		"coldRuntimeBindingToken": s.ColdRuntimeBindingToken,
		"readyGeneration":         s.ReadyGeneration,
		"unreadyGeneration":       s.UnreadyGeneration,
		"toolName":                s.ToolName,
	})
	if err != nil {
		return nil, fmt.Errorf("encode Runtime composition input: %w", err)
	}
	if err := os.WriteFile(s.InputPath, inputJSON, 0o600); err != nil {
		return nil, fmt.Errorf("write Runtime composition input: %w", err)
	}
	command := exec.CommandContext(ctx, "bun", "packages/runtime-pod/test/fixtures/mcp-manifest-composition.ts", s.InputPath) //nolint:gosec // Fixed repository script and test-owned input.
	command.Dir = "../agent-runtime"
	output, err := command.CombinedOutput()
	if err != nil {
		return nil, fmt.Errorf("run Runtime manifest composition: %w: %s", err, output)
	}
	if err := json.Unmarshal(output, &s.Result); err != nil {
		return nil, fmt.Errorf("decode Runtime manifest composition: %w: %s", err, output)
	}
	return &agentruntimev1.RuntimeInputCommandResponse{
		Status:            agentruntimev1.RuntimeCommandStatus(s.Result.CommandResponse.Status),
		Retryable:         s.Result.CommandResponse.Retryable,
		ErrorCode:         agentruntimev1.RuntimeInputErrorCode(s.Result.CommandResponse.ErrorCode),
		SessionId:         s.Result.CommandResponse.SessionID,
		RuntimeInputId:    s.Result.CommandResponse.RuntimeInputID,
		BindingId:         s.Result.CommandResponse.BindingID,
		BindingGeneration: s.Result.CommandResponse.BindingGeneration,
	}, nil
}

func TestJobRunnerRuntimeDeliveryStoreDiscoversInitialMCPManifestThroughProductionAssembly(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		workspaceID = "default"
		sessionID   = "sesn_job_runner_initial_mcp"
		threadID    = "thr_job_runner_initial_mcp"
		eventID     = "evt_job_runner_initial_mcp"
	)
	seedBridgeAPISession(t, admin, workspaceID, sessionID, threadID)
	if _, err := admin.ExecContext(context.Background(), `UPDATE sessions SET installed_tools_json = '{"tools":[{"type":"tetral_agent_toolset","family":"claude"},{"type":"mcp_toolset","mcp_server_name":"github","default_config":{"enabled":false,"permission_policy":{"type":"always_ask"}},"configs":[{"name":"github_search","enabled":true,"permission_policy":{"type":"always_allow"}}]}],"mcp_servers":[{"type":"url","name":"github","url":"https://api.githubcopilot.com/mcp/"}]}' WHERE workspace_id = $1 AND id = $2`, workspaceID, sessionID); err != nil {
		t.Fatalf("seed durable initial MCP config: %v", err)
	}
	seedBridgeAPIEvent(t, admin, workspaceID, sessionID, threadID, eventID, 1, "user.message", `{"content":[{"type":"text","text":"hello"}]}`)

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen for MCP connector: %v", err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	connector := &recordingMCPConnectorServer{}
	server := grpc.NewServer()
	providergatewayv1.RegisterMcpConnectorServiceServer(server, connector)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)

	tokenPath := t.TempDir() + "/gateway-token"
	if err := os.WriteFile(tokenPath, []byte("test-gateway-token"), 0o600); err != nil {
		t.Fatalf("write gateway token: %v", err)
	}
	candidate := enginekubernetes.BindingCandidate{
		Namespace: "tetral-agent-runtime",
		PodName:   "runtime-pod-initial-mcp",
		PodUID:    "pod-uid-initial-mcp",
		PodIP:     "10.0.0.42",
	}
	store := NewJobRunnerRuntimeDeliveryStore(
		dbconnect.NewClientForTesting(runtime),
		nil,
		JobRunnerConfig{
			AgentRuntimeGRPCPort:    9090,
			MCPConnectorGRPCAddress: listener.Addr().String(),
			GatewayTokenPath:        tokenPath,
		},
		func() enginekubernetes.BindingVisibilitySnapshot {
			return enginekubernetes.NewBindingVisibilitySnapshotForTest(true, []enginekubernetes.BindingCandidate{candidate})
		},
	)
	store.Clock = func() time.Time { return time.Date(2026, 1, 1, 0, 0, 30, 0, time.UTC) }
	job := RuntimeJob{
		JobID:           "qjob_job_runner_initial_mcp",
		LeaseToken:      "lease_job_runner_initial_mcp",
		Kind:            queue.KindRuntimeInput,
		WorkspaceID:     workspaceID,
		SessionID:       sessionID,
		SessionThreadID: threadID,
		RuntimeInputID:  "rin_job_runner_initial_mcp",
		EventIDs:        []string{eventID},
		SequenceFrom:    1,
		SequenceTo:      1,
		InputKind:       "messages",
		CommandKind:     agentruntimev1.RuntimeCommandKind_RUNTIME_COMMAND_KIND_MESSAGES,
		PayloadJSON:     `{"type":"messages"}`,
	}
	seedRuntimeInboxBirthForJob(t, admin, job)

	plan, err := store.PrepareRuntimeCommand(context.Background(), job)
	if err != nil {
		t.Fatalf("PrepareRuntimeCommand: %v", err)
	}
	requests := connector.recordedRequests()
	if len(requests) != 1 ||
		requests[0].GetWorkspaceId() != workspaceID ||
		requests[0].GetSessionId() != sessionID ||
		requests[0].GetMcpServerName() != "github" {
		t.Errorf("connector requests = %#v; want one request for %s/%s/github", requests, workspaceID, sessionID)
	}
	var readiness string
	if err := admin.QueryRowContext(context.Background(),
		`SELECT readiness FROM session_mcp_manifests WHERE workspace_id = $1 AND session_id = $2 AND mcp_server_name = 'github'`,
		workspaceID, sessionID,
	).Scan(&readiness); err != nil {
		t.Fatalf("read captured MCP manifest: %v", err)
	}
	if readiness != "ready" {
		t.Fatalf("captured MCP manifest readiness = %q; want ready", readiness)
	}
	if plan.Request == nil || plan.Request.GetRuntimeInputId() != job.RuntimeInputID {
		t.Fatalf("PrepareRuntimeCommand plan = %#v; want same-attempt runtime input delivery", plan)
	}
}

func TestPostgreSQLInitialMCPRefreshReachesRuntimeWithReadyToolCatalog(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID = "sesn_oauth_initial_manifest"
		threadID  = "thr_oauth_initial_manifest"
		eventID   = "evt_oauth_initial_manifest"
		inputID   = "rin_oauth_initial_manifest"
	)
	seedBridgeAPISession(t, admin, "default", sessionID, threadID)
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, "bind_oauth_manifest", 1, "pod_oauth_manifest")
	seedBridgeAPIAgentConfig(t, admin, "default", sessionID, `{"name":"agent","model":"anthropic/claude-opus-4-8","tools":[{"type":"mcp_toolset","mcp_server_name":"github"}],"mcp_servers":[{"type":"url","name":"github","url":"https://api.githubcopilot.com/mcp/"}],"skills":[],"metadata":{}}`)
	if _, err := admin.ExecContext(context.Background(), `UPDATE sessions SET installed_tools_json = '{"tools":[{"type":"tetral_agent_toolset","family":"claude"},{"type":"mcp_toolset","mcp_server_name":"github"}],"mcp_servers":[{"type":"url","name":"github","url":"https://api.githubcopilot.com/mcp/"}]}' WHERE workspace_id='default' AND id=$1`, sessionID); err != nil {
		t.Fatalf("seed installed MCP toolset: %v", err)
	}
	seedBridgeAPIEvent(t, admin, "default", sessionID, threadID, eventID, 1, "user.message", `{"content":[{"type":"text","text":"search"}]}`)
	connector := startOAuthInitialManifestConnector(t, admin, sessionID)

	job := RuntimeJob{
		Kind: queue.KindRuntimeInput, WorkspaceID: "default", SessionID: sessionID, SessionThreadID: threadID,
		RuntimeInputID: inputID, EventIDs: []string{eventID}, SequenceFrom: 1, SequenceTo: 1,
		InputKind: "messages", CommandKind: agentruntimev1.RuntimeCommandKind_RUNTIME_COMMAND_KIND_MESSAGES,
	}
	seedRuntimeInboxBirthForJob(t, admin, job)
	now := time.Now().UTC()
	queueStore := queue.NewPostgreSQLStoreWithRetryPolicy(dbconnect.NewClientForTesting(runtime), queue.RetryPolicy{
		BaseDelay: time.Millisecond, MaxDelay: time.Millisecond, RandomInt64: func(int64) int64 { return 0 },
	})
	enqueueExhaustionJob(t, queueStore, job, now)
	deliveryStore := NewPostgreSQLRuntimeDeliveryStore(dbconnect.NewClientForTesting(runtime), 9090)
	deliveryStore.MCPManifestLister = NewGatewayMCPManifestLister(connector.address, &countingRuntimeCommandTokenSource{})
	deliveryStore.TargetResolver = KubernetesRuntimeTargetResolver{Snapshot: func() enginekubernetes.BindingVisibilitySnapshot {
		return enginekubernetes.NewBindingVisibilitySnapshotForTest(true, []enginekubernetes.BindingCandidate{{
			Namespace: "tetral-agent-runtime", PodName: "runtime-pod-0", PodUID: "pod_oauth_manifest", PodIP: "10.0.0.10",
		}})
	}}
	var runtimeConfigPayload string
	client := dbconnect.NewClientForTesting(runtime)
	if err := client.WithWorkspaceTx(context.Background(), "default", "test.oauth_manifest_runtime_config", func(tx *dbconnect.Tx) error {
		var err error
		runtimeConfigPayload, _, err = runtimeCommandPayloadForJobTx(context.Background(), tx, RuntimeJob{
			Kind: queue.KindRuntimeConfigUpdate, WorkspaceID: "default", SessionID: sessionID, ConfigGeneration: "1",
		})
		return err
	}); err != nil {
		t.Fatalf("build OAuth Runtime config: %v", err)
	}
	sender := &oauthReadyRuntimeSender{admin: admin, client: client, inputPath: t.TempDir() + "/oauth-ready-provider.json", runtimeConfigPayload: runtimeConfigPayload}
	runner := &JobRunner{
		Queue: tetralqueue.NewServer(queueStore, nil), Workspaces: staticWorkspaceLister{workspace.DefaultID},
		Deliverer: RuntimePodDirectDeliverer{Store: deliveryStore, Sender: sender},
		Config:    JobRunnerConfig{LeaseOwner: "oauth-manifest-composition", MaxJobs: 1, LeaseDuration: time.Minute, HeartbeatInterval: time.Hour},
	}
	active, err := runner.RunOnceWithActivity(context.Background())
	if err != nil || !active {
		t.Fatalf("deliver OAuth-backed initial manifest = active:%t err:%v", active, err)
	}
	if !sender.result.ToolPresent || sender.result.NextProviderRequests != 1 {
		t.Fatalf("Runtime ready Tool Catalog composition = %+v", sender.result)
	}

	var readiness, inboxStatus, inputQueueStatus string
	var diagnostic sql.NullString
	var generation, manifestJobs, sessionErrors int
	if err := admin.QueryRowContext(context.Background(), `SELECT readiness, diagnostic, manifest_generation
		FROM session_mcp_manifests WHERE workspace_id='default' AND session_id=$1 AND mcp_server_name='github'`, sessionID).Scan(&readiness, &diagnostic, &generation); err != nil {
		t.Fatalf("read OAuth-backed manifest: %v", err)
	}
	if err := admin.QueryRowContext(context.Background(), `SELECT status FROM session_runtime_inbox
		WHERE workspace_id='default' AND runtime_input_id=$1`, inputID).Scan(&inboxStatus); err != nil {
		t.Fatalf("read accepted OAuth input: %v", err)
	}
	if err := admin.QueryRowContext(context.Background(), `SELECT status FROM queue_jobs
		WHERE workspace_id='default' AND dedupe_key=$1`, queue.FormatRuntimeInputDedupeKey(workspace.DefaultID, sessionID, inputID)).Scan(&inputQueueStatus); err != nil {
		t.Fatalf("read OAuth input Queue status: %v", err)
	}
	if err := admin.QueryRowContext(context.Background(), `SELECT
		(SELECT count(*) FROM queue_jobs WHERE workspace_id='default' AND kind='runtime_config_update'
		 AND payload_json::jsonb->>'session_id'=$1 AND payload_json::jsonb->>'mcp_server_name'='github'),
		(SELECT count(*) FROM session_events WHERE workspace_id='default' AND session_id=$1 AND type='session.error')`, sessionID).Scan(&manifestJobs, &sessionErrors); err != nil {
		t.Fatalf("count OAuth manifest custody: %v", err)
	}
	if readiness != "ready" || diagnostic.Valid || generation != 1 || inboxStatus != "accepted" ||
		inputQueueStatus != queue.StatusAcknowledged || manifestJobs != 1 || sessionErrors != 0 {
		t.Fatalf("OAuth durable progression = ready:%s diagnostic:%q generation:%d Inbox:%s Queue:%s jobs:%d errors:%d sender:%s",
			readiness, diagnostic.String, generation, inboxStatus, inputQueueStatus, manifestJobs, sessionErrors, sender.lastError)
	}
	stats := connector.finish(t)
	if stats.TokenEndpointAttempts != 1 || stats.ToolsListCalls != 1 || !stats.UsedRotatedToken ||
		!stats.DurableRotation || !stats.LogsRedacted || len(stats.RefreshOutcomes) != 1 ||
		stats.RefreshOutcomes[0].Outcome != "refreshed" || stats.RefreshOutcomes[0].DurableWrite != "committed" ||
		stats.RefreshOutcomes[0].HTTPStatusClass != "2xx" {
		t.Fatalf("OAuth Connector composition stats = %+v", stats)
	}
}

type oauthReadyRuntimeSender struct {
	admin                *sql.DB
	client               *dbconnect.Client
	inputPath            string
	runtimeConfigPayload string
	result               struct {
		ToolPresent          bool `json:"toolPresent"`
		NextProviderRequests int  `json:"nextProviderRequests"`
	}
	lastError string
}

func (s *oauthReadyRuntimeSender) SendRuntimeCommand(ctx context.Context, _ RuntimePodTarget, request *agentruntimev1.RuntimeInputCommandRequest) (*agentruntimev1.RuntimeInputCommandResponse, error) {
	var manifestJobID, manifestJobEnvelope string
	if err := s.admin.QueryRowContext(ctx, `SELECT id, payload_json FROM queue_jobs
		WHERE workspace_id=$1 AND kind='runtime_config_update'
		AND payload_json::jsonb->>'session_id'=$2 AND payload_json::jsonb->>'mcp_server_name'='github'
		ORDER BY created_at LIMIT 1`, request.GetWorkspaceId(), request.GetSessionId()).Scan(&manifestJobID, &manifestJobEnvelope); err != nil {
		s.lastError = fmt.Sprintf("read ready manifest carrier: %v", err)
		return nil, errors.New(s.lastError)
	}
	manifestJob, err := DecodeRuntimeJob(&queuev1.QueueJob{
		Id: manifestJobID, WorkspaceId: request.GetWorkspaceId(), Kind: queue.KindRuntimeConfigUpdate,
		LeaseToken: "lease_oauth_manifest_projection", PayloadJson: manifestJobEnvelope,
	})
	if err != nil {
		s.lastError = fmt.Sprintf("decode ready manifest carrier: %v", err)
		return nil, errors.New(s.lastError)
	}
	var manifestPayload string
	if err := s.client.WithWorkspaceTx(ctx, request.GetWorkspaceId(), "test.oauth_ready_manifest_payload", func(tx *dbconnect.Tx) error {
		var err error
		manifestPayload, _, err = runtimeCommandPayloadForJobTx(ctx, tx, manifestJob)
		return err
	}); err != nil {
		s.lastError = fmt.Sprintf("build ready manifest payload: %v", err)
		return nil, errors.New(s.lastError)
	}
	raw, err := json.Marshal(map[string]any{
		"workspaceId": request.GetWorkspaceId(), "sessionId": request.GetSessionId(),
		"sessionThreadId": request.GetSessionThreadId(), "readyManifestPayloadJson": manifestPayload,
		"runtimeConfigPayloadJson": s.runtimeConfigPayload, "toolName": "github_search",
	})
	if err != nil {
		s.lastError = fmt.Sprintf("encode ready provider input: %v", err)
		return nil, errors.New(s.lastError)
	}
	if err := os.WriteFile(s.inputPath, raw, 0o600); err != nil {
		s.lastError = fmt.Sprintf("write ready provider input: %v", err)
		return nil, errors.New(s.lastError)
	}
	command := exec.CommandContext(ctx, "bun", "packages/runtime-pod/test/fixtures/mcp-ready-provider-composition.ts", s.inputPath) //nolint:gosec // Fixed repository fixture and test-owned input.
	command.Dir = "../agent-runtime"
	output, err := command.CombinedOutput()
	if err != nil {
		s.lastError = fmt.Sprintf("run ready MCP provider composition: %v: %s", err, output)
		return nil, errors.New(s.lastError)
	}
	if err := json.Unmarshal(output, &s.result); err != nil {
		s.lastError = fmt.Sprintf("decode ready MCP provider composition: %v", err)
		return nil, errors.New(s.lastError)
	}
	return &agentruntimev1.RuntimeInputCommandResponse{
		Status:    agentruntimev1.RuntimeCommandStatus_RUNTIME_COMMAND_STATUS_ACCEPTED,
		SessionId: request.GetSessionId(), RuntimeInputId: request.GetRuntimeInputId(),
		BindingId: request.GetBindingId(), BindingGeneration: request.GetBindingGeneration(),
	}, nil
}

type oauthInitialManifestConnector struct {
	address  string
	command  *exec.Cmd
	scanner  *bufio.Scanner
	stdin    io.WriteCloser
	stderr   bytes.Buffer
	finished bool
}

type oauthInitialManifestStats struct {
	TokenEndpointAttempts int  `json:"tokenEndpointAttempts"`
	ToolsListCalls        int  `json:"toolsListCalls"`
	UsedRotatedToken      bool `json:"usedRotatedToken"`
	DurableRotation       bool `json:"durableRotation"`
	LogsRedacted          bool `json:"logsRedacted"`
	RefreshOutcomes       []struct {
		Outcome         string `json:"outcome"`
		DurableWrite    string `json:"durableWrite"`
		HTTPStatusClass string `json:"httpStatusClass"`
	} `json:"refreshOutcomes"`
}

func startOAuthInitialManifestConnector(t *testing.T, admin *sql.DB, sessionID string) *oauthInitialManifestConnector {
	t.Helper()
	var schema string
	if err := admin.QueryRowContext(context.Background(), `SELECT current_schema()`).Scan(&schema); err != nil {
		t.Fatalf("read isolated schema: %v", err)
	}
	input, err := json.Marshal(map[string]string{"schema": schema, "workspaceId": "default", "sessionId": sessionID})
	if err != nil {
		t.Fatalf("encode OAuth Connector fixture input: %v", err)
	}
	inputPath := t.TempDir() + "/oauth-connector.json"
	if err := os.WriteFile(inputPath, input, 0o600); err != nil {
		t.Fatalf("write OAuth Connector fixture input: %v", err)
	}
	fixture := &oauthInitialManifestConnector{}
	fixture.command = exec.Command("bun", "packages/mcp-connector/test/fixtures/oauth-initial-manifest-server.ts", inputPath) //nolint:gosec // Fixed repository fixture and test-owned input.
	fixture.command.Dir = "../gateway"
	stdout, err := fixture.command.StdoutPipe()
	if err != nil {
		t.Fatalf("open OAuth Connector stdout: %v", err)
	}
	fixture.stdin, err = fixture.command.StdinPipe()
	if err != nil {
		t.Fatalf("open OAuth Connector stdin: %v", err)
	}
	fixture.command.Stderr = &fixture.stderr
	fixture.scanner = bufio.NewScanner(stdout)
	if err := fixture.command.Start(); err != nil {
		t.Fatalf("start OAuth Connector fixture: %v", err)
	}
	t.Cleanup(func() {
		if fixture.finished || fixture.command.Process == nil {
			return
		}
		_ = fixture.command.Process.Kill()
		_ = fixture.command.Wait()
	})
	if !fixture.scanner.Scan() {
		t.Fatalf("OAuth Connector fixture did not start: %v: %s", fixture.scanner.Err(), fixture.stderr.String())
	}
	var started struct {
		Address string `json:"address"`
	}
	if err := json.Unmarshal(fixture.scanner.Bytes(), &started); err != nil || started.Address == "" {
		t.Fatalf("decode OAuth Connector address: %v: %q", err, fixture.scanner.Text())
	}
	fixture.address = started.Address
	return fixture
}

func (f *oauthInitialManifestConnector) finish(t *testing.T) oauthInitialManifestStats {
	t.Helper()
	if _, err := f.stdin.Write([]byte("finish\n")); err != nil {
		t.Fatalf("finish OAuth Connector fixture: %v", err)
	}
	if !f.scanner.Scan() {
		t.Fatalf("OAuth Connector fixture omitted stats: %v: %s", f.scanner.Err(), f.stderr.String())
	}
	var stats oauthInitialManifestStats
	if err := json.Unmarshal(f.scanner.Bytes(), &stats); err != nil {
		t.Fatalf("decode OAuth Connector stats: %v: %q", err, f.scanner.Text())
	}
	if err := f.command.Wait(); err != nil {
		t.Fatalf("OAuth Connector fixture exit: %v: %s", err, f.stderr.String())
	}
	f.finished = true
	return stats
}

func newUntypedFailureMCPManifestLister(t *testing.T, code codes.Code) MCPManifestLister {
	return newExactFailureMCPManifestLister(t, code, nil)
}

func newExactFailureMCPManifestLister(t *testing.T, code codes.Code, values []string) MCPManifestLister {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen for untyped MCP failure: %v", err)
	}
	server := grpc.NewServer()
	providergatewayv1.RegisterMcpConnectorServiceServer(server, failingMCPManifestTransportServer{code: code, values: values})
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(func() {
		server.Stop()
		_ = listener.Close()
	})
	return NewGatewayMCPManifestLister(listener.Addr().String(), &countingRuntimeCommandTokenSource{})
}

type initialManifestFailureConnector struct {
	address  string
	command  *exec.Cmd
	scanner  *bufio.Scanner
	stdin    io.WriteCloser
	stderr   bytes.Buffer
	finished bool
}

type initialManifestFailureStats struct {
	ListCalls    int  `json:"listCalls"`
	LogsRedacted bool `json:"logsRedacted"`
}

func startInitialManifestFailureConnector(t *testing.T) *initialManifestFailureConnector {
	t.Helper()
	fixture := &initialManifestFailureConnector{}
	fixture.command = exec.Command("bun", "packages/mcp-connector/test/fixtures/initial-manifest-failure-server.ts") //nolint:gosec // Fixed repository fixture.
	fixture.command.Dir = "../gateway"
	stdout, err := fixture.command.StdoutPipe()
	if err != nil {
		t.Fatalf("open failure Connector stdout: %v", err)
	}
	fixture.stdin, err = fixture.command.StdinPipe()
	if err != nil {
		t.Fatalf("open failure Connector stdin: %v", err)
	}
	fixture.command.Stderr = &fixture.stderr
	fixture.scanner = bufio.NewScanner(stdout)
	if err := fixture.command.Start(); err != nil {
		t.Fatalf("start failure Connector fixture: %v", err)
	}
	t.Cleanup(func() {
		if fixture.finished || fixture.command.Process == nil {
			return
		}
		_ = fixture.command.Process.Kill()
		_ = fixture.command.Wait()
	})
	if !fixture.scanner.Scan() {
		t.Fatalf("failure Connector fixture did not start: %v: %s", fixture.scanner.Err(), fixture.stderr.String())
	}
	var started struct {
		Address string `json:"address"`
	}
	if err := json.Unmarshal(fixture.scanner.Bytes(), &started); err != nil || started.Address == "" {
		t.Fatalf("decode failure Connector address: %v: %q", err, fixture.scanner.Text())
	}
	fixture.address = started.Address
	return fixture
}

func (f *initialManifestFailureConnector) finish(t *testing.T) initialManifestFailureStats {
	t.Helper()
	if _, err := f.stdin.Write([]byte("finish\n")); err != nil {
		t.Fatalf("finish failure Connector fixture: %v", err)
	}
	if !f.scanner.Scan() {
		t.Fatalf("failure Connector fixture omitted stats: %v: %s", f.scanner.Err(), f.stderr.String())
	}
	var stats initialManifestFailureStats
	if err := json.Unmarshal(f.scanner.Bytes(), &stats); err != nil {
		t.Fatalf("decode failure Connector stats: %v: %q", err, f.scanner.Text())
	}
	if err := f.command.Wait(); err != nil {
		t.Fatalf("failure Connector fixture exit: %v: %s", err, f.stderr.String())
	}
	f.finished = true
	return stats
}

func TestInitialMCPManifestListUsesFreshRPCOnlyDeadline(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const sessionID = "sesn_mcp_list_deadline"
	seedBridgeAPISession(t, admin, "default", sessionID, "thr_mcp_list_deadline")
	if _, err := admin.ExecContext(context.Background(), `UPDATE sessions SET installed_tools_json = '{"tools":[{"type":"tetral_agent_toolset","family":"claude"},{"type":"mcp_toolset","mcp_server_name":"github"}]}' WHERE workspace_id = 'default' AND id = $1`, sessionID); err != nil {
		t.Fatalf("seed deadline MCP toolsets: %v", err)
	}
	lister := &expiringMCPManifestLister{}
	store := NewPostgreSQLRuntimeDeliveryStore(dbconnect.NewClientForTesting(runtime), 9090)
	store.MCPManifestLister = lister
	err := store.captureInitialMCPManifestsWithListTimeout(
		context.Background(),
		RuntimeJob{WorkspaceID: "default", SessionID: sessionID},
		[]MCPManifestToolsetConfig{
			{MCPServerName: "github", BuiltinFamily: "claude"},
			{MCPServerName: "github", BuiltinFamily: "claude"},
		},
		time.Date(2026, 1, 1, 0, 0, 30, 0, time.UTC),
		10*time.Millisecond,
	)
	if err != nil {
		t.Fatalf("captureInitialMCPManifestsWithListTimeout: %v", err)
	}
	if len(lister.contexts) != 2 || lister.contexts[0] == lister.contexts[1] {
		t.Fatalf("MCP list contexts = %#v; want one fresh context per toolset", lister.contexts)
	}
	for index, listCtx := range lister.contexts {
		if listCtx.Err() != context.DeadlineExceeded {
			t.Fatalf("MCP list context %d error = %v; want deadline exceeded before persistence", index, listCtx.Err())
		}
	}
	var readyRows int
	if err := admin.QueryRowContext(context.Background(),
		`SELECT count(*) FROM session_mcp_manifests WHERE workspace_id = 'default' AND session_id = $1 AND readiness = 'ready'`,
		sessionID,
	).Scan(&readyRows); err != nil {
		t.Fatalf("count persisted MCP manifests: %v", err)
	}
	if readyRows != 1 {
		t.Fatalf("persisted ready MCP manifests = %d; want 1 duplicate-safe row after both list contexts expired", readyRows)
	}
}

func TestPostgreSQLRuntimeDeliveryStoreBuildsTaskNotificationFromBackgroundTask(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	seedBridgeAPISession(t, admin, "default", "sesn_bridge_task_delivery", "thr_bridge_task_delivery")
	seedBridgeAPIRuntimeBinding(t, admin, "default", "sesn_bridge_task_delivery", "bind_bridge_task_delivery", 1, "pod_uid_task_delivery")
	seedBridgeAPINotifiableBackgroundTask(t, admin, "default", "sesn_bridge_task_delivery", "thr_bridge_task_delivery", "bind_bridge_task_delivery", "task_bridge_delivery", "sevt_tool_delivery")
	storedResult := fmt.Sprintf(
		`{"status":"completed","stdout":{"text":%q,"truncated":false,"total_bytes":51200,"total_lines":5000},"stderr":{"text":%q,"truncated":false,"total_bytes":51200,"total_lines":6000},"provider_command_id":"must_not_escape"}`,
		strings.Repeat("out-", 12800),
		strings.Repeat("err-", 12800),
	)
	if _, err := admin.ExecContext(context.Background(), `UPDATE session_background_tasks
		SET status='completed', terminal_result_json=$1, terminal_result_digest='digest', terminal_at='2026-01-01T00:01:00Z'
		WHERE workspace_id='default' AND session_id='sesn_bridge_task_delivery' AND task_id='task_bridge_delivery'`, storedResult); err != nil {
		t.Fatalf("seed terminal background task: %v", err)
	}
	if _, err := admin.ExecContext(context.Background(), `INSERT INTO session_runtime_inbox (
		workspace_id, session_id, session_thread_id, runtime_input_id, input_kind, event_ids_json,
		status, created_at, updated_at
	) VALUES ('default','sesn_bridge_task_delivery','thr_bridge_task_delivery','task_notification:task_bridge_delivery',
		'task_notification','[]','queued','2026-01-01T00:01:00Z','2026-01-01T00:01:00Z')`); err != nil {
		t.Fatalf("seed queued task notification: %v", err)
	}
	store := NewPostgreSQLRuntimeDeliveryStore(dbconnect.NewClientForTesting(runtime), 9090)
	store.Clock = func() time.Time { return time.Date(2026, 1, 1, 0, 2, 0, 0, time.UTC) }
	resolver := &recordingRuntimeTargetResolver{binding: runtimeBindingForDelivery{
		BindingID:         "bind_bridge_task_delivery",
		BindingGeneration: 1,
		Namespace:         "runtime-ns",
		PodName:           "runtime-pod",
		PodUID:            "pod_uid_task_delivery",
		PodIP:             "10.0.0.1",
	}}
	store.TargetResolver = resolver

	job := RuntimeJob{
		JobID:           "qjob_bridge_task_delivery",
		LeaseToken:      "lease_bridge_task_delivery",
		Kind:            queue.KindRuntimeInput,
		WorkspaceID:     "default",
		SessionID:       "sesn_bridge_task_delivery",
		SessionThreadID: "thr_bridge_task_delivery",
		RuntimeInputID:  "task_notification:task_bridge_delivery",
		EventIDs:        []string{},
		InputKind:       "task_notification",
		CommandKind:     agentruntimev1.RuntimeCommandKind_RUNTIME_COMMAND_KIND_TASK_NOTIFICATION,
		PayloadJSON:     `{"workspace_id":"default","session_id":"sesn_bridge_task_delivery","session_thread_id":"thr_bridge_task_delivery","runtime_input_id":"task_notification:task_bridge_delivery","event_ids":[],"input_kind":"task_notification"}`,
	}
	plan, err := store.PrepareRuntimeCommand(context.Background(), job)
	if err != nil {
		t.Fatalf("PrepareRuntimeCommand task notification: %v", err)
	}
	if plan.TaskNotification == nil || plan.TaskNotification.TaskID != "task_bridge_delivery" {
		t.Fatalf("prepared task notification plan = %#v", plan)
	}
	if len(resolver.jobs) != 1 || resolver.jobs[0].RuntimeInputID != job.RuntimeInputID || resolver.jobs[0].SessionThreadID != "thr_bridge_task_delivery" {
		t.Fatalf("target resolver jobs = %+v; want task notification delivery resolved through runtime target", resolver.jobs)
	}
	if strings.Contains(plan.Request.GetPayloadJson(), "provider_command_id") {
		t.Fatalf("runtime task notification leaked provider metadata: %s", plan.Request.GetPayloadJson())
	}
	var payload struct {
		TaskID               string `json:"task_id"`
		SourceToolUseEventID string `json:"source_tool_use_event_id"`
		Status               string `json:"status"`
		Stdout               struct {
			Truncated     bool  `json:"truncated"`
			OriginalBytes int64 `json:"original_bytes"`
			OriginalLines int64 `json:"original_lines"`
		} `json:"stdout"`
		Stderr struct {
			Truncated     bool  `json:"truncated"`
			OriginalBytes int64 `json:"original_bytes"`
			OriginalLines int64 `json:"original_lines"`
		} `json:"stderr"`
	}
	if err := json.Unmarshal([]byte(plan.Request.GetPayloadJson()), &payload); err != nil {
		t.Fatalf("decode runtime task notification payload: %v", err)
	}
	if payload.TaskID != "task_bridge_delivery" || payload.SourceToolUseEventID != "sevt_tool_delivery" || payload.Status != "completed" {
		t.Fatalf("runtime task notification payload = %#v", payload)
	}
	if len([]byte(plan.Request.GetPayloadJson())) > runtimeTaskNotificationPayloadMaxBytes {
		t.Fatalf("runtime task notification payload bytes = %d; want <= %d", len([]byte(plan.Request.GetPayloadJson())), runtimeTaskNotificationPayloadMaxBytes)
	}
	if !payload.Stdout.Truncated || !payload.Stderr.Truncated ||
		payload.Stdout.OriginalBytes != 51200 || payload.Stdout.OriginalLines != 5000 ||
		payload.Stderr.OriginalBytes != 51200 || payload.Stderr.OriginalLines != 6000 {
		t.Fatalf("runtime task notification stream bounds = stdout:%+v stderr:%+v", payload.Stdout, payload.Stderr)
	}
	assertNoTaskOutputPaths(t, plan.Request.GetPayloadJson())
	apiStore := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	apiStore.Clock = store.Clock
	committed, err := apiStore.CommitTaskNotificationResult(context.Background(), bridgeTaskNotificationRequestForTest(
		t,
		runtimeScopeFromCommandRequest(plan.Request),
		job.RuntimeInputID,
		plan.TaskNotification.TaskID,
		plan.TaskNotification.ResultJSON,
	))
	if err != nil {
		t.Fatalf("CommitTaskNotificationResult delivery: %v", err)
	}
	if committed.GetAck().GetStatus() != bridgev1.BridgeWriteStatus_BRIDGE_WRITE_STATUS_COMMITTED {
		t.Fatalf("CommitTaskNotificationResult ack = %s; want committed", committed.GetAck().GetStatus())
	}
	var taskStatus string
	var inboxStatus string
	if err := admin.QueryRowContext(context.Background(),
		`SELECT status FROM session_background_tasks WHERE workspace_id = 'default' AND session_id = 'sesn_bridge_task_delivery' AND task_id = 'task_bridge_delivery'`).Scan(&taskStatus); err != nil {
		t.Fatalf("read task delivery status: %v", err)
	}
	if err := admin.QueryRowContext(context.Background(),
		`SELECT status FROM session_runtime_inbox WHERE workspace_id = 'default' AND runtime_input_id = 'task_notification:task_bridge_delivery'`).Scan(&inboxStatus); err != nil {
		t.Fatalf("read task delivery inbox status: %v", err)
	}
	if taskStatus != "completed" || inboxStatus != "committed" {
		t.Fatalf("task delivery status=%q inbox=%q; want completed/committed", taskStatus, inboxStatus)
	}
}

func TestRuntimeConfigDeliveryRebuildsTheColdBootstrapAgentSettings(t *testing.T) {
	for _, test := range []struct {
		name          string
		sessionID     string
		agentConfig   string
		installedJSON string
		wantSystem    string
	}{
		{
			name:          "configured MCP policy",
			sessionID:     "sesn_runtime_config_policy",
			agentConfig:   `{"name":"agent","model":"anthropic/claude-opus-4-8","system":"Operate as the session specialist.","tools":[{"type":"mcp_toolset","mcp_server_name":"github","default_config":{"enabled":false,"permission_policy":{"type":"always_ask"}},"configs":[{"name":"github_search","enabled":true,"permission_policy":{"type":"always_allow"}}]}],"mcp_servers":[{"type":"url","name":"github","url":"https://api.githubcopilot.com/mcp/"}],"skills":[],"metadata":{}}`,
			installedJSON: `{"tools":[{"type":"tetral_agent_toolset","family":"claude"},{"type":"mcp_toolset","mcp_server_name":"github","default_config":{"enabled":false,"permission_policy":{"type":"always_ask"}},"configs":[{"name":"github_search","enabled":true,"permission_policy":{"type":"always_allow"}}]}],"mcp_servers":[{"type":"url","name":"github","url":"https://api.githubcopilot.com/mcp/"}]}`,
			wantSystem:    "Operate as the session specialist.",
		},
		{
			name:          "empty MCP policy",
			sessionID:     "sesn_runtime_config_policy_empty",
			agentConfig:   `{"name":"agent","model":"anthropic/claude-opus-4-8","system":null,"tools":[],"mcp_servers":[],"skills":[],"metadata":{}}`,
			installedJSON: `{"tools":[{"type":"tetral_agent_toolset","family":"claude"}],"mcp_servers":[]}`,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
			seedBridgeAPISession(t, admin, "default", test.sessionID, "thr_"+test.sessionID)
			seedBridgeAPIAgentConfig(t, admin, "default", test.sessionID, test.agentConfig)
			seedBridgeAPIWritableMemoryStore(t, admin, "default", test.sessionID, "memstore_runtime_config")
			if _, err := admin.ExecContext(context.Background(),
				`UPDATE session_memory_store_resources
				    SET name = 'Project notes', instructions = 'Preserve this guidance.'
				  WHERE workspace_id = 'default' AND session_id = $1`, test.sessionID); err != nil {
				t.Fatalf("seed runtime memory guidance: %v", err)
			}
			if _, err := admin.ExecContext(context.Background(),
				`UPDATE sessions SET approval_mode = 'approve_for_me', config_generation = 7, installed_tools_json = $1
				  WHERE workspace_id = 'default' AND id = $2`, test.installedJSON, test.sessionID); err != nil {
				t.Fatalf("seed runtime config: %v", err)
			}

			var payloadJSON string
			var runtimeInputID string
			client := dbconnect.NewClientForTesting(runtime)
			if err := client.WithWorkspaceTx(context.Background(), "default", "test.rebuild_runtime_config", func(tx *dbconnect.Tx) error {
				var err error
				payloadJSON, runtimeInputID, err = runtimeCommandPayloadForJobTx(context.Background(), tx, RuntimeJob{
					Kind: queue.KindRuntimeConfigUpdate, WorkspaceID: "default", SessionID: test.sessionID, ConfigGeneration: "1",
				})
				return err
			}); err != nil {
				t.Fatalf("rebuild runtime config: %v", err)
			}
			if runtimeInputID != runtimeConfigUpdateInputID(test.sessionID, "7") {
				t.Fatalf("rebuilt runtime input id = %q; want current generation 7", runtimeInputID)
			}
			var payload struct {
				ToolPolicy   any                        `json:"tool_policy"`
				System       *string                    `json:"system"`
				MemoryStores []bridgeRuntimeMemoryStore `json:"memory_stores"`
			}
			if err := json.Unmarshal([]byte(payloadJSON), &payload); err != nil {
				t.Fatalf("decode rebuilt runtime config: %v", err)
			}
			var payloadFields map[string]json.RawMessage
			if err := json.Unmarshal([]byte(payloadJSON), &payloadFields); err != nil {
				t.Fatalf("decode rebuilt runtime config fields: %v", err)
			}
			if _, ok := payloadFields["system"]; !ok {
				t.Fatalf("rebuilt runtime config omits nullable system: %s", payloadJSON)
			}
			settings, err := bridgeRuntimeSessionAgentSettings("approve_for_me", test.agentConfig, test.installedJSON, payload.MemoryStores)
			if err != nil {
				t.Fatalf("resolve cold-bootstrap agent settings: %v", err)
			}
			wantJSON, err := json.Marshal(settings.ToolPolicy)
			if err != nil {
				t.Fatalf("marshal cold-bootstrap policy: %v", err)
			}
			var want any
			if err := json.Unmarshal(wantJSON, &want); err != nil {
				t.Fatalf("decode cold-bootstrap policy: %v", err)
			}
			if !reflect.DeepEqual(payload.ToolPolicy, want) {
				t.Fatalf("rebuilt policy = %#v; cold-bootstrap policy = %#v", payload.ToolPolicy, want)
			}
			if !reflect.DeepEqual(payload.MemoryStores, settings.MemoryStores) || len(payload.MemoryStores) != 1 ||
				payload.MemoryStores[0].MemoryStoreID != "memstore_runtime_config" ||
				payload.MemoryStores[0].Name != "Project notes" ||
				payload.MemoryStores[0].Access != "read_write" ||
				payload.MemoryStores[0].Instructions == nil || *payload.MemoryStores[0].Instructions != "Preserve this guidance." {
				t.Fatalf("rebuilt memory stores = %#v; cold-bootstrap memory stores = %#v", payload.MemoryStores, settings.MemoryStores)
			}
			if test.wantSystem == "" {
				if payload.System != nil || settings.System != nil {
					t.Fatalf("rebuilt system = %#v; cold-bootstrap system = %#v; want nil", payload.System, settings.System)
				}
			} else if payload.System == nil || settings.System == nil || *payload.System != test.wantSystem || *settings.System != test.wantSystem {
				t.Fatalf("rebuilt system = %#v; cold-bootstrap system = %#v; want %q", payload.System, settings.System, test.wantSystem)
			}
		})
	}
}

func TestManifestServerSetIsSubsetOfColdToolPolicyMCPToolsets(t *testing.T) {
	const sessionID = "sesn_manifest_policy_subset"
	const agentConfig = `{"name":"agent","model":"anthropic/claude-opus-4-8","tools":[],"mcp_servers":[],"skills":[],"metadata":{}}`
	const installedConfig = `{"tools":[{"type":"tetral_agent_toolset","family":"claude"},{"type":"mcp_toolset","mcp_server_name":"github"}],"mcp_servers":[{"type":"url","name":"github","url":"https://api.githubcopilot.com/mcp/"},{"type":"url","name":"unused","url":"https://unused.example/mcp/"}]}`
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	seedBridgeAPISession(t, admin, "default", sessionID, "thr_manifest_policy_subset")
	seedBridgeAPIAgentConfig(t, admin, "default", sessionID, agentConfig)
	if _, err := admin.ExecContext(context.Background(),
		`UPDATE sessions SET installed_tools_json = $1 WHERE workspace_id = 'default' AND id = $2`, installedConfig, sessionID); err != nil {
		t.Fatalf("seed installed tool policy: %v", err)
	}

	var manifestToolsets []MCPManifestToolsetConfig
	client := dbconnect.NewClientForTesting(runtime)
	if err := client.WithWorkspaceTx(context.Background(), "default", "test.manifest_policy_subset", func(tx *dbconnect.Tx) error {
		var err error
		manifestToolsets, err = sessionAgentMCPManifestToolsetsTx(context.Background(), tx, "default", sessionID)
		return err
	}); err != nil {
		t.Fatalf("read manifest toolsets: %v", err)
	}
	config, err := effectiveBridgeRuntimeAgentConfig(agentConfig, installedConfig)
	if err != nil {
		t.Fatalf("resolve cold tool policy: %v", err)
	}
	policyJSON, err := json.Marshal(bridgeRuntimeToolPolicy("approve_for_me", config))
	if err != nil {
		t.Fatalf("marshal cold tool policy: %v", err)
	}
	var policy struct {
		MCPToolsets []struct {
			MCPServerName string `json:"mcpServerName"`
		} `json:"mcpToolsets"`
	}
	if err := json.Unmarshal(policyJSON, &policy); err != nil {
		t.Fatalf("decode cold tool policy: %v", err)
	}
	held := make(map[string]bool, len(policy.MCPToolsets))
	for _, toolset := range policy.MCPToolsets {
		held[toolset.MCPServerName] = true
	}
	for _, toolset := range manifestToolsets {
		if !held[toolset.MCPServerName] {
			t.Fatalf("manifest server %q is absent from cold policy MCP toolsets %v", toolset.MCPServerName, held)
		}
	}
	if len(manifestToolsets) != 1 || manifestToolsets[0].MCPServerName != "github" || held["unused"] {
		t.Fatalf("manifest toolsets=%+v held=%v; want only declared github toolset", manifestToolsets, held)
	}
}

func TestRuntimeConfigDeliveryRebuildsManifestWithoutConsultingToolPolicy(t *testing.T) {
	const sessionID = "sesn_manifest_single_row"
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	seedBridgeAPISession(t, admin, "default", sessionID, "thr_manifest_single_row")
	seedBridgeAPIAgentConfig(t, admin, "default", sessionID, `{
		"name":"agent",
		"model":"anthropic/claude-opus-4-8",
		"tools":[],
		"mcp_servers":[],
		"skills":[],
		"metadata":{}
	}`)
	insertAcceptedMCPManifest(t, admin, sessionID, "etag_single_row", 4, "github_single_row")

	var payloadJSON string
	var runtimeInputID string
	client := dbconnect.NewClientForTesting(runtime)
	if err := client.WithWorkspaceTx(context.Background(), "default", "test.manifest_single_row", func(tx *dbconnect.Tx) error {
		var err error
		payloadJSON, runtimeInputID, err = runtimeCommandPayloadForJobTx(context.Background(), tx, RuntimeJob{
			Kind: queue.KindRuntimeConfigUpdate, WorkspaceID: "default", SessionID: sessionID,
			MCPServerName: "github", MCPManifestGeneration: "1",
		})
		return err
	}); err != nil {
		t.Fatalf("rebuild MCP manifest without held policy: %v", err)
	}
	if runtimeInputID != runtimeMCPManifestInputID(sessionID, "github", 4) {
		t.Fatalf("rebuilt runtime input id = %q; want durable generation 4", runtimeInputID)
	}
	var payload struct {
		Manifest struct {
			ServerName string `json:"mcp_server_name"`
			Tools      []struct {
				Name string `json:"name"`
			} `json:"tools"`
		} `json:"mcp_manifest"`
	}
	if err := json.Unmarshal([]byte(payloadJSON), &payload); err != nil {
		t.Fatalf("decode rebuilt MCP manifest: %v", err)
	}
	if payload.Manifest.ServerName != "github" || len(payload.Manifest.Tools) != 1 || payload.Manifest.Tools[0].Name != "github_single_row" {
		t.Fatalf("rebuilt MCP manifest = %+v; want durable row content", payload.Manifest)
	}
}

func TestRuntimeConfigDeliveryRebuildsSupersededManifestAtTheCurrentGeneration(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const sessionID = "sesn_runtime_manifest_superseded"
	seedMCPFamilySession(t, admin, sessionID, "thr_"+sessionID, "claude")
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, "bind_runtime_manifest_superseded", 1, "pod_runtime_manifest_superseded")
	insertAcceptedMCPManifest(t, admin, sessionID, "etag_old", 1, "github_old")
	if _, err := admin.ExecContext(context.Background(),
		`UPDATE session_mcp_manifests
		    SET tools_json = '[{"name":"github_current","description":"current","input_schema":{"type":"object"}}]',
		        manifest_etag = 'etag_current', manifest_generation = 2, updated_at = '2026-01-01T00:00:20Z'
		  WHERE workspace_id = 'default' AND session_id = $1 AND mcp_server_name = 'github'`, sessionID); err != nil {
		t.Fatalf("advance durable manifest: %v", err)
	}

	store := NewPostgreSQLRuntimeDeliveryStore(dbconnect.NewClientForTesting(runtime), 9090)
	store.Clock = func() time.Time { return time.Date(2026, 1, 1, 0, 0, 30, 0, time.UTC) }
	sender := &recordingRuntimeCommandSender{response: &agentruntimev1.RuntimeInputCommandResponse{
		Status: agentruntimev1.RuntimeCommandStatus_RUNTIME_COMMAND_STATUS_ACCEPTED,
	}}
	result, err := (RuntimePodDirectDeliverer{Store: store, Sender: sender}).DeliverRuntimeJob(context.Background(), RuntimeJob{
		Kind: queue.KindRuntimeConfigUpdate, WorkspaceID: "default", SessionID: sessionID,
		RuntimeInputID: runtimeMCPManifestInputID(sessionID, "github", 1), MCPServerName: "github", MCPManifestGeneration: "1",
	})
	if err != nil {
		t.Fatalf("deliver superseded manifest intent: %v", err)
	}
	if result.Status != RuntimeDeliveryAccepted || len(sender.requests) != 1 {
		t.Fatalf("superseded manifest result = %#v requests=%d; want one accepted fresh apply", result, len(sender.requests))
	}
	request := sender.requests[0]
	if request.GetRuntimeInputId() != runtimeMCPManifestInputID(sessionID, "github", 2) ||
		!strings.Contains(request.GetPayloadJson(), `"manifest_generation":2`) ||
		!strings.Contains(request.GetPayloadJson(), `"name":"github_current"`) ||
		strings.Contains(request.GetPayloadJson(), `github_old`) {
		t.Fatalf("rebuilt superseded command id/payload = %q/%s; want current generation and content", request.GetRuntimeInputId(), request.GetPayloadJson())
	}
}

func TestPostgreSQLRuntimeDeliveryStoreTaskNotificationTerminalDuplicateIsStale(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	seedBridgeAPISession(t, admin, "default", "sesn_bridge_task_delivery_terminal_dup", "thr_bridge_task_delivery_terminal_dup")
	seedBridgeAPIRuntimeBinding(t, admin, "default", "sesn_bridge_task_delivery_terminal_dup", "bind_bridge_task_delivery_terminal_dup", 1, "pod_uid_task_delivery_terminal_dup")
	seedBridgeAPINotifiableBackgroundTask(t, admin, "default", "sesn_bridge_task_delivery_terminal_dup", "thr_bridge_task_delivery_terminal_dup", "bind_bridge_task_delivery_terminal_dup", "task_bridge_delivery_terminal_dup", "sevt_tool_delivery_terminal_dup")
	if _, err := admin.ExecContext(context.Background(),
		`UPDATE session_background_tasks
		    SET status = 'completed', terminal_event_id = 'sevt_terminal_dup', updated_at = '2026-01-01T00:20:00Z'
		  WHERE workspace_id = 'default'
		    AND session_id = 'sesn_bridge_task_delivery_terminal_dup'
		    AND task_id = 'task_bridge_delivery_terminal_dup'`); err != nil {
		t.Fatalf("mark terminal task: %v", err)
	}

	store := NewPostgreSQLRuntimeDeliveryStore(dbconnect.NewClientForTesting(runtime), 9090)
	store.Clock = func() time.Time { return time.Date(2026, 1, 1, 0, 40, 1, 0, time.UTC) }
	resolver := &recordingRuntimeTargetResolver{err: errors.New("resolver must not run for terminal duplicate task notification")}
	store.TargetResolver = resolver
	plan, err := store.PrepareRuntimeCommand(context.Background(), RuntimeJob{
		JobID:           "qjob_bridge_task_delivery_terminal_dup",
		LeaseToken:      "lease_bridge_task_delivery_terminal_dup",
		Kind:            queue.KindRuntimeInput,
		WorkspaceID:     "default",
		SessionID:       "sesn_bridge_task_delivery_terminal_dup",
		SessionThreadID: "thr_bridge_task_delivery_terminal_dup",
		RuntimeInputID:  "task_notification:task_bridge_delivery_terminal_dup",
		InputKind:       "task_notification",
		CommandKind:     agentruntimev1.RuntimeCommandKind_RUNTIME_COMMAND_KIND_TASK_NOTIFICATION,
		PayloadJSON:     `{"workspace_id":"default","session_id":"sesn_bridge_task_delivery_terminal_dup","session_thread_id":"thr_bridge_task_delivery_terminal_dup","runtime_input_id":"task_notification:task_bridge_delivery_terminal_dup","event_ids":[],"input_kind":"task_notification"}`,
	})
	if err != nil {
		t.Fatalf("PrepareRuntimeCommand terminal duplicate task notification: %v", err)
	}
	if !plan.StaleAccepted || plan.Request != nil || plan.TaskNotification != nil {
		t.Fatalf("plan = %#v; want terminal duplicate stale accepted without runtime command", plan)
	}
	assertNoRuntimeInboxRow(t, admin, "task_notification:task_bridge_delivery_terminal_dup")
	if len(resolver.jobs) != 0 {
		t.Fatalf("target resolver jobs = %+v; want none for terminal duplicate task notification", resolver.jobs)
	}
}

func TestPostgreSQLRuntimeDeliveryStoreMarksInboxAcceptedAfterRuntimeAccepts(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	seedBridgeAPISession(t, admin, "default", "sesn_bridge_inbox_accept", "thr_bridge_inbox_accept")
	seedBridgeAPIRuntimeBinding(t, admin, "default", "sesn_bridge_inbox_accept", "bind_bridge_inbox_accept", 1, "pod_uid_inbox_accept")
	seedBridgeAPIEvent(t, admin, "default", "sesn_bridge_inbox_accept", "thr_bridge_inbox_accept", "sevt_inbox_accept", 1, "user.message", `{"content":[{"type":"text","text":"hello"}]}`)

	store := NewPostgreSQLRuntimeDeliveryStore(dbconnect.NewClientForTesting(runtime), 9090)
	store.Clock = func() time.Time { return time.Date(2026, 1, 1, 0, 4, 4, 0, time.UTC) }
	sender := &recordingRuntimeCommandSender{response: &agentruntimev1.RuntimeInputCommandResponse{
		Status: agentruntimev1.RuntimeCommandStatus_RUNTIME_COMMAND_STATUS_ACCEPTED,
	}}
	job := RuntimeJob{
		JobID:           "qjob_inbox_accept",
		LeaseToken:      "lease_inbox_accept",
		Kind:            queue.KindRuntimeInput,
		WorkspaceID:     "default",
		SessionID:       "sesn_bridge_inbox_accept",
		SessionThreadID: "thr_bridge_inbox_accept",
		RuntimeInputID:  "rin_inbox_accept",
		EventIDs:        []string{"sevt_inbox_accept"},
		SequenceFrom:    1,
		SequenceTo:      1,
		InputKind:       "messages",
		CommandKind:     agentruntimev1.RuntimeCommandKind_RUNTIME_COMMAND_KIND_MESSAGES,
		PayloadJSON:     `{"workspace_id":"default","session_id":"sesn_bridge_inbox_accept","session_thread_id":"thr_bridge_inbox_accept","runtime_input_id":"rin_inbox_accept","event_ids":["sevt_inbox_accept"],"sequence_from":1,"sequence_to":1,"input_kind":"messages"}`,
	}
	seedRuntimeInboxBirthForJob(t, admin, job)

	result, err := (RuntimePodDirectDeliverer{Store: store, Sender: sender}).DeliverRuntimeJob(context.Background(), job)
	if err != nil {
		t.Fatalf("DeliverRuntimeJob: %v", err)
	}
	if result.Status != RuntimeDeliveryAccepted {
		t.Fatalf("delivery result = %#v; want accepted", result)
	}
	var inboxStatus string
	if err := admin.QueryRowContext(context.Background(),
		`SELECT status
		   FROM session_runtime_inbox
		  WHERE workspace_id = 'default'
		    AND runtime_input_id = 'rin_inbox_accept'`).Scan(&inboxStatus); err != nil {
		t.Fatalf("read accepted inbox status: %v", err)
	}
	if inboxStatus != "accepted" {
		t.Fatalf("inbox status = %q; want accepted after Runtime ACK", inboxStatus)
	}
}

func TestPostgreSQLRuntimeDeliveryStorePersistsDistinctInboxesForChunkedBacklog(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID = "sesn_bridge_chunked_backlog"
		threadID  = "thr_bridge_chunked_backlog"
	)
	seedBridgeAPISession(t, admin, "default", sessionID, threadID)
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, "bind_bridge_chunked_backlog", 1, "pod_uid_chunked_backlog")
	eventIDs := make([]string, queue.MaxRuntimeInputEventRefsPerJob+1)
	for index := range eventIDs {
		eventIDs[index] = fmt.Sprintf("sevt_bridge_chunk_%04d", index+1)
		seedBridgeAPIEvent(t, admin, "default", sessionID, threadID, eventIDs[index], int64(index+1), "user.message", `{"content":[{"type":"text","text":"queued"}]}`)
	}

	store := NewPostgreSQLRuntimeDeliveryStore(dbconnect.NewClientForTesting(runtime), 9090)
	store.Clock = func() time.Time { return time.Date(2026, 1, 1, 0, 4, 4, 0, time.UTC) }
	chunks := [][]string{eventIDs[:queue.MaxRuntimeInputEventRefsPerJob], eventIDs[queue.MaxRuntimeInputEventRefsPerJob:]}
	for index, chunk := range chunks {
		runtimeInputID := fmt.Sprintf("rin_bridge_chunk_%d", index+1)
		payload, err := json.Marshal(map[string]any{
			"workspace_id": "default", "session_id": sessionID, "session_thread_id": threadID,
			"runtime_input_id": runtimeInputID, "event_ids": chunk,
			"sequence_from": index*queue.MaxRuntimeInputEventRefsPerJob + 1,
			"sequence_to":   index*queue.MaxRuntimeInputEventRefsPerJob + len(chunk),
			"input_kind":    "messages",
		})
		if err != nil {
			t.Fatalf("marshal chunk %d: %v", index+1, err)
		}
		job := RuntimeJob{
			JobID: fmt.Sprintf("qjob_bridge_chunk_%d", index+1), LeaseToken: fmt.Sprintf("lease_bridge_chunk_%d", index+1),
			Kind: queue.KindRuntimeInput, WorkspaceID: "default", SessionID: sessionID,
			SessionThreadID: threadID,
			RuntimeInputID:  runtimeInputID, EventIDs: chunk,
			SequenceFrom: int64(index*queue.MaxRuntimeInputEventRefsPerJob + 1),
			SequenceTo:   int64(index*queue.MaxRuntimeInputEventRefsPerJob + len(chunk)),
			InputKind:    "messages", CommandKind: agentruntimev1.RuntimeCommandKind_RUNTIME_COMMAND_KIND_MESSAGES,
			PayloadJSON: string(payload),
		}
		seedRuntimeInboxBirthForJob(t, admin, job)
		plan, err := store.PrepareRuntimeCommand(context.Background(), job)
		if err != nil {
			t.Fatalf("PrepareRuntimeCommand chunk %d: %v", index+1, err)
		}
		if plan.Request == nil || plan.Request.GetRuntimeInputId() != runtimeInputID {
			t.Fatalf("chunk %d plan = %#v; want runtime input %s", index+1, plan, runtimeInputID)
		}
		if _, err := admin.ExecContext(context.Background(),
			`UPDATE session_runtime_inbox SET status = 'committed', updated_at = '2026-01-01T00:04:05Z'
			  WHERE workspace_id = 'default' AND runtime_input_id = $1`, runtimeInputID); err != nil {
			t.Fatalf("commit chunk %d inbox: %v", index+1, err)
		}
		if _, err := admin.ExecContext(context.Background(),
			`UPDATE session_events SET processed_at = '2026-01-01T00:04:05Z'
			  WHERE workspace_id = 'default' AND session_id = $1 AND session_thread_id = $2
			    AND sequence BETWEEN $3 AND $4`, sessionID, threadID,
			index*queue.MaxRuntimeInputEventRefsPerJob+1,
			index*queue.MaxRuntimeInputEventRefsPerJob+len(chunk)); err != nil {
			t.Fatalf("settle chunk %d events: %v", index+1, err)
		}
	}

	var inboxCount int
	var distinctInputs int
	if err := admin.QueryRowContext(context.Background(),
		`SELECT count(*), count(DISTINCT runtime_input_id)
		   FROM session_runtime_inbox
		  WHERE workspace_id = 'default' AND session_id = $1`, sessionID).Scan(&inboxCount, &distinctInputs); err != nil {
		t.Fatalf("count chunked inbox rows: %v", err)
	}
	if inboxCount != 2 || distinctInputs != 2 {
		t.Fatalf("chunked inbox rows/distinct inputs = %d/%d; want 2/2", inboxCount, distinctInputs)
	}
}

func TestAcceptedMessageCommandPayloadAvoidsHTMLExpansionAtTheAdmissionLimit(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID = "sesn_bridge_escape_fuse"
		threadID  = "thr_bridge_escape_fuse"
		eventID   = "sevt_bridge_escape_fuse"
	)
	seedBridgeAPISession(t, admin, "default", sessionID, threadID)

	const bodyCap = 1 << 20
	prefix := `{"content":[{"type":"text","text":"`
	suffix := `"}]}`
	repeated := strings.Repeat(`&<\"`, (bodyCap-len(prefix)-len(suffix))/4)
	payloadJSON := prefix + repeated + suffix
	if len(payloadJSON) > bodyCap {
		t.Fatalf("fixture payload bytes = %d; want at most %d", len(payloadJSON), bodyCap)
	}
	seedBridgeAPIEvent(t, admin, "default", sessionID, threadID, eventID, 1, "user.message", payloadJSON)

	commandPayload := bridgeAcceptedMessageDeliveryPayload(
		t,
		runtime,
		"default",
		sessionID,
		threadID,
		"rin_bridge_escape_fuse",
		[]string{eventID},
		1,
		1,
	)

	if len(commandPayload) > 2*1024*1024 {
		t.Fatalf("command payload bytes = %d; want within 2 MiB payload fuse", len(commandPayload))
	}
	if len(commandPayload) >= 4*1024*1024 {
		t.Fatalf("command payload bytes = %d; want headroom within 4 MiB channel fuse", len(commandPayload))
	}
	for _, escaped := range []string{`\u0026`, `\u003c`} {
		if strings.Contains(commandPayload, escaped) {
			t.Fatalf("command payload contains HTML escape %q", escaped)
		}
	}
}

func TestPostgreSQLRuntimeDeliveryStoreMarkAcceptedFencesRuntimeInboxBinding(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	seedBridgeAPISession(t, admin, "default", "sesn_bridge_inbox_accept_fence", "thr_bridge_inbox_accept_fence")
	seedBridgeAPIRuntimeBinding(t, admin, "default", "sesn_bridge_inbox_accept_fence", "bind_bridge_inbox_accept_fence", 1, "pod_uid_inbox_accept_fence")
	seedBridgeAPIEvent(t, admin, "default", "sesn_bridge_inbox_accept_fence", "thr_bridge_inbox_accept_fence", "sevt_inbox_accept_fence", 1, "user.message", `{"content":[{"type":"text","text":"hello"}]}`)

	store := NewPostgreSQLRuntimeDeliveryStore(dbconnect.NewClientForTesting(runtime), 9090)
	store.Clock = func() time.Time { return time.Date(2026, 1, 1, 0, 4, 4, 0, time.UTC) }
	job := RuntimeJob{
		JobID:           "qjob_inbox_accept_fence",
		LeaseToken:      "lease_inbox_accept_fence",
		Kind:            queue.KindRuntimeInput,
		WorkspaceID:     "default",
		SessionID:       "sesn_bridge_inbox_accept_fence",
		SessionThreadID: "thr_bridge_inbox_accept_fence",
		RuntimeInputID:  "rin_inbox_accept_fence",
		EventIDs:        []string{"sevt_inbox_accept_fence"},
		SequenceFrom:    1,
		SequenceTo:      1,
		InputKind:       "messages",
		CommandKind:     agentruntimev1.RuntimeCommandKind_RUNTIME_COMMAND_KIND_MESSAGES,
		PayloadJSON:     `{"workspace_id":"default","session_id":"sesn_bridge_inbox_accept_fence","session_thread_id":"thr_bridge_inbox_accept_fence","runtime_input_id":"rin_inbox_accept_fence","event_ids":["sevt_inbox_accept_fence"],"sequence_from":1,"sequence_to":1,"input_kind":"messages"}`,
	}
	seedRuntimeInboxBirthForJob(t, admin, job)
	plan, err := store.PrepareRuntimeCommand(context.Background(), job)
	if err != nil {
		t.Fatalf("PrepareRuntimeCommand: %v", err)
	}
	replayedPlan, err := store.PrepareRuntimeCommand(context.Background(), job)
	if err != nil {
		t.Fatalf("PrepareRuntimeCommand replay: %v", err)
	}
	if replayedPlan.Request.GetPayloadJson() != plan.Request.GetPayloadJson() {
		t.Fatalf("replayed message payload = %q; want byte-identical %q", replayedPlan.Request.GetPayloadJson(), plan.Request.GetPayloadJson())
	}
	var payload struct {
		Messages []struct {
			Origin string `json:"origin"`
			Role   string `json:"role"`
			Parts  []struct {
				Type string `json:"type"`
				Text string `json:"text"`
			} `json:"parts"`
		} `json:"messages"`
	}
	if err := json.Unmarshal([]byte(plan.Request.GetPayloadJson()), &payload); err != nil {
		t.Fatalf("decode accepted message payload: %v", err)
	}
	if len(payload.Messages) != 1 || payload.Messages[0].Origin != "user" || payload.Messages[0].Role != "user" ||
		len(payload.Messages[0].Parts) != 1 || payload.Messages[0].Parts[0].Type != "text" || payload.Messages[0].Parts[0].Text != "hello" {
		t.Fatalf("accepted message payload = %#v; want canonical SDK user message", payload.Messages)
	}
	if _, err := admin.ExecContext(context.Background(),
		`UPDATE session_runtime_inbox
		    SET target_pod_uid = 'pod_uid_inbox_accept_fence_other'
		  WHERE workspace_id = 'default'
		    AND runtime_input_id = 'rin_inbox_accept_fence'`); err != nil {
		t.Fatalf("mutate inbox fence: %v", err)
	}
	_, err = store.MarkRuntimeInputAccepted(context.Background(), job, plan.Request)
	var prepareErr runtimeDeliveryPrepareError
	if !errors.As(err, &prepareErr) || prepareErr.kind != "runtime_inbox_accept_missing" || !prepareErr.retryable {
		t.Fatalf("MarkRuntimeInputAccepted fenced err = %v; want retryable runtime_inbox_accept_missing", err)
	}
	var inboxStatus string
	if err := admin.QueryRowContext(context.Background(),
		`SELECT status
		   FROM session_runtime_inbox
		  WHERE workspace_id = 'default'
		    AND runtime_input_id = 'rin_inbox_accept_fence'`).Scan(&inboxStatus); err != nil {
		t.Fatalf("read fenced accepted inbox status: %v", err)
	}
	if inboxStatus != "delivering" {
		t.Fatalf("fenced accepted inbox status = %q; want delivering", inboxStatus)
	}
}

func TestPostgreSQLRuntimeDeliveryStoreRejectsRuntimeInboxPayloadConflict(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	seedBridgeAPISession(t, admin, "default", "sesn_bridge_inbox_conflict", "thr_bridge_inbox_conflict")
	seedBridgeAPIRuntimeBinding(t, admin, "default", "sesn_bridge_inbox_conflict", "bind_bridge_inbox_conflict", 1, "pod_uid_inbox_conflict")
	seedBridgeAPIEvent(t, admin, "default", "sesn_bridge_inbox_conflict", "thr_bridge_inbox_conflict", "sevt_inbox_conflict_one", 1, "user.message", `{"content":[{"type":"text","text":"one"}]}`)
	seedBridgeAPIEvent(t, admin, "default", "sesn_bridge_inbox_conflict", "thr_bridge_inbox_conflict", "sevt_inbox_conflict_two", 2, "user.message", `{"content":[{"type":"text","text":"two"}]}`)

	store := NewPostgreSQLRuntimeDeliveryStore(dbconnect.NewClientForTesting(runtime), 9090)
	store.Clock = func() time.Time { return time.Date(2026, 1, 1, 0, 4, 5, 0, time.UTC) }
	first := RuntimeJob{
		JobID:           "qjob_inbox_conflict_one",
		LeaseToken:      "lease_inbox_conflict_one",
		Kind:            queue.KindRuntimeInput,
		WorkspaceID:     "default",
		SessionID:       "sesn_bridge_inbox_conflict",
		SessionThreadID: "thr_bridge_inbox_conflict",
		RuntimeInputID:  "rin_inbox_conflict",
		EventIDs:        []string{"sevt_inbox_conflict_one"},
		SequenceFrom:    1,
		SequenceTo:      1,
		InputKind:       "messages",
		CommandKind:     agentruntimev1.RuntimeCommandKind_RUNTIME_COMMAND_KIND_MESSAGES,
		PayloadJSON:     `{"workspace_id":"default","session_id":"sesn_bridge_inbox_conflict","session_thread_id":"thr_bridge_inbox_conflict","runtime_input_id":"rin_inbox_conflict","event_ids":["sevt_inbox_conflict_one"],"sequence_from":1,"sequence_to":1,"input_kind":"messages"}`,
	}
	seedRuntimeInboxBirthForJob(t, admin, first)
	if _, err := store.PrepareRuntimeCommand(context.Background(), first); err != nil {
		t.Fatalf("PrepareRuntimeCommand first: %v", err)
	}
	second := first
	second.JobID = "qjob_inbox_conflict_two"
	second.LeaseToken = "lease_inbox_conflict_two"
	second.EventIDs = []string{"sevt_inbox_conflict_two"}
	second.SequenceFrom = 2
	second.SequenceTo = 2
	second.PayloadJSON = `{"workspace_id":"default","session_id":"sesn_bridge_inbox_conflict","session_thread_id":"thr_bridge_inbox_conflict","runtime_input_id":"rin_inbox_conflict","event_ids":["sevt_inbox_conflict_two"],"sequence_from":2,"sequence_to":2,"input_kind":"messages"}`

	_, err := store.PrepareRuntimeCommand(context.Background(), second)
	var prepareErr runtimeDeliveryPrepareError
	if !errors.As(err, &prepareErr) || prepareErr.kind != "runtime_inbox_payload_conflict" || prepareErr.retryable {
		t.Fatalf("PrepareRuntimeCommand conflicting replay err = %v; want terminal runtime_inbox_payload_conflict", err)
	}
	var eventIDsJSON string
	if err := admin.QueryRowContext(context.Background(),
		`SELECT event_ids_json
		   FROM session_runtime_inbox
		  WHERE workspace_id = 'default'
		    AND runtime_input_id = 'rin_inbox_conflict'`).Scan(&eventIDsJSON); err != nil {
		t.Fatalf("read conflict inbox event ids: %v", err)
	}
	if eventIDsJSON != `["sevt_inbox_conflict_one"]` {
		t.Fatalf("event_ids_json = %s; want original payload preserved", eventIDsJSON)
	}
}

func TestPostgreSQLRuntimeDeliveryStoreBuildsControlPayloadsFromSourceEvents(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	seedBridgeAPISession(t, admin, "default", "sesn_bridge_control_delivery", "thr_bridge_control_delivery")
	seedBridgeAPIRuntimeBinding(t, admin, "default", "sesn_bridge_control_delivery", "bind_bridge_control_delivery", 1, "pod_uid_control_delivery")
	seedBridgeAPIEvent(t, admin, "default", "sesn_bridge_control_delivery", "thr_bridge_control_delivery", "sevt_interrupt_control", 1, "user.interrupt", `{}`)
	seedBridgeAPIEvent(t, admin, "default", "sesn_bridge_control_delivery", "thr_bridge_control_delivery", "sevt_confirmation_control", 2, "user.tool_confirmation", `{"tool_use_id":"sevt_tool_control","result":"deny","deny_message":"not now"}`)

	store := NewPostgreSQLRuntimeDeliveryStore(dbconnect.NewClientForTesting(runtime), 9090)
	store.Clock = func() time.Time { return time.Date(2026, 1, 1, 0, 3, 0, 0, time.UTC) }

	interruptJob := RuntimeJob{
		JobID:           "qjob_bridge_interrupt",
		LeaseToken:      "lease_bridge_interrupt",
		Kind:            queue.KindRuntimeInput,
		WorkspaceID:     "default",
		SessionID:       "sesn_bridge_control_delivery",
		SessionThreadID: "thr_bridge_control_delivery",
		RuntimeInputID:  "rin_bridge_interrupt",
		EventIDs:        []string{"sevt_interrupt_control"},
		SequenceFrom:    1,
		SequenceTo:      1,
		InputKind:       "interrupt_control",
		CommandKind:     agentruntimev1.RuntimeCommandKind_RUNTIME_COMMAND_KIND_INTERRUPT_CONTROL,
		PayloadJSON:     `{"workspace_id":"default","session_id":"sesn_bridge_control_delivery","session_thread_id":"thr_bridge_control_delivery","runtime_input_id":"rin_bridge_interrupt","event_ids":["sevt_interrupt_control"],"sequence_from":1,"sequence_to":1,"input_kind":"interrupt_control"}`,
	}
	seedRuntimeInboxBirthForJob(t, admin, interruptJob)
	interrupt, err := store.PrepareRuntimeCommand(context.Background(), interruptJob)
	if err != nil {
		t.Fatalf("PrepareRuntimeCommand interrupt: %v", err)
	}
	var interruptPayload struct {
		SourceEventID          string `json:"source_event_id"`
		InterruptFenceSequence int64  `json:"interrupt_fence_sequence"`
	}
	if err := json.Unmarshal([]byte(interrupt.Request.GetPayloadJson()), &interruptPayload); err != nil {
		t.Fatalf("decode interrupt payload: %v", err)
	}
	if interruptPayload.SourceEventID != "sevt_interrupt_control" || interruptPayload.InterruptFenceSequence != 1 || strings.Contains(interrupt.Request.GetPayloadJson(), "runtime_input_id") {
		t.Fatalf("interrupt runtime payload = %s; want runtime control payload", interrupt.Request.GetPayloadJson())
	}
	if _, err := admin.ExecContext(context.Background(),
		`UPDATE session_events
		    SET processed_at = '2026-01-01T00:03:01Z'
		  WHERE workspace_id = 'default'
		    AND session_id = 'sesn_bridge_control_delivery'
		    AND event_id = 'sevt_interrupt_control'`); err != nil {
		t.Fatalf("mark interrupt event processed: %v", err)
	}
	if _, err := admin.ExecContext(context.Background(),
		`UPDATE session_runtime_inbox
		    SET status = 'committed',
		        committed_at = '2026-01-01T00:03:01Z'
		  WHERE workspace_id = 'default'
		    AND session_id = 'sesn_bridge_control_delivery'
		    AND runtime_input_id = 'rin_bridge_interrupt'`); err != nil {
		t.Fatalf("mark interrupt inbox committed: %v", err)
	}

	confirmationJob := RuntimeJob{
		JobID:           "qjob_bridge_confirmation",
		LeaseToken:      "lease_bridge_confirmation",
		Kind:            queue.KindRuntimeInput,
		WorkspaceID:     "default",
		SessionID:       "sesn_bridge_control_delivery",
		SessionThreadID: "thr_bridge_control_delivery",
		RuntimeInputID:  "rin_bridge_confirmation",
		EventIDs:        []string{"sevt_confirmation_control"},
		SequenceFrom:    2,
		SequenceTo:      2,
		InputKind:       "tool_confirmation",
		CommandKind:     agentruntimev1.RuntimeCommandKind_RUNTIME_COMMAND_KIND_TOOL_CONFIRMATION,
		PayloadJSON:     `{"workspace_id":"default","session_id":"sesn_bridge_control_delivery","session_thread_id":"thr_bridge_control_delivery","runtime_input_id":"rin_bridge_confirmation","event_ids":["sevt_confirmation_control"],"sequence_from":2,"sequence_to":2,"input_kind":"tool_confirmation"}`,
	}
	seedRuntimeInboxBirthForJob(t, admin, confirmationJob)
	confirmation, err := store.PrepareRuntimeCommand(context.Background(), confirmationJob)
	if err != nil {
		t.Fatalf("PrepareRuntimeCommand tool confirmation: %v", err)
	}
	var confirmationPayload struct {
		SourceEventID  string `json:"source_event_id"`
		ToolUseEventID string `json:"tool_use_event_id"`
		Decision       string `json:"decision"`
		DenyMessage    string `json:"deny_message"`
	}
	if err := json.Unmarshal([]byte(confirmation.Request.GetPayloadJson()), &confirmationPayload); err != nil {
		t.Fatalf("decode confirmation payload: %v", err)
	}
	if confirmationPayload.SourceEventID != "sevt_confirmation_control" ||
		confirmationPayload.ToolUseEventID != "sevt_tool_control" ||
		confirmationPayload.Decision != "deny" ||
		confirmationPayload.DenyMessage != "not now" ||
		strings.Contains(confirmation.Request.GetPayloadJson(), "runtime_input_id") ||
		strings.Contains(confirmation.Request.GetPayloadJson(), `"result"`) ||
		strings.Contains(confirmation.Request.GetPayloadJson(), `"tool_use_id"`) {
		t.Fatalf("tool confirmation runtime payload = %s; want runtime command payload", confirmation.Request.GetPayloadJson())
	}
}

func TestPostgreSQLRuntimeDeliveryStoreAppliesProcessedInterruptFence(t *testing.T) {
	t.Run("all superseded messages stale ack without inbox", func(t *testing.T) {
		runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
		seedBridgeAPISession(t, admin, "default", "sesn_bridge_interrupt_fence", "thr_bridge_interrupt_fence")
		seedBridgeAPIEvent(t, admin, "default", "sesn_bridge_interrupt_fence", "thr_bridge_interrupt_fence", "sevt_old_message_1", 1, "user.message", `{"content":[{"type":"text","text":"old 1"}]}`)
		seedBridgeAPIEvent(t, admin, "default", "sesn_bridge_interrupt_fence", "thr_bridge_interrupt_fence", "sevt_old_message_2", 2, "user.message", `{"content":[{"type":"text","text":"old 2"}]}`)
		seedBridgeAPIEvent(t, admin, "default", "sesn_bridge_interrupt_fence", "thr_bridge_interrupt_fence", "sevt_processed_interrupt", 3, "user.interrupt", `{}`)
		if _, err := admin.ExecContext(context.Background(),
			`UPDATE session_events
			    SET processed_at = '2026-01-01T00:03:01Z'
			  WHERE workspace_id = 'default'
			    AND session_id = 'sesn_bridge_interrupt_fence'
			    AND event_id = 'sevt_processed_interrupt'`); err != nil {
			t.Fatalf("mark interrupt processed: %v", err)
		}
		store := NewPostgreSQLRuntimeDeliveryStore(dbconnect.NewClientForTesting(runtime), 9090)
		store.Clock = func() time.Time { return time.Date(2026, 1, 1, 0, 3, 2, 0, time.UTC) }

		job := RuntimeJob{
			JobID:           "qjob_superseded_messages",
			LeaseToken:      "lease_superseded_messages",
			Kind:            queue.KindRuntimeInput,
			WorkspaceID:     "default",
			SessionID:       "sesn_bridge_interrupt_fence",
			SessionThreadID: "thr_bridge_interrupt_fence",
			RuntimeInputID:  "rin_superseded_messages",
			EventIDs:        []string{"sevt_old_message_1", "sevt_old_message_2"},
			SequenceFrom:    1,
			SequenceTo:      2,
			InputKind:       "messages",
			CommandKind:     agentruntimev1.RuntimeCommandKind_RUNTIME_COMMAND_KIND_MESSAGES,
			PayloadJSON:     `{"workspace_id":"default","session_id":"sesn_bridge_interrupt_fence","session_thread_id":"thr_bridge_interrupt_fence","runtime_input_id":"rin_superseded_messages","event_ids":["sevt_old_message_1","sevt_old_message_2"],"sequence_from":1,"sequence_to":2,"input_kind":"messages"}`,
		}
		seedRuntimeInboxBirthForJob(t, admin, job)
		plan, err := store.PrepareRuntimeCommand(context.Background(), job)
		if err != nil {
			t.Fatalf("PrepareRuntimeCommand superseded messages: %v", err)
		}
		if !plan.StaleAccepted || plan.Request != nil {
			t.Fatalf("superseded plan = %+v; want stale accepted without Runtime request", plan)
		}
		var inboxStatus string
		if err := admin.QueryRowContext(context.Background(),
			`SELECT status
			   FROM session_runtime_inbox
			  WHERE workspace_id = 'default'
			    AND runtime_input_id = 'rin_superseded_messages'`).Scan(&inboxStatus); err != nil {
			t.Fatalf("read superseded inbox custody: %v", err)
		}
		if inboxStatus != "cancelled" {
			t.Fatalf("superseded inbox status = %q; want cancelled", inboxStatus)
		}
	})

	t.Run("mixed superseded and deliverable messages are fatal", func(t *testing.T) {
		runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
		seedBridgeAPISession(t, admin, "default", "sesn_bridge_interrupt_mixed", "thr_bridge_interrupt_mixed")
		seedBridgeAPIEvent(t, admin, "default", "sesn_bridge_interrupt_mixed", "thr_bridge_interrupt_mixed", "sevt_old_message", 1, "user.message", `{"content":[{"type":"text","text":"old"}]}`)
		seedBridgeAPIEvent(t, admin, "default", "sesn_bridge_interrupt_mixed", "thr_bridge_interrupt_mixed", "sevt_processed_interrupt", 3, "user.interrupt", `{}`)
		seedBridgeAPIEvent(t, admin, "default", "sesn_bridge_interrupt_mixed", "thr_bridge_interrupt_mixed", "sevt_new_message", 4, "user.message", `{"content":[{"type":"text","text":"new"}]}`)
		if _, err := admin.ExecContext(context.Background(),
			`UPDATE session_events
			    SET processed_at = '2026-01-01T00:03:01Z'
			  WHERE workspace_id = 'default'
			    AND session_id = 'sesn_bridge_interrupt_mixed'
			    AND event_id = 'sevt_processed_interrupt'`); err != nil {
			t.Fatalf("mark mixed interrupt processed: %v", err)
		}
		store := NewPostgreSQLRuntimeDeliveryStore(dbconnect.NewClientForTesting(runtime), 9090)
		store.Clock = func() time.Time { return time.Date(2026, 1, 1, 0, 3, 2, 0, time.UTC) }

		_, err := store.PrepareRuntimeCommand(context.Background(), RuntimeJob{
			JobID:           "qjob_mixed_messages",
			LeaseToken:      "lease_mixed_messages",
			Kind:            queue.KindRuntimeInput,
			WorkspaceID:     "default",
			SessionID:       "sesn_bridge_interrupt_mixed",
			SessionThreadID: "thr_bridge_interrupt_mixed",
			RuntimeInputID:  "rin_mixed_messages",
			EventIDs:        []string{"sevt_old_message", "sevt_new_message"},
			SequenceFrom:    1,
			SequenceTo:      4,
			InputKind:       "messages",
			CommandKind:     agentruntimev1.RuntimeCommandKind_RUNTIME_COMMAND_KIND_MESSAGES,
			PayloadJSON:     `{"workspace_id":"default","session_id":"sesn_bridge_interrupt_mixed","session_thread_id":"thr_bridge_interrupt_mixed","runtime_input_id":"rin_mixed_messages","event_ids":["sevt_old_message","sevt_new_message"],"sequence_from":1,"sequence_to":4,"input_kind":"messages"}`,
		})
		var prepareErr runtimeDeliveryPrepareError
		if !errors.As(err, &prepareErr) || prepareErr.kind != "runtime_input_superseded_by_interrupt" || prepareErr.retryable {
			t.Fatalf("mixed messages err = %v; want nonretryable runtime_input_superseded_by_interrupt", err)
		}
	})

	t.Run("post fence messages remain deliverable", func(t *testing.T) {
		runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
		seedBridgeAPISession(t, admin, "default", "sesn_bridge_interrupt_post_fence", "thr_bridge_interrupt_post_fence")
		seedBridgeAPIRuntimeBinding(t, admin, "default", "sesn_bridge_interrupt_post_fence", "bind_bridge_interrupt_post_fence", 1, "pod_uid_interrupt_post_fence")
		seedBridgeAPIEvent(t, admin, "default", "sesn_bridge_interrupt_post_fence", "thr_bridge_interrupt_post_fence", "sevt_processed_interrupt", 3, "user.interrupt", `{}`)
		seedBridgeAPIEvent(t, admin, "default", "sesn_bridge_interrupt_post_fence", "thr_bridge_interrupt_post_fence", "sevt_new_message", 4, "user.message", `{"content":[{"type":"text","text":"new"}]}`)
		if _, err := admin.ExecContext(context.Background(),
			`UPDATE session_events
			    SET processed_at = '2026-01-01T00:03:01Z'
			  WHERE workspace_id = 'default'
			    AND session_id = 'sesn_bridge_interrupt_post_fence'
			    AND event_id = 'sevt_processed_interrupt'`); err != nil {
			t.Fatalf("mark post-fence interrupt processed: %v", err)
		}
		store := NewPostgreSQLRuntimeDeliveryStore(dbconnect.NewClientForTesting(runtime), 9090)
		store.Clock = func() time.Time { return time.Date(2026, 1, 1, 0, 3, 2, 0, time.UTC) }

		job := RuntimeJob{
			JobID:           "qjob_post_fence_message",
			LeaseToken:      "lease_post_fence_message",
			Kind:            queue.KindRuntimeInput,
			WorkspaceID:     "default",
			SessionID:       "sesn_bridge_interrupt_post_fence",
			SessionThreadID: "thr_bridge_interrupt_post_fence",
			RuntimeInputID:  "rin_post_fence_message",
			EventIDs:        []string{"sevt_new_message"},
			SequenceFrom:    4,
			SequenceTo:      4,
			InputKind:       "messages",
			CommandKind:     agentruntimev1.RuntimeCommandKind_RUNTIME_COMMAND_KIND_MESSAGES,
			PayloadJSON:     `{"workspace_id":"default","session_id":"sesn_bridge_interrupt_post_fence","session_thread_id":"thr_bridge_interrupt_post_fence","runtime_input_id":"rin_post_fence_message","event_ids":["sevt_new_message"],"sequence_from":4,"sequence_to":4,"input_kind":"messages"}`,
		}
		seedRuntimeInboxBirthForJob(t, admin, job)
		plan, err := store.PrepareRuntimeCommand(context.Background(), job)
		if err != nil {
			t.Fatalf("PrepareRuntimeCommand post-fence messages: %v", err)
		}
		if plan.StaleAccepted || plan.Request == nil {
			t.Fatalf("post-fence plan = %+v; want Runtime request", plan)
		}
		var inboxStatus string
		var sequenceFrom int64
		if err := admin.QueryRowContext(context.Background(),
			`SELECT status, sequence_from
			   FROM session_runtime_inbox
			  WHERE workspace_id = 'default'
			    AND runtime_input_id = 'rin_post_fence_message'`).Scan(&inboxStatus, &sequenceFrom); err != nil {
			t.Fatalf("read post-fence inbox: %v", err)
		}
		if inboxStatus != "delivering" || sequenceFrom != 4 {
			t.Fatalf("post-fence inbox status=%q sequence_from=%d; want delivering/4", inboxStatus, sequenceFrom)
		}
	})
}

func TestPostgreSQLRuntimeDeliveryStoreClaimsBindingFromKubernetesVisibility(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	seedBridgeAPISession(t, admin, "default", "sesn_bridge_resolve", "thr_bridge_resolve")
	seedBridgeAPIEvent(t, admin, "default", "sesn_bridge_resolve", "thr_bridge_resolve", "evt_bridge_resolve", 1, "user.message", `{"content":[{"type":"text","text":"hello"}]}`)

	candidate := enginekubernetes.BindingCandidate{
		Namespace: "tetral-agent-runtime",
		PodName:   "runtime-pod-a",
		PodUID:    "pod-uid-a",
		PodIP:     "10.0.0.25",
	}
	store := NewPostgreSQLRuntimeDeliveryStore(dbconnect.NewClientForTesting(runtime), 9090)
	store.Clock = func() time.Time { return time.Date(2026, 1, 1, 0, 0, 30, 0, time.UTC) }
	store.TargetResolver = KubernetesRuntimeTargetResolver{
		Snapshot: func() enginekubernetes.BindingVisibilitySnapshot {
			return enginekubernetes.NewBindingVisibilitySnapshotForTest(true, []enginekubernetes.BindingCandidate{candidate})
		},
	}
	job := RuntimeJob{
		JobID:           "qjob_bridge_resolve",
		LeaseToken:      "lease_bridge_resolve",
		Kind:            queue.KindRuntimeInput,
		WorkspaceID:     "default",
		SessionID:       "sesn_bridge_resolve",
		SessionThreadID: "thr_bridge_resolve",
		RuntimeInputID:  "rin_bridge_resolve",
		EventIDs:        []string{"evt_bridge_resolve"},
		SequenceFrom:    1,
		SequenceTo:      1,
		InputKind:       "messages",
		CommandKind:     agentruntimev1.RuntimeCommandKind_RUNTIME_COMMAND_KIND_MESSAGES,
		PayloadJSON:     `{"type":"messages"}`,
	}
	seedRuntimeInboxBirthForJob(t, admin, job)
	plan, err := store.PrepareRuntimeCommand(context.Background(), job)
	if err != nil {
		t.Fatalf("PrepareRuntimeCommand: %v", err)
	}
	if plan.Target.PodName != candidate.PodName || plan.Target.PodUID != candidate.PodUID || plan.Target.PodIP != candidate.PodIP {
		t.Fatalf("target = %+v; want candidate %+v", plan.Target, candidate)
	}
	if plan.Request.GetBindingId() == "" || plan.Request.GetBindingGeneration() == 0 || plan.Request.GetTargetPodName() != candidate.PodName {
		t.Fatalf("request binding envelope = %+v; want claimed binding and target identity", plan.Request)
	}

	var podName string
	var podUID string
	var podIP string
	var generation int64
	if err := admin.QueryRowContext(context.Background(),
		`SELECT agent_runtime_pod_name, agent_runtime_pod_uid, agent_runtime_pod_ip, binding_generation
		   FROM session_runtime_bindings
		  WHERE workspace_id = 'default' AND session_id = 'sesn_bridge_resolve'`).Scan(&podName, &podUID, &podIP, &generation); err != nil {
		t.Fatalf("read claimed binding: %v", err)
	}
	if podName != candidate.PodName || podUID != candidate.PodUID || podIP != candidate.PodIP || generation != plan.Request.GetBindingGeneration() {
		t.Fatalf("claimed binding = %s/%s/%s gen %d; want %s/%s/%s gen %d", podName, podUID, podIP, generation, candidate.PodName, candidate.PodUID, candidate.PodIP, plan.Request.GetBindingGeneration())
	}
}

func TestPostgreSQLRuntimeDeliveryStoreConvertsOversizedInputToBoundedLoopRejection(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	seedBridgeAPISession(t, admin, "default", "sesn_bridge_runtime_rejected", "thr_bridge_runtime_rejected")
	seedBridgeAPIRuntimeBinding(t, admin, "default", "sesn_bridge_runtime_rejected", "bind_bridge_runtime_rejected", 1, "pod_uid_runtime_rejected")
	seedBridgeAPIEvent(t, admin, "default", "sesn_bridge_runtime_rejected", "thr_bridge_runtime_rejected", "evt_bridge_runtime_rejected", 1, "user.message", `{"type":"user.message","content":[{"type":"text","text":"hello"}]}`)
	store := NewPostgreSQLRuntimeDeliveryStore(dbconnect.NewClientForTesting(runtime), 9090)
	store.Clock = func() time.Time { return time.Date(2026, 1, 1, 0, 2, 0, 0, time.UTC) }
	job := RuntimeJob{
		JobID:           "qjob_bridge_runtime_rejected",
		LeaseToken:      "lease_bridge_runtime_rejected",
		Kind:            queue.KindRuntimeInput,
		WorkspaceID:     "default",
		SessionID:       "sesn_bridge_runtime_rejected",
		SessionThreadID: "thr_bridge_runtime_rejected",
		RuntimeInputID:  "rin_bridge_runtime_rejected",
		EventIDs:        []string{"evt_bridge_runtime_rejected"},
		SequenceFrom:    1,
		SequenceTo:      1,
		InputKind:       "messages",
		CommandKind:     agentruntimev1.RuntimeCommandKind_RUNTIME_COMMAND_KIND_MESSAGES,
		PayloadJSON:     `{"workspace_id":"default","session_id":"sesn_bridge_runtime_rejected"}`,
	}
	seedRuntimeInboxBirthForJob(t, admin, job)

	plan, err := store.PrepareRuntimeCommand(context.Background(), job)
	if err != nil {
		t.Fatalf("PrepareRuntimeCommand runtime rejection: %v", err)
	}
	if got := plan.Request.GetRuntimeInputId(); got != "rin_bridge_runtime_rejected" {
		t.Fatalf("prepared runtime input id = %q; want rin_bridge_runtime_rejected", got)
	}
	result := RuntimeDeliveryResult{
		Status:       RuntimeDeliveryRejected,
		Retryable:    false,
		ErrorKind:    "runtime_command_payload_too_large",
		ErrorMessage: "runtime command exceeds the transport fuse",
	}
	converted, err := store.PrepareRuntimeInputRejection(context.Background(), job, result)
	if err != nil {
		t.Fatalf("PrepareRuntimeInputRejection: %T %v", err, err)
	}
	if !converted {
		t.Fatal("PrepareRuntimeInputRejection converted = false; want true")
	}

	var inboxStatus string
	var inboxKind string
	var rejectionReason string
	var inboxUpdatedAt string
	if err := admin.QueryRowContext(context.Background(),
		`SELECT status, input_kind, rejection_reason_code, updated_at
		   FROM session_runtime_inbox
		  WHERE workspace_id = 'default'
		    AND runtime_input_id = 'rin_bridge_runtime_rejected'`).Scan(&inboxStatus, &inboxKind, &rejectionReason, &inboxUpdatedAt); err != nil {
		t.Fatalf("read rejected runtime inbox: %v", err)
	}
	if inboxStatus != "delivering" || inboxKind != "rejection" ||
		rejectionReason != "runtime_command_payload_too_large" ||
		inboxUpdatedAt != "2026-01-01T00:02:00Z" {
		t.Fatalf("rejected inbox status/kind/reason/updatedAt = %q/%q/%q/%q; want delivering bounded rejection",
			inboxStatus, inboxKind, rejectionReason, inboxUpdatedAt)
	}
	var inputProcessedAt sql.NullString
	var inputRevision int64
	if err := admin.QueryRowContext(context.Background(),
		`SELECT processed_at, revision
		   FROM session_events
		  WHERE workspace_id = 'default'
		    AND event_id = 'evt_bridge_runtime_rejected'`).Scan(&inputProcessedAt, &inputRevision); err != nil {
		t.Fatalf("read rejected input event: %v", err)
	}
	if inputProcessedAt.Valid || inputRevision != 1 {
		t.Fatalf("rejected input event processed=%v revision=%d; want untouched source until loop commit", inputProcessedAt.Valid, inputRevision)
	}

	var errorEventCount int
	var messageProjectionCount int
	if err := admin.QueryRowContext(context.Background(),
		`SELECT count(*)
		   FROM session_events
		  WHERE workspace_id = 'default'
		    AND session_id = 'sesn_bridge_runtime_rejected'
		    AND type = 'session.error'`).Scan(&errorEventCount); err != nil {
		t.Fatalf("count runtime rejection session.error rows: %v", err)
	}
	if err := admin.QueryRowContext(context.Background(),
		`SELECT count(*)
		   FROM session_messages
		  WHERE workspace_id = 'default'
		    AND session_id = 'sesn_bridge_runtime_rejected'`).Scan(&messageProjectionCount); err != nil {
		t.Fatalf("count runtime rejection message projections: %v", err)
	}
	if errorEventCount != 0 || messageProjectionCount != 0 {
		t.Fatalf("Bridge-authored rejection content = events %d messages %d; want none", errorEventCount, messageProjectionCount)
	}

	retryPlan, err := store.PrepareRuntimeCommand(context.Background(), job)
	if err != nil {
		t.Fatalf("PrepareRuntimeCommand bounded rejection: %v", err)
	}
	var bounded struct {
		InputKind  string `json:"input_kind"`
		ReasonCode string `json:"reason_code"`
	}
	if err := json.Unmarshal([]byte(retryPlan.Request.GetPayloadJson()), &bounded); err != nil {
		t.Fatalf("decode bounded rejection payload: %v", err)
	}
	if bounded.InputKind != "rejection" || bounded.ReasonCode != "runtime_command_payload_too_large" {
		t.Fatalf("bounded rejection payload = %+v; want closed source-free fact", bounded)
	}
}
