package agentruntimebridge

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/tetral-ai/tetral/internal/blob"
	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	bridgev1 "github.com/tetral-ai/tetral/services/bridge/gen/tetral/bridge/v1"
)

// This file owns the Bridge mcp protocol-family boundary.

func TestPostgreSQLMCPInfrastructureFailureSettlesOneToolResultAndReducerContinues(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID    = "sesn_mcp_tool_failure_composition"
		threadID     = "thr_mcp_tool_failure_composition"
		bindingID    = "bind_mcp_tool_failure_composition"
		podUID       = "pod_mcp_tool_failure_composition"
		modelRequest = "mreq_mcp_tool_failure_composition"
		modelCall    = "call_mcp_tool_failure_composition"
	)
	seedBridgeAPISession(t, admin, "default", sessionID, threadID)
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, bindingID, 1, podUID)
	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	store.RuntimeBindingTokenHMACKey = []byte("mcp-tool-failure-composition-key")
	scope := bridgeAPIScope(sessionID, threadID, bindingID, 1, podUID)
	seedBridgeAPIRequestStart(t, store, scope, "rwrite_mcp_tool_failure_start", modelRequest, "agent_provider_request", 0)
	toolUse, err := store.WriteEvent(context.Background(), &bridgev1.WriteEventRequest{
		Scope: scope, RuntimeWriteId: "rwrite_mcp_tool_failure_use", ModelRequestId: modelRequest,
		ToolDeclaration: bridgeMCPToolDeclarationForTest(modelCall, "github_search", "github", `{"query":"tetral"}`, "allow"),
	})
	if err != nil {
		t.Fatalf("write MCP Tool Use: %v", err)
	}
	if _, err := store.WriteRequestEnd(context.Background(), &bridgev1.WriteRequestEndRequest{
		Scope: scope, RuntimeWriteId: "rwrite_mcp_tool_failure_end", ModelRequestId: modelRequest,
		FinishReason: "tool-calls", UsageJson: `{}`,
		ProviderContextRetention: &bridgev1.ProviderContextRetention{
			Disposition: "completed", AssistantMessageSequence: toolUse.GetCommitted().AssignedMessageSequence,
			ToolUseEventIds: []string{toolUse.GetCommitted().GetEventId()},
		},
	}); err != nil {
		t.Fatalf("write request end: %v", err)
	}
	claim := &bridgev1.ClaimMcpToolResultRequest{
		Scope: scope, ToolUseEventId: toolUse.GetCommitted().GetEventId(), ClaimId: "claim_mcp_tool_failure",
	}
	claimed, err := store.ClaimMcpToolResult(context.Background(), claim)
	if err != nil || claimed.GetAcquired() == nil {
		t.Fatalf("claim MCP execution = %#v/%v", claimed, err)
	}
	store.Logger = slog.New(panicSlogHandler{})
	committedResult, err := store.CommitMcpToolResult(context.Background(), &bridgev1.CommitMcpToolResultRequest{
		Scope: scope, ToolUseEventId: claim.GetToolUseEventId(), ClaimId: claim.GetClaimId(),
		ResultJson: `{"response":{"status":3,"result_text":"credential and provider response must not escape","attachments":[],"error_kind":2,"retry_status":2},"content_items":0,"refresh_triggered":true}`,
	})
	if err != nil || committedResult.GetCommitted() == nil {
		t.Fatalf("commit MCP result with failing telemetry = %#v/%v", committedResult, err)
	}
	fixtureInput := map[string]any{
		"workspaceId": "default", "sessionId": sessionID, "sessionThreadId": threadID,
		"bindingId": bindingID, "bindingGeneration": 1, "targetPodUid": podUID,
		"modelRequestId": modelRequest, "modelToolCallId": modelCall, "toolUseEventId": toolUse.GetCommitted().GetEventId(),
	}
	rawInput, err := json.Marshal(fixtureInput)
	if err != nil {
		t.Fatalf("encode MCP failure composition input: %v", err)
	}
	inputPath := filepath.Join(t.TempDir(), "mcp-tool-failure.json")
	if err := os.WriteFile(inputPath, rawInput, 0o600); err != nil {
		t.Fatalf("write MCP failure composition input: %v", err)
	}
	command := exec.CommandContext(context.Background(), "bun", "packages/runtime-pod/test/fixtures/mcp-tool-failure-composition.ts", inputPath) //nolint:gosec // Fixed repository fixture and test-owned path.
	command.Dir = "../agent-runtime"
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("run MCP failure composition: %v: %s", err, output)
	}
	var composed struct {
		ConnectorCalls int `json:"connectorCalls"`
		Result         struct {
			Type  string `json:"type"`
			Error struct {
				Message   string `json:"message"`
				Retryable bool   `json:"retryable"`
			} `json:"error"`
		} `json:"result"`
		Settlement struct {
			Type  string          `json:"type"`
			Error json.RawMessage `json:"error"`
		} `json:"settlement"`
		DeclaredError json.RawMessage `json:"declaredError"`
	}
	if err := json.Unmarshal(output, &composed); err != nil {
		t.Fatalf("decode MCP failure composition: %v: %s", err, output)
	}
	if composed.ConnectorCalls != 1 || composed.Result.Type != "error" || composed.Result.Error.Retryable ||
		composed.Result.Error.Message != "MCP tool execution is unavailable." || composed.Settlement.Type != "error" ||
		len(composed.DeclaredError) == 0 {
		t.Fatalf("MCP failure composition = %+v; want one generic non-retryable Tool settlement", composed)
	}
	request := bridgeToolSettlementRequestForTest(scope, &bridgev1.RuntimeToolSettlement{
		ToolUseEventId: toolUse.GetCommitted().GetEventId(),
		Outcome:        &bridgev1.RuntimeToolSettlement_Error{Error: &bridgev1.RuntimeToolError{ErrorJson: string(composed.DeclaredError)}},
	})
	committed, err := store.SettleToolResult(context.Background(), request)
	if err != nil {
		t.Fatalf("commit MCP Tool Result: %v", err)
	}
	bridgeRequireToolSettlementOutcomeForTest(t, committed, "committed")
	replayed, err := store.SettleToolResult(context.Background(), proto.Clone(request).(*bridgev1.SettleToolResultRequest))
	if err != nil {
		t.Fatalf("replay MCP Tool Result settlement: %v", err)
	}
	bridgeRequireToolSettlementOutcomeForTest(t, replayed, "duplicate")
	if _, err := store.SettleToolResult(context.Background(), bridgeToolSettlementRequestForTest(
		scope,
		bridgeCompletedToolSettlementForTest(toolUse.GetCommitted().GetEventId(), "different"),
	)); status.Code(err) != codes.AlreadyExists {
		t.Fatalf("conflicting MCP Tool Result settlement = %v; want AlreadyExists", err)
	}
	var toolResults, sessionErrors int
	if err := admin.QueryRowContext(context.Background(), `SELECT
		(SELECT count(*) FROM session_events WHERE workspace_id='default' AND session_id=$1 AND type='agent.mcp_tool_result' AND payload_json::jsonb ->> 'mcp_tool_use_id'=$2),
		(SELECT count(*) FROM session_events WHERE workspace_id='default' AND session_id=$1 AND type='session.error')`, sessionID, toolUse.GetCommitted().GetEventId()).Scan(&toolResults, &sessionErrors); err != nil {
		t.Fatalf("count MCP failure durable results: %v", err)
	}
	if toolResults != 1 || sessionErrors != 0 {
		t.Fatalf("durable MCP failure = Tool Results %d Session errors %d; want 1/0", toolResults, sessionErrors)
	}
	var durableMessage string
	if err := admin.QueryRowContext(context.Background(), `SELECT data_json FROM session_messages
		WHERE workspace_id='default' AND session_id=$1 AND session_thread_id=$2 AND model_request_id=$3`,
		sessionID, threadID, modelRequest).Scan(&durableMessage); err != nil {
		t.Fatalf("read MCP Tool projection: %v", err)
	}
	var mcpProjection struct {
		Parts []map[string]any `json:"parts"`
	}
	if err := json.Unmarshal([]byte(durableMessage), &mcpProjection); err != nil || len(mcpProjection.Parts) != 2 {
		t.Fatalf("MCP Tool error was not normalized: %s", durableMessage)
	}
	result, _ := mcpProjection.Parts[1]["result"].(map[string]any)
	failure, _ := result["error"].(map[string]any)
	if result["type"] != "error" || failure["message"] != "MCP tool execution is unavailable." {
		t.Fatalf("MCP Tool error was not normalized: %s", durableMessage)
	}
	loaded, err := store.LoadContext(context.Background(), &bridgev1.LoadContextRequest{Scope: scope})
	if err != nil {
		t.Fatalf("LoadContext after MCP Tool failure: %v", err)
	}
	for _, forbidden := range []string{"credential and provider response must not escape", "access_token", "refresh_token"} {
		if strings.Contains(string(output), forbidden) || strings.Contains(loaded.GetContextJson(), forbidden) {
			t.Fatalf("MCP failure surface exposed %q", forbidden)
		}
	}
}

func TestPostgreSQLMCPErrorSettlementPreservesAnActiveClaim(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID = "sesn_mcp_in_flight_error"
		threadID  = "thr_mcp_in_flight_error"
		bindingID = "bind_mcp_in_flight_error"
		podUID    = "pod_mcp_in_flight_error"
	)
	seedBridgeAPISession(t, admin, "default", sessionID, threadID)
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, bindingID, 1, podUID)
	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	store.RuntimeBindingTokenHMACKey = []byte("mcp-in-flight-error-signing-key")
	scope := bridgeAPIScope(sessionID, threadID, bindingID, 1, podUID)
	toolUseEventID := writeDurableMCPToolUseForTest(t, store, scope)
	claim, err := store.ClaimMcpToolResult(context.Background(), &bridgev1.ClaimMcpToolResultRequest{
		Scope: scope, ToolUseEventId: toolUseEventID, ClaimId: "claim_mcp_in_flight_error",
	})
	if err != nil || claim.GetAcquired() == nil {
		t.Fatalf("claim MCP execution = %#v/%v; want acquired", claim, err)
	}

	const errorJSON = `{"type":"runtime_invalid_sequence","message":"durable MCP error settlement","retryable":false}`
	settled, err := store.SettleToolResult(context.Background(), bridgeToolSettlementRequestForTest(scope, &bridgev1.RuntimeToolSettlement{
		ToolUseEventId: toolUseEventID,
		Outcome: &bridgev1.RuntimeToolSettlement_Error{Error: &bridgev1.RuntimeToolError{
			ErrorJson: errorJSON,
		}},
	}))
	if err != nil || settled.GetCommitted() == nil {
		t.Fatalf("settle model-visible MCP error = %#v/%v; want committed", settled, err)
	}

	var claimStatus string
	var claimID, lease sql.NullString
	if err := admin.QueryRowContext(context.Background(), `SELECT mcp_claim_status, mcp_claim_id, mcp_claim_lease_expires_at
		FROM session_runtime_tool_results
		WHERE workspace_id='default' AND session_id=$1 AND session_thread_id=$2 AND tool_use_event_id=$3`,
		sessionID, threadID, toolUseEventID).Scan(&claimStatus, &claimID, &lease); err != nil {
		t.Fatalf("read MCP claim after error settlement: %v", err)
	}
	if claimStatus != mcpClaimStatusInFlight || claimID.String != "claim_mcp_in_flight_error" || !lease.Valid {
		t.Fatalf("MCP claim after error settlement = %q/%v/%v; want original in-flight lease", claimStatus, claimID, lease)
	}
	loaded, err := store.LoadContext(context.Background(), &bridgev1.LoadContextRequest{Scope: scope})
	if err != nil {
		t.Fatalf("load model-visible in-flight MCP error: %v", err)
	}
	var payload bridgeLoadContextPayload
	if err := json.Unmarshal([]byte(loaded.GetContextJson()), &payload); err != nil {
		t.Fatalf("decode model-visible in-flight MCP error: %v", err)
	}
	var parts []json.RawMessage
	if len(payload.ContextEntries) == 1 {
		parts = payload.ContextEntries[0].Parts
	} else if payload.OpenRequestDraft != nil {
		parts = payload.OpenRequestDraft.Parts
	}
	if len(parts) != 2 {
		t.Fatalf("in-flight MCP error context = entries=%#v draft=%#v", payload.ContextEntries, payload.OpenRequestDraft)
	}
	var resultPart struct {
		Result struct {
			Type  string `json:"type"`
			Error struct {
				Message string `json:"message"`
			} `json:"error"`
		} `json:"result"`
	}
	if err := json.Unmarshal(parts[1], &resultPart); err != nil ||
		resultPart.Result.Type != "error" || resultPart.Result.Error.Message != "durable MCP error settlement" {
		t.Fatalf("model-visible MCP error settlement = %#v err=%v", resultPart, err)
	}
}

func TestPostgreSQLMCPUncertaintySettlesWithoutResultAlias(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID    = "sesn_mcp_uncertain_without_result_alias"
		threadID     = "thr_mcp_uncertain_without_result_alias"
		modelRequest = "mreq_mcp_uncertain_without_result_alias"
	)
	seedBridgeAPISession(t, admin, "default", sessionID, threadID)
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, "bind_mcp_uncertain", 1, "pod_mcp_uncertain")
	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	scope := bridgeAPIScope(sessionID, threadID, "bind_mcp_uncertain", 1, "pod_mcp_uncertain")
	seedBridgeAPIRequestStart(t, store, scope, "rwrite_mcp_uncertain_start", modelRequest, "agent_provider_request", 0)
	toolUse, err := store.WriteEvent(context.Background(), &bridgev1.WriteEventRequest{
		Scope: scope, RuntimeWriteId: "rwrite_mcp_uncertain_use", ModelRequestId: modelRequest,
		ToolDeclaration: bridgeMCPToolDeclarationForTest("call_mcp_uncertain", "github_search", "github", `{"query":"tetral"}`, "allow"),
	})
	if err != nil {
		t.Fatalf("write MCP Tool Use: %v", err)
	}
	message := "The MCP tool execution is still in progress. Check the external service before retrying."
	errorSettlement := bridgeErrorToolSettlementForTest(toolUse.GetCommitted().GetEventId(), message)
	result, err := store.SettleToolResult(context.Background(), bridgeToolSettlementRequestForTest(scope, errorSettlement))
	if err != nil {
		t.Fatalf("write MCP uncertainty = %#v/%v", result, err)
	}
	bridgeRequireToolSettlementOutcomeForTest(t, result, "committed")

	var durableResults int
	if err := admin.QueryRowContext(context.Background(), `SELECT count(*) FROM session_events
		WHERE workspace_id='default' AND session_id=$1 AND type='agent.mcp_tool_result'
		  AND payload_json::jsonb ->> 'mcp_tool_use_id'=$2`, sessionID, toolUse.GetCommitted().GetEventId()).Scan(&durableResults); err != nil {
		t.Fatalf("count durable MCP uncertainty: %v", err)
	}
	if durableResults != 1 {
		t.Fatalf("durable MCP uncertainty results = %d; want one", durableResults)
	}
}

func TestPostgreSQLBridgeAPIStoreMcpManifestCommitAndAckLossReplayKeepOneQueueGeneration(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	seedBridgeAPISession(t, admin, "default", "sesn_bridge_mcp_manifest", "thr_bridge_mcp_manifest")
	seedBridgeAPIAgentConfig(t, admin, "default", "sesn_bridge_mcp_manifest", `{"name":"agent","model":"anthropic/claude-opus-4-8","tools":[{"type":"mcp_toolset","mcp_server_name":"github","default_config":{"enabled":false,"permission_policy":{"type":"always_ask"}},"configs":[{"name":"github_search","enabled":true,"permission_policy":{"type":"always_allow"}}]}],"mcp_servers":[{"type":"url","name":"github","url":"https://api.githubcopilot.com/mcp/"}],"skills":[],"metadata":{}}`)
	if _, err := admin.ExecContext(context.Background(), `UPDATE sessions SET installed_tools_json = '{"tools":[{"type":"tetral_agent_toolset","family":"claude"},{"type":"mcp_toolset","mcp_server_name":"github","default_config":{"enabled":false,"permission_policy":{"type":"always_ask"}},"configs":[{"name":"github_search","enabled":true,"permission_policy":{"type":"always_allow"}}]}],"mcp_servers":[{"type":"url","name":"github","url":"https://api.githubcopilot.com/mcp/"}]}' WHERE workspace_id = 'default' AND id = 'sesn_bridge_mcp_manifest'`); err != nil {
		t.Fatalf("seed durable MCP config: %v", err)
	}
	lister := &recordingMCPManifestLister{
		results: []MCPManifestListResult{
			{
				ManifestETag: "etag_1",
				Tools: []MCPManifestTool{
					{Name: "github_search", Description: "Search GitHub", InputSchemaJSON: `{"type":"object","properties":{"query":{"type":"string"}}}`},
				},
			},
			{
				ManifestETag: "etag_2",
				Tools: []MCPManifestTool{
					{Name: "github_search", Description: "Search GitHub", InputSchemaJSON: `{"type":"object"}`},
				},
			},
		},
	}
	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	store.MCPManifestLister = lister

	request := &bridgev1.McpManifestChangedRequest{
		WorkspaceId:   "default",
		SessionId:     "sesn_bridge_mcp_manifest",
		McpServerName: "github",
		ManifestEtag:  "etag_1",
	}
	response, err := store.McpManifestChanged(context.Background(), request)
	if err != nil {
		t.Fatalf("McpManifestChanged: %v", err)
	}
	if response.GetCommitted() == nil {
		t.Fatalf("McpManifestChanged = %#v; want committed", response)
	}
	assertRuntimeMCPManifestQueueJob(t, admin, "default", "sesn_bridge_mcp_manifest", "github", 1)

	// The caller may lose the committed ACK after dispatch. Replaying the exact
	// desired identity must recover durable evidence without another list,
	// generation, or Queue producer.
	replay, err := store.McpManifestChanged(context.Background(), request)
	if err != nil {
		t.Fatalf("McpManifestChanged replay: %v", err)
	}
	if replay.GetDuplicate() == nil || len(lister.requests) != 1 {
		t.Fatalf("McpManifestChanged replay=%#v lister calls=%d; want duplicate without re-list", replay, len(lister.requests))
	}

	changed := proto.Clone(request).(*bridgev1.McpManifestChangedRequest)
	changed.ManifestEtag = "etag_2"
	if _, err := store.McpManifestChanged(context.Background(), changed); err != nil {
		t.Fatalf("McpManifestChanged changed etag: %v", err)
	}
	assertRuntimeMCPManifestQueueJob(t, admin, "default", "sesn_bridge_mcp_manifest", "github", 2)
	if len(lister.requests) != 2 {
		t.Fatalf("lister calls = %d; want 2 after changed etag", len(lister.requests))
	}
}

func TestPostgreSQLBridgeAPIStoreMcpManifestChangedUsesSessionRuntimeAgentConfig(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	seedBridgeAPISession(t, admin, "default", "sesn_bridge_mcp_runtime_agent", "thr_bridge_mcp_runtime_agent")
	seedBridgeAPIAgentConfig(t, admin, "default", "sesn_bridge_mcp_runtime_agent", `{"name":"agent","model":"anthropic/claude-opus-4-8","tools":[{"type":"mcp_toolset","mcp_server_name":"github","default_config":{"enabled":true,"permission_policy":{"type":"always_allow"}}}],"mcp_servers":[{"type":"url","name":"github","url":"https://api.githubcopilot.com/mcp/"}],"skills":[],"metadata":{}}`)
	if _, err := admin.ExecContext(context.Background(),
		`UPDATE sessions
		    SET installed_tools_json = '{"tools":[{"type":"tetral_agent_toolset","family":"claude"},{"type":"mcp_toolset","mcp_server_name":"github","default_config":{"enabled":false,"permission_policy":{"type":"always_ask"}},"configs":[{"name":"github_search","enabled":true,"permission_policy":{"type":"always_allow"}}]}],"mcp_servers":[{"type":"url","name":"github","url":"https://api.githubcopilot.com/mcp/"}]}'
		  WHERE workspace_id = 'default'
		    AND id = 'sesn_bridge_mcp_runtime_agent'`,
	); err != nil {
		t.Fatalf("seed session runtime agent config: %v", err)
	}
	lister := &recordingMCPManifestLister{results: []MCPManifestListResult{{
		ManifestETag: "etag_runtime_agent",
		Tools: []MCPManifestTool{
			{Name: "github_search", Description: "Search GitHub", InputSchemaJSON: `{"type":"object"}`},
			{Name: "Read", Description: "MCP Read", InputSchemaJSON: `{"type":"object"}`},
			{Name: "exec_command", Description: "MCP exec", InputSchemaJSON: `{"type":"object"}`},
			{Name: "memory", Description: "MCP memory", InputSchemaJSON: `{"type":"object"}`},
		},
	}}}
	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	store.MCPManifestLister = lister

	if _, err := store.McpManifestChanged(context.Background(), &bridgev1.McpManifestChangedRequest{
		WorkspaceId:   "default",
		SessionId:     "sesn_bridge_mcp_runtime_agent",
		McpServerName: "github",
		ManifestEtag:  "etag_runtime_agent",
	}); err != nil {
		t.Fatalf("McpManifestChanged: %v", err)
	}

	assertRuntimeMCPManifestQueueJob(t, admin, "default", "sesn_bridge_mcp_runtime_agent", "github", 1)
	var toolsJSON string
	if err := admin.QueryRowContext(context.Background(),
		`SELECT tools_json FROM session_mcp_manifests
		  WHERE workspace_id = 'default' AND session_id = 'sesn_bridge_mcp_runtime_agent' AND mcp_server_name = 'github'`,
	).Scan(&toolsJSON); err != nil {
		t.Fatalf("read family-filtered durable MCP manifest: %v", err)
	}
	if strings.Contains(toolsJSON, `"name":"Read"`) || !strings.Contains(toolsJSON, `"name":"memory"`) || !strings.Contains(toolsJSON, `"name":"exec_command"`) {
		t.Fatalf("Claude durable MCP manifest = %s; want only Claude collision omitted and platform/GPT names retained", toolsJSON)
	}
}

func TestFilterMCPManifestCollisionsUsesOnlyPinnedFamilyTools(t *testing.T) {
	tools := []MCPManifestTool{{Name: "Read"}, {Name: "exec_command"}, {Name: "memory"}, {Name: "github_search"}}
	for _, test := range []struct {
		family string
		want   []string
	}{
		{family: "claude", want: []string{"exec_command", "memory", "github_search"}},
		{family: "gpt", want: []string{"Read", "memory", "github_search"}},
	} {
		filtered, _ := filterMCPManifestCollisions(test.family, tools)
		got := make([]string, 0, len(filtered))
		for _, tool := range filtered {
			got = append(got, tool.Name)
		}
		if !reflect.DeepEqual(got, test.want) {
			t.Fatalf("filterMCPManifestCollisions(%s) = %v; want %v", test.family, got, test.want)
		}
	}
}

func TestPostgreSQLBridgeAPIStoreMcpManifestChangedRejectsMismatchedEtag(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	seedBridgeAPISession(t, admin, "default", "sesn_bridge_mcp_manifest_mismatch", "thr_bridge_mcp_manifest_mismatch")
	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	store.MCPManifestLister = &recordingMCPManifestLister{results: []MCPManifestListResult{{
		ManifestETag: "etag_other",
		Tools:        []MCPManifestTool{{Name: "github_search", Description: "Search GitHub", InputSchemaJSON: `{"type":"object"}`}},
	}}}

	_, err := store.McpManifestChanged(context.Background(), &bridgev1.McpManifestChangedRequest{
		WorkspaceId:   "default",
		SessionId:     "sesn_bridge_mcp_manifest_mismatch",
		McpServerName: "github",
		ManifestEtag:  "etag_expected",
	})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("McpManifestChanged mismatch err = %v; want FailedPrecondition", err)
	}
	assertNoRuntimeMCPManifestQueueJob(t, admin, "default", "sesn_bridge_mcp_manifest_mismatch", "github")
}

func TestPostgreSQLBridgeAPIStoreMCPToolResultDurableReplay(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	seedBridgeAPISession(t, admin, "default", "sesn_bridge_mcp_tool", "thr_bridge_mcp_tool")
	seedBridgeAPIRuntimeBinding(t, admin, "default", "sesn_bridge_mcp_tool", "bind_bridge_mcp_tool", 1, "pod_uid_mcp_tool")

	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	store.Clock = func() time.Time { return time.Date(2026, 1, 1, 0, 0, 30, 0, time.UTC) }
	scope := bridgeAPIScope("sesn_bridge_mcp_tool", "thr_bridge_mcp_tool", "bind_bridge_mcp_tool", 1, "pod_uid_mcp_tool")
	toolUseEventID := writeDurableMCPToolUseForTest(t, store, scope)
	claim := &bridgev1.ClaimMcpToolResultRequest{
		Scope: scope, ToolUseEventId: toolUseEventID, ClaimId: "claim_mcp_tool",
	}
	claimed, err := store.ClaimMcpToolResult(context.Background(), claim)
	if err != nil || claimed.GetAcquired() == nil {
		t.Fatalf("ClaimMcpToolResult first = %#v/%v; want acquired", claimed, err)
	}
	if claimed.GetAcquired().GetMcpServerName() != "github" ||
		claimed.GetAcquired().GetToolName() != "create_issue" ||
		claimed.GetAcquired().GetInputJson() != `{"body":"Details","title":"Bug"}` {
		t.Fatalf("acquired executor payload = %#v; want durable Tool facts", claimed.GetAcquired())
	}
	var claimStatus string
	var claimOwner, claimExpires sql.NullString
	if err := admin.QueryRowContext(context.Background(),
		`SELECT mcp_claim_status, mcp_claim_id, mcp_claim_lease_expires_at
		   FROM session_runtime_tool_results
		  WHERE workspace_id = 'default' AND session_id = 'sesn_bridge_mcp_tool' AND tool_use_event_id = $1`,
		toolUseEventID,
	).Scan(&claimStatus, &claimOwner, &claimExpires); err != nil {
		t.Fatalf("read mcp claim: %v", err)
	}
	if claimStatus != "in_flight" || !claimOwner.Valid || claimOwner.String != claim.GetClaimId() ||
		!claimExpires.Valid || claimExpires.String != "2026-01-01T00:03:30Z" {
		t.Fatalf("initial MCP claim = status %q owner %+v expires %+v; want in_flight owned lease", claimStatus, claimOwner, claimExpires)
	}
	renewed, err := store.ClaimMcpToolResult(context.Background(), claim)
	if err != nil || renewed.GetAcquired() == nil {
		t.Fatalf("same-claim renewal = %#v/%v; want acquired", renewed, err)
	}
	inFlight, err := store.ClaimMcpToolResult(context.Background(), &bridgev1.ClaimMcpToolResultRequest{
		Scope: scope, ToolUseEventId: toolUseEventID, ClaimId: "claim_mcp_tool_other",
	})
	if err != nil || inFlight.GetInFlight() == nil {
		t.Fatalf("different active claim = %#v/%v; want in-flight", inFlight, err)
	}

	const resultJSON = `{"response":{"status":1,"result_text":"created","attachments":[]},"content_items":1,"refresh_triggered":false}`
	commitRequest := &bridgev1.CommitMcpToolResultRequest{
		Scope: scope, ToolUseEventId: toolUseEventID, ClaimId: claim.GetClaimId(), ResultJson: resultJSON,
	}
	committed, err := store.CommitMcpToolResult(context.Background(), commitRequest)
	if err != nil || committed.GetCommitted() == nil {
		t.Fatalf("CommitMcpToolResult = %#v/%v; want committed", committed, err)
	}
	if committed.GetCommitted().GetAttachmentRef() != "" {
		t.Fatalf("committed attachment ref = %q; want no generated fact", committed.GetCommitted().GetAttachmentRef())
	}
	duplicateCommit, err := store.CommitMcpToolResult(context.Background(), commitRequest)
	if err != nil || duplicateCommit.GetDuplicate() == nil {
		t.Fatalf("CommitMcpToolResult replay = %#v/%v; want duplicate", duplicateCommit, err)
	}
	if duplicateCommit.GetDuplicate().GetAttachmentRef() != "" {
		t.Fatalf("duplicate attachment ref = %q; want no generated fact", duplicateCommit.GetDuplicate().GetAttachmentRef())
	}
	completed, err := store.ClaimMcpToolResult(context.Background(), &bridgev1.ClaimMcpToolResultRequest{
		Scope: scope, ToolUseEventId: toolUseEventID, ClaimId: "claim_mcp_tool_read",
	})
	if err != nil || completed.GetAlreadyCompleted().GetResultJson() != resultJSON {
		t.Fatalf("direct durable read = %#v/%v; want exact stored result", completed, err)
	}
	var toolKind, toolName, storedResult string
	if err := admin.QueryRowContext(context.Background(),
		`SELECT tool_kind, tool_name, result_json, mcp_claim_status, mcp_claim_id, mcp_claim_lease_expires_at
		   FROM session_runtime_tool_results
		  WHERE workspace_id = 'default' AND session_id = 'sesn_bridge_mcp_tool' AND tool_use_event_id = $1`,
		toolUseEventID,
	).Scan(&toolKind, &toolName, &storedResult, &claimStatus, &claimOwner, &claimExpires); err != nil {
		t.Fatalf("read mcp runtime tool result: %v", err)
	}
	if toolKind != "mcp" || toolName != "github/create_issue" || storedResult != resultJSON ||
		claimStatus != "stored" || claimOwner.Valid || claimExpires.Valid {
		t.Fatalf("stored MCP result = kind %q tool %q json %q claim %q owner %+v expires %+v", toolKind, toolName, storedResult, claimStatus, claimOwner, claimExpires)
	}

	if _, err := admin.ExecContext(context.Background(),
		`UPDATE session_runtime_bindings
		    SET binding_id = 'bind_bridge_mcp_tool_replacement',
		        binding_generation = 2,
		        agent_runtime_pod_uid = 'pod_uid_mcp_tool_replacement',
		        updated_at = '2026-01-01T00:00:31Z'
		  WHERE workspace_id = 'default' AND session_id = 'sesn_bridge_mcp_tool'`,
	); err != nil {
		t.Fatalf("replace MCP replay binding: %v", err)
	}
	staleReplay, err := store.ClaimMcpToolResult(context.Background(), claim)
	if err != nil || staleReplay.GetStale() == nil {
		t.Fatalf("ClaimMcpToolResult stale replay = %#v/%v; want stale", staleReplay, err)
	}
}

func TestPostgreSQLBridgeAPIStoreMCPToolResultIdentityIncludesThread(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID = "sesn_bridge_mcp_thread_identity"
		mainID    = "thr_bridge_mcp_thread_identity_main"
		childID   = "thr_bridge_mcp_thread_identity_child"
	)
	seedBridgeAPISession(t, admin, "default", sessionID, mainID)
	seedBridgeAPIChildThread(t, admin, "default", sessionID, mainID, childID)
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, "bind_bridge_mcp_thread_identity", 1, "pod_uid_mcp_thread_identity")

	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	store.Clock = func() time.Time { return time.Date(2026, 1, 1, 0, 0, 30, 0, time.UTC) }
	results := map[string]string{
		mainID:  `{"response":{"status":1,"result_text":"main","attachments":[]},"content_items":1,"refresh_triggered":false}`,
		childID: `{"response":{"status":1,"result_text":"child","attachments":[]},"content_items":1,"refresh_triggered":false}`,
	}
	for threadID, resultJSON := range results {
		scope := bridgeAPIScope(sessionID, threadID, "bind_bridge_mcp_thread_identity", 1, "pod_uid_mcp_thread_identity")
		toolUseEventID := writeDurableMCPToolUseForTest(t, store, scope)
		claim := &bridgev1.ClaimMcpToolResultRequest{
			Scope: scope, ToolUseEventId: toolUseEventID, ClaimId: "claim_" + threadID,
		}
		claimed, err := store.ClaimMcpToolResult(context.Background(), claim)
		if err != nil || claimed.GetAcquired() == nil {
			t.Fatalf("ClaimMcpToolResult thread %s = %#v/%v", threadID, claimed, err)
		}
		committed, err := store.CommitMcpToolResult(context.Background(), &bridgev1.CommitMcpToolResultRequest{
			Scope: scope, ToolUseEventId: toolUseEventID, ClaimId: claim.GetClaimId(), ResultJson: resultJSON,
		})
		if err != nil || committed.GetCommitted() == nil {
			t.Fatalf("CommitMcpToolResult thread %s = %#v/%v", threadID, committed, err)
		}
		replay, err := store.ClaimMcpToolResult(context.Background(), &bridgev1.ClaimMcpToolResultRequest{
			Scope: scope, ToolUseEventId: toolUseEventID, ClaimId: "read_" + threadID,
		})
		if err != nil || replay.GetAlreadyCompleted().GetResultJson() != resultJSON {
			t.Fatalf("thread %s direct read = %#v/%v; want %q", threadID, replay, err, resultJSON)
		}
	}
	var rowCount int
	if err := admin.QueryRowContext(context.Background(),
		`SELECT count(*) FROM session_runtime_tool_results
		  WHERE workspace_id = 'default' AND session_id = $1 AND tool_kind = 'mcp'`,
		sessionID,
	).Scan(&rowCount); err != nil {
		t.Fatalf("count thread-scoped MCP results: %v", err)
	}
	if rowCount != 2 {
		t.Fatalf("thread-scoped MCP result rows = %d; want 2", rowCount)
	}
}

func TestPostgreSQLBridgeAPIStoreMCPToolResultCommitsInlineMediaAsRefsOnly(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	seedBridgeAPISession(t, admin, "default", "sesn_bridge_mcp_media", "thr_bridge_mcp_media")
	seedBridgeAPIRuntimeBinding(t, admin, "default", "sesn_bridge_mcp_media", "bind_bridge_mcp_media", 1, "pod_uid_mcp_media")

	blobStore := blob.NewFakeBlobStore()
	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	store.AttachmentBlobStore = blobStore
	store.Clock = func() time.Time { return time.Date(2026, 1, 1, 0, 0, 30, 0, time.UTC) }
	scope := bridgeAPIScope("sesn_bridge_mcp_media", "thr_bridge_mcp_media", "bind_bridge_mcp_media", 1, "pod_uid_mcp_media")
	seedBridgeAPIRequestStart(t, store, scope, "rwrite_mcp_media_start", "mreq_mcp_media", "agent_provider_request", 0)
	toolUse, err := store.WriteEvent(context.Background(), &bridgev1.WriteEventRequest{
		Scope:          scope,
		RuntimeWriteId: "rwrite_mcp_media_tool_use",
		ModelRequestId: "mreq_mcp_media",
		ToolDeclaration: bridgeMCPToolDeclarationForTest(
			"call_mcp_media", "get_file_contents", "github", `{"path":"plot.png"}`, "allow",
		),
	})
	if err != nil {
		t.Fatalf("WriteEvent MCP tool use: %v", err)
	}
	claim := &bridgev1.ClaimMcpToolResultRequest{
		Scope: scope, ToolUseEventId: toolUse.GetCommitted().GetEventId(), ClaimId: "claim_mcp_media",
	}
	claimed, err := store.ClaimMcpToolResult(context.Background(), claim)
	if err != nil || claimed.GetAcquired() == nil {
		t.Fatalf("ClaimMcpToolResult = %#v/%v", claimed, err)
	}
	pendingJSON := `{"response":{"status":1,"result_text":"[MCP attachment: plot.png]","attachments":[{"mime":"image/png","size_bytes":3,"suggested_filename":"plot.png"}]},"content_items":1,"refresh_triggered":false}`
	request := &bridgev1.CommitMcpToolResultRequest{
		Scope: scope, ToolUseEventId: claim.GetToolUseEventId(), ClaimId: claim.GetClaimId(), ResultJson: pendingJSON,
		InlineMedia: []*bridgev1.McpInlineMedia{{
			Data:              []byte{1, 2, 3},
			Mime:              "image/png",
			SuggestedFilename: "plot.png",
		}},
	}
	committed, err := store.CommitMcpToolResult(context.Background(), request)
	if err != nil {
		t.Fatalf("CommitMcpToolResult: %v", err)
	}
	if committed.GetCommitted() == nil {
		t.Fatalf("commit = %+v; want committed", committed)
	}
	if committed.GetCommitted().GetAttachmentRef() == "" {
		t.Fatal("committed attachment ref is empty; want Bridge-generated fact")
	}
	read, err := store.ClaimMcpToolResult(context.Background(), &bridgev1.ClaimMcpToolResultRequest{
		Scope: scope, ToolUseEventId: toolUse.GetCommitted().GetEventId(), ClaimId: "claim_mcp_media_read",
	})
	if err != nil || read.GetAlreadyCompleted() == nil {
		t.Fatalf("read committed MCP result = %#v/%v", read, err)
	}
	refsOnlyJSON := read.GetAlreadyCompleted().GetResultJson()
	if refsOnlyJSON == "" || strings.Contains(refsOnlyJSON, "data_base64") || strings.Contains(refsOnlyJSON, "AQID") {
		t.Fatalf("direct durable result = %q; want non-empty result without media bytes", refsOnlyJSON)
	}
	var result struct {
		Response struct {
			Attachments []struct {
				AttachmentRef     string `json:"attachment_ref"`
				Mime              string `json:"mime"`
				SizeBytes         int    `json:"size_bytes"`
				SuggestedFilename string `json:"suggested_filename"`
			} `json:"attachments"`
		} `json:"response"`
	}
	if err := json.Unmarshal([]byte(refsOnlyJSON), &result); err != nil {
		t.Fatalf("decode refs-only result: %v", err)
	}
	if len(result.Response.Attachments) != 1 {
		t.Fatalf("refs-only attachments = %+v; want one", result.Response.Attachments)
	}
	attachment := result.Response.Attachments[0]
	if !strings.HasPrefix(attachment.AttachmentRef, "att_") || attachment.Mime != "image/png" || attachment.SizeBytes != 3 || attachment.SuggestedFilename != "plot.png" {
		t.Fatalf("refs-only attachment = %+v; want completed ref metadata", attachment)
	}
	if committed.GetCommitted().GetAttachmentRef() != attachment.AttachmentRef {
		t.Fatalf("committed attachment ref = %q; want generated ref %q", committed.GetCommitted().GetAttachmentRef(), attachment.AttachmentRef)
	}

	var storedResult string
	if err := admin.QueryRowContext(context.Background(),
		`SELECT result_json FROM session_runtime_tool_results
		  WHERE workspace_id = 'default' AND session_id = 'sesn_bridge_mcp_media' AND tool_use_event_id = $1`,
		toolUse.GetCommitted().GetEventId()).Scan(&storedResult); err != nil {
		t.Fatalf("read stored MCP result: %v", err)
	}
	if storedResult != refsOnlyJSON {
		t.Fatalf("stored result = %q; want commit reply %q", storedResult, refsOnlyJSON)
	}
	var workspaceID, sessionID, threadID, sourceEventID, blobPointer, mime, metadataJSON, attachmentStatus string
	if err := admin.QueryRowContext(context.Background(),
		`SELECT workspace_id, session_id, session_thread_id, source_tool_use_event_id,
		        blob_pointer, mime, metadata_json, status
		   FROM session_transient_attachments WHERE attachment_ref = $1`, attachment.AttachmentRef).
		Scan(&workspaceID, &sessionID, &threadID, &sourceEventID, &blobPointer, &mime, &metadataJSON, &attachmentStatus); err != nil {
		t.Fatalf("read committed MCP attachment: %v", err)
	}
	if workspaceID != "default" || sessionID != "sesn_bridge_mcp_media" || threadID != "thr_bridge_mcp_media" || sourceEventID != toolUse.GetCommitted().GetEventId() || mime != "image/png" || attachmentStatus != "staged" {
		t.Fatalf("attachment tenancy/status = %q/%q/%q/%q %q %q; want scoped staged row", workspaceID, sessionID, threadID, sourceEventID, mime, attachmentStatus)
	}
	if !strings.Contains(metadataJSON, `"filename":"plot.png"`) {
		t.Fatalf("attachment metadata = %q; want suggested filename", metadataJSON)
	}
	if data, ok := blobStore.Bytes(blobPointer); !ok || !bytes.Equal(data, []byte{1, 2, 3}) {
		t.Fatalf("attachment blob = %v present=%v; want committed bytes", data, ok)
	}

	duplicate, err := store.CommitMcpToolResult(context.Background(), request)
	if err != nil {
		t.Fatalf("CommitMcpToolResult duplicate: %v", err)
	}
	if duplicate.GetDuplicate() == nil {
		t.Fatalf("duplicate commit = %+v; want duplicate", duplicate)
	}
	if duplicate.GetDuplicate().GetAttachmentRef() != attachment.AttachmentRef {
		t.Fatalf("duplicate attachment ref = %q; want committed ref %q", duplicate.GetDuplicate().GetAttachmentRef(), attachment.AttachmentRef)
	}
	if deletes := blobStore.Deletes(); len(deletes) != 0 {
		t.Fatalf("blob deletes after durable duplicate = %v; want none", deletes)
	}
	if data, ok := blobStore.Bytes(blobPointer); !ok || !bytes.Equal(data, []byte{1, 2, 3}) {
		t.Fatalf("attachment blob after durable duplicate = %v present=%v; want referenced bytes", data, ok)
	}
	var attachmentCount int
	if err := admin.QueryRowContext(context.Background(), `SELECT count(*) FROM session_transient_attachments WHERE source_tool_use_event_id = $1`, toolUse.GetCommitted().GetEventId()).Scan(&attachmentCount); err != nil {
		t.Fatalf("count MCP attachments: %v", err)
	}
	if attachmentCount != 1 {
		t.Fatalf("MCP attachment rows = %d; want one after replay", attachmentCount)
	}

	activeReplay, err := store.ClaimMcpToolResult(context.Background(), claim)
	if err != nil {
		t.Fatalf("ClaimMcpToolResult active attachment replay: %v", err)
	}
	if activeReplay.GetAlreadyCompleted().GetResultJson() != refsOnlyJSON {
		t.Fatalf("active attachment replay = %#v; want byte-identical %q", activeReplay, refsOnlyJSON)
	}

	resultWrite := bridgeToolSettlementRequestForTest(scope, bridgeCompletedToolSettlementForTest(toolUse.GetCommitted().GetEventId(), "[MCP attachment: plot.png]"))
	resultEvent, err := store.SettleToolResult(context.Background(), resultWrite)
	if err != nil {
		t.Fatalf("SettleToolResult MCP result: %v", err)
	}
	bridgeRequireToolSettlementOutcomeForTest(t, resultEvent, "committed")
	var resultStatus string
	if err := admin.QueryRowContext(context.Background(),
		`SELECT mcp_claim_status
		   FROM session_runtime_tool_results
		  WHERE workspace_id = 'default'
		    AND session_id = 'sesn_bridge_mcp_media'
		    AND tool_use_event_id = $1`,
		toolUse.GetCommitted().GetEventId(),
	).Scan(&resultStatus); err != nil {
		t.Fatalf("read consumed MCP result: %v", err)
	}
	if resultStatus != "consumed" {
		t.Fatalf("MCP result status = %q; want consumed", resultStatus)
	}
	if err := admin.QueryRowContext(context.Background(),
		`SELECT status FROM session_transient_attachments WHERE attachment_ref = $1`,
		attachment.AttachmentRef,
	).Scan(&attachmentStatus); err != nil {
		t.Fatalf("read activated MCP attachment: %v", err)
	}
	if attachmentStatus != "active" {
		t.Fatalf("MCP attachment status = %q; want active", attachmentStatus)
	}
	replayedResult, err := store.SettleToolResult(context.Background(), proto.Clone(resultWrite).(*bridgev1.SettleToolResultRequest))
	if err != nil {
		t.Fatalf("SettleToolResult MCP result replay: %v", err)
	}
	bridgeRequireToolSettlementOutcomeForTest(t, replayedResult, "duplicate")
	var resultEventID string
	var committedResultPayload string
	if err := admin.QueryRowContext(context.Background(),
		`SELECT event_id, payload_json FROM session_events
		  WHERE workspace_id='default' AND session_id='sesn_bridge_mcp_media'
		    AND type='agent.mcp_tool_result' AND payload_json::jsonb->>'mcp_tool_use_id'=$1`,
		toolUse.GetCommitted().GetEventId(),
	).Scan(&resultEventID, &committedResultPayload); err != nil {
		t.Fatalf("read committed MCP result before changed replay: %v", err)
	}
	changedResultWrite := bridgeToolSettlementRequestForTest(scope, bridgeCompletedToolSettlementForTest(toolUse.GetCommitted().GetEventId(), "changed after consumption"))
	if _, err := store.SettleToolResult(context.Background(), changedResultWrite); status.Code(err) != codes.AlreadyExists {
		t.Fatalf("SettleToolResult consumed MCP result with changed outcome err = %v; want AlreadyExists", err)
	}
	var payloadAfterChangedReplay string
	if err := admin.QueryRowContext(context.Background(),
		`SELECT payload_json FROM session_events WHERE workspace_id='default' AND event_id=$1`,
		resultEventID,
	).Scan(&payloadAfterChangedReplay); err != nil {
		t.Fatalf("read committed MCP result after changed replay: %v", err)
	}
	if payloadAfterChangedReplay != committedResultPayload {
		t.Fatalf("committed MCP result changed during conflicting replay = %q; want %q", payloadAfterChangedReplay, committedResultPayload)
	}

	for _, lifecycle := range []struct {
		name      string
		status    string
		expiresAt time.Time
	}{
		{name: "consumed", status: "consumed", expiresAt: store.Clock().Add(time.Hour)},
		{name: "expired", status: "active", expiresAt: store.Clock().Add(-time.Second)},
	} {
		t.Run(lifecycle.name, func(t *testing.T) {
			if _, err := admin.ExecContext(context.Background(),
				`UPDATE session_transient_attachments SET status = $2, expires_at = $3 WHERE attachment_ref = $1`,
				attachment.AttachmentRef, lifecycle.status, lifecycle.expiresAt); err != nil {
				t.Fatalf("set attachment %s: %v", lifecycle.name, err)
			}
			replay, err := store.ClaimMcpToolResult(context.Background(), claim)
			if err != nil {
				t.Fatalf("ClaimMcpToolResult %s replay: %v", lifecycle.name, err)
			}
			if replay.GetAlreadyCompleted().GetResultJson() != refsOnlyJSON {
				t.Fatalf("%s replay = %#v; want exact durable result %q", lifecycle.name, replay, refsOnlyJSON)
			}
		})
	}
	var durableResultAfterReplay string
	if err := admin.QueryRowContext(context.Background(),
		`SELECT result_json FROM session_runtime_tool_results
		  WHERE workspace_id = 'default' AND session_id = 'sesn_bridge_mcp_media' AND tool_use_event_id = $1`,
		toolUse.GetCommitted().GetEventId()).Scan(&durableResultAfterReplay); err != nil {
		t.Fatalf("read durable result after unavailable replay: %v", err)
	}
	if durableResultAfterReplay != storedResult {
		t.Fatalf("durable MCP result changed during replay = %q; want %q", durableResultAfterReplay, storedResult)
	}
}

type commitBarrierBlobStore struct {
	*blob.FakeBlobStore
	puts    chan string
	release chan struct{}
}

func (s *commitBarrierBlobStore) Put(ctx context.Context, key string, content io.Reader, size int64) error {
	if err := s.FakeBlobStore.Put(ctx, key, content, size); err != nil {
		return err
	}
	s.puts <- key
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-s.release:
		return nil
	}
}

func TestPostgreSQLMCPMediaCommitRaceCleansTheDefinitiveReplayBlob(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID = "sesn_mcp_media_commit_race"
		threadID  = "thr_mcp_media_commit_race"
		bindingID = "bind_mcp_media_commit_race"
		podUID    = "pod_mcp_media_commit_race"
	)
	seedBridgeAPISession(t, admin, "default", sessionID, threadID)
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, bindingID, 1, podUID)
	blobStore := &commitBarrierBlobStore{
		FakeBlobStore: blob.NewFakeBlobStore(),
		puts:          make(chan string, 2),
		release:       make(chan struct{}),
	}
	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	store.AttachmentBlobStore = blobStore
	scope := bridgeAPIScope(sessionID, threadID, bindingID, 1, podUID)
	toolUseEventID := writeDurableMCPToolUseForTest(t, store, scope)
	claimID := "claim_mcp_media_commit_race"
	claimed, err := store.ClaimMcpToolResult(context.Background(), &bridgev1.ClaimMcpToolResultRequest{
		Scope: scope, ToolUseEventId: toolUseEventID, ClaimId: claimID,
	})
	if err != nil || claimed.GetAcquired() == nil {
		t.Fatalf("claim MCP media execution = %#v/%v", claimed, err)
	}
	request := &bridgev1.CommitMcpToolResultRequest{
		Scope: scope, ToolUseEventId: toolUseEventID, ClaimId: claimID,
		ResultJson:  `{"response":{"status":1,"result_text":"[MCP attachment: plot.png]","attachments":[{"mime":"image/png","size_bytes":3,"suggested_filename":"plot.png"}]},"content_items":1,"refresh_triggered":false}`,
		InlineMedia: []*bridgev1.McpInlineMedia{{Data: []byte{1, 2, 3}, Mime: "image/png", SuggestedFilename: "plot.png"}},
	}
	type commitResult struct {
		response *bridgev1.CommitMcpToolResultResponse
		err      error
	}
	results := make(chan commitResult, 2)
	for range 2 {
		go func() {
			response, err := store.CommitMcpToolResult(context.Background(), proto.Clone(request).(*bridgev1.CommitMcpToolResultRequest))
			results <- commitResult{response: response, err: err}
		}()
	}
	putKeys := []string{<-blobStore.puts, <-blobStore.puts}
	close(blobStore.release)
	first, second := <-results, <-results
	if first.err != nil || second.err != nil {
		t.Fatalf("racing MCP media commits failed: %v / %v", first.err, second.err)
	}
	responses := []*bridgev1.CommitMcpToolResultResponse{first.response, second.response}
	var durableRef string
	var committed, duplicate int
	for _, response := range responses {
		switch {
		case response.GetCommitted() != nil:
			committed++
			durableRef = response.GetCommitted().GetAttachmentRef()
		case response.GetDuplicate() != nil:
			duplicate++
			if durableRef == "" {
				durableRef = response.GetDuplicate().GetAttachmentRef()
			}
		default:
			t.Fatalf("racing MCP media response = %#v", response)
		}
	}
	if committed != 1 || duplicate != 1 || durableRef == "" ||
		first.response.GetCommitted().GetAttachmentRef() != "" && first.response.GetCommitted().GetAttachmentRef() != durableRef ||
		second.response.GetCommitted().GetAttachmentRef() != "" && second.response.GetCommitted().GetAttachmentRef() != durableRef ||
		first.response.GetDuplicate().GetAttachmentRef() != "" && first.response.GetDuplicate().GetAttachmentRef() != durableRef ||
		second.response.GetDuplicate().GetAttachmentRef() != "" && second.response.GetDuplicate().GetAttachmentRef() != durableRef {
		t.Fatalf("racing MCP media outcomes = %#v / %#v; want one shared committed ref", first.response, second.response)
	}
	durablePointer := transientAttachmentBlobPointer(scope, durableRef)
	if !blobStore.Has(durablePointer) {
		t.Fatalf("durable MCP media blob %q is absent", durablePointer)
	}
	deletes := blobStore.Deletes()
	if len(deletes) != 1 || deletes[0] == durablePointer ||
		deletes[0] != putKeys[0] && deletes[0] != putKeys[1] {
		t.Fatalf("MCP media replay cleanup = puts %v deletes %v durable %q", putKeys, deletes, durablePointer)
	}
	if blobStore.Has(deletes[0]) {
		t.Fatalf("definitive replay blob %q was not removed", deletes[0])
	}
	var attachmentCount int
	if err := admin.QueryRowContext(context.Background(), `SELECT count(*) FROM session_transient_attachments
		WHERE workspace_id='default' AND session_id=$1 AND session_thread_id=$2 AND source_tool_use_event_id=$3`,
		sessionID, threadID, toolUseEventID).Scan(&attachmentCount); err != nil {
		t.Fatalf("count durable MCP media attachments: %v", err)
	}
	if attachmentCount != 1 {
		t.Fatalf("durable MCP media attachment rows = %d; want 1", attachmentCount)
	}
}

func TestPostgreSQLBridgeAPIStoreMCPToolResultPreservesBlobWhenCommitOutcomeIsUnknown(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	seedBridgeAPISession(t, admin, "default", "sesn_bridge_mcp_commit_unknown", "thr_bridge_mcp_commit_unknown")
	seedBridgeAPIRuntimeBinding(t, admin, "default", "sesn_bridge_mcp_commit_unknown", "bind_bridge_mcp_commit_unknown", 1, "pod_uid_mcp_commit_unknown")

	if _, err := admin.ExecContext(context.Background(), `CREATE TABLE mcp_commit_probe (
		attachment_ref TEXT PRIMARY KEY
	)`); err != nil {
		t.Fatalf("create deferred commit probe: %v", err)
	}
	if _, err := admin.ExecContext(context.Background(), `GRANT SELECT, REFERENCES ON mcp_commit_probe TO tetral_runtime_test`); err != nil {
		t.Fatalf("grant deferred commit probe: %v", err)
	}
	if _, err := admin.ExecContext(context.Background(), `ALTER TABLE session_transient_attachments
		ADD CONSTRAINT session_transient_attachments_commit_probe
		FOREIGN KEY (attachment_ref) REFERENCES mcp_commit_probe(attachment_ref)
		DEFERRABLE INITIALLY DEFERRED`); err != nil {
		t.Fatalf("install deferred commit failure: %v", err)
	}

	blobStore := blob.NewFakeBlobStore()
	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	store.AttachmentBlobStore = blobStore
	scope := bridgeAPIScope("sesn_bridge_mcp_commit_unknown", "thr_bridge_mcp_commit_unknown", "bind_bridge_mcp_commit_unknown", 1, "pod_uid_mcp_commit_unknown")
	toolUseEventID := writeDurableMCPToolUseForTest(t, store, scope)
	claimID := "claim_mcp_commit_unknown"
	claimed, err := store.ClaimMcpToolResult(context.Background(), &bridgev1.ClaimMcpToolResultRequest{
		Scope: scope, ToolUseEventId: toolUseEventID, ClaimId: claimID,
	})
	if err != nil || claimed.GetAcquired() == nil {
		t.Fatalf("ClaimMcpToolResult = %#v/%v; want acquired", claimed, err)
	}

	_, err = store.CommitMcpToolResult(context.Background(), &bridgev1.CommitMcpToolResultRequest{
		Scope: scope, ToolUseEventId: toolUseEventID, ClaimId: claimID,
		ResultJson: `{"response":{"status":1,"result_text":"[MCP attachment: plot.png]","attachments":[{"mime":"image/png","size_bytes":3,"suggested_filename":"plot.png"}]},"content_items":1,"refresh_triggered":false}`,
		InlineMedia: []*bridgev1.McpInlineMedia{{
			Data: []byte{1, 2, 3}, Mime: "image/png", SuggestedFilename: "plot.png",
		}},
	})
	if err == nil {
		t.Fatal("CommitMcpToolResult with deferred constraint = nil; want commit failure")
	}
	if blobStore.Len() != 1 {
		t.Fatalf("blob count after unknown commit outcome = %d; want conservative retention", blobStore.Len())
	}
	if deletes := blobStore.Deletes(); len(deletes) != 0 {
		t.Fatalf("blob deletes after unknown commit outcome = %v; want none", deletes)
	}
	var attachmentCount int
	if err := admin.QueryRowContext(context.Background(),
		`SELECT count(*) FROM session_transient_attachments WHERE source_tool_use_event_id = $1`,
		toolUseEventID,
	).Scan(&attachmentCount); err != nil {
		t.Fatalf("count rolled-back MCP attachments: %v", err)
	}
	if attachmentCount != 0 {
		t.Fatalf("attachment rows after forced rollback = %d; want zero", attachmentCount)
	}
}

func TestPostgreSQLBridgeAPIStoreMCPToolResultConcurrentClaimLease(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	seedBridgeAPISession(t, admin, "default", "sesn_bridge_mcp_claim_race", "thr_bridge_mcp_claim_race")
	seedBridgeAPIRuntimeBinding(t, admin, "default", "sesn_bridge_mcp_claim_race", "bind_bridge_mcp_claim_race", 1, "pod_uid_mcp_claim_race")

	base := time.Date(2026, 1, 1, 0, 0, 30, 0, time.UTC)
	newStore := func() *PostgreSQLBridgeAPIStore {
		store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
		store.Clock = func() time.Time { return base }
		return store
	}
	scope := bridgeAPIScope("sesn_bridge_mcp_claim_race", "thr_bridge_mcp_claim_race", "bind_bridge_mcp_claim_race", 1, "pod_uid_mcp_claim_race")
	toolUseEventID := writeDurableMCPToolUseForTest(t, newStore(), scope)
	claim := func(claimID string) *bridgev1.ClaimMcpToolResultRequest {
		return &bridgev1.ClaimMcpToolResultRequest{Scope: scope, ToolUseEventId: toolUseEventID, ClaimId: claimID}
	}

	type claimResult struct {
		response *bridgev1.ClaimMcpToolResultResponse
		err      error
	}
	start := make(chan struct{})
	results := make(chan claimResult, 2)
	var ready sync.WaitGroup
	ready.Add(2)
	for _, request := range []*bridgev1.ClaimMcpToolResultRequest{claim("claim_mcp_race_a"), claim("claim_mcp_race_b")} {
		go func(request *bridgev1.ClaimMcpToolResultRequest) {
			ready.Done()
			<-start
			response, err := newStore().ClaimMcpToolResult(context.Background(), request)
			results <- claimResult{response: response, err: err}
		}(request)
	}
	ready.Wait()
	close(start)

	var acquired, inFlight int
	for range 2 {
		result := <-results
		if result.err != nil {
			t.Fatalf("ClaimMcpToolResult concurrent err = %v", result.err)
		}
		switch {
		case result.response.GetAcquired() != nil:
			acquired++
		case result.response.GetInFlight() != nil:
			inFlight++
		default:
			t.Fatalf("concurrent claim = %+v; want acquired or in-flight", result.response)
		}
	}
	if acquired != 1 || inFlight != 1 {
		t.Fatalf("concurrent claim outcomes acquired=%d inFlight=%d; want 1/1", acquired, inFlight)
	}

	var rowCount int
	var claimStatus string
	if err := admin.QueryRowContext(context.Background(),
		`SELECT count(*), COALESCE(max(mcp_claim_status), '')
		   FROM session_runtime_tool_results
		  WHERE workspace_id = 'default' AND session_id = 'sesn_bridge_mcp_claim_race' AND tool_use_event_id = $1`,
		toolUseEventID,
	).Scan(&rowCount, &claimStatus); err != nil {
		t.Fatalf("read concurrent mcp claim row: %v", err)
	}
	if rowCount != 1 || claimStatus != "in_flight" {
		t.Fatalf("concurrent mcp claim rows = %d status=%q; want one in_flight reservation", rowCount, claimStatus)
	}
}

func TestPostgreSQLBridgeAPIStoreMCPClaimEnforcesDurablePermissionHandoff(t *testing.T) {
	for _, testCase := range []struct {
		name                string
		evaluatedPermission string
		approvalStatus      string
		approvalDecision    string
		wantCode            codes.Code
		wantAcquired        bool
	}{
		{name: "direct allow", evaluatedPermission: "allow", wantCode: codes.OK, wantAcquired: true},
		{name: "resolved ask allow", evaluatedPermission: "ask", approvalStatus: "resolving", approvalDecision: "allow", wantCode: codes.OK, wantAcquired: true},
		{name: "deny", evaluatedPermission: "deny", wantCode: codes.FailedPrecondition},
		{name: "ask missing approval", evaluatedPermission: "ask", wantCode: codes.FailedPrecondition},
		{name: "ask unresolved", evaluatedPermission: "ask", approvalStatus: "pending", wantCode: codes.FailedPrecondition},
		{name: "ask denied", evaluatedPermission: "ask", approvalStatus: "resolving", approvalDecision: "deny", wantCode: codes.FailedPrecondition},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
			suffix := strings.ReplaceAll(testCase.name, " ", "_")
			sessionID := "sesn_mcp_permission_" + suffix
			threadID := "thr_mcp_permission_" + suffix
			toolUseID := "evt_mcp_permission_" + suffix
			modelRequestID := "mreq_mcp_permission_" + suffix
			modelToolCallID := "call_mcp_permission_" + suffix
			seedBridgeAPISession(t, admin, "default", sessionID, threadID)
			seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, "bind_mcp_permission_"+suffix, 1, "pod_mcp_permission_"+suffix)
			seedBridgeAPIEvent(t, admin, "default", sessionID, threadID, toolUseID, 1, "agent.mcp_tool_use",
				`{"name":"github_search","mcp_server_name":"github","input":{"query":"tetral"},"evaluated_permission":"`+testCase.evaluatedPermission+`"}`)
			if _, err := admin.ExecContext(context.Background(),
				`UPDATE session_events SET model_request_id = $2, projection_json = $4 WHERE workspace_id = $1 AND event_id = $3`,
				"default", modelRequestID, toolUseID, `{"model_tool_call_id":"`+modelToolCallID+`"}`,
			); err != nil {
				t.Fatalf("stamp MCP permission model request: %v", err)
			}
			seedBridgeAPIDurableToolMessage(t, admin, "default", sessionID, threadID, modelRequestID, toolUseID, modelToolCallID, "github_search")
			if testCase.evaluatedPermission == "allow" {
				seedBridgeAPIAllowedToolRoute(t, admin, "default", sessionID, threadID, toolUseID)
			}
			if testCase.approvalStatus != "" {
				var approvalDecision any
				if testCase.approvalDecision != "" {
					approvalDecision = testCase.approvalDecision
				}
				if _, err := admin.ExecContext(context.Background(),
					`INSERT INTO session_pending_tool_uses (
						workspace_id, session_id, session_thread_id, tool_use_event_id, model_tool_call_id,
						tool_name, input_json, decision, status, created_at, updated_at
					) VALUES ('default', $1, $2, $3, $4, 'github_search', '{"query":"tetral"}', $5, $6,
						'2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z')`,
					sessionID, threadID, toolUseID, modelToolCallID, approvalDecision, testCase.approvalStatus,
				); err != nil {
					t.Fatalf("seed MCP permission approval: %v", err)
				}
			}

			store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
			claimed, err := store.ClaimMcpToolResult(context.Background(), &bridgev1.ClaimMcpToolResultRequest{
				Scope:          bridgeAPIScope(sessionID, threadID, "bind_mcp_permission_"+suffix, 1, "pod_mcp_permission_"+suffix),
				ToolUseEventId: toolUseID,
				ClaimId:        "claim_mcp_permission_" + suffix,
			})
			if status.Code(err) != testCase.wantCode {
				t.Fatalf("ClaimMcpToolResult error = %v; want %s", err, testCase.wantCode)
			}
			if (claimed != nil && claimed.GetAcquired() != nil) != testCase.wantAcquired {
				t.Fatalf("ClaimMcpToolResult = %#v; acquired=%t", claimed, testCase.wantAcquired)
			}
			var claimCount int
			if err := admin.QueryRowContext(context.Background(),
				`SELECT count(*) FROM session_runtime_tool_results WHERE workspace_id='default' AND session_id=$1 AND tool_use_event_id=$2`,
				sessionID, toolUseID,
			).Scan(&claimCount); err != nil {
				t.Fatalf("read MCP permission claim count: %v", err)
			}
			if claimCount != map[bool]int{true: 1, false: 0}[testCase.wantAcquired] {
				t.Fatalf("MCP permission claim rows = %d; acquired=%t", claimCount, testCase.wantAcquired)
			}
		})
	}
}

func TestPostgreSQLBridgeAPIStoreMCPToolResultClaimLease(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	seedBridgeAPISession(t, admin, "default", "sesn_bridge_mcp_lease", "thr_bridge_mcp_lease")
	seedBridgeAPIRuntimeBinding(t, admin, "default", "sesn_bridge_mcp_lease", "bind_bridge_mcp_lease", 1, "pod_uid_mcp_lease")

	base := time.Date(2026, 1, 1, 0, 0, 30, 0, time.UTC)
	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	store.Clock = func() time.Time { return base }
	scope := bridgeAPIScope("sesn_bridge_mcp_lease", "thr_bridge_mcp_lease", "bind_bridge_mcp_lease", 1, "pod_uid_mcp_lease")
	toolUseEventID := writeDurableMCPToolUseForTest(t, store, scope)
	firstClaim := &bridgev1.ClaimMcpToolResultRequest{
		Scope: scope, ToolUseEventId: toolUseEventID, ClaimId: "claim_mcp_lease_first",
	}
	first, err := store.ClaimMcpToolResult(context.Background(), firstClaim)
	if err != nil || first.GetAcquired() == nil {
		t.Fatalf("ClaimMcpToolResult first = %#v/%v; want acquired", first, err)
	}
	active, err := store.ClaimMcpToolResult(context.Background(), &bridgev1.ClaimMcpToolResultRequest{
		Scope: scope, ToolUseEventId: toolUseEventID, ClaimId: "claim_mcp_lease_retry",
	})
	if err != nil || active.GetInFlight() == nil {
		t.Fatalf("ClaimMcpToolResult active = %#v/%v; want in-flight", active, err)
	}

	store.Clock = func() time.Time { return base.Add(181 * time.Second) }
	retryClaim := &bridgev1.ClaimMcpToolResultRequest{
		Scope: scope, ToolUseEventId: toolUseEventID, ClaimId: "claim_mcp_lease_retry",
	}
	reclaimed, err := store.ClaimMcpToolResult(context.Background(), retryClaim)
	if err != nil || reclaimed.GetAcquired() == nil {
		t.Fatalf("ClaimMcpToolResult expired retry = %#v/%v; want acquired", reclaimed, err)
	}

	const resultJSON = `{"response":{"status":1,"result_text":"created","attachments":[]},"content_items":1,"refresh_triggered":false}`
	oldOwnerCommit, err := store.CommitMcpToolResult(context.Background(), &bridgev1.CommitMcpToolResultRequest{
		Scope: scope, ToolUseEventId: toolUseEventID, ClaimId: firstClaim.GetClaimId(), ResultJson: resultJSON,
	})
	if err != nil || oldOwnerCommit.GetStale() == nil {
		t.Fatalf("CommitMcpToolResult old owner = %#v/%v; want stale", oldOwnerCommit, err)
	}
	newOwnerCommit, err := store.CommitMcpToolResult(context.Background(), &bridgev1.CommitMcpToolResultRequest{
		Scope: scope, ToolUseEventId: toolUseEventID, ClaimId: retryClaim.GetClaimId(), ResultJson: resultJSON,
	})
	if err != nil || newOwnerCommit.GetCommitted() == nil {
		t.Fatalf("CommitMcpToolResult new owner = %#v/%v; want committed", newOwnerCommit, err)
	}
	replay, err := store.ClaimMcpToolResult(context.Background(), &bridgev1.ClaimMcpToolResultRequest{
		Scope: scope, ToolUseEventId: toolUseEventID, ClaimId: "claim_mcp_lease_read",
	})
	if err != nil || replay.GetAlreadyCompleted().GetResultJson() != resultJSON {
		t.Fatalf("direct read after lease commit = %#v/%v; want exact result", replay, err)
	}
}

func TestPostgreSQLBridgeAPIStoreMCPOperationsReturnTypedStaleCustody(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID = "sesn_mcp_typed_stale"
		threadID  = "thr_mcp_typed_stale"
		bindingID = "bind_mcp_typed_stale"
		podUID    = "pod_mcp_typed_stale"
	)
	seedBridgeAPISession(t, admin, "default", sessionID, threadID)
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, bindingID, 1, podUID)
	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	stale := bridgeAPIScope(sessionID, threadID, bindingID, 2, podUID)

	claimed, err := store.ClaimMcpToolResult(context.Background(), &bridgev1.ClaimMcpToolResultRequest{Scope: stale, ToolUseEventId: "evt_mcp", ClaimId: "claim_stale"})
	if err != nil || claimed.GetStale() == nil {
		t.Fatalf("ClaimMcpToolResult stale = %#v/%v", claimed, err)
	}
	committed, err := store.CommitMcpToolResult(context.Background(), &bridgev1.CommitMcpToolResultRequest{
		Scope: stale, ToolUseEventId: "evt_mcp", ClaimId: "claim_stale", ResultJson: `{"response":{"attachments":[]}}`,
	})
	if err != nil || committed.GetStale() == nil {
		t.Fatalf("CommitMcpToolResult stale = %#v/%v", committed, err)
	}
}

func TestPostgreSQLBridgeAPIStoreMCPClaimUsesDurableToolFactsAndFencesTakeover(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID = "sesn_mcp_durable_claim"
		threadID  = "thr_mcp_durable_claim"
		bindingID = "bind_mcp_durable_claim"
		podUID    = "pod_mcp_durable_claim"
	)
	seedBridgeAPISession(t, admin, "default", sessionID, threadID)
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, bindingID, 1, podUID)
	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	store.Clock = func() time.Time { return now }
	scope := bridgeAPIScope(sessionID, threadID, bindingID, 1, podUID)
	toolUseEventID := writeDurableMCPToolUseForTest(t, store, scope)

	firstClaim := &bridgev1.ClaimMcpToolResultRequest{
		Scope: scope, ToolUseEventId: toolUseEventID, ClaimId: "claim_mcp_first",
	}
	first, err := store.ClaimMcpToolResult(context.Background(), firstClaim)
	if err != nil {
		t.Fatalf("ClaimMcpToolResult first: %v", err)
	}
	if first.GetAcquired() == nil || first.GetAcquired().GetMcpServerName() != "github" ||
		first.GetAcquired().GetToolName() != "create_issue" ||
		first.GetAcquired().GetInputJson() != `{"body":"Details","title":"Bug"}` {
		t.Fatalf("first claim = %#v; want Bridge-derived executor payload", first)
	}
	sameOwner, err := store.ClaimMcpToolResult(context.Background(), firstClaim)
	if err != nil || sameOwner.GetAcquired() == nil {
		t.Fatalf("same-owner replay = %#v/%v; want acquired with renewed lease", sameOwner, err)
	}
	activeOther, err := store.ClaimMcpToolResult(context.Background(), &bridgev1.ClaimMcpToolResultRequest{
		Scope: scope, ToolUseEventId: toolUseEventID, ClaimId: "claim_mcp_other",
	})
	if err != nil || activeOther.GetInFlight() == nil {
		t.Fatalf("other active claim = %#v/%v; want in-flight", activeOther, err)
	}

	now = now.Add(mcpClaimLeaseTTL + time.Second)
	takeoverClaim := &bridgev1.ClaimMcpToolResultRequest{
		Scope: scope, ToolUseEventId: toolUseEventID, ClaimId: "claim_mcp_takeover",
	}
	takeover, err := store.ClaimMcpToolResult(context.Background(), takeoverClaim)
	if err != nil || takeover.GetAcquired() == nil {
		t.Fatalf("expired lease takeover = %#v/%v; want acquired", takeover, err)
	}

	const resultJSON = `{"response":{"status":1,"result_text":"created","attachments":[]},"content_items":1,"refresh_triggered":false}`
	staleCommit, err := store.CommitMcpToolResult(context.Background(), &bridgev1.CommitMcpToolResultRequest{
		Scope: scope, ToolUseEventId: toolUseEventID, ClaimId: firstClaim.GetClaimId(), ResultJson: resultJSON,
	})
	if err != nil || staleCommit.GetStale() == nil {
		t.Fatalf("displaced owner commit = %#v/%v; want stale", staleCommit, err)
	}
	commitRequest := &bridgev1.CommitMcpToolResultRequest{
		Scope: scope, ToolUseEventId: toolUseEventID, ClaimId: takeoverClaim.GetClaimId(), ResultJson: resultJSON,
	}
	committed, err := store.CommitMcpToolResult(context.Background(), commitRequest)
	if err != nil || committed.GetCommitted() == nil {
		t.Fatalf("active owner commit = %#v/%v; want committed", committed, err)
	}
	duplicate, err := store.CommitMcpToolResult(context.Background(), commitRequest)
	if err != nil || duplicate.GetDuplicate() == nil {
		t.Fatalf("commit replay = %#v/%v; want duplicate", duplicate, err)
	}
	completed, err := store.ClaimMcpToolResult(context.Background(), &bridgev1.ClaimMcpToolResultRequest{
		Scope: scope, ToolUseEventId: toolUseEventID, ClaimId: "claim_mcp_after_completion",
	})
	if err != nil || completed.GetAlreadyCompleted().GetResultJson() != resultJSON {
		t.Fatalf("completed claim = %#v/%v; want exact direct durable result", completed, err)
	}

	var toolName, inputJSON, claimStatus string
	var owner, lease sql.NullString
	if err := admin.QueryRowContext(context.Background(), `SELECT tool_name, input_json, mcp_claim_status,
		mcp_claim_id, mcp_claim_lease_expires_at
		FROM session_runtime_tool_results
		WHERE workspace_id='default' AND session_id=$1 AND session_thread_id=$2 AND tool_use_event_id=$3`,
		sessionID, threadID, toolUseEventID,
	).Scan(&toolName, &inputJSON, &claimStatus, &owner, &lease); err != nil {
		t.Fatalf("read durable MCP result: %v", err)
	}
	if toolName != "github/create_issue" || inputJSON != `{"body":"Details","title":"Bug"}` ||
		claimStatus != mcpClaimStatusStored || owner.Valid || lease.Valid {
		t.Fatalf("durable MCP result = %q/%q/%q/%v/%v", toolName, inputJSON, claimStatus, owner, lease)
	}
}

func TestPostgreSQLBridgeAPIStoreMCPRelinquishReleasesOnlyExactClaimAndReplays(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID = "sesn_mcp_relinquish"
		threadID  = "thr_mcp_relinquish"
		bindingID = "bind_mcp_relinquish"
		podUID    = "pod_mcp_relinquish"
	)
	seedBridgeAPISession(t, admin, "default", sessionID, threadID)
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, bindingID, 1, podUID)
	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	scope := bridgeAPIScope(sessionID, threadID, bindingID, 1, podUID)
	toolUseEventID := writeDurableMCPToolUseForTest(t, store, scope)

	firstClaim := &bridgev1.ClaimMcpToolResultRequest{Scope: scope, ToolUseEventId: toolUseEventID, ClaimId: "claim_relinquish_first"}
	claimed, err := store.ClaimMcpToolResult(context.Background(), firstClaim)
	if err != nil || claimed.GetAcquired() == nil {
		t.Fatalf("first MCP claim = %#v/%v; want acquired", claimed, err)
	}
	relinquish := &bridgev1.RelinquishMcpToolResultRequest{Scope: scope, ToolUseEventId: toolUseEventID, ClaimId: firstClaim.GetClaimId()}
	released, err := store.RelinquishMcpToolResult(context.Background(), relinquish)
	if err != nil || released.GetRelinquished() == nil {
		t.Fatalf("first MCP relinquish = %#v/%v; want relinquished", released, err)
	}
	var remaining int
	if err := admin.QueryRowContext(context.Background(), `SELECT count(*) FROM session_runtime_tool_results
		WHERE workspace_id='default' AND session_id=$1 AND tool_use_event_id=$2`, sessionID, toolUseEventID).Scan(&remaining); err != nil {
		t.Fatalf("count relinquished MCP claims: %v", err)
	}
	if remaining != 0 {
		t.Fatalf("relinquished MCP claims = %d; want 0", remaining)
	}
	replayed, err := store.RelinquishMcpToolResult(context.Background(), proto.Clone(relinquish).(*bridgev1.RelinquishMcpToolResultRequest))
	if err != nil || replayed.GetDuplicate() == nil {
		t.Fatalf("MCP relinquish replay = %#v/%v; want duplicate", replayed, err)
	}

	secondClaim := &bridgev1.ClaimMcpToolResultRequest{Scope: scope, ToolUseEventId: toolUseEventID, ClaimId: "claim_relinquish_second"}
	claimed, err = store.ClaimMcpToolResult(context.Background(), secondClaim)
	if err != nil || claimed.GetAcquired() == nil {
		t.Fatalf("post-relinquish MCP claim = %#v/%v; want immediate acquired", claimed, err)
	}
	stale, err := store.RelinquishMcpToolResult(context.Background(), &bridgev1.RelinquishMcpToolResultRequest{
		Scope: scope, ToolUseEventId: toolUseEventID, ClaimId: "claim_relinquish_wrong",
	})
	if err != nil || stale.GetStale() == nil {
		t.Fatalf("wrong MCP relinquish = %#v/%v; want stale", stale, err)
	}
	var activeClaim string
	if err := admin.QueryRowContext(context.Background(), `SELECT mcp_claim_id FROM session_runtime_tool_results
		WHERE workspace_id='default' AND session_id=$1 AND tool_use_event_id=$2`, sessionID, toolUseEventID).Scan(&activeClaim); err != nil {
		t.Fatalf("read active MCP claim: %v", err)
	}
	if activeClaim != secondClaim.GetClaimId() {
		t.Fatalf("active MCP claim = %q; want %q", activeClaim, secondClaim.GetClaimId())
	}
	if _, err := admin.ExecContext(context.Background(), `DELETE FROM session_runtime_bindings
		WHERE workspace_id='default' AND session_id=$1`, sessionID); err != nil {
		t.Fatalf("supersede MCP relinquish Runtime scope: %v", err)
	}
	staleReplay, err := store.RelinquishMcpToolResult(context.Background(), relinquish)
	if err != nil || staleReplay.GetStale() == nil {
		t.Fatalf("stale-scope MCP relinquish replay = %#v/%v; want typed stale", staleReplay, err)
	}
	var relinquishOperations int
	if err := admin.QueryRowContext(context.Background(), `SELECT count(*) FROM session_bridge_operations
		WHERE workspace_id='default' AND session_id=$1 AND operation=$2`,
		sessionID, bridgeOpRelinquishMcpToolResult).Scan(&relinquishOperations); err != nil {
		t.Fatalf("count stale-scope MCP relinquish operations: %v", err)
	}
	if relinquishOperations != 1 {
		t.Fatalf("stale-scope MCP relinquish operations = %d; want 1", relinquishOperations)
	}
}

func TestPostgreSQLBridgeAPIStoreMCPClaimRejectsNonAuthoritativeTarget(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	seedBridgeAPISession(t, admin, "default", "sesn_mcp_target", "thr_mcp_target")
	seedBridgeAPIRuntimeBinding(t, admin, "default", "sesn_mcp_target", "bind_mcp_target", 1, "pod_mcp_target")
	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	scope := bridgeAPIScope("sesn_mcp_target", "thr_mcp_target", "bind_mcp_target", 1, "pod_mcp_target")
	if _, err := store.ClaimMcpToolResult(context.Background(), &bridgev1.ClaimMcpToolResultRequest{
		Scope: scope, ToolUseEventId: "evt_missing_mcp_target", ClaimId: "claim_missing_mcp_target",
	}); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("missing durable target error = %v; want FailedPrecondition", err)
	}
}

func writeDurableMCPToolUseForTest(t *testing.T, store *PostgreSQLBridgeAPIStore, scope *bridgev1.RuntimeScope) string {
	t.Helper()
	const modelRequestID = "mreq_mcp_durable_claim"
	seedBridgeAPIRequestStart(t, store, scope, "rwrite_mcp_durable_claim_start", modelRequestID, "agent_provider_request", 0)
	response, err := store.WriteEvent(context.Background(), &bridgev1.WriteEventRequest{
		Scope: scope, RuntimeWriteId: "rwrite_mcp_durable_claim_use", ModelRequestId: modelRequestID,
		ToolDeclaration: bridgeMCPToolDeclarationForTest(
			"call_mcp_durable_claim", "create_issue", "github", `{"title":"Bug","body":"Details"}`, "allow",
		),
	})
	if err != nil {
		t.Fatalf("write durable MCP Tool use: %v", err)
	}
	return response.GetCommitted().GetEventId()
}
