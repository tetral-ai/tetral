package agentruntimebridge

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
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
	requestStart := seedBridgeAPIRequestStart(t, store, scope, "rwrite_mcp_tool_failure_start", modelRequest, "agent_provider_request", 0)
	toolUse, err := store.WriteEvent(context.Background(), &bridgev1.WriteEventRequest{
		Scope: scope, RuntimeWriteId: "rwrite_mcp_tool_failure_use", ModelRequestId: modelRequest,
		EventType:      "agent.mcp_tool_use",
		PayloadJson:    `{"type":"agent.mcp_tool_use","name":"github_search","mcp_server_name":"github","input":{"query":"tetral"},"evaluated_permission":"allow"}`,
		SessionVisible: true,
		Declaration: &bridgev1.WriteEventRequest_AssistantPartAppend{AssistantPartAppend: bridgeRuntimeOutputAppendForTest(
			t, scope, "rwrite_mcp_tool_failure_use", "agent.mcp_tool_use", "streaming",
			bridgeRuntimePartCreateForTest{kind: "tool", json: `{"type":"tool","toolCallId":"` + modelCall + `","toolName":"github_search","toolEvent":{"kind":"mcp","mcpServerName":"github"},"state":{"status":"running","input":{"value":{"query":"tetral"},"preview":"{\"query\":\"tetral\"}","truncated":false}}}`},
		)},
	})
	if err != nil {
		t.Fatalf("write MCP Tool Use: %v", err)
	}
	if _, err := store.WriteRequestEnd(context.Background(), &bridgev1.WriteRequestEndRequest{
		Scope: scope, RuntimeWriteId: "rwrite_mcp_tool_failure_end", ModelRequestId: modelRequest,
		ModelRequestStartEventId: requestStart.GetEventId(), RequestKind: "agent_provider_request",
		FinishReason: "tool-calls", UsageJson: `{}`,
	}); err != nil {
		t.Fatalf("write request end: %v", err)
	}
	claim := &bridgev1.ClaimMcpToolResultRequest{
		Scope: scope, ToolUseEventId: toolUse.GetEventId(), ClaimId: "claim_mcp_tool_failure",
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
		"modelRequestId": modelRequest, "modelToolCallId": modelCall, "toolUseEventId": toolUse.GetEventId(),
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
		Event         struct {
			Type       string `json:"type"`
			MCPToolUse string `json:"mcp_tool_use_id"`
			IsError    bool   `json:"is_error"`
			Content    []struct {
				Text string `json:"text"`
			} `json:"content"`
		} `json:"event"`
	}
	if err := json.Unmarshal(output, &composed); err != nil {
		t.Fatalf("decode MCP failure composition: %v: %s", err, output)
	}
	if composed.ConnectorCalls != 1 || composed.Result.Type != "error" || composed.Result.Error.Retryable ||
		composed.Result.Error.Message != "MCP tool execution is unavailable." || composed.Settlement.Type != "error" ||
		composed.Event.Type != "agent.mcp_tool_result" || composed.Event.MCPToolUse != toolUse.GetEventId() ||
		!composed.Event.IsError || len(composed.Event.Content) != 1 || composed.Event.Content[0].Text != "MCP tool execution is unavailable." {
		t.Fatalf("MCP failure composition = %+v; want one generic non-retryable Tool Result", composed)
	}
	payload, err := json.Marshal(composed.Event)
	if err != nil {
		t.Fatalf("encode MCP Tool Result event: %v", err)
	}
	committed, err := store.WriteEvent(context.Background(), &bridgev1.WriteEventRequest{
		Scope: scope, RuntimeWriteId: "rwrite_mcp_tool_failure_result", ModelRequestId: modelRequest,
		EventType: "agent.mcp_tool_result", PayloadJson: string(payload), SessionVisible: true,
		Declaration: &bridgev1.WriteEventRequest_ToolSettlement{ToolSettlement: &bridgev1.RuntimeToolSettlement{
			ToolUseEventId: toolUse.GetEventId(),
			Outcome:        &bridgev1.RuntimeToolSettlement_Error{Error: &bridgev1.RuntimeToolError{ErrorJson: string(composed.DeclaredError)}},
		}},
	})
	if err != nil {
		t.Fatalf("commit MCP Tool Result: %v", err)
	}
	if _, err := store.WriteEvent(context.Background(), &bridgev1.WriteEventRequest{
		Scope: scope, RuntimeWriteId: "rwrite_mcp_tool_failure_result_second", ModelRequestId: modelRequest,
		EventType: "agent.mcp_tool_result", PayloadJson: string(payload), SessionVisible: true,
		Declaration: &bridgev1.WriteEventRequest_ToolSettlement{ToolSettlement: bridgeErrorToolSettlementForTest(toolUse.GetEventId(), "MCP tool execution is unavailable.")},
	}); status.Code(err) != codes.AlreadyExists {
		t.Fatalf("second MCP Tool Result settlement = %v; want AlreadyExists", err)
	}
	var toolResults, sessionErrors int
	if err := admin.QueryRowContext(context.Background(), `SELECT
		(SELECT count(*) FROM session_events WHERE workspace_id='default' AND session_id=$1 AND type='agent.mcp_tool_result' AND payload_json::jsonb ->> 'mcp_tool_use_id'=$2),
		(SELECT count(*) FROM session_events WHERE workspace_id='default' AND session_id=$1 AND type='session.error')`, sessionID, toolUse.GetEventId()).Scan(&toolResults, &sessionErrors); err != nil {
		t.Fatalf("count MCP failure durable results: %v", err)
	}
	if toolResults != 1 || sessionErrors != 0 || committed.GetEventId() == "" {
		t.Fatalf("durable MCP failure = Tool Results %d Session errors %d event %q; want 1/0/nonempty", toolResults, sessionErrors, committed.GetEventId())
	}
	var durableMessage string
	if err := admin.QueryRowContext(context.Background(), `SELECT data_json FROM session_messages
		WHERE workspace_id='default' AND session_id=$1 AND session_thread_id=$2 AND model_request_id=$3`,
		sessionID, threadID, modelRequest).Scan(&durableMessage); err != nil {
		t.Fatalf("read MCP Tool projection: %v", err)
	}
	var mcpProjection struct {
		Parts []struct {
			State struct {
				Error struct {
					Type    string `json:"type"`
					Message string `json:"message"`
				} `json:"error"`
			} `json:"state"`
		} `json:"parts"`
	}
	if err := json.Unmarshal([]byte(durableMessage), &mcpProjection); err != nil || len(mcpProjection.Parts) != 1 ||
		mcpProjection.Parts[0].State.Error.Type == "" || mcpProjection.Parts[0].State.Error.Message != "MCP tool execution is unavailable." {
		t.Fatalf("MCP Tool error was not normalized: %s", durableMessage)
	}
	loaded, err := store.LoadContext(context.Background(), &bridgev1.LoadContextRequest{Scope: scope, RuntimeInputId: "rin_mcp_tool_failure_cold"})
	if err != nil {
		t.Fatalf("LoadContext after MCP Tool failure: %v", err)
	}
	checkpoint := runColdCheckpointComposition(t, loaded.GetContextJson())
	if checkpoint.Checkpoint.Request == nil || checkpoint.Checkpoint.Request.ModelRequestID != modelRequest ||
		checkpoint.Checkpoint.Request.ToolMemberCount != 1 || checkpoint.ReducerAction != "prepare_next_request" {
		t.Fatalf("MCP failure checkpoint = %+v; want one terminal member and next request", checkpoint)
	}
	for _, forbidden := range []string{"credential and provider response must not escape", "access_token", "refresh_token"} {
		if strings.Contains(string(output), forbidden) || strings.Contains(loaded.GetContextJson(), forbidden) {
			t.Fatalf("MCP failure surface exposed %q", forbidden)
		}
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
		EventType: "agent.mcp_tool_use", PayloadJson: `{"type":"agent.mcp_tool_use","name":"github_search","mcp_server_name":"github","input":{"query":"tetral"},"evaluated_permission":"allow"}`,
		Declaration: &bridgev1.WriteEventRequest_AssistantPartAppend{AssistantPartAppend: bridgeRuntimeOutputAppendForTest(
			t, scope, "rwrite_mcp_uncertain_use", "agent.mcp_tool_use", "streaming",
			bridgeRuntimePartCreateForTest{kind: "tool", json: `{"type":"tool","toolCallId":"call_mcp_uncertain","toolName":"github_search","toolEvent":{"kind":"mcp","mcpServerName":"github"},"state":{"status":"running","input":{"value":{"query":"tetral"},"preview":"{\"query\":\"tetral\"}","truncated":false}}}`},
		)},
	})
	if err != nil {
		t.Fatalf("write MCP Tool Use: %v", err)
	}
	message := "The MCP tool execution is still in progress. Check the external service before retrying."
	errorSettlement := bridgeErrorToolSettlementForTest(toolUse.GetEventId(), message)
	result, err := store.WriteEvent(context.Background(), &bridgev1.WriteEventRequest{
		Scope: scope, RuntimeWriteId: "rwrite_mcp_uncertain_result", ModelRequestId: modelRequest,
		EventType: "agent.mcp_tool_result", PayloadJson: `{"type":"agent.mcp_tool_result","mcp_tool_use_id":"` + toolUse.GetEventId() + `","content":[{"type":"text","text":` + fmt.Sprintf("%q", message) + `}],"is_error":true}`,
		Declaration: &bridgev1.WriteEventRequest_ToolSettlement{ToolSettlement: errorSettlement},
	})
	if err != nil || result.GetEventId() == "" {
		t.Fatalf("write MCP uncertainty = %#v/%v", result, err)
	}

	var durableResults int
	if err := admin.QueryRowContext(context.Background(), `SELECT count(*) FROM session_events
		WHERE workspace_id='default' AND session_id=$1 AND type='agent.mcp_tool_result'
		  AND payload_json::jsonb ->> 'mcp_tool_use_id'=$2`, sessionID, toolUse.GetEventId()).Scan(&durableResults); err != nil {
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
		`SELECT mcp_claim_status, mcp_claim_owner_request_id, mcp_claim_lease_expires_at
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
	duplicateCommit, err := store.CommitMcpToolResult(context.Background(), commitRequest)
	if err != nil || duplicateCommit.GetDuplicate() == nil {
		t.Fatalf("CommitMcpToolResult replay = %#v/%v; want duplicate", duplicateCommit, err)
	}
	completed, err := store.ClaimMcpToolResult(context.Background(), &bridgev1.ClaimMcpToolResultRequest{
		Scope: scope, ToolUseEventId: toolUseEventID, ClaimId: "claim_mcp_tool_read",
	})
	if err != nil || completed.GetAlreadyCompleted().GetResultJson() != resultJSON {
		t.Fatalf("direct durable read = %#v/%v; want exact stored result", completed, err)
	}
	var toolKind, toolName, storedResult string
	if err := admin.QueryRowContext(context.Background(),
		`SELECT tool_kind, tool_name, result_json, mcp_claim_status, mcp_claim_owner_request_id, mcp_claim_lease_expires_at
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
		EventType:      "agent.mcp_tool_use",
		PayloadJson:    `{"type":"agent.mcp_tool_use","name":"get_file_contents","input":{"path":"plot.png"},"mcp_server_name":"github","evaluated_permission":"allow"}`,
		Declaration: &bridgev1.WriteEventRequest_AssistantPartAppend{AssistantPartAppend: bridgeRuntimeOutputAppendForTest(
			t,
			scope,
			"rwrite_mcp_media_tool_use",
			"agent.mcp_tool_use",
			"streaming",
			bridgeRuntimePartCreateForTest{
				kind: "tool",
				json: `{"type":"tool","toolCallId":"call_mcp_media","toolName":"get_file_contents","toolEvent":{"kind":"mcp","mcpServerName":"github"},"state":{"status":"running","input":{"value":{"path":"plot.png"},"preview":"{\"path\":\"plot.png\"}","truncated":false}}}`,
			},
		)},
	})
	if err != nil {
		t.Fatalf("WriteEvent MCP tool use: %v", err)
	}
	claim := &bridgev1.ClaimMcpToolResultRequest{
		Scope: scope, ToolUseEventId: toolUse.GetEventId(), ClaimId: "claim_mcp_media",
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
	read, err := store.ClaimMcpToolResult(context.Background(), &bridgev1.ClaimMcpToolResultRequest{
		Scope: scope, ToolUseEventId: toolUse.GetEventId(), ClaimId: "claim_mcp_media_read",
	})
	if err != nil || read.GetAlreadyCompleted() == nil {
		t.Fatalf("read committed MCP result = %#v/%v", read, err)
	}
	refsOnlyJSON := read.GetAlreadyCompleted().GetResultJson()
	if refsOnlyJSON == "" || strings.Contains(refsOnlyJSON, "data_base64") || strings.Contains(refsOnlyJSON, "AQID") {
		t.Fatalf("refs-only result = %q; want non-empty result without media bytes", refsOnlyJSON)
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

	var storedResult string
	if err := admin.QueryRowContext(context.Background(),
		`SELECT result_json FROM session_runtime_tool_results
		  WHERE workspace_id = 'default' AND session_id = 'sesn_bridge_mcp_media' AND tool_use_event_id = $1`,
		toolUse.GetEventId()).Scan(&storedResult); err != nil {
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
	if workspaceID != "default" || sessionID != "sesn_bridge_mcp_media" || threadID != "thr_bridge_mcp_media" || sourceEventID != toolUse.GetEventId() || mime != "image/png" || attachmentStatus != "staged" {
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
	var attachmentCount int
	if err := admin.QueryRowContext(context.Background(), `SELECT count(*) FROM session_transient_attachments WHERE source_tool_use_event_id = $1`, toolUse.GetEventId()).Scan(&attachmentCount); err != nil {
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

	resultWrite := &bridgev1.WriteEventRequest{
		Scope: scope, RuntimeWriteId: "rwrite_mcp_media_tool_result", ModelRequestId: "mreq_mcp_media",
		EventType:   "agent.mcp_tool_result",
		PayloadJson: `{"type":"agent.mcp_tool_result","mcp_tool_use_id":"` + toolUse.GetEventId() + `","content":[{"type":"text","text":"[MCP attachment: plot.png]"}]}`,
		Declaration: &bridgev1.WriteEventRequest_ToolSettlement{ToolSettlement: bridgeCompletedToolSettlementForTest(toolUse.GetEventId(), "[MCP attachment: plot.png]")},
	}
	resultEvent, err := store.WriteEvent(context.Background(), resultWrite)
	if err != nil {
		t.Fatalf("WriteEvent MCP tool result: %v", err)
	}
	if resultEvent.GetAck().GetStatus() != bridgev1.BridgeWriteStatus_BRIDGE_WRITE_STATUS_COMMITTED {
		t.Fatalf("MCP result ack = %+v; want committed", resultEvent.GetAck())
	}
	var resultStatus string
	if err := admin.QueryRowContext(context.Background(),
		`SELECT mcp_claim_status
		   FROM session_runtime_tool_results
		  WHERE workspace_id = 'default'
		    AND session_id = 'sesn_bridge_mcp_media'
		    AND tool_use_event_id = $1`,
		toolUse.GetEventId(),
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
	replayedResult, err := store.WriteEvent(context.Background(), proto.Clone(resultWrite).(*bridgev1.WriteEventRequest))
	if err != nil {
		t.Fatalf("WriteEvent MCP tool result replay: %v", err)
	}
	if replayedResult.GetAck().GetStatus() != bridgev1.BridgeWriteStatus_BRIDGE_WRITE_STATUS_DUPLICATE ||
		replayedResult.GetEventId() != resultEvent.GetEventId() {
		t.Fatalf("MCP result replay = %+v; want duplicate original event", replayedResult)
	}
	var committedResultPayload string
	if err := admin.QueryRowContext(context.Background(),
		`SELECT payload_json FROM session_events WHERE workspace_id='default' AND event_id=$1`,
		resultEvent.GetEventId(),
	).Scan(&committedResultPayload); err != nil {
		t.Fatalf("read committed MCP result before changed replay: %v", err)
	}
	changedResultWrite := proto.Clone(resultWrite).(*bridgev1.WriteEventRequest)
	changedResultWrite.PayloadJson = `{"type":"agent.mcp_tool_result","mcp_tool_use_id":"` + toolUse.GetEventId() + `","content":[{"type":"text","text":"changed after consumption"}]}`
	if _, err := store.WriteEvent(context.Background(), changedResultWrite); status.Code(err) != codes.AlreadyExists {
		t.Fatalf("WriteEvent consumed MCP result replay with changed body err = %v; want AlreadyExists", err)
	}
	var payloadAfterChangedReplay string
	if err := admin.QueryRowContext(context.Background(),
		`SELECT payload_json FROM session_events WHERE workspace_id='default' AND event_id=$1`,
		resultEvent.GetEventId(),
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
		toolUse.GetEventId()).Scan(&durableResultAfterReplay); err != nil {
		t.Fatalf("read durable result after unavailable replay: %v", err)
	}
	if durableResultAfterReplay != storedResult {
		t.Fatalf("durable MCP result changed during replay = %q; want %q", durableResultAfterReplay, storedResult)
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
		mcp_claim_owner_request_id, mcp_claim_lease_expires_at
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
		EventType:      "agent.mcp_tool_use",
		PayloadJson:    `{"type":"agent.mcp_tool_use","name":"create_issue","mcp_server_name":"github","input":{"title":"Bug","body":"Details"},"evaluated_permission":"allow"}`,
		SessionVisible: true,
		Declaration: &bridgev1.WriteEventRequest_AssistantPartAppend{AssistantPartAppend: bridgeRuntimeOutputAppendForTest(
			t, scope, "rwrite_mcp_durable_claim_use", "agent.mcp_tool_use", "streaming",
			bridgeRuntimePartCreateForTest{kind: "tool", json: `{"type":"tool","toolCallId":"call_mcp_durable_claim","toolName":"create_issue","toolEvent":{"kind":"mcp","mcpServerName":"github"},"state":{"status":"running","input":{"value":{"title":"Bug","body":"Details"},"preview":"{}","truncated":false}}}`},
		)},
	})
	if err != nil {
		t.Fatalf("write durable MCP Tool use: %v", err)
	}
	return response.GetEventId()
}
