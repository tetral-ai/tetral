package agentruntimebridge

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
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

func TestPostgreSQLBridgeAPIStoreMcpManifestChangedEnqueuesRuntimeConfigUpdate(t *testing.T) {
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
	runtimeInputID := "runtime_config_update:mcp_manifest:sesn_bridge_mcp_manifest:github:1"
	if response.GetAck().GetStatus() != bridgev1.BridgeWriteStatus_BRIDGE_WRITE_STATUS_COMMITTED ||
		response.GetAck().GetRuntimeInputId() != runtimeInputID {
		t.Fatalf("McpManifestChanged ack = %#v; want committed runtime input id", response.GetAck())
	}
	assertRuntimeMCPManifestQueueJob(t, admin, "default", "sesn_bridge_mcp_manifest", "github", 1)

	replay, err := store.McpManifestChanged(context.Background(), request)
	if err != nil {
		t.Fatalf("McpManifestChanged replay: %v", err)
	}
	if replay.GetAck().GetStatus() != bridgev1.BridgeWriteStatus_BRIDGE_WRITE_STATUS_DUPLICATE ||
		len(lister.requests) != 1 {
		t.Fatalf("McpManifestChanged replay ack=%#v lister calls=%d; want duplicate without re-list", replay.GetAck(), len(lister.requests))
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
	claim := &bridgev1.ClaimMcpToolResultRequest{
		Scope:               scope,
		ToolUseEventId:      "evt_mcp_tool",
		NormalizedInputHash: "hash_mcp_tool",
		McpServerName:       "github",
		ToolName:            "create_issue",
		InputJson:           `{"title":"Bug","body":"Details"}`,
	}
	claimed, err := store.ClaimMcpToolResult(context.Background(), claim)
	if err != nil {
		t.Fatalf("ClaimMcpToolResult first: %v", err)
	}
	if claimed.GetAck().GetStatus() != bridgev1.BridgeWriteStatus_BRIDGE_WRITE_STATUS_COMMITTED || claimed.GetResultJson() != "" {
		t.Fatalf("first claim = %+v; want accepted empty result", claimed)
	}
	var claimStatus string
	var claimOwner sql.NullString
	var claimExpires sql.NullString
	if err := admin.QueryRowContext(context.Background(),
		`SELECT mcp_claim_status, mcp_claim_owner_request_id, mcp_claim_lease_expires_at
		   FROM session_runtime_tool_results
		  WHERE workspace_id = 'default' AND session_id = 'sesn_bridge_mcp_tool' AND tool_use_event_id = 'evt_mcp_tool'`).Scan(&claimStatus, &claimOwner, &claimExpires); err != nil {
		t.Fatalf("read mcp claim: %v", err)
	}
	if claimStatus != "in_flight" || !claimOwner.Valid || claimOwner.String != scope.GetRequestId() || !claimExpires.Valid || claimExpires.String != "2026-01-01T00:03:30Z" {
		t.Fatalf("initial MCP claim = status %q owner %+v expires %+v; want in_flight owned lease", claimStatus, claimOwner, claimExpires)
	}
	activeClaim, err := store.ClaimMcpToolResult(context.Background(), claim)
	if err != nil {
		t.Fatalf("ClaimMcpToolResult active duplicate: %v", err)
	}
	if activeClaim.GetAck().GetStatus() != bridgev1.BridgeWriteStatus_BRIDGE_WRITE_STATUS_REJECTED || activeClaim.GetAck().GetErrorCode() != "mcp_claim_in_flight" {
		t.Fatalf("active duplicate claim = %+v; want in-flight rejected ack", activeClaim)
	}
	reorderedClaim := proto.Clone(claim).(*bridgev1.ClaimMcpToolResultRequest)
	reorderedClaim.InputJson = `{"body":"Details","title":"Bug"}`
	activeReorderedClaim, err := store.ClaimMcpToolResult(context.Background(), reorderedClaim)
	if err != nil {
		t.Fatalf("ClaimMcpToolResult active duplicate with reordered raw JSON: %v", err)
	}
	if activeReorderedClaim.GetAck().GetStatus() != bridgev1.BridgeWriteStatus_BRIDGE_WRITE_STATUS_REJECTED || activeReorderedClaim.GetAck().GetErrorCode() != "mcp_claim_in_flight" {
		t.Fatalf("active reordered duplicate claim = %+v; want in-flight rejected ack", activeReorderedClaim)
	}

	resultJSON := `{"response":{"status":1,"result_text":"created","attachments":[]},"content_items":1,"refresh_triggered":false}`
	commitRequest := &bridgev1.CommitMcpToolResultRequest{
		Scope:               scope,
		ToolUseEventId:      claim.GetToolUseEventId(),
		NormalizedInputHash: claim.GetNormalizedInputHash(),
		McpServerName:       claim.GetMcpServerName(),
		ToolName:            claim.GetToolName(),
		InputJson:           claim.GetInputJson(),
		ResultJson:          resultJSON,
	}
	committed, err := store.CommitMcpToolResult(context.Background(), commitRequest)
	if err != nil {
		t.Fatalf("CommitMcpToolResult: %v", err)
	}
	if committed.GetAck().GetStatus() != bridgev1.BridgeWriteStatus_BRIDGE_WRITE_STATUS_COMMITTED || committed.GetRefsOnlyResultJson() != resultJSON {
		t.Fatalf("commit = %+v; want committed stored result", committed)
	}
	assertMCPMaterializationDeclaration(t, committed, commitRequest, scope)
	var toolKind string
	var toolName string
	var storedResult string
	if err := admin.QueryRowContext(context.Background(),
		`SELECT tool_kind, tool_name, result_json, mcp_claim_status, mcp_claim_owner_request_id, mcp_claim_lease_expires_at
		   FROM session_runtime_tool_results
		  WHERE workspace_id = 'default' AND session_id = 'sesn_bridge_mcp_tool' AND tool_use_event_id = 'evt_mcp_tool'`).Scan(&toolKind, &toolName, &storedResult, &claimStatus, &claimOwner, &claimExpires); err != nil {
		t.Fatalf("read mcp runtime tool result: %v", err)
	}
	if toolKind != "mcp" || toolName != "github/create_issue" || storedResult != resultJSON || claimStatus != "stored" || claimOwner.Valid || claimExpires.Valid {
		t.Fatalf("stored MCP result = kind %q tool %q json %q claim %q owner %+v expires %+v; want mcp github/create_issue stored result", toolKind, toolName, storedResult, claimStatus, claimOwner, claimExpires)
	}

	replayed, err := store.ClaimMcpToolResult(context.Background(), claim)
	if err != nil {
		t.Fatalf("ClaimMcpToolResult replay: %v", err)
	}
	if replayed.GetAck().GetStatus() != bridgev1.BridgeWriteStatus_BRIDGE_WRITE_STATUS_DUPLICATE || replayed.GetResultJson() != resultJSON {
		t.Fatalf("replay claim = %+v; want duplicate stored result", replayed)
	}
	if replayed.GetDeclaration() == nil ||
		len(replayed.GetDeclaration().GetReceipts()) != 1 ||
		!proto.Equal(replayed.GetDeclaration().GetReceipts()[0], committed.GetDeclaration().GetReceipts()[0]) {
		t.Fatalf("replay declaration = %+v; want durable commit receipt %+v", replayed.GetDeclaration(), committed.GetDeclaration())
	}
	reorderedReplay, err := store.ClaimMcpToolResult(context.Background(), reorderedClaim)
	if err != nil {
		t.Fatalf("ClaimMcpToolResult replay with reordered raw JSON: %v", err)
	}
	if reorderedReplay.GetAck().GetStatus() != bridgev1.BridgeWriteStatus_BRIDGE_WRITE_STATUS_DUPLICATE || reorderedReplay.GetResultJson() != resultJSON {
		t.Fatalf("reordered replay claim = %+v; want duplicate stored result", reorderedReplay)
	}
	duplicateRequest := &bridgev1.CommitMcpToolResultRequest{
		Scope:               scope,
		ToolUseEventId:      claim.GetToolUseEventId(),
		NormalizedInputHash: claim.GetNormalizedInputHash(),
		McpServerName:       claim.GetMcpServerName(),
		ToolName:            claim.GetToolName(),
		InputJson:           reorderedClaim.GetInputJson(),
		ResultJson:          resultJSON,
	}
	duplicateCommit, err := store.CommitMcpToolResult(context.Background(), duplicateRequest)
	if err != nil {
		t.Fatalf("CommitMcpToolResult duplicate: %v", err)
	}
	if duplicateCommit.GetAck().GetStatus() != bridgev1.BridgeWriteStatus_BRIDGE_WRITE_STATUS_DUPLICATE || duplicateCommit.GetRefsOnlyResultJson() != resultJSON {
		t.Fatalf("duplicate commit = %+v; want duplicate stored result", duplicateCommit)
	}
	assertMCPMaterializationDeclaration(t, duplicateCommit, duplicateRequest, scope)
	if !proto.Equal(committed.GetDeclaration(), duplicateCommit.GetDeclaration()) {
		t.Fatalf("duplicate declaration = %+v; want committed declaration %+v", duplicateCommit.GetDeclaration(), committed.GetDeclaration())
	}

	conflict := proto.Clone(claim).(*bridgev1.ClaimMcpToolResultRequest)
	conflict.NormalizedInputHash = "hash_mcp_other"
	if _, err := store.ClaimMcpToolResult(context.Background(), conflict); status.Code(err) != codes.AlreadyExists {
		t.Fatalf("conflicting ClaimMcpToolResult err = %v; want AlreadyExists", err)
	}
	if _, err := admin.ExecContext(context.Background(),
		`UPDATE session_runtime_bindings
		    SET binding_id = 'bind_bridge_mcp_tool_replacement',
		        binding_generation = 2,
		        agent_runtime_pod_uid = 'pod_uid_mcp_tool_replacement',
		        updated_at = '2026-01-01T00:00:31Z'
		  WHERE workspace_id = 'default'
		    AND session_id = 'sesn_bridge_mcp_tool'`,
	); err != nil {
		t.Fatalf("replace MCP replay binding: %v", err)
	}
	staleReplay, err := store.ClaimMcpToolResult(context.Background(), claim)
	if err != nil {
		t.Fatalf("ClaimMcpToolResult stale replay: %v", err)
	}
	if staleReplay.GetResultJson() != resultJSON ||
		staleReplay.GetDeclaration().GetApplicationDisposition() != bridgev1.ReceiptApplicationDisposition_RECEIPT_APPLICATION_DISPOSITION_STALE_CUSTODY ||
		staleReplay.GetDeclaration().GetObservedBindingId() != "bind_bridge_mcp_tool_replacement" ||
		staleReplay.GetDeclaration().GetObservedBindingGeneration() != 2 {
		t.Fatalf("stale replay = %+v; want exact result with replacement-binding observation", staleReplay)
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
		claim := &bridgev1.ClaimMcpToolResultRequest{
			Scope:               scope,
			ToolUseEventId:      "evt_shared_mcp_tool",
			NormalizedInputHash: "hash_shared_mcp_tool",
			McpServerName:       "github",
			ToolName:            "create_issue",
			InputJson:           `{"title":"shared"}`,
		}
		if _, err := store.ClaimMcpToolResult(context.Background(), claim); err != nil {
			t.Fatalf("ClaimMcpToolResult thread %s: %v", threadID, err)
		}
		if _, err := store.CommitMcpToolResult(context.Background(), &bridgev1.CommitMcpToolResultRequest{
			Scope:               scope,
			ToolUseEventId:      claim.GetToolUseEventId(),
			NormalizedInputHash: claim.GetNormalizedInputHash(),
			McpServerName:       claim.GetMcpServerName(),
			ToolName:            claim.GetToolName(),
			InputJson:           claim.GetInputJson(),
			ResultJson:          resultJSON,
		}); err != nil {
			t.Fatalf("CommitMcpToolResult thread %s: %v", threadID, err)
		}
		replay, err := store.ClaimMcpToolResult(context.Background(), claim)
		if err != nil {
			t.Fatalf("ClaimMcpToolResult replay thread %s: %v", threadID, err)
		}
		if replay.GetResultJson() != resultJSON {
			t.Fatalf("thread %s replay = %q; want %q", threadID, replay.GetResultJson(), resultJSON)
		}
	}
	var rowCount int
	if err := admin.QueryRowContext(context.Background(),
		`SELECT count(*)
		   FROM session_runtime_tool_results
		  WHERE workspace_id = 'default'
		    AND session_id = $1
		    AND tool_use_event_id = 'evt_shared_mcp_tool'`,
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
		Drafts: []*bridgev1.RuntimeMessageDraft{bridgeRuntimeOutputDraftForTest(
			t,
			scope,
			"rwrite_mcp_media_tool_use",
			"agent.mcp_tool_use",
			"streaming",
			bridgeRuntimePartDraftForTest{
				kind: "tool",
				json: `{"type":"tool","toolCallId":"call_mcp_media","toolName":"get_file_contents","toolEvent":{"kind":"mcp","mcpServerName":"github"},"state":{"status":"running","input":{"value":{"path":"plot.png"},"preview":"{\"path\":\"plot.png\"}","truncated":false}}}`,
			},
		)},
	})
	if err != nil {
		t.Fatalf("WriteEvent MCP tool use: %v", err)
	}
	claim := &bridgev1.ClaimMcpToolResultRequest{
		Scope:               scope,
		ToolUseEventId:      toolUse.GetEventId(),
		NormalizedInputHash: "hash_mcp_media",
		McpServerName:       "github",
		ToolName:            "get_file_contents",
		InputJson:           `{"path":"plot.png"}`,
	}
	if _, err := store.ClaimMcpToolResult(context.Background(), claim); err != nil {
		t.Fatalf("ClaimMcpToolResult: %v", err)
	}
	pendingJSON := `{"response":{"status":1,"result_text":"[MCP attachment: plot.png]","attachments":[{"mime":"image/png","size_bytes":3,"suggested_filename":"plot.png"}]},"content_items":1,"refresh_triggered":false}`
	request := &bridgev1.CommitMcpToolResultRequest{
		Scope:               scope,
		ToolUseEventId:      claim.GetToolUseEventId(),
		NormalizedInputHash: claim.GetNormalizedInputHash(),
		McpServerName:       claim.GetMcpServerName(),
		ToolName:            claim.GetToolName(),
		InputJson:           claim.GetInputJson(),
		ResultJson:          pendingJSON,
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
	if committed.GetAck().GetStatus() != bridgev1.BridgeWriteStatus_BRIDGE_WRITE_STATUS_COMMITTED {
		t.Fatalf("commit ack = %+v; want committed", committed.GetAck())
	}
	assertMCPMaterializationDeclaration(t, committed, request, scope)
	if committed.GetMaterializationHandle() != toolUse.GetEventId() {
		t.Fatalf("materialization handle = %q; want %q", committed.GetMaterializationHandle(), toolUse.GetEventId())
	}
	refsOnlyJSON := committed.GetRefsOnlyResultJson()
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
	if duplicate.GetAck().GetStatus() != bridgev1.BridgeWriteStatus_BRIDGE_WRITE_STATUS_DUPLICATE ||
		duplicate.GetRefsOnlyResultJson() != refsOnlyJSON ||
		duplicate.GetMaterializationHandle() != toolUse.GetEventId() {
		t.Fatalf("duplicate commit = %+v; want identical refs-only replay", duplicate)
	}
	assertMCPMaterializationDeclaration(t, duplicate, request, scope)
	if !proto.Equal(committed.GetDeclaration(), duplicate.GetDeclaration()) {
		t.Fatalf("duplicate declaration = %+v; want committed declaration %+v", duplicate.GetDeclaration(), committed.GetDeclaration())
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
	if activeReplay.GetResultJson() != refsOnlyJSON {
		t.Fatalf("active attachment replay = %q; want byte-identical %q", activeReplay.GetResultJson(), refsOnlyJSON)
	}
	if activeReplay.GetMaterializationHandle() != toolUse.GetEventId() {
		t.Fatalf("claim replay handle = %q; want %q", activeReplay.GetMaterializationHandle(), toolUse.GetEventId())
	}

	materializationHandle := toolUse.GetEventId()
	resultWrite := &bridgev1.WriteEventRequest{
		Scope:                    scope,
		RuntimeWriteId:           "rwrite_mcp_media_tool_result",
		ModelRequestId:           "mreq_mcp_media",
		EventType:                "agent.mcp_tool_result",
		McpMaterializationHandle: &materializationHandle,
		PayloadJson:              `{"type":"agent.mcp_tool_result","mcp_tool_use_id":"` + toolUse.GetEventId() + `","content":[{"type":"text","text":"[MCP attachment: plot.png]"}]}`,
		Drafts: []*bridgev1.RuntimeMessageDraft{bridgeRuntimeOutputDraftForTest(
			t,
			scope,
			"rwrite_mcp_media_tool_result",
			"agent.mcp_tool_result",
			"completed",
			bridgeRuntimePartDraftForTest{
				kind: "tool",
				json: `{"type":"tool","toolCallId":"call_mcp_media","toolName":"get_file_contents","toolUseEventId":"` + toolUse.GetEventId() + `","toolEvent":{"kind":"mcp","mcpServerName":"github"},"state":{"status":"completed","input":{"value":{"path":"plot.png"},"preview":"{\"path\":\"plot.png\"}","truncated":false},"output":{"text":"[MCP attachment: plot.png]","truncated":false}}}`,
			},
		)},
	}
	resultEvent, err := store.WriteEvent(context.Background(), resultWrite)
	if err != nil {
		t.Fatalf("WriteEvent MCP tool result: %v", err)
	}
	if resultEvent.GetAck().GetStatus() != bridgev1.BridgeWriteStatus_BRIDGE_WRITE_STATUS_COMMITTED {
		t.Fatalf("MCP result ack = %+v; want committed", resultEvent.GetAck())
	}
	var materializationStatus string
	if err := admin.QueryRowContext(context.Background(),
		`SELECT mcp_claim_status
		   FROM session_runtime_tool_results
		  WHERE workspace_id = 'default'
		    AND session_id = 'sesn_bridge_mcp_media'
		    AND tool_use_event_id = $1`,
		toolUse.GetEventId(),
	).Scan(&materializationStatus); err != nil {
		t.Fatalf("read consumed MCP materialization: %v", err)
	}
	if materializationStatus != "consumed" {
		t.Fatalf("MCP materialization status = %q; want consumed", materializationStatus)
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
			if replay.GetResultJson() != refsOnlyJSON {
				t.Fatalf("%s replay = %q; want exact durable result %q", lifecycle.name, replay.GetResultJson(), refsOnlyJSON)
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
	if _, err := admin.ExecContext(context.Background(),
		`UPDATE session_bridge_operations
		    SET receipt_json = replace(receipt_json, $1, 'att_wrong')
		  WHERE workspace_id = 'default'
		    AND session_id = 'sesn_bridge_mcp_media'
		    AND session_thread_id = 'thr_bridge_mcp_media'
		    AND operation = 'commit_mcp_tool_result'`,
		attachment.AttachmentRef,
	); err != nil {
		t.Fatalf("corrupt MCP attachment receipt: %v", err)
	}
	if _, err := store.ClaimMcpToolResult(context.Background(), claim); status.Code(err) != codes.FailedPrecondition ||
		status.Convert(err).Message() != "mcp materialization receipt is invalid" {
		t.Fatalf("claim with mismatched attachment receipt err = %v; want invalid receipt", err)
	}
	if _, err := admin.ExecContext(context.Background(),
		`DELETE FROM session_bridge_operations
		  WHERE workspace_id = 'default'
		    AND session_id = 'sesn_bridge_mcp_media'
		    AND session_thread_id = 'thr_bridge_mcp_media'
		    AND operation = 'commit_mcp_tool_result'`,
	); err != nil {
		t.Fatalf("delete MCP materialization receipt: %v", err)
	}
	if _, err := store.ClaimMcpToolResult(context.Background(), claim); status.Code(err) != codes.FailedPrecondition ||
		status.Convert(err).Message() != "mcp materialization receipt is missing" {
		t.Fatalf("claim without materialization receipt err = %v; want missing receipt", err)
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
	scopeA := bridgeAPIScope("sesn_bridge_mcp_claim_race", "thr_bridge_mcp_claim_race", "bind_bridge_mcp_claim_race", 1, "pod_uid_mcp_claim_race")
	scopeA.RequestId = "req_mcp_claim_race_a"
	scopeB := proto.Clone(scopeA).(*bridgev1.RuntimeScope)
	scopeB.RequestId = "req_mcp_claim_race_b"
	claim := func(scope *bridgev1.RuntimeScope) *bridgev1.ClaimMcpToolResultRequest {
		return &bridgev1.ClaimMcpToolResultRequest{
			Scope:               scope,
			ToolUseEventId:      "evt_mcp_claim_race",
			NormalizedInputHash: "hash_mcp_claim_race",
			McpServerName:       "github",
			ToolName:            "create_issue",
			InputJson:           `{"title":"Race"}`,
		}
	}

	type claimResult struct {
		response *bridgev1.ClaimMcpToolResultResponse
		err      error
	}
	start := make(chan struct{})
	results := make(chan claimResult, 2)
	var ready sync.WaitGroup
	ready.Add(2)
	for _, request := range []*bridgev1.ClaimMcpToolResultRequest{claim(scopeA), claim(scopeB)} {
		go func(request *bridgev1.ClaimMcpToolResultRequest) {
			ready.Done()
			<-start
			response, err := newStore().ClaimMcpToolResult(context.Background(), request)
			results <- claimResult{response: response, err: err}
		}(request)
	}
	ready.Wait()
	close(start)

	var committed int
	var inFlight int
	for range 2 {
		result := <-results
		if result.err != nil {
			t.Fatalf("ClaimMcpToolResult concurrent err = %v", result.err)
		}
		switch result.response.GetAck().GetStatus() {
		case bridgev1.BridgeWriteStatus_BRIDGE_WRITE_STATUS_COMMITTED:
			committed++
		case bridgev1.BridgeWriteStatus_BRIDGE_WRITE_STATUS_REJECTED:
			if result.response.GetAck().GetErrorCode() != "mcp_claim_in_flight" {
				t.Fatalf("rejected concurrent claim = %+v; want mcp_claim_in_flight", result.response)
			}
			inFlight++
		default:
			t.Fatalf("concurrent claim = %+v; want committed or in-flight rejected", result.response)
		}
	}
	if committed != 1 || inFlight != 1 {
		t.Fatalf("concurrent claim statuses committed=%d inFlight=%d; want 1/1", committed, inFlight)
	}

	var rowCount int
	var claimStatus string
	if err := admin.QueryRowContext(context.Background(),
		`SELECT count(*), COALESCE(max(mcp_claim_status), '')
		   FROM session_runtime_tool_results
		  WHERE workspace_id = 'default'
		    AND session_id = 'sesn_bridge_mcp_claim_race'
		    AND tool_use_event_id = 'evt_mcp_claim_race'`).Scan(&rowCount, &claimStatus); err != nil {
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
	claim := &bridgev1.ClaimMcpToolResultRequest{
		Scope:               scope,
		ToolUseEventId:      "evt_mcp_lease",
		NormalizedInputHash: "hash_mcp_lease",
		McpServerName:       "github",
		ToolName:            "create_issue",
		InputJson:           `{"title":"Race"}`,
	}
	first, err := store.ClaimMcpToolResult(context.Background(), claim)
	if err != nil {
		t.Fatalf("ClaimMcpToolResult first: %v", err)
	}
	if first.GetAck().GetStatus() != bridgev1.BridgeWriteStatus_BRIDGE_WRITE_STATUS_COMMITTED {
		t.Fatalf("first claim = %+v; want committed reservation", first)
	}
	active, err := store.ClaimMcpToolResult(context.Background(), claim)
	if err != nil {
		t.Fatalf("ClaimMcpToolResult active: %v", err)
	}
	if active.GetAck().GetStatus() != bridgev1.BridgeWriteStatus_BRIDGE_WRITE_STATUS_REJECTED || active.GetAck().GetErrorCode() != "mcp_claim_in_flight" {
		t.Fatalf("active claim = %+v; want in-flight rejected ack", active)
	}

	retryScope := proto.Clone(scope).(*bridgev1.RuntimeScope)
	retryScope.RequestId = "req_mcp_lease_retry"
	retryClaim := proto.Clone(claim).(*bridgev1.ClaimMcpToolResultRequest)
	retryClaim.Scope = retryScope
	store.Clock = func() time.Time { return base.Add(181 * time.Second) }
	reclaimed, err := store.ClaimMcpToolResult(context.Background(), retryClaim)
	if err != nil {
		t.Fatalf("ClaimMcpToolResult expired retry: %v", err)
	}
	if reclaimed.GetAck().GetStatus() != bridgev1.BridgeWriteStatus_BRIDGE_WRITE_STATUS_COMMITTED {
		t.Fatalf("expired retry claim = %+v; want committed renewed reservation", reclaimed)
	}

	resultJSON := `{"response":{"status":1,"result_text":"created","attachments":[]},"content_items":1,"refresh_triggered":false}`
	oldOwnerCommit, err := store.CommitMcpToolResult(context.Background(), &bridgev1.CommitMcpToolResultRequest{
		Scope:               scope,
		ToolUseEventId:      claim.GetToolUseEventId(),
		NormalizedInputHash: claim.GetNormalizedInputHash(),
		McpServerName:       claim.GetMcpServerName(),
		ToolName:            claim.GetToolName(),
		InputJson:           claim.GetInputJson(),
		ResultJson:          resultJSON,
	})
	if err != nil {
		t.Fatalf("CommitMcpToolResult old owner: %v", err)
	}
	if oldOwnerCommit.GetAck().GetStatus() != bridgev1.BridgeWriteStatus_BRIDGE_WRITE_STATUS_REJECTED || oldOwnerCommit.GetAck().GetErrorCode() != "mcp_claim_not_owned" {
		t.Fatalf("old owner commit = %+v; want stale owner rejected ack", oldOwnerCommit)
	}

	newOwnerCommit, err := store.CommitMcpToolResult(context.Background(), &bridgev1.CommitMcpToolResultRequest{
		Scope:               retryScope,
		ToolUseEventId:      retryClaim.GetToolUseEventId(),
		NormalizedInputHash: retryClaim.GetNormalizedInputHash(),
		McpServerName:       retryClaim.GetMcpServerName(),
		ToolName:            retryClaim.GetToolName(),
		InputJson:           retryClaim.GetInputJson(),
		ResultJson:          resultJSON,
	})
	if err != nil {
		t.Fatalf("CommitMcpToolResult new owner: %v", err)
	}
	if newOwnerCommit.GetAck().GetStatus() != bridgev1.BridgeWriteStatus_BRIDGE_WRITE_STATUS_COMMITTED || newOwnerCommit.GetRefsOnlyResultJson() != resultJSON {
		t.Fatalf("new owner commit = %+v; want committed stored result", newOwnerCommit)
	}
	replay, err := store.ClaimMcpToolResult(context.Background(), retryClaim)
	if err != nil {
		t.Fatalf("ClaimMcpToolResult replay after lease commit: %v", err)
	}
	if replay.GetAck().GetStatus() != bridgev1.BridgeWriteStatus_BRIDGE_WRITE_STATUS_DUPLICATE || replay.GetResultJson() != resultJSON {
		t.Fatalf("replay after lease commit = %+v; want duplicate stored result", replay)
	}
}

func assertMCPMaterializationDeclaration(
	t *testing.T,
	response *bridgev1.CommitMcpToolResultResponse,
	request *bridgev1.CommitMcpToolResultRequest,
	scope *bridgev1.RuntimeScope,
) {
	t.Helper()
	declaration := response.GetDeclaration()
	if declaration == nil {
		t.Fatal("MCP materialization declaration is nil")
	}
	if declaration.GetObservedBindingId() != scope.GetBinding().GetBindingId() ||
		declaration.GetObservedBindingGeneration() != scope.GetBinding().GetBindingGeneration() ||
		declaration.GetApplicationDisposition() != bridgev1.ReceiptApplicationDisposition_RECEIPT_APPLICATION_DISPOSITION_CURRENT_CUSTODY {
		t.Fatalf("MCP declaration observation = %+v; want current binding %q/%d", declaration, scope.GetBinding().GetBindingId(), scope.GetBinding().GetBindingGeneration())
	}
	if len(declaration.GetReceipts()) != 1 {
		t.Fatalf("MCP declaration receipts = %d; want one", len(declaration.GetReceipts()))
	}
	receipt := declaration.GetReceipts()[0]
	digest, err := mcpMaterializationDeclarationDigest(request)
	if err != nil {
		t.Fatalf("compute MCP declaration digest: %v", err)
	}
	if receipt.GetSessionThreadId() != scope.GetSessionThreadId() ||
		receipt.GetOperationKind() != bridgeOpCommitMcpToolResult ||
		receipt.GetSourceKind() != "mcp_tool_execution" ||
		receipt.GetSourceId() != mcpMaterializationSourceID(request) ||
		receipt.GetDeclarationDigest() != digest {
		t.Fatalf("MCP declaration receipt = %+v; want matching materialization identity", receipt)
	}
	if len(receipt.GetEvents()) != 0 ||
		len(receipt.GetMessages()) != 0 ||
		len(receipt.GetPendingAttachmentDeltaJson()) != len(request.GetInlineMedia()) ||
		len(receipt.GetPendingToolDeltaJson()) != 0 ||
		len(receipt.GetPrefixConsumptions()) != 0 ||
		receipt.GetRequestReschedule() != nil ||
		len(receipt.GetChildLifecycle()) != 0 ||
		receipt.GetIdleCloseout() != nil ||
		receipt.CompactedThroughMessageSequence != nil {
		t.Fatalf("MCP declaration receipt deltas = %+v; want attachment-only delta", receipt)
	}
	if !validMCPMaterializationReceipt(
		receipt,
		request,
		mcpMaterializationSourceID(request),
		digest,
		response.GetRefsOnlyResultJson(),
	) {
		t.Fatalf("MCP declaration receipt = %+v; want exact refs-only attachment delta", receipt)
	}
}
