package agentruntimebridge

import (
	"context"
	"testing"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	bridgev1 "github.com/tetral-ai/tetral/services/bridge/gen/tetral/bridge/v1"
)

func TestPostgreSQLBridgeAPIStoreAuthorizesOnlyExecutableWebRoute(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		workspaceID    = "default"
		sessionID      = "sesn_bridge_web_authority"
		threadID       = "thr_bridge_web_authority"
		bindingID      = "bind_bridge_web_authority"
		toolUseEventID = "evt_bridge_web_authority"
	)
	seedBridgeAPISession(t, admin, workspaceID, sessionID, threadID)
	seedBridgeAPIRuntimeBinding(t, admin, workspaceID, sessionID, bindingID, 1, "pod_uid_web_authority")
	seedBridgeAPIEvent(t, admin, workspaceID, sessionID, threadID, toolUseEventID, 1, "agent.tool_use", `{"name":"web","input":{"search_query":[{"q":"tetral"}]},"evaluated_permission":"allow"}`)
	seedBridgeAPIToolDeclarationProjection(t, admin, workspaceID, sessionID, threadID, toolUseEventID, "call_bridge_web_authority", "web", `{"search_query":[{"q":"tetral"}]}`, "web_execute")
	seedBridgeAPIAllowedToolRoute(t, admin, workspaceID, sessionID, threadID, toolUseEventID)

	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	request := &bridgev1.AuthorizeWebToolExecutionRequest{
		Scope:          bridgeAPIScope(sessionID, threadID, bindingID, 1, "pod_uid_web_authority"),
		ToolUseEventId: toolUseEventID,
	}
	authorized, err := store.AuthorizeWebToolExecution(context.Background(), request)
	if err != nil || authorized.GetAuthorized() == nil {
		t.Fatalf("AuthorizeWebToolExecution executable = response %+v err %v; want authorized", authorized, err)
	}
	if _, err := admin.ExecContext(context.Background(), `UPDATE session_pending_tool_uses
		SET status='cancelled', decision='allow', updated_at=now()
		WHERE workspace_id=$1 AND session_id=$2 AND session_thread_id=$3 AND tool_use_event_id=$4`,
		workspaceID, sessionID, threadID, toolUseEventID); err != nil {
		t.Fatalf("cancel Web route: %v", err)
	}
	stale, err := store.AuthorizeWebToolExecution(context.Background(), request)
	if err != nil || stale.GetStale() == nil {
		t.Fatalf("AuthorizeWebToolExecution cancelled = response %+v err %v; want stale", stale, err)
	}
}
