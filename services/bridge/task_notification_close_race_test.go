package agentruntimebridge

import (
	"context"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/runtimecontrol"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	bridgev1 "github.com/tetral-ai/tetral/services/bridge/gen/tetral/bridge/v1"
)

func TestPostgreSQLChildControlAdmissionAndParentRequestEndSerializeBothOrders(t *testing.T) {
	for _, admissionFirst := range []bool{true, false} {
		name := "request_end_first"
		if admissionFirst {
			name = "child_control_first"
		}
		t.Run(name, func(t *testing.T) {
			runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			suffix := "end"
			if admissionFirst {
				suffix = "control"
			}
			sessionID := "sesn_child_end_" + suffix
			parentID := "thr_child_end_parent_" + suffix
			childID := "thr_child_end_child_" + suffix
			bindingID := "bind_child_end_" + suffix
			podUID := "pod_child_end_" + suffix
			modelRequestID := "mreq_child_end_" + suffix
			sourceID := "evt_child_end_tool_" + suffix
			seedBridgeAPISession(t, admin, "default", sessionID, parentID)
			seedBridgeAPIChildThread(t, admin, "default", sessionID, parentID, childID)
			seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, bindingID, 1, podUID)
			store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
			scope := bridgeAPIScope(sessionID, parentID, bindingID, 1, podUID)
			seedBridgeAPIRequestStart(t, store, scope, "rwrite_child_end_start_"+suffix, modelRequestID, runtimecontrol.RequestKindAgentProviderRequest, 0)
			seedBridgeAPIChildLifecycleToolSource(t, admin, sessionID, parentID, sourceID)

			admitRequest := &bridgev1.AdmitChildInterruptRequest{
				Scope: scope, SourceToolUseEventId: sourceID, TargetChildThreadId: childID,
				Action: bridgev1.ChildControlAction_CHILD_CONTROL_ACTION_CLOSE,
			}
			endRequest := &bridgev1.WriteRequestEndRequest{
				Scope: scope, RuntimeWriteId: "rwrite_child_end_finish_" + suffix, ModelRequestId: modelRequestID,
				FinishReason: "stop", UsageJson: `{}`,
				ProviderContextRetention: &bridgev1.ProviderContextRetention{Disposition: "completed"},
			}

			blocker, err := admin.BeginTx(ctx, nil)
			if err != nil {
				t.Fatalf("begin Session blocker: %v", err)
			}
			defer func() { _ = blocker.Rollback() }()
			var locked string
			if err := blocker.QueryRowContext(ctx, `SELECT id FROM sessions WHERE workspace_id='default' AND id=$1 FOR UPDATE`, sessionID).Scan(&locked); err != nil {
				t.Fatalf("lock Session: %v", err)
			}
			var blockerPID int
			if err := blocker.QueryRowContext(ctx, `SELECT pg_backend_pid()`).Scan(&blockerPID); err != nil {
				t.Fatalf("read Session blocker pid: %v", err)
			}
			type result struct {
				kind  string
				admit *bridgev1.AdmitChildInterruptResponse
				end   *bridgev1.WriteRequestEndResponse
				err   error
			}
			results := make(chan result, 2)
			startAdmission := func() {
				go func() {
					response, err := store.AdmitChildInterrupt(ctx, admitRequest)
					results <- result{kind: "admit", admit: response, err: err}
				}()
			}
			startEnd := func() {
				go func() {
					response, err := store.WriteRequestEnd(ctx, endRequest)
					results <- result{kind: "end", end: response, err: err}
				}()
			}
			if admissionFirst {
				startAdmission()
				waitForPostgreSQLLockWaiters(t, admin, blockerPID, 1)
				startEnd()
			} else {
				startEnd()
				waitForPostgreSQLLockWaiters(t, admin, blockerPID, 1)
				startAdmission()
			}
			waitForPostgreSQLLockWaiters(t, admin, blockerPID, 2)
			if err := blocker.Commit(); err != nil {
				t.Fatalf("release Session owner: %v", err)
			}
			var admission result
			for range 2 {
				outcome := <-results
				if outcome.kind == "end" {
					if outcome.err != nil || outcome.end.GetCommitted() == nil {
						t.Fatalf("parent Request End = %#v/%v; want independent committed closeout", outcome.end, outcome.err)
					}
				} else {
					admission = outcome
				}
			}
			if admission.err != nil || admission.admit.GetCommitted().GetControlOperationId() == "" {
				t.Fatalf("child control with parent closeout order %t = %#v/%v; want committed durable Tool custody",
					admissionFirst, admission.admit, admission.err)
			}
			var ends int
			if err := admin.QueryRowContext(ctx, `SELECT count(*) FROM session_events
				WHERE workspace_id='default' AND session_id=$1 AND session_thread_id=$2 AND type='span.model_request_end'`, sessionID, parentID).Scan(&ends); err != nil || ends != 1 {
				t.Fatalf("parent Request End rows = %d/%v; want one", ends, err)
			}
		})
	}
}
