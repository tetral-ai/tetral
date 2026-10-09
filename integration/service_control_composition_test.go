package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	enginekubernetes "github.com/tetral-ai/tetral/internal/kubernetes"
	"github.com/tetral-ai/tetral/internal/queue"
	"github.com/tetral-ai/tetral/internal/runtimecontrol"
	sandboxdriver "github.com/tetral-ai/tetral/internal/sandbox/driver"
	"github.com/tetral-ai/tetral/internal/sessionevent"
	"github.com/tetral-ai/tetral/internal/storage/storagetest/sessionfixture"
	"github.com/tetral-ai/tetral/internal/workspace"
	agentruntimebridge "github.com/tetral-ai/tetral/services/bridge"
	bridgev1 "github.com/tetral-ai/tetral/services/bridge/gen/tetral/bridge/v1"
	jobrunner "github.com/tetral-ai/tetral/services/job-runner"
	tetralqueue "github.com/tetral-ai/tetral/services/queue"
	queuev1 "github.com/tetral-ai/tetral/services/queue/gen/tetral/queue/v1"
	tetralsandbox "github.com/tetral-ai/tetral/services/sandbox"
)

func separatedDeclareTool(t *testing.T, f *separatedOwners, scope *bridgev1.RuntimeScope, requestID, call, permission string) string {
	t.Helper()
	response, err := f.bridge.WriteEvent(f.ctx, &bridgev1.WriteEventRequest{Scope: scope, RuntimeWriteId: "rwrite_" + call, ModelRequestId: requestID, ToolDeclaration: sessionfixture.BridgeToolDeclarationForTest(call, "Read", `{"file_path":"README.md"}`, permission, "sandbox_execute")})
	if err != nil || response.GetCommitted() == nil {
		t.Fatalf("actual tool declaration=%v/%v", response, err)
	}
	return response.GetCommitted().GetEventId()
}

func separatedActiveFence(t *testing.T, f *separatedOwners, scope *bridgev1.RuntimeScope) {
	t.Helper()
	f.sql(t, `INSERT INTO session_runtime_status(workspace_id,session_id,status,binding_id,binding_generation,created_at,updated_at) VALUES('default',$1,'running',$2,$3,clock_timestamp(),clock_timestamp()) ON CONFLICT(workspace_id,session_id) DO UPDATE SET status='running',binding_id=$2,binding_generation=$3`, f.sessionID, scope.Binding.BindingId, scope.Binding.BindingGeneration)
}

// Observe real PostgreSQL lock queues, including the selected first backend.
// A process launch or an elapsed delay is not evidence of the Session winner.
func separatedBlockedPIDs(t *testing.T, f *separatedOwners, blocker, want int) []int {
	t.Helper()
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	deadline := time.NewTimer(10 * time.Second)
	defer deadline.Stop()
	for {
		rows, err := f.admin.QueryContext(f.ctx, `WITH RECURSIVE blocked(pid) AS (SELECT pid FROM pg_stat_activity WHERE datname=current_database() AND wait_event_type='Lock' AND $1=ANY(pg_blocking_pids(pid)) UNION SELECT a.pid FROM pg_stat_activity a JOIN blocked b ON b.pid=ANY(pg_blocking_pids(a.pid)) WHERE a.datname=current_database() AND a.wait_event_type='Lock') SELECT DISTINCT pid FROM blocked ORDER BY pid`, blocker)
		if err != nil {
			t.Fatal(err)
		}
		var pids []int
		for rows.Next() {
			var pid int
			if err := rows.Scan(&pid); err != nil {
				_ = rows.Close()
				t.Fatal(err)
			}
			pids = append(pids, pid)
		}
		err = rows.Err()
		_ = rows.Close()
		if err != nil {
			t.Fatal(err)
		}
		if len(pids) >= want {
			t.Logf("SQL Session barrier blocker_backend=%d blocked_backends=%v", blocker, pids)
			return pids
		}
		select {
		case <-ticker.C:
		case <-deadline.C:
			t.Fatalf("SQL Session barrier missing %d waiters within bounded observation", want)
		case <-f.ctx.Done():
			t.Fatalf("SQL Session barrier missing %d waiters: %v", want, f.ctx.Err())
		}
	}
}

func TestPostgreSQLSeparatedOwnersSettlementRecovery(t *testing.T) {
	for _, thread := range []string{"main", "child"} {
		for _, first := range []string{"settlement", "recovery"} {
			t.Run(thread+"_"+first+"_first", func(t *testing.T) {
				f := newSeparatedOwners(t, "settle_"+thread+"_"+first, false)
				scope := f.declare(t)
				if thread == "child" {
					sessionfixture.SeedBridgeAPIChildThread(t, f.admin, "default", f.sessionID, f.threadID, "thr_racing_child")
					scope.SessionThreadId = "thr_racing_child"
				}
				separatedActiveFence(t, f, scope)
				requestID := "mreq_race"
				seedBridgeAPIRequestStart(t, f.bridge, scope, "rwrite_race_start", requestID, runtimecontrol.RequestKindAgentProviderRequest, 0)
				toolID := separatedDeclareTool(t, f, scope, requestID, "call_race", "allow")
				separatedRouteFacts(t, f, scope, toolID, "before_race")
				if rows := f.count(t, `SELECT count(*) FROM session_runtime_tool_results WHERE session_id=$1 AND tool_use_event_id=$2`, f.sessionID, toolID); rows != 0 {
					t.Fatal("racing declaration has already accepted execution")
				}
				// A sibling has separate durable owners whose identities must survive.
				sessionfixture.SeedBridgeAPIChildThread(t, f.admin, "default", f.sessionID, f.threadID, "thr_preserved_sibling")
				sibling := proto.Clone(scope).(*bridgev1.RuntimeScope)
				sibling.SessionThreadId = "thr_preserved_sibling"
				seedBridgeAPIRequestStart(t, f.bridge, sibling, "rwrite_sibling_start", "mreq_sibling", runtimecontrol.RequestKindAgentProviderRequest, 0)
				acceptedID := separatedDeclareTool(t, f, sibling, "mreq_sibling", "call_accepted", "allow")
				accepted, err := f.bridge.AcceptSandboxExecution(f.ctx, &bridgev1.AcceptSandboxExecutionRequest{Scope: sibling, ToolUseEventId: acceptedID})
				if err != nil || accepted.GetCommitted() == nil {
					t.Fatalf("accepted execution=%v/%v", accepted, err)
				}
				approvalID := separatedDeclareTool(t, f, sibling, "mreq_sibling", "call_approval", "ask")
				repair, err := f.bridge.CommitInternalToolRepair(f.ctx, &bridgev1.CommitInternalToolRepairRequest{Scope: sibling, ModelRequestId: "mreq_sibling", ModelToolCallId: "call_internal_repair", ToolName: "unknown_tool", RepairKey: "internal_invalid_tool_07e8b9e605f4fd441939c9b6f8b56aa020566c68d2f7491d81d1af184b7037cf", CanonicalInputJson: `{"q":"x"}`, Error: &bridgev1.RuntimeToolError{ErrorJson: `{"type":"provider_tool_protocol_error","message":"invalid tool","retryable":false}`}})
				if err != nil || repair.GetCommitted() == nil {
					t.Fatalf("sibling internal repair=%v/%v", repair, err)
				}
				confirmationStarted, allowConfirmation := make(chan struct{}), make(chan struct{})
				var confirmationOnce sync.Once
				releaseConfirmation := func() { confirmationOnce.Do(func() { close(allowConfirmation) }) }
				t.Cleanup(releaseConfirmation)
				f.runner.TargetResolver = jobrunner.KubernetesRuntimeTargetResolver{GetPod: func(ctx context.Context, namespace, name string) (*enginekubernetes.PodObservation, error) {
					close(confirmationStarted)
					select {
					case <-allowConfirmation:
					case <-ctx.Done():
						return nil, ctx.Err()
					}
					return fixtureConfirmedMissingRuntimePod(ctx, namespace, name)
				}, LoadClient: fixtureRuntimeLoadClient(t), Snapshot: func() enginekubernetes.BindingVisibilitySnapshot {
					return enginekubernetes.NewBindingVisibilitySnapshotForTest(true, nil)
				}}
				type outcome struct {
					owner      string
					settlement *bridgev1.SettleToolResultResponse
					repaired   int
					err        error
				}
				results := make(chan outcome, 2)
				settle := func(ctx context.Context) error {
					response, err := f.bridge.SettleToolResult(ctx, sessionfixture.BridgeToolSettlementRequestForTest(scope, sessionfixture.BridgeCompletedToolSettlementForTest(toolID, "settled-before-loss")))
					results <- outcome{owner: "settlement", settlement: response, err: err}
					return nil
				}
				recover := func(ctx context.Context) error {
					count, err := f.runner.RepairLostRuntimeBindings(ctx, "default")
					results <- outcome{owner: "recovery", repaired: count, err: err}
					return nil
				}
				// Let the initial census transaction finish before arbitrating the
				// mutation transaction against settlement. Fresh Pod confirmation
				// now deliberately runs outside the session lock.
				separatedStartRun(f.ctx, t, recover)
				select {
				case <-confirmationStarted:
				case <-f.ctx.Done():
					t.Fatal(f.ctx.Err())
				}
				lock, err := f.admin.BeginTx(f.ctx, nil)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = lock.Rollback() })
				var blocker int
				if err := lock.QueryRowContext(f.ctx, `SELECT pg_backend_pid()`).Scan(&blocker); err != nil {
					t.Fatal(err)
				}
				if _, err := lock.ExecContext(f.ctx, `SELECT id FROM sessions WHERE workspace_id='default' AND id=$1 FOR UPDATE`, f.sessionID); err != nil {
					t.Fatal(err)
				}
				if first == "settlement" {
					separatedStartRun(f.ctx, t, settle)
				} else {
					releaseConfirmation()
				}
				firstPIDs := separatedBlockedPIDs(t, f, blocker, 1)
				if first == "settlement" {
					releaseConfirmation()
				} else {
					separatedStartRun(f.ctx, t, settle)
				}
				pids := separatedBlockedPIDs(t, f, blocker, 2)
				if len(firstPIDs) != 1 || len(pids) != 2 {
					t.Fatalf("unambiguous two-owner arbitration=%v/%v", firstPIDs, pids)
				}
				if err := lock.Commit(); err != nil {
					t.Fatal(err)
				}
				var settled *bridgev1.SettleToolResultResponse
				for i := 0; i < 2; i++ {
					value := separatedWait(f.ctx, t, results)
					if value.owner == "settlement" {
						settled = value.settlement
						if first == "settlement" && (value.err != nil || settled.GetCommitted() == nil) {
							t.Fatalf("settlement winner=%v/%v", settled, value.err)
						}
					} else if value.err != nil || value.repaired != 1 {
						t.Fatalf("recovery=%d/%v", value.repaired, value.err)
					}
				}
				var resultID, payload string
				separatedRouteFacts(t, f, scope, toolID, "after_race")
				if f.count(t, `SELECT count(*) FROM session_runtime_bindings WHERE session_id=$1`, f.sessionID) != 0 {
					t.Fatal("loss did not retire the binding before replacement")
				}
				factsBeforeFence := []int{f.count(t, `SELECT count(*) FROM session_events WHERE session_id=$1`, f.sessionID), f.count(t, `SELECT count(*) FROM session_bridge_operations WHERE session_id=$1`, f.sessionID), f.count(t, `SELECT count(*) FROM session_runtime_tool_results WHERE session_id=$1`, f.sessionID)}
				lateProbe, lateProbeErr := f.bridge.SettleToolResult(f.ctx, sessionfixture.BridgeToolSettlementRequestForTest(scope, sessionfixture.BridgeCompletedToolSettlementForTest(toolID, "late-probe")))
				t.Logf("old_scope_fence committed=%t stale=%t status=%s", lateProbe.GetCommitted() != nil, lateProbe.GetStale() != nil, status.Code(lateProbeErr))
				if lateProbeErr != nil || lateProbe.GetStale() == nil {
					t.Fatalf("old binding must produce typed stale, got %v/%v", lateProbe, lateProbeErr)
				}
				separatedEqual(t, []int{f.count(t, `SELECT count(*) FROM session_events WHERE session_id=$1`, f.sessionID), f.count(t, `SELECT count(*) FROM session_bridge_operations WHERE session_id=$1`, f.sessionID), f.count(t, `SELECT count(*) FROM session_runtime_tool_results WHERE session_id=$1`, f.sessionID)}, factsBeforeFence)
				if first == "recovery" {
					if f.count(t, `SELECT count(*) FROM session_events WHERE session_id=$1 AND type='agent.tool_result' AND tool_use_event_id=$2`, f.sessionID, toolID) != 0 || f.count(t, `SELECT count(*) FROM session_pending_tool_uses WHERE session_id=$1 AND tool_use_event_id=$2 AND status='resolving' AND decision='allow' AND model_tool_call_id='call_race'`, f.sessionID, toolID) != 1 {
						t.Fatal("recovery must preserve original unaccepted resolving route without synthetic Result")
					}
					replacement := declareReplacementScope(t, f.bridge.Client, f.runner.Client, scope)
					coldBefore := f.cold(t, replacement)
					rawBefore, _ := json.Marshal(coldBefore)
					if !strings.Contains(string(rawBefore), toolID) || !strings.Contains(string(rawBefore), "call_race") {
						t.Fatal("cold replacement lost original unaccepted call identity")
					}
					request := &bridgev1.AcceptSandboxExecutionRequest{Scope: replacement, ToolUseEventId: toolID}
					accepted, err := f.bridge.AcceptSandboxExecution(f.ctx, request)
					if err != nil || accepted.GetCommitted() == nil {
						t.Fatalf("replacement owning execution acceptance=%v/%v", accepted, err)
					}
					joined, err := f.bridge.AcceptSandboxExecution(f.ctx, request)
					if err != nil || joined.GetDuplicate() == nil {
						t.Fatalf("replacement execution acceptance replay=%v/%v", joined, err)
					}
					terminal := tetralsandbox.SandboxExecutionSettlement{Kind: tetralsandbox.SandboxExecutionCompleted, ResultJSON: `{"status":"success","result":{"content":"settled-after-replacement"}}`}
					providerCalls := separatedExecuteSandbox(t, f, replacement, toolID, terminal.ResultJSON)
					awaited, err := f.bridge.AwaitSandboxExecution(f.ctx, &bridgev1.AwaitSandboxExecutionRequest{Scope: replacement, ToolUseEventId: toolID})
					if err != nil || awaited.GetCompleted().GetResultJson() != terminal.ResultJSON {
						t.Fatalf("replacement durable result read=%v/%v", awaited, err)
					}
					settlement := sessionfixture.BridgeToolSettlementRequestForTest(replacement, sessionfixture.BridgeCompletedToolSettlementForTest(toolID, "settled-after-replacement"))
					completed, err := f.bridge.SettleToolResult(f.ctx, settlement)
					if err != nil || completed.GetCommitted() == nil {
						t.Fatalf("replacement literal terminal settlement=%v/%v", completed, err)
					}
					duplicated, err := f.bridge.SettleToolResult(f.ctx, settlement)
					if err != nil || duplicated.GetDuplicate() == nil {
						t.Fatalf("terminal result replay=%v/%v", duplicated, err)
					}
					if f.count(t, `SELECT count(*) FROM session_runtime_tool_results WHERE session_id=$1 AND tool_use_event_id=$2 AND execution_state='consumed' AND execution_attempt_generation=1`, f.sessionID, toolID) != 1 || f.count(t, `SELECT count(*) FROM queue_jobs WHERE kind='sandbox_tool_execute' AND payload_json::jsonb->>'session_id'=$1 AND payload_json::jsonb->>'tool_use_event_id'=$2`, f.sessionID, toolID) != 1 {
						t.Fatal("replacement replay duplicated accepted execution identity or Queue work")
					}
					t.Logf("replacement continuation: actual acceptance+duplicate, production Sandbox Runner+Coordinator terminal commit, Bridge await+settlement+duplicate; execution rows=1 work jobs=1 paired results=1 controlled provider invocations=%d", providerCalls)
				}
				var resultSequence, endSequence int64
				if err := f.admin.QueryRowContext(f.ctx, `SELECT event_id,payload_json,sequence FROM session_events WHERE session_id=$1 AND type='agent.tool_result' AND tool_use_event_id=$2`, f.sessionID, toolID).Scan(&resultID, &payload, &resultSequence); err != nil {
					t.Fatalf("one durable result for original Tool Use %s: %v", toolID, err)
				}
				if first == "settlement" {
					if !strings.Contains(payload, `"settled-before-loss"`) || strings.Contains(payload, "runtime_pod_lost") {
						t.Fatalf("settlement literal durable output=%s", payload)
					}
				} else {
					if !strings.Contains(payload, `"settled-after-replacement"`) || strings.Contains(payload, "runtime_pod_lost") {
						t.Fatalf("replacement literal durable result=%s", payload)
					}
					if settled.GetCommitted() != nil {
						t.Fatal("obsolete scope committed late success")
					}
				}
				if err := f.admin.QueryRowContext(f.ctx, `SELECT sequence FROM session_events WHERE session_id=$1 AND session_thread_id=$2 AND type='span.model_request_end' AND model_request_id=$3`, f.sessionID, scope.SessionThreadId, requestID).Scan(&endSequence); err != nil {
					t.Fatal(err)
				}
				if first == "settlement" && resultSequence >= endSequence {
					t.Fatal("persisted order does not show settlement before loss closeout")
				}
				if f.count(t, `SELECT count(*) FROM session_events WHERE session_id=$1 AND type='agent.tool_result' AND tool_use_event_id=$2`, f.sessionID, toolID) != 1 {
					t.Fatal("duplicate Tool Result")
				}
				if f.count(t, `SELECT count(*) FROM session_runtime_bindings WHERE session_id=$1 AND binding_id=$2 AND binding_generation=$3`, f.sessionID, scope.Binding.BindingId, scope.Binding.BindingGeneration) != 0 {
					t.Fatal("lost binding survived repair")
				}
				if first == "settlement" && f.count(t, `SELECT count(*) FROM session_runtime_bindings WHERE session_id=$1`, f.sessionID) != 0 {
					t.Fatal("settlement-first path created an unrequested replacement")
				}
				if f.count(t, `SELECT count(*) FROM session_runtime_tool_results WHERE session_id=$1 AND tool_use_event_id=$2 AND execution_state='pending' AND result_json IS NULL`, f.sessionID, acceptedID) != 1 || f.count(t, `SELECT count(*) FROM session_pending_tool_uses WHERE session_id=$1 AND tool_use_event_id=$2 AND status='pending' AND decision IS NULL`, f.sessionID, approvalID) != 1 {
					t.Fatal("loss changed sibling accepted execution or pending approval")
				}
				replacement := declareReplacementScope(t, f.bridge.Client, f.runner.Client, scope)
				if f.count(t, `SELECT count(*) FROM session_runtime_bindings WHERE session_id=$1`, f.sessionID) != 1 || f.count(t, `SELECT count(*) FROM session_runtime_bindings WHERE session_id=$1 AND binding_id=$2 AND binding_generation=$3 AND agent_runtime_pod_uid=$4`, f.sessionID, replacement.Binding.BindingId, replacement.Binding.BindingGeneration, replacement.Binding.TargetPodUid) != 1 {
					t.Fatal("current binding does not match actual replacement receipt identity")
				}
				cold := f.cold(t, replacement)
				if bytes, _ := json.Marshal(cold); !strings.Contains(string(bytes), toolID) || !strings.Contains(string(bytes), "runtime_pod_lost") {
					t.Fatal("replacement cold context lost original pair or loss boundary")
				}
				late, lateErr := f.bridge.SettleToolResult(f.ctx, sessionfixture.BridgeToolSettlementRequestForTest(scope, sessionfixture.BridgeCompletedToolSettlementForTest(toolID, "late-forbidden")))
				if lateErr != nil || late.GetStale() == nil {
					t.Fatalf("old binding typed stale=%v/%v", late, lateErr)
				}
				childColdScope := proto.Clone(replacement).(*bridgev1.RuntimeScope)
				childColdScope.SessionThreadId = sibling.SessionThreadId
				siblingCold := f.cold(t, childColdScope)
				siblingRaw, _ := json.Marshal(siblingCold)
				if !strings.Contains(string(siblingRaw), acceptedID) || !strings.Contains(string(siblingRaw), approvalID) || !strings.Contains(string(siblingRaw), repair.GetCommitted().GetRepairEventId()) {
					t.Fatalf("sibling cold identities not retained=%s", siblingRaw)
				}
				t.Logf("winner=%s thread=%s selected_backend=%d waiters=%v persisted_result=%s sequence=%d request_end_sequence=%d; sibling execution/approval/repair retained; actual Runner replacement cold load", first, thread, firstPIDs[0], pids, resultID, resultSequence, endSequence)
			})
		}
	}
}

func TestPostgreSQLSeparatedOwnersCustodyReplay(t *testing.T) {
	t.Run("bridge_commit_response_loss", func(t *testing.T) {
		f := newSeparatedOwners(t, "custody_rpc", false)
		scope := f.declare(t)
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		var once sync.Once
		var committedEvent string
		server := grpc.NewServer(grpc.UnaryInterceptor(func(ctx context.Context, request any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
			response, err := handler(ctx, request)
			if err != nil {
				return response, err
			}
			lost := false
			if strings.HasSuffix(info.FullMethod, "/WriteEvent") {
				once.Do(func() {
					lost = true
					committedEvent = response.(*bridgev1.WriteEventResponse).GetCommitted().GetEventId()
				})
			}
			if lost {
				return nil, status.Error(codes.Unavailable, "test dropped committed reply")
			}
			return response, nil
		}))
		agentruntimebridge.RegisterBridgeAPI(server, f.bridge)
		done := make(chan error, 1)
		t.Cleanup(func() {
			server.Stop()
			_ = listener.Close()
			select {
			case <-done:
			case <-time.After(10 * time.Second):
				t.Error("Bridge RPC server did not join")
			}
		})
		go func() { done <- server.Serve(listener) }()
		conn, err := grpc.NewClient(listener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = conn.Close() })
		client := bridgev1.NewAgentRuntimeBridgeServiceClient(conn)
		request := &bridgev1.WriteEventRequest{Scope: scope, RuntimeWriteId: "rwrite_rpc_lost", ModelRequestId: "mreq_rpc_lost", EventType: "span.model_request_start", PayloadJson: `{"type":"span.model_request_start","model_request_id":"mreq_rpc_lost"}`, ContextThroughMessageSequence: sessionfixture.BridgeAPIInt64(0), RequestKind: runtimecontrol.RequestKindAgentProviderRequest}
		if _, err := client.WriteEvent(f.ctx, request); status.Code(err) != codes.Unavailable {
			t.Fatalf("committed response loss=%v", err)
		}
		if committedEvent == "" || f.count(t, `SELECT count(*) FROM session_events WHERE event_id=$1`, committedEvent) != 1 {
			t.Fatal("lost reply did not follow actual committed receipt")
		}
		replayed, err := client.WriteEvent(f.ctx, request)
		if err != nil || replayed.GetDuplicate().GetEventId() != committedEvent {
			t.Fatalf("identical RPC replay=%v/%v original=%s", replayed, err, committedEvent)
		}
		divergent := proto.Clone(request).(*bridgev1.WriteEventRequest)
		divergent.PayloadJson = `{"type":"span.model_request_start","model_request_id":"mreq_rpc_lost","changed":true}`
		if _, err := client.WriteEvent(f.ctx, divergent); status.Code(err) != codes.AlreadyExists {
			t.Fatalf("divergent replay=%v; want conflict", err)
		}
		if f.count(t, `SELECT count(*) FROM session_events WHERE session_id=$1 AND runtime_write_id='rwrite_rpc_lost'`, f.sessionID) != 1 {
			t.Fatal("RPC replay duplicated durable effect")
		}
		t.Logf("actual Bridge RPC response dropped after commit; identical receipt=%s; divergent replay=AlreadyExists; durable events=1", committedEvent)
	})
	t.Run("input_finalization_ack_loss_and_reclaimed_lease", func(t *testing.T) {
		f := newSeparatedOwners(t, "custody_input", false)
		job, lease := f.input(t)
		sender := separatedSender()
		runner := f.worker(sender)
		lostAck := &separatedLostAckQueue{QueueClient: runner.Queue}
		runner.Queue = lostAck
		if err := runIssuedLeaseThroughRunner(f.ctx, runner, cleanupQueueJobProto(lease), runner.Config); err == nil || !lostAck.lost {
			t.Fatalf("actual committed Queue ACK response loss=%t/%v", lostAck.lost, err)
		}
		if len(sender.requests) != 1 || f.count(t, `SELECT count(*) FROM queue_jobs WHERE id=$1 AND status='acknowledged'`, job.JobID) != 1 {
			t.Fatal("actual Runner did not commit one input delivery")
		}
		scope := f.declare(t)
		committed, err := f.bridge.CommitInputs(f.ctx, &bridgev1.CommitInputsRequest{Scope: scope, RuntimeInputId: job.RuntimeInputID})
		if err != nil || committed.GetCommitted() == nil {
			t.Fatalf("actual accepted input commit=%v/%v", committed, err)
		}
		// The Queue receipt remains committed when its caller loses the ACK.
		// A stale lease must fail before transport; the store joins its receipt.
		if err := runIssuedLeaseThroughRunner(f.ctx, runner, cleanupQueueJobProto(lease), runner.Config); err == nil {
			t.Fatal("old acknowledged lease unexpectedly retained authority")
		}
		if _, found, err := f.runner.ReplayRuntimeDeliveryFinalization(f.ctx, job); err != nil || !found {
			t.Fatalf("durable finalization replay=%t/%v", found, err)
		}
		if len(sender.requests) != 1 {
			t.Fatalf("ACK loss replay sent input %d times", len(sender.requests))
		}
		before := separatedInputFacts(t, f, job)
		other := *f
		other.sessionID = "sesn_unrelated_custody"
		other.threadID = "thr_unrelated_custody"
		sessionfixture.SeedBridgeAPISession(t, f.admin, "default", other.sessionID, other.threadID)
		other.sql(t, `INSERT INTO session_runtime_status(workspace_id,session_id,status,created_at,updated_at) VALUES('default',$1,'idle',clock_timestamp(),clock_timestamp())`, other.sessionID)
		otherJob, otherLease := other.input(t)
		f.sql(t, `UPDATE queue_jobs SET leased_until=clock_timestamp()-interval '1 second' WHERE id=$1`, otherJob.JobID)
		if count, err := f.queue.ReclaimExpiredLeases(f.ctx, queue.ReclaimExpiredLeasesRequest{WorkspaceID: workspace.DefaultID, Kind: queue.KindRuntimeInput, Limit: 1}); err != nil || count != 1 {
			t.Fatalf("actual Queue reclaim=%d/%v", count, err)
		}
		reclaimed, err := f.queue.Lease(f.ctx, queue.LeaseRequest{WorkspaceID: workspace.DefaultID, Kinds: []string{queue.KindRuntimeInput}, LeaseOwner: "new-owner", MaxJobs: 1, LeaseDuration: time.Minute})
		if err != nil || len(reclaimed) != 1 {
			t.Fatalf("reclaimed exact input lease=%v/%v", reclaimed, err)
		}
		if reclaimed[0].LeaseToken == otherLease.LeaseToken {
			t.Fatal("reclaimed lease reused token")
		}
		otherBefore := separatedInputFacts(t, &other, otherJob)
		if err := runIssuedLeaseThroughRunner(f.ctx, runner, cleanupQueueJobProto(otherLease), runner.Config); err == nil {
			t.Fatal("obsolete lease unexpectedly obtained successor delivery authority")
		}
		var successorToken string
		if err := f.admin.QueryRowContext(f.ctx, `SELECT lease_token FROM queue_jobs WHERE id=$1 AND status='leased'`, otherJob.JobID).Scan(&successorToken); err != nil || successorToken != reclaimed[0].LeaseToken {
			t.Fatal("obsolete finalization changed new lease")
		}
		separatedEqual(t, separatedInputFacts(t, f, job), before)
		separatedEqual(t, separatedInputFacts(t, &other, otherJob), otherBefore)
		t.Log("Runner committed input once; Queue ACK-loss replay sends=1; reclaimed token differs; obsolete finalizer preserves successor lease and accepted event/input identities")
	})
	t.Run("interrupt_precedence_main_and_child", func(t *testing.T) {
		for _, child := range []bool{false, true} {
			for _, interruptFirst := range []bool{false, true} {
				t.Run(fmt.Sprintf("child_%t_interrupt_first_%t", child, interruptFirst), func(t *testing.T) {
					f := newSeparatedOwners(t, fmt.Sprintf("custody_interrupt_%t_%t", child, interruptFirst), false)
					scope := f.declare(t)
					if child {
						sessionfixture.SeedBridgeAPIChildThread(t, f.admin, "default", f.sessionID, f.threadID, "thr_interrupt_child")
						scope.SessionThreadId = "thr_interrupt_child"
					}
					seedBridgeAPIRequestStart(t, f.bridge, scope, "rwrite_interrupt_start", "mreq_interrupt", runtimecontrol.RequestKindAgentProviderRequest, 0)
					toolID := separatedDeclareTool(t, f, scope, "mreq_interrupt", "call_interrupt", "allow")
					settle := func() {
						response, err := f.bridge.SettleToolResult(f.ctx, sessionfixture.BridgeToolSettlementRequestForTest(scope, sessionfixture.BridgeCompletedToolSettlementForTest(toolID, "settled-before-interrupt")))
						if err != nil {
							t.Fatal(err)
						}
						if interruptFirst {
							if response.GetStale() == nil {
								t.Fatalf("post-interrupt settlement=%v", response)
							}
						} else if response.GetCommitted() == nil {
							t.Fatalf("pre-interrupt settlement=%v", response)
						}
					}
					interrupt := func() {
						service := sessionevent.NewService(sessionevent.NewPostgreSQLStore(dbconnect.NewClientForTesting(f.peerDB)))
						_, err := service.AppendClientEvents(f.ctx, workspace.DefaultID, f.sessionID, "interrupt-separation", sessionevent.AppendRequest{Events: []sessionevent.IncomingEvent{{Type: sessionevent.EventTypeUserInterrupt, SessionThreadID: scope.SessionThreadId}}})
						if err != nil {
							t.Fatal(err)
						}
					}
					if interruptFirst {
						interrupt()
						settle()
					} else {
						settle()
						interrupt()
					}
					if f.count(t, `SELECT count(*) FROM session_runtime_inbox WHERE session_id=$1 AND session_thread_id=$2 AND input_kind='interrupt_control' AND status='queued'`, f.sessionID, scope.SessionThreadId) != 1 {
						t.Fatal("interrupt did not create exact Thread custody")
					}
					if child && f.count(t, `SELECT count(*) FROM session_events WHERE session_id=$1 AND session_thread_id=$2 AND type='user.interrupt'`, f.sessionID, f.threadID) != 0 {
						t.Fatal("child interrupt leaked to main Thread")
					}
					t.Logf("interrupt_first=%t child=%t stale/committed precedence persisted; one queued interrupt owner and parent isolation", interruptFirst, child)
				})
			}
		}
	})
}

type separatedLostAckQueue struct {
	jobrunner.QueueClient
	lost bool
}

func (q *separatedLostAckQueue) Ack(ctx context.Context, request *queuev1.AckRequest) (*queuev1.TransitionResponse, error) {
	response, err := q.QueueClient.Ack(ctx, request)
	if err != nil {
		return response, err
	}
	if !q.lost && response.GetUpdated() {
		q.lost = true
		return nil, status.Error(codes.Unavailable, "test dropped durable Queue ACK reply")
	}
	return response, nil
}

func separatedInputFacts(t *testing.T, f *separatedOwners, job jobrunner.RuntimeJob) []any {
	t.Helper()
	var state, events string
	var from, to int64
	if err := f.admin.QueryRowContext(f.ctx, `SELECT status,event_ids_json,sequence_from,sequence_to FROM session_runtime_inbox WHERE runtime_input_id=$1`, job.RuntimeInputID).Scan(&state, &events, &from, &to); err != nil {
		t.Fatal(err)
	}
	return []any{job.RuntimeInputID, state, events, from, to}
}

func separatedRouteFacts(t *testing.T, f *separatedOwners, scope *bridgev1.RuntimeScope, toolID, phase string) {
	t.Helper()
	var route, decision string
	var executions, results, ends, bindings int
	if err := f.admin.QueryRowContext(f.ctx, `SELECT p.status,COALESCE(p.decision,''),(SELECT count(*) FROM session_runtime_tool_results WHERE session_id=$1 AND tool_use_event_id=$2),(SELECT count(*) FROM session_events WHERE session_id=$1 AND type='agent.tool_result' AND tool_use_event_id=$2),(SELECT count(*) FROM session_events WHERE session_id=$1 AND session_thread_id=$3 AND type='span.model_request_end' AND model_request_id='mreq_race'),(SELECT count(*) FROM session_runtime_bindings WHERE session_id=$1) FROM session_pending_tool_uses p WHERE p.session_id=$1 AND p.tool_use_event_id=$2`, f.sessionID, toolID, scope.SessionThreadId).Scan(&route, &decision, &executions, &results, &ends, &bindings); err != nil {
		t.Fatal(err)
	}
	t.Logf("phase=%s original_tool=%s route=%s decision=%s accepted_executions=%d paired_results=%d request_ends=%d live_bindings=%d", phase, toolID, route, decision, executions, results, ends, bindings)
}

type separatedSandboxProvider struct {
	*hotReceiptSandboxProvider
	calls  int
	toolID string
}

func (p *separatedSandboxProvider) ExecuteTool(ctx context.Context, request tetralsandbox.ToolExecutionRequest) tetralsandbox.ProviderOutcome[sandboxdriver.ToolExecution] {
	p.calls++
	p.toolID = request.Invocation.ToolUseEventID
	return p.hotReceiptSandboxProvider.ExecuteTool(ctx, request)
}

// The controlled provider is the only fake here. Placement, exact Queue
// custody, execution authorization and terminal settlement use their owners.
func separatedExecuteSandbox(t *testing.T, f *separatedOwners, scope *bridgev1.RuntimeScope, toolID, resultJSON string) int {
	t.Helper()
	sessionfixture.SeedReadySandboxForSharedToolExecution(t, f.admin, "default", f.sessionID)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer()
	queuev1.RegisterQueueServiceServer(server, tetralqueue.NewServer(f.queue, nil))
	done := make(chan error, 1)
	t.Cleanup(func() {
		server.Stop()
		_ = listener.Close()
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			t.Error("Sandbox Queue RPC did not join")
		}
	})
	go func() { done <- server.Serve(listener) }()
	connection, err := grpc.NewClient(listener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = connection.Close() })
	client := queuev1.NewQueueServiceClient(connection)
	leased, err := client.Lease(f.ctx, &queuev1.LeaseRequest{WorkspaceId: "default", Kinds: []string{queue.KindSandboxToolExecute}, LeaseOwner: "separated-sandbox", MaxJobs: 10, LeaseDurationMs: time.Minute.Milliseconds()})
	if err != nil {
		t.Fatal(err)
	}
	var target *queuev1.QueueJob
	var unrelated []*queuev1.QueueJob
	for _, job := range leased.GetJobs() {
		var payload map[string]any
		if err := json.Unmarshal([]byte(job.PayloadJson), &payload); err != nil {
			t.Fatal(err)
		}
		if payload["tool_use_event_id"] == toolID {
			target = job
		} else {
			unrelated = append(unrelated, job)
		}
	}
	if target == nil {
		t.Fatal("replacement execution Queue custody not found")
	}
	provider := &separatedSandboxProvider{hotReceiptSandboxProvider: &hotReceiptSandboxProvider{bridgeMemoryProjectionProvider: &bridgeMemoryProjectionProvider{}, resultJSON: resultJSON}}
	registry, err := tetralsandbox.NewProviderRegistry(map[string]tetralsandbox.ProviderAdapter{sandboxdriver.DaytonaProviderName: provider})
	if err != nil {
		t.Fatal(err)
	}
	queueClient := jobrunner.QueueClientFromGRPC(client)
	runner := &tetralsandbox.SandboxToolExecutionJobRunner{Queue: &issuedLeaseQueueFixture{QueueClient: queueClient, job: target}, Coordinator: tetralsandbox.NewPostgreSQLSandboxExecutionCoordinator(dbconnect.NewClientForTesting(f.peerDB), 30*time.Minute), Providers: registry, Media: backgroundNotificationMedia{}, Config: tetralsandbox.SandboxToolExecutionRunnerConfig{WorkspaceID: "default", LeaseOwner: "separated-sandbox", MaxJobs: 1, LeaseDuration: time.Minute, HeartbeatInterval: 10 * time.Second, PreparationTimeout: 45 * time.Second}}
	if active, err := runner.RunOnceWithActivity(f.ctx); err != nil || !active {
		t.Fatalf("actual Sandbox Runner terminal commit=%t/%v", active, err)
	}
	if provider.calls != 1 || provider.toolID != toolID {
		t.Fatalf("controlled Sandbox provider invocation=%d/%s; want1/%s", provider.calls, provider.toolID, toolID)
	}
	runner.Queue = &issuedLeaseQueueFixture{QueueClient: queueClient, job: target}
	if err := runner.RunOnce(f.ctx); err == nil {
		t.Fatal("old acknowledged Sandbox lease unexpectedly retained execution authority")
	}
	if provider.calls != 1 {
		t.Fatal("old execution replay invoked provider again")
	}
	for _, job := range unrelated {
		if f.count(t, `SELECT count(*) FROM queue_jobs WHERE id=$1 AND status='leased' AND lease_token=$2`, job.Id, job.LeaseToken) != 1 {
			t.Fatal("replacement execution released unrelated sibling Queue lease")
		}
	}
	if f.count(t, `SELECT count(*) FROM queue_jobs WHERE id=$1 AND status='acknowledged'`, target.Id) != 1 {
		t.Fatal("execution outcome did not settle exact Queue lease")
	}
	return provider.calls
}
