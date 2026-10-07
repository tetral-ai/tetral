package agentruntimebridge

import (
	"context"
	"database/sql"
	"fmt"
	"net"
	"reflect"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"google.golang.org/genproto/googleapis/rpc/errdetails"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/internalgrpc/auth"
	"github.com/tetral-ai/tetral/internal/queue"
	"github.com/tetral-ai/tetral/internal/runtimecontrol"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	"github.com/tetral-ai/tetral/internal/storage/storagetest/sessionfixture"
	bridgev1 "github.com/tetral-ai/tetral/services/bridge/gen/tetral/bridge/v1"
)

type backgroundReceiptAdmission struct {
	scope                              *bridgev1.RuntimeScope
	receiptID, taskID, requestID, kind string
	totalJobs                          int
	call                               func(context.Context) error
}

// A caller deadline alone does not prove admission. Observe the first committed
// read boundary and exact Queue capability before deliberately expiring the real
// TCP caller; join its server owner before replacement or result publication.
func admitPendingBackgroundReceipt(t *testing.T, admin *sql.DB, tracer *bridgeExecutionQueryTracer, returned <-chan struct{}, admission backgroundReceiptAdmission) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	fired, release := tracer.armBarrier("", "/* runtime receipt scope validation */", 1)
	var releaseOnce sync.Once
	releaseBarrier := func() { releaseOnce.Do(func() { close(release) }) }
	defer releaseBarrier()
	done := make(chan error, 1)
	go func() { done <- admission.call(ctx) }()
	select {
	case <-fired:
	case err := <-done:
		t.Fatalf("background call returned before committed receipt boundary: %v", err)
	case <-ctx.Done():
		t.Fatal("caller expired before committed background receipt boundary")
	}
	var state, kind, requestID, taskID string
	if err := admin.QueryRow(`SELECT background_operation_state, background_operation_kind, background_request_id, background_task_id
	 FROM session_runtime_tool_results WHERE workspace_id=$1 AND session_id=$2 AND session_thread_id=$3 AND tool_use_event_id=$4`,
		admission.scope.WorkspaceId, admission.scope.SessionId, admission.scope.SessionThreadId, admission.receiptID,
	).Scan(&state, &kind, &requestID, &taskID); err != nil || state != "pending" || kind != admission.kind || requestID != admission.requestID || taskID != admission.taskID {
		t.Fatalf("committed background receipt=%s/%s/%s/%s/%v", state, kind, requestID, taskID, err)
	}
	var jobs, matching int
	if err := admin.QueryRow(`SELECT count(*), count(*) FILTER (WHERE workspace_id=$2 AND payload_json::jsonb->>'session_id'=$3
	 AND payload_json::jsonb->>'task_id'=$4 AND payload_json::jsonb->>'request_id'=$5)
	 FROM queue_jobs WHERE kind=$1`, queue.KindSandboxBackgroundCommand, admission.scope.WorkspaceId, admission.scope.SessionId, admission.taskID, admission.requestID,
	).Scan(&jobs, &matching); err != nil || jobs != admission.totalJobs || matching != 1 {
		t.Fatalf("accepted background Queue=%d matching=%d/%v; want %d/1", jobs, matching, err, admission.totalJobs)
	}
	before := receiptTenantSnapshot(t, admin)
	if err := ctx.Err(); err != nil {
		t.Fatalf("caller expired before independent committed receipt/Queue proof: %v", err)
	}
	<-ctx.Done()
	releaseBarrier()
	select {
	case err := <-done:
		if status.Code(err) != codes.DeadlineExceeded {
			t.Fatalf("actual accepted %s wait=%v", admission.kind, err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("expired background caller did not join")
	}
	select {
	case <-returned:
	case <-time.After(3 * time.Second):
		t.Fatal("expired background server handler did not join")
	}
	if after := receiptTenantSnapshot(t, admin); !reflect.DeepEqual(before, after) {
		t.Fatal("caller expiry mutated accepted background receipt/Sandbox/Queue custody")
	}
}

func TestPostgreSQLBackgroundReceiptBindingCut(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	scope, _ := seedAwaitExecutionNotificationFixture(t, store, admin, "review_background_cut")
	const task = "task_review_background_cut"
	const tool = "evt_review_background_cut"
	seedBridgeAPIEvent(t, admin, "default", scope.SessionId, scope.SessionThreadId, tool, sessionfixture.NextBridgeAPIEventSequenceForTest(t, admin, scope.SessionId, scope.SessionThreadId), "agent.tool_use", `{"name":"write_stdin","input":{"session_id":"task_review_background_cut","chars":"once"},"evaluated_permission":"allow"}`)
	seedBridgeAPIToolDeclarationProjection(t, admin, "default", scope.SessionId, scope.SessionThreadId, tool, "call_review_background_cut", "write_stdin", `{"session_id":"task_review_background_cut","chars":"once"}`, "background_command")
	sessionfixture.SeedBridgeAPIAllowedToolRoute(t, admin, "default", scope.SessionId, scope.SessionThreadId, tool)
	seedBridgeAPIBackgroundTask(t, admin, "default", scope.SessionId, scope.SessionThreadId, scope.Binding.BindingId, task, "evt_review_background_source")
	tracer := &bridgeExecutionQueryTracer{}
	traced := newAwaitNotificationTracedStore(t, runtime, tracer)
	rpc, returned := receiptJoinedRPC(t, traced, scope.Binding.TargetPodUid)
	admitPendingBackgroundReceipt(t, admin, tracer, returned, backgroundReceiptAdmission{
		scope: scope, receiptID: tool, taskID: task, requestID: "op_review_background", kind: "stdin", totalJobs: 1,
		call: func(ctx context.Context) error {
			_, err := rpc.SendCommandInput(ctx, &bridgev1.SendCommandInputRequest{Scope: scope, TaskId: task, ToolUseEventId: tool, OperationId: "op_review_background"})
			return err
		},
	})
	updated, err := admin.Exec(`UPDATE session_runtime_tool_results SET background_operation_state='terminal',result_json='{"original":true}',result_digest='fixture_terminal' WHERE workspace_id='default' AND tool_use_event_id=$1`, tool)
	if err != nil {
		t.Fatal(err)
	}
	if count, err := updated.RowsAffected(); err != nil || count != 1 {
		t.Fatalf("terminal background receipt=%d/%v; want 1", count, err)
	}
	fired, release := tracer.armBarrier("", "/* runtime receipt scope validation */", 1)
	var releaseOnce sync.Once
	releaseBarrier := func() { releaseOnce.Do(func() { close(release) }) }
	defer releaseBarrier()
	type outcome struct {
		result commandOperationResult
		err    error
	}
	done := make(chan outcome, 1)
	go func() {
		result, err := traced.waitForBackgroundResult(context.Background(), scope, tool)
		done <- outcome{result, err}
	}()
	select {
	case <-fired:
	case <-time.After(time.Second * 3):
		releaseBarrier()
		t.Fatal("scope validation barrier unreached")
	}
	if _, err := admin.Exec(`DELETE FROM session_runtime_bindings WHERE workspace_id='default' AND session_id=$1`, scope.SessionId); err != nil {
		releaseBarrier()
		t.Fatal(err)
	}
	before := receiptTenantSnapshot(t, admin)
	releaseBarrier()
	select {
	case got := <-done:
		t.Logf("after binding deletion: result=%s error=%v", got.result.ResultJSON, got.err)
		assertReceiptBindingStale(t, got.err)
		if got.result.ResultJSON != "" {
			t.Fatal("denied background receipt returned content")
		}
	case <-time.After(time.Second * 3):
		t.Fatal("wait did not finish")
	}
	if after := receiptTenantSnapshot(t, admin); !reflect.DeepEqual(before, after) {
		t.Fatal("receipt denial mutated accepted work or tenant rows")
	}
}

func TestPostgreSQLMemoryReceiptBindingCut(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	scope, _ := seedAwaitExecutionNotificationFixture(t, store, admin, "review_memory_cut")
	sessionfixture.SeedBridgeAPIWritableMemoryStore(t, admin, "default", scope.SessionId, "memory_review_cut")
	request := durableMemoryRequestForTest(t, admin, scope, "evt_review_memory", `{"action":"create","path":"notes/replay.md","content":"original"}`)
	first, err := store.RunMemory(context.Background(), request)
	if err != nil || first.GetCommitted() == nil {
		t.Fatalf("memory admission=%v/%v", first, err)
	}
	tracer := &bridgeExecutionQueryTracer{}
	traced := newAwaitNotificationTracedStore(t, runtime, tracer)
	fired, release := tracer.armBarrier("", "/* runtime receipt scope validation */", 1)
	type outcome struct {
		result *bridgev1.RunMemoryResponse
		err    error
	}
	done := make(chan outcome, 1)
	go func() {
		result, err := traced.completePendingMemoryProjection(context.Background(), request, true)
		done <- outcome{result, err}
	}()
	select {
	case <-fired:
	case <-time.After(time.Second * 3):
		close(release)
		t.Fatal("scope validation barrier unreached")
	}
	if _, err := admin.Exec(`DELETE FROM session_runtime_bindings WHERE workspace_id='default' AND session_id=$1`, scope.SessionId); err != nil {
		close(release)
		t.Fatal(err)
	}
	before := receiptTenantSnapshot(t, admin)
	close(release)
	select {
	case got := <-done:
		t.Logf("after binding deletion: result=%v error=%v", got.result, got.err)
		assertReceiptBindingStale(t, got.err)
		if got.result != nil {
			t.Fatal("denied memory receipt returned content")
		}
	case <-time.After(time.Second * 3):
		t.Fatal("wait did not finish")
	}
	if after := receiptTenantSnapshot(t, admin); !reflect.DeepEqual(before, after) {
		t.Fatal("receipt denial mutated accepted work or tenant rows")
	}
}

func assertReceiptBindingStale(t *testing.T, err error) {
	t.Helper()
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("receipt denial=%v", err)
	}
	for _, detail := range status.Convert(err).Details() {
		if info, ok := detail.(*errdetails.ErrorInfo); ok && info.Reason == "RUNTIME_BINDING_STALE" {
			return
		}
	}
	t.Fatalf("receipt denial omitted typed binding reason: %v", err)
}

// These public consumers retain independent accepted Sandbox Queue work. The
// result becomes terminal after the read-only validation's completed snapshot;
// unchanged retired binding permits replay, but cutting it denies disclosure.
func TestPostgreSQLBackgroundReceiptConsumersBindingRace(t *testing.T) {
	for _, kind := range []string{"poll", "stdin", "cancel"} {
		for _, cut := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/cut_%t", kind, cut), func(t *testing.T) {
				runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
				store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
				scope, _ := seedAwaitExecutionNotificationFixture(t, store, admin, "receipt_consumer")
				const task = "task_receipt_consumer"
				const tool = "evt_receipt_consumer"
				seedBridgeAPIEvent(t, admin, "default", scope.SessionId, scope.SessionThreadId, tool, sessionfixture.NextBridgeAPIEventSequenceForTest(t, admin, scope.SessionId, scope.SessionThreadId), "agent.tool_use", `{"name":"write_stdin","input":{"session_id":"task_receipt_consumer","chars":"once"},"evaluated_permission":"allow"}`)
				seedBridgeAPIToolDeclarationProjection(t, admin, "default", scope.SessionId, scope.SessionThreadId, tool, "call_receipt_consumer", "write_stdin", `{"session_id":"task_receipt_consumer","chars":"once"}`, "background_command")
				sessionfixture.SeedBridgeAPIAllowedToolRoute(t, admin, "default", scope.SessionId, scope.SessionThreadId, tool)
				seedBridgeAPIBackgroundTask(t, admin, "default", scope.SessionId, scope.SessionThreadId, scope.Binding.BindingId, task, "evt_receipt_source")
				tracer := &bridgeExecutionQueryTracer{}
				traced := newAwaitNotificationTracedStore(t, runtime, tracer)
				rpc, returned := receiptJoinedRPC(t, traced, scope.Binding.TargetPodUid)
				type outcome struct {
					content string
					stale   bool
					err     error
				}
				call := func(ctx context.Context, operation string) outcome {
					switch operation {
					case "poll":
						v, e := rpc.ReadCommandResult(ctx, &bridgev1.ReadCommandResultRequest{Scope: scope, TaskId: task, ToolUseEventId: tool, OperationId: "op_receipt_poll"})
						return outcome{v.GetCompleted().GetResultJson(), v.GetStale() != nil, e}
					case "stdin":
						v, e := rpc.SendCommandInput(ctx, &bridgev1.SendCommandInputRequest{Scope: scope, TaskId: task, ToolUseEventId: tool, OperationId: "op_receipt_stdin"})
						return outcome{v.GetDuplicate().GetResultJson(), v.GetStale() != nil, e}
					default:
						v, e := rpc.CancelCommand(ctx, &bridgev1.CancelCommandRequest{Scope: scope, TaskId: task, ToolUseEventId: tool, OperationId: "op_receipt_cancel", Reason: "fixture"})
						return outcome{v.GetDuplicate().GetResultJson(), v.GetStale() != nil, e}
					}
				}
				receiptFor := func(operation string) string {
					if operation == "cancel" {
						return backgroundCommandReceiptID("op_receipt_cancel")
					}
					return tool
				}
				jobs := 0
				admit := func(operation string) {
					jobs++
					admitPendingBackgroundReceipt(t, admin, tracer, returned, backgroundReceiptAdmission{
						scope: scope, receiptID: receiptFor(operation), taskID: task, requestID: "op_receipt_" + operation, kind: operation, totalJobs: jobs,
						call: func(ctx context.Context) error { return call(ctx, operation).err },
					})
				}
				if kind == "cancel" {
					admit("stdin")
				}
				admit(kind)
				receipt := receiptFor(kind)
				replacement := runtimecontrol.ProcessIdentity{Namespace: "tetral-agent-runtime", PodUID: scope.Binding.TargetPodUid, ID: "replacement_receipt_consumer"}
				registered, err := runtimecontrol.RegisterProcess(context.Background(), dbconnect.NewClientForTesting(runtime), replacement)
				if err != nil {
					t.Fatal(err)
				}
				if _, _, err := runtimecontrol.ReportProcess(context.Background(), dbconnect.NewClientForTesting(runtime), replacement, registered.RegistrationReceipt, runtimecontrol.ProcessAccepting); err != nil {
					t.Fatal(err)
				}
				fired, release := tracer.armBarrier("", "/* runtime receipt scope validation */", 1)
				var releaseOnce sync.Once
				releaseBarrier := func() { releaseOnce.Do(func() { close(release) }) }
				defer releaseBarrier()
				done := make(chan outcome, 1)
				go func() { done <- call(context.Background(), kind) }()
				select {
				case <-fired:
				case got := <-done:
					t.Fatalf("actual public receipt returned before read barrier: %+v", got)
				case <-time.After(3 * time.Second):
					t.Fatal("actual public receipt read barrier not reached")
				}
				updated, err := admin.Exec(`UPDATE session_runtime_tool_results SET background_operation_state='terminal',result_json='{"original":true}',result_digest='fixture_terminal' WHERE workspace_id='default' AND tool_use_event_id=$1`, receipt)
				if err != nil {
					t.Fatal(err)
				}
				if count, err := updated.RowsAffected(); err != nil || count != 1 {
					t.Fatalf("terminal background receipt=%d/%v; want 1", count, err)
				}
				if cut {
					if _, err := admin.Exec(`DELETE FROM session_runtime_bindings WHERE workspace_id='default' AND session_id=$1`, scope.SessionId); err != nil {
						releaseBarrier()
						t.Fatal(err)
					}
				}
				before := receiptTenantSnapshot(t, admin)
				releaseBarrier()
				select {
				case got := <-done:
					if got.err != nil || got.stale != cut || (cut && got.content != "") || (!cut && got.content != `{"original":true}`) {
						t.Fatalf("actual %s replay cut=%t result=%+v", kind, cut, got)
					}
				case <-time.After(3 * time.Second):
					t.Fatal("actual public receipt consumer failed to join")
				}
				if after := receiptTenantSnapshot(t, admin); !reflect.DeepEqual(before, after) {
					t.Fatal("ordinary receipt read mutated accepted Sandbox/Queue custody")
				}
			})
		}
	}
}

// Observe the actual handler's return separately from the TCP caller deadline:
// client expiry can precede server cancellation and transaction cleanup.
func receiptJoinedRPC(t *testing.T, store *PostgreSQLBridgeAPIStore, podUID string) (bridgev1.AgentRuntimeBridgeServiceClient, <-chan struct{}) {
	t.Helper()
	identity := auth.Identity{ServiceAccount: auth.ServiceAccount{Namespace: "tetral-agent-runtime", Name: "agent-runtime"}, KubernetesPodUID: podUID}
	returned := make(chan struct{}, 2)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer(grpc.UnaryInterceptor(func(ctx context.Context, request any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		defer func() { returned <- struct{}{} }()
		if err := BridgeAPIMethodAuthorizer(identity, info.FullMethod); err != nil {
			return nil, err
		}
		return handler(auth.ContextWithIdentity(ctx, identity), request)
	}))
	RegisterBridgeAPI(server, store)
	served := make(chan error, 1)
	go func() { served <- server.Serve(listener) }()
	conn, err := grpc.NewClient(listener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = conn.Close()
		// A released tracer lets GracefulStop join all admitted handlers;
		// Stop/Serve exit alone would not establish that ownership proof.
		joined := make(chan struct{})
		go func() { server.GracefulStop(); close(joined) }()
		select {
		case <-joined:
		case <-time.After(5 * time.Second):
			server.Stop()
			<-joined
			t.Error("receipt RPC exceeded graceful join")
		}
		_ = listener.Close()
		<-served
	})
	return bridgev1.NewAgentRuntimeBridgeServiceClient(conn), returned
}

// Actual RunMemory converges a pending projection after at least one poll. The
// independent publisher completes accepted work between validation and receipt
// SELECT; retirement alone permits replay, while a binding cut denies content.
func TestPostgreSQLMemoryProjectionReceiptBindingRace(t *testing.T) {
	for _, cut := range []bool{false, true} {
		t.Run(fmt.Sprintf("cut_%t", cut), func(t *testing.T) {
			runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
			store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
			scope, _ := seedAwaitExecutionNotificationFixture(t, store, admin, "memory_projection_receipt")
			sessionfixture.SeedBridgeAPIWritableMemoryStore(t, admin, "default", scope.SessionId, "memory_projection_receipt")
			sessionfixture.SeedReadySandboxForSharedToolExecution(t, admin, "default", scope.SessionId)
			request := durableMemoryRequestForTest(t, admin, scope, "evt_memory_projection_receipt", `{"action":"create","path":"notes/replay.md","content":"original"}`)
			tracer := &bridgeExecutionQueryTracer{}
			traced := newAwaitNotificationTracedStore(t, runtime, tracer)
			rpc, handlerReturned := receiptJoinedRPC(t, traced, scope.Binding.TargetPodUid)
			// The real caller deadline is outside the unchanged 3s admission
			// phase and inside the unchanged 30s projection wait. Admission is
			// proved by the committed scope boundary and independent SQL.
			firstCtx, stop := context.WithTimeout(context.Background(), 5*time.Second)
			defer stop()
			firstFired, firstRelease := tracer.armBarrier("", "/* runtime receipt scope validation */", 1)
			firstReleased := false
			defer func() {
				if !firstReleased {
					close(firstRelease)
				}
			}()
			type outcome struct {
				response *bridgev1.RunMemoryResponse
				err      error
			}
			firstDone := make(chan outcome, 1)
			go func() { response, err := rpc.RunMemory(firstCtx, request); firstDone <- outcome{response, err} }()
			select {
			case <-firstFired:
			case <-firstCtx.Done():
				t.Fatal("caller expired before committed projection scope boundary")
			}
			assertPending := func() {
				t.Helper()
				var projectionState string
				var queued, matching int
				if err := admin.QueryRow(`SELECT memory_projection_state FROM session_runtime_tool_results WHERE tool_use_event_id=$1`, request.ToolUseEventId).Scan(&projectionState); err != nil || projectionState != "pending" {
					t.Fatalf("projection=%s/%v", projectionState, err)
				}
				if err := admin.QueryRow(`SELECT count(*), count(*) FILTER (WHERE workspace_id=$2 AND payload_json::jsonb->>'session_id'=$3 AND payload_json::jsonb->>'memory_write_id'=$4)
				 FROM queue_jobs WHERE kind=$1`, queue.KindSandboxMemoryProjection, scope.WorkspaceId, scope.SessionId, request.ToolUseEventId).Scan(&queued, &matching); err != nil || queued != 1 || matching != 1 {
					t.Fatalf("independently accepted projection Queue custody=%d matching=%d/%v", queued, matching, err)
				}
			}
			assertPending()
			beforeExpiry := receiptTenantSnapshot(t, admin)
			if err := firstCtx.Err(); err != nil {
				t.Fatalf("caller expired before independent committed custody proof: %v", err)
			}
			<-firstCtx.Done()
			close(firstRelease)
			firstReleased = true
			select {
			case first := <-firstDone:
				if status.Code(first.err) != codes.DeadlineExceeded || first.response != nil {
					t.Fatalf("pending admission=%v/%v", first.response, first.err)
				}
			case <-time.After(3 * time.Second):
				t.Fatal("expired caller did not join")
			}
			select {
			case <-handlerReturned:
			case <-time.After(3 * time.Second):
				t.Fatal("expired server handler did not join before replay")
			}
			assertPending()
			if after := receiptTenantSnapshot(t, admin); !reflect.DeepEqual(beforeExpiry, after) {
				t.Fatal("caller expiry changed accepted memory/Queue custody")
			}
			replacement := runtimecontrol.ProcessIdentity{Namespace: "tetral-agent-runtime", PodUID: scope.Binding.TargetPodUid, ID: "replacement_memory_projection"}
			registered, err := runtimecontrol.RegisterProcess(context.Background(), dbconnect.NewClientForTesting(runtime), replacement)
			if err != nil {
				t.Fatal(err)
			}
			if _, _, err := runtimecontrol.ReportProcess(context.Background(), dbconnect.NewClientForTesting(runtime), replacement, registered.RegistrationReceipt, runtimecontrol.ProcessAccepting); err != nil {
				t.Fatal(err)
			}
			fired, release := tracer.armBarrier("", "/* runtime receipt scope validation */", 2)
			var releaseOnce sync.Once
			releaseBarrier := func() { releaseOnce.Do(func() { close(release) }) }
			defer releaseBarrier()
			done := make(chan outcome, 1)
			go func() { response, err := rpc.RunMemory(context.Background(), request); done <- outcome{response, err} }()
			select {
			case <-fired:
			case <-time.After(3 * time.Second):
				releaseBarrier()
				t.Fatal("second actual pending receipt poll did not reach barrier")
			}
			if _, err := admin.Exec(`UPDATE session_runtime_tool_results SET memory_projection_state='refreshed',result_json='{"status":"completed","original":true}' WHERE tool_use_event_id=$1`, request.ToolUseEventId); err != nil {
				releaseBarrier()
				t.Fatal(err)
			}
			if cut {
				if _, err := admin.Exec(`DELETE FROM session_runtime_bindings WHERE workspace_id='default' AND session_id=$1`, scope.SessionId); err != nil {
					releaseBarrier()
					t.Fatal(err)
				}
			}
			before := receiptTenantSnapshot(t, admin)
			releaseBarrier()
			select {
			case got := <-done:
				if got.err != nil || (got.response.GetStale() != nil) != cut || (!cut && got.response.GetDuplicate().GetResultJson() != `{"status":"completed","original":true}`) {
					t.Fatalf("pending convergence cut=%t receipt=%v/%v", cut, got.response, got.err)
				}
				if cut && got.response.GetDuplicate() != nil {
					t.Fatal("binding cut disclosed memory result")
				}
			case <-time.After(3 * time.Second):
				t.Fatal("actual memory receipt did not join")
			}
			if after := receiptTenantSnapshot(t, admin); !reflect.DeepEqual(before, after) {
				t.Fatal("receipt convergence changed memory/Queue/accepted work")
			}
		})
	}
}

// Missing accepted projection data is an integrity error under an unchanged
// scope, rather than a supersession outcome. The actual RunMemory admission and
// Queue projection work remain committed while the waiter observes the missing row.
func TestPostgreSQLMemoryProjectionMissingReceiptKeepsBoundScopeError(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	scope, _ := seedAwaitExecutionNotificationFixture(t, store, admin, "memory_missing_receipt")
	sessionfixture.SeedBridgeAPIWritableMemoryStore(t, admin, "default", scope.SessionId, "memory_missing_receipt")
	sessionfixture.SeedReadySandboxForSharedToolExecution(t, admin, "default", scope.SessionId)
	request := durableMemoryRequestForTest(t, admin, scope, "evt_memory_missing_receipt", `{"action":"create","path":"notes/missing.md","content":"original"}`)
	tracer := &bridgeExecutionQueryTracer{}
	rpc := processRegistryRPCWithStore(t, newAwaitNotificationTracedStore(t, runtime, tracer), scope.Binding.TargetPodUid, nil)
	fired, release := tracer.armBarrier("", "/* runtime receipt scope validation */", 1)
	type outcome struct {
		response *bridgev1.RunMemoryResponse
		err      error
	}
	done := make(chan outcome, 1)
	go func() { response, err := rpc.RunMemory(context.Background(), request); done <- outcome{response, err} }()
	select {
	case <-fired:
	case <-time.After(3 * time.Second):
		close(release)
		t.Fatal("actual memory waiter did not reach read-only scope boundary")
	}
	var state string
	var queued int
	if err := admin.QueryRow(`SELECT memory_projection_state FROM session_runtime_tool_results WHERE tool_use_event_id=$1`, request.ToolUseEventId).Scan(&state); err != nil || state != "pending" {
		close(release)
		t.Fatalf("actual committed memory admission=%s/%v", state, err)
	}
	if err := admin.QueryRow(`SELECT count(*) FROM queue_jobs WHERE kind=$1`, queue.KindSandboxMemoryProjection).Scan(&queued); err != nil || queued != 1 {
		close(release)
		t.Fatalf("accepted projection Queue custody=%d/%v", queued, err)
	}
	if _, err := admin.Exec(`DELETE FROM session_runtime_tool_results WHERE tool_use_event_id=$1`, request.ToolUseEventId); err != nil {
		close(release)
		t.Fatal(err)
	}
	before := receiptTenantSnapshot(t, admin)
	close(release)
	select {
	case got := <-done:
		if got.response.GetStale() != nil || status.Code(got.err) != codes.FailedPrecondition || status.Convert(got.err).Message() != "memory tool result is missing" {
			t.Fatalf("bound missing-memory receipt=%v/%v; want original integrity error", got.response, got.err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("missing memory waiter did not join")
	}
	if after := receiptTenantSnapshot(t, admin); !reflect.DeepEqual(before, after) {
		t.Fatal("missing memory classification mutated accepted memory/Queue work")
	}
}
