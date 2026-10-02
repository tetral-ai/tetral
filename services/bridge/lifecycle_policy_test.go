package agentruntimebridge

import (
	"context"
	"reflect"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	bridgev1 "github.com/tetral-ai/tetral/services/bridge/gen/tetral/bridge/v1"
)

func TestBridgeLifecyclePolicyConfiguration(t *testing.T) {
	values := map[string]string{"TETRAL_DRAIN_TIMEOUT_MS": "90", "TETRAL_CANCEL_JOIN_TIMEOUT_MS": "20", "TETRAL_BRIDGE_ADMISSION_TIMEOUT_MS": "10", "TETRAL_BRIDGE_RELEASE_RUNTIME_BINDING_TIMEOUT_MS": "15", "TETRAL_BRIDGE_SANDBOX_RESULT_WAIT_TIMEOUT_MS": "30", "TETRAL_BRIDGE_BACKGROUND_RESULT_WAIT_TIMEOUT_MS": "31", "TETRAL_BRIDGE_MEMORY_PROJECTION_WAIT_TIMEOUT_MS": "32", "TETRAL_BRIDGE_OUTPUT_CAPTURE_WAIT_TIMEOUT_MS": "33"}
	p, err := BridgeLifecyclePolicyFromEnv(func(k string) string { return values[k] })
	if err != nil {
		t.Fatal(err)
	}
	if p.DrainTimeout != 90*time.Millisecond || p.AdmissionTimeout != 10*time.Millisecond || p.SandboxResultWait != 30*time.Millisecond || p.OutputCaptureWait != 33*time.Millisecond {
		t.Fatalf("resolved policy=%+v", p)
	}
	for _, bad := range []string{"0", "-1", "x", "9223372036854775807"} {
		values["TETRAL_DRAIN_TIMEOUT_MS"] = bad
		if _, err := BridgeLifecyclePolicyFromEnv(func(k string) string { return values[k] }); err == nil {
			t.Fatalf("accepted invalid drain %q", bad)
		}
	}
	values["TETRAL_DRAIN_TIMEOUT_MS"] = "10"
	if _, err := BridgeLifecyclePolicyFromEnv(func(k string) string { return values[k] }); err == nil {
		t.Fatal("accepted admission exceeding enclosing drain")
	}
}
func TestPostgreSQLBridgeConfiguredWaitAndCallerCancellation(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	tracer := &bridgeExecutionQueryTracer{}
	store := newAwaitNotificationTracedStore(t, runtime, tracer)
	scope, event := seedAwaitExecutionNotificationFixture(t, store, admin, "configuredwait")
	store.LifecyclePolicy = DefaultBridgeLifecyclePolicy()
	store.LifecyclePolicy.SandboxResultWait = 90 * time.Millisecond
	request := &bridgev1.AwaitSandboxExecutionRequest{Scope: scope, ToolUseEventId: event}
	started := time.Now()
	_, err := store.AwaitSandboxExecution(context.Background(), request)
	if status.Code(err) != codes.DeadlineExceeded || time.Since(started) < 80*time.Millisecond || time.Since(started) > time.Second {
		t.Fatalf("configured wait=%s/%v", time.Since(started), err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	started = time.Now()
	_, err = store.AwaitSandboxExecution(ctx, request)
	if status.Code(err) != codes.DeadlineExceeded || time.Since(started) > 150*time.Millisecond {
		t.Fatalf("short caller deadline=%s/%v", time.Since(started), err)
	}
	ctx, cancel = context.WithCancel(context.Background())
	cancel()
	_, err = store.AwaitSandboxExecution(ctx, request)
	if status.Code(err) != codes.Canceled {
		t.Fatalf("caller cancellation=%v", err)
	}
	requireExecutionResultWaiters(t, store.executionResultWake(), 0, 0)
	// Wait expiry and cancellation preserve the accepted execution and its job.
	var state string
	if err := admin.QueryRowContext(context.Background(), `SELECT execution_state FROM session_runtime_tool_results WHERE workspace_id=$1 AND tool_use_event_id=$2`, scope.GetWorkspaceId(), event).Scan(&state); err != nil || state != "pending" {
		t.Fatalf("durable execution after wait=%q/%v", state, err)
	}
}

func TestPostgreSQLBridgeAdmissionPhaseDeadlineRollsBack(t *testing.T) {
	for _, method := range []string{"AcceptSandboxExecution", "RunMemory", "ReadCommandResult", "SendCommandInput", "CancelCommand", "FinishIdle", "ReleaseRuntimeBinding"} {
		t.Run(method, func(t *testing.T) {
			store, admin, rpc, release := handoffFixture(t)
			store.LifecyclePolicy = DefaultBridgeLifecyclePolicy()
			store.LifecyclePolicy.AdmissionTimeout = 40 * time.Millisecond
			store.LifecyclePolicy.ReleaseTimeout = 40 * time.Millisecond
			scope := bridgeAPIScope("sesn_handoff", "thr_handoff", "bind_handoff", 1, "pod_handoff")
			ctx, stop := context.WithTimeout(context.Background(), 5*time.Second)
			defer stop()
			before := receiptTenantSnapshot(t, admin)
			blocker, err := admin.BeginTx(ctx, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = blocker.Rollback() }()
			if _, err := blocker.ExecContext(ctx, `SELECT id FROM sessions WHERE workspace_id='default' AND id='sesn_handoff' FOR UPDATE`); err != nil {
				t.Fatal(err)
			}
			start := time.Now()
			switch method {
			case "AcceptSandboxExecution":
				_, err = rpc.AcceptSandboxExecution(ctx, &bridgev1.AcceptSandboxExecutionRequest{Scope: scope, ToolUseEventId: "tool_admission"})
			case "RunMemory":
				_, err = rpc.RunMemory(ctx, &bridgev1.RunMemoryRequest{Scope: scope, ToolUseEventId: "tool_admission"})
			case "ReadCommandResult":
				_, err = rpc.ReadCommandResult(ctx, &bridgev1.ReadCommandResultRequest{Scope: scope, TaskId: "task_admission", ToolUseEventId: "tool_admission", OperationId: "operation_admission"})
			case "SendCommandInput":
				_, err = rpc.SendCommandInput(ctx, &bridgev1.SendCommandInputRequest{Scope: scope, TaskId: "task_admission", ToolUseEventId: "tool_admission", OperationId: "operation_admission"})
			case "CancelCommand":
				_, err = rpc.CancelCommand(ctx, &bridgev1.CancelCommandRequest{Scope: scope, TaskId: "task_admission", ToolUseEventId: "tool_admission", OperationId: "operation_admission"})
			case "FinishIdle":
				_, err = rpc.FinishIdle(ctx, &bridgev1.FinishIdleRequest{Scope: scope, DurableTurnId: "turn_admission"})
			case "ReleaseRuntimeBinding":
				_, err = rpc.ReleaseRuntimeBinding(ctx, release)
			}
			if status.Code(err) != codes.DeadlineExceeded || time.Since(start) < 30*time.Millisecond || time.Since(start) > time.Second {
				t.Fatalf("actual40ms phase %s elapsed=%s error=%v", method, time.Since(start), err)
			}
			if !reflect.DeepEqual(before, receiptTenantSnapshot(t, admin)) {
				t.Fatal("expired owning phase changed durable state")
			}
			if err := blocker.Commit(); err != nil {
				t.Fatal(err)
			}
			// Successful lock reuse independently proves cancellation released its
			// transaction and Session advisory/row locks before returning.
			followup, err := admin.BeginTx(ctx, nil)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := followup.ExecContext(ctx, `SELECT id FROM sessions WHERE workspace_id='default' AND id='sesn_handoff' FOR UPDATE NOWAIT`); err != nil {
				_ = followup.Rollback()
				t.Fatal(err)
			}
			if err := followup.Commit(); err != nil {
				t.Fatal(err)
			}
		})
	}
}
