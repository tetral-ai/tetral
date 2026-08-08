package tetralsandbox

import (
	"context"
	"database/sql"
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/queue"
	"github.com/tetral-ai/tetral/internal/sandbox"
	sandboxdriver "github.com/tetral-ai/tetral/internal/sandbox/driver"
	"github.com/tetral-ai/tetral/internal/workspace"
)

func TestSandboxActivationAdoptsStableResourceAcrossLifecycleOperations(t *testing.T) {
	tests := []struct {
		name            string
		resolution      ProviderOutcome[ActivationResolution]
		inspection      ProviderOutcome[ExecutionReadiness]
		inspections     []ProviderOutcome[ExecutionReadiness]
		activation      ProviderOutcome[sandbox.ProviderHandle]
		atAttemptBudget bool
		wantCalls       []string
		wantOperation   string
		wantQueue       string
	}{
		{
			name: "ready resource",
			resolution: ProviderOutcome[ActivationResolution]{Value: ActivationResolution{Found: true, Handle: sandbox.ProviderHandle{
				Provider: sandboxdriver.DaytonaProviderName, SandboxID: "provider_adopted", Metadata: map[string]string{"daytona_state": "started"},
			}}},
			inspection: ProviderOutcome[ExecutionReadiness]{Value: ExecutionReady},
			wantCalls:  []string{"resolve", "inspect"}, wantOperation: "completed", wantQueue: queue.StatusAcknowledged,
		},
		{
			name: "stopped resource",
			resolution: ProviderOutcome[ActivationResolution]{Value: ActivationResolution{Found: true, Handle: sandbox.ProviderHandle{
				Provider: sandboxdriver.DaytonaProviderName, SandboxID: "provider_adopted", Metadata: map[string]string{"daytona_state": "stopped"},
			}}},
			inspections: []ProviderOutcome[ExecutionReadiness]{{Value: ExecutionNeedsActivation}, {Value: ExecutionReady}},
			activation: ProviderOutcome[sandbox.ProviderHandle]{Value: sandbox.ProviderHandle{
				Provider: sandboxdriver.DaytonaProviderName, SandboxID: "provider_adopted",
			}},
			wantCalls: []string{"resolve", "inspect", "activate:start", "inspect"}, wantOperation: "completed", wantQueue: queue.StatusAcknowledged,
		},
		{
			name: "provider transition exhausts",
			resolution: ProviderOutcome[ActivationResolution]{Value: ActivationResolution{Found: true, Handle: sandbox.ProviderHandle{
				Provider: sandboxdriver.DaytonaProviderName, SandboxID: "provider_adopted", Metadata: map[string]string{"daytona_state": "starting"},
			}}},
			inspection: ProviderOutcome[ExecutionReadiness]{
				EffectBoundary: ProviderProvedNotStarted, Disposition: ProviderRetryable, ErrorKind: "provider_transition_in_progress",
			},
			atAttemptBudget: true,
			wantCalls:       []string{"resolve", "inspect"}, wantOperation: "failed", wantQueue: queue.StatusDeadLettered,
		},
		{
			name: "ownership mismatch never creates",
			resolution: ProviderOutcome[ActivationResolution]{
				EffectBoundary: ProviderProvedNotStarted, Disposition: ProviderTerminal,
				ErrorKind: "sandbox_identity_mismatch", SafeMessage: "sandbox provider ownership identity does not match",
			},
			wantCalls: []string{"resolve"}, wantOperation: "failed", wantQueue: queue.StatusDeadLettered,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			runtimeDB, adminDB := newSandboxServiceTestDB(t)
			seedSandboxExecutionStoreFixture(t, adminDB)
			client := dbconnect.NewClientForTesting(runtimeDB)
			queueStore := queue.NewPostgreSQLStore(client)
			coordinator := NewPostgreSQLSandboxExecutionCoordinator(client, 30*time.Minute)
			lifecycle := NewPostgreSQLSandboxLifecycleStore(client, &fixedSandboxResourceSource{}, 30*time.Minute)
			setupCtx := sandboxTestQueueContext(t, runtimeDB)
			now := time.Now().UTC()

			operationA := loadSandboxExecutionWork(t, coordinator, "evt_execution_a")
			if err := coordinator.WaitForActivation(setupCtx, operationA, ExecutionNeedsCreation); err != nil {
				t.Fatalf("create first activation: %v", err)
			}
			jobA := leaseSandboxLifecycleJob(t, queueStore, queue.KindSandboxActivate, "activation-a")
			ctxA := withLifecycleJobQueueAuthority(context.Background(), jobA)
			workA, disposition, err := lifecycle.ClaimActivation(ctxA, jobA, now)
			if err != nil || disposition != SandboxLifecycleApplied {
				t.Fatalf("claim first activation = %s, %v", disposition, err)
			}
			if disposition, err = lifecycle.ObserveUnknownActivation(ctxA, workA, "activation_outcome_unknown", now.Add(time.Second)); err != nil || disposition != SandboxLifecycleApplied {
				t.Fatalf("record first unknown outcome = %s, %v", disposition, err)
			}
			if disposition, err = lifecycle.FinalizeExhaustedLifecycle(ctxA, jobA.QueueJob, queue.KindSandboxActivate, now.Add(2*time.Second)); err != nil || disposition != SandboxLifecycleApplied {
				t.Fatalf("finalize first activation = %s, %v", disposition, err)
			}
			if updated, err := queueStore.DeadLetter(context.Background(), queue.DeadLetterRequest{
				WorkspaceID: workspace.ID(jobA.WorkspaceID), JobID: jobA.JobID, LeaseToken: jobA.LeaseToken,
				ErrorKind: "sandbox_activation_attempts_exhausted", ErrorMessage: "sandbox activation attempt budget exhausted", Now: now.Add(2 * time.Second),
			}); err != nil || !updated {
				t.Fatalf("dead-letter first activation = %t, %v", updated, err)
			}

			operationB := loadBoundSandboxExecutionWork(t, coordinator, "evt_execution_b")
			if err := coordinator.WaitForActivation(setupCtx, operationB, ExecutionNeedsCreation); err != nil {
				t.Fatalf("create later activation: %v", err)
			}
			var operationBID, jobBID, labelsJSON, dedupeKey, payloadJSON string
			if err := adminDB.QueryRow(`SELECT o.operation_id, o.queue_job_id, o.provider_request_labels_json, q.dedupe_key, q.payload_json
				FROM sandbox_lifecycle_operations o JOIN queue_jobs q
				  ON q.workspace_id=o.workspace_id AND q.id=o.queue_job_id
				WHERE o.workspace_id='ws_execution_store' AND o.state='pending' AND o.kind='create'`).Scan(
				&operationBID, &jobBID, &labelsJSON, &dedupeKey, &payloadJSON,
			); err != nil {
				t.Fatalf("read later activation: %v", err)
			}
			if operationBID == jobA.OperationID {
				t.Fatalf("later operation reused first identity %q", operationBID)
			}
			var labels map[string]string
			if err := json.Unmarshal([]byte(labelsJSON), &labels); err != nil {
				t.Fatalf("decode provider labels: %v", err)
			}
			wantLabels := stableSandboxOwnershipLabels("ws_execution_store", "sesn_execution_store", "env_execution_store", jobA.LogicalSandboxID)
			if !reflect.DeepEqual(labels, wantLabels) {
				t.Fatalf("provider labels = %v; want stable ownership %v", labels, wantLabels)
			}
			if !strings.Contains(dedupeKey, operationBID) || !strings.Contains(payloadJSON, operationBID) {
				t.Fatalf("later Queue identity = dedupe %q payload %s; want operation %q", dedupeKey, payloadJSON, operationBID)
			}
			var waiterOperationID string
			if err := adminDB.QueryRow(`SELECT waiting_activation_operation_id FROM session_runtime_tool_results
				WHERE workspace_id='ws_execution_store' AND tool_use_event_id='evt_execution_b'`).Scan(&waiterOperationID); err != nil {
				t.Fatalf("read later waiter: %v", err)
			}
			if waiterOperationID != operationBID {
				t.Fatalf("later waiter operation = %q; want %q", waiterOperationID, operationBID)
			}
			if test.atAttemptBudget {
				if _, err := adminDB.Exec(`UPDATE queue_jobs SET attempt_count=max_attempts-1 WHERE workspace_id='ws_execution_store' AND id=$1`, jobBID); err != nil {
					t.Fatalf("move later activation to final attempt: %v", err)
				}
			}

			adapter := &recordingLifecycleAdapter{
				resolution: test.resolution, inspection: test.inspection,
				inspectionSequence: append([]ProviderOutcome[ExecutionReadiness](nil), test.inspections...),
				activation:         test.activation,
			}
			registry, err := NewProviderRegistry(map[string]ProviderAdapter{sandboxdriver.DaytonaProviderName: adapter})
			if err != nil {
				t.Fatalf("NewProviderRegistry: %v", err)
			}
			runner := &SandboxActivationJobRunner{
				Queue: sandboxProductionQueueClient(t, queueStore), Store: lifecycle, Providers: registry,
				Config: SandboxLifecycleRunnerConfig{
					WorkspaceID: "ws_execution_store", LeaseOwner: "activation-b", MaxJobs: 1,
					LeaseDuration: time.Minute, HeartbeatInterval: 15 * time.Second,
				},
			}
			if err := runner.RunOnce(context.Background()); err != nil {
				t.Fatalf("run later activation: %v", err)
			}
			if !reflect.DeepEqual(adapter.calls, test.wantCalls) {
				t.Fatalf("provider calls = %v; want %v", adapter.calls, test.wantCalls)
			}
			for _, call := range adapter.calls {
				if call == "activate:create" || call == "activate:replace" {
					t.Fatalf("later activation submitted a second Create: %v", adapter.calls)
				}
			}
			var operationState, queueState string
			if err := adminDB.QueryRow(`SELECT state FROM sandbox_lifecycle_operations
				WHERE workspace_id='ws_execution_store' AND operation_id=$1`, operationBID).Scan(&operationState); err != nil {
				t.Fatalf("read later operation state: %v", err)
			}
			if err := adminDB.QueryRow(`SELECT status FROM queue_jobs
				WHERE workspace_id='ws_execution_store' AND id=$1`, jobBID).Scan(&queueState); err != nil {
				t.Fatalf("read later Queue state: %v", err)
			}
			if operationState != test.wantOperation || queueState != test.wantQueue {
				t.Fatalf("later activation state = %s/%s; want %s/%s", operationState, queueState, test.wantOperation, test.wantQueue)
			}
			if test.wantOperation == "completed" {
				var providerID sql.NullString
				if err := adminDB.QueryRow(`SELECT provider_resource_id FROM session_sandbox_bindings
					WHERE workspace_id='ws_execution_store' AND session_id='sesn_execution_store'`).Scan(&providerID); err != nil {
					t.Fatalf("read adopted binding: %v", err)
				}
				if providerID.String != "provider_adopted" {
					t.Fatalf("adopted provider = %q; want provider_adopted", providerID.String)
				}
			}
		})
	}
}
