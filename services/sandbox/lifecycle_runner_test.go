package tetralsandbox

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/queue"
	"github.com/tetral-ai/tetral/internal/sandbox"
	sandboxdriver "github.com/tetral-ai/tetral/internal/sandbox/driver"
	queuev1 "github.com/tetral-ai/tetral/services/queue/gen/tetral/queue/v1"
)

func TestSandboxActivationRunnerLogsResolutionAndDurableAttemptOutcome(t *testing.T) {
	var logs bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&logs, nil))
	queueClient := &recordingSandboxQueue{leased: []*queuev1.QueueJob{sandboxActivationQueueJob()}}
	work := sandboxActivationTestWork(true)
	work.Labels["private.test"] = "must-not-appear"
	store := &recordingSandboxLifecycleStore{activation: work, current: true}
	adapter := &recordingLifecycleAdapter{
		resolution: ProviderOutcome[ActivationResolution]{Value: ActivationResolution{
			Found:  true,
			Handle: sandbox.ProviderHandle{Provider: sandboxdriver.DaytonaProviderName, SandboxID: "provider_adopted", Metadata: map[string]string{"daytona_state": "started"}},
		}},
		inspection: ProviderOutcome[ExecutionReadiness]{Value: ExecutionReady},
	}
	registry, err := NewProviderRegistry(map[string]ProviderAdapter{sandboxdriver.DaytonaProviderName: adapter})
	if err != nil {
		t.Fatalf("NewProviderRegistry: %v", err)
	}
	runner := &SandboxActivationJobRunner{
		Queue: queueClient, Store: store, Providers: registry, Logger: logger,
		Config: SandboxLifecycleRunnerConfig{WorkspaceID: "ws_lifecycle", LeaseDuration: time.Minute, HeartbeatInterval: 15 * time.Second},
	}
	if err := runner.RunOnce(context.Background()); err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	got := logs.String()
	for _, want := range []string{
		`"event":"sandbox_activation_resolved"`, `"resolution":"owned_found"`, `"provider.state":"started"`,
		`"event":"sandbox_activation_attempt_completed"`, `"outcome":"success"`,
		`"queue.attempt.count":1`, `"queue.attempt.max":5`,
		`"workspace.id":"ws_lifecycle"`, `"session.id":"sesn_lifecycle"`, `"sandbox.id":"sbox_lifecycle"`,
		`"operation.id":"sop_activation"`, `"job.id":"qjob_activation"`, `"provider.name":"daytona"`, `"duration.ms":`,
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("activation log missing %s: %s", want, got)
		}
	}
	if strings.Count(got, `"event":"sandbox_activation_resolved"`) != 1 || strings.Count(got, `"event":"sandbox_activation_attempt_completed"`) != 1 {
		t.Fatalf("activation lifecycle events were not emitted exactly once: %s", got)
	}
	if strings.Contains(got, "must-not-appear") || strings.Contains(got, "private.test") {
		t.Fatalf("activation log contains provider-label payload: %s", got)
	}
	if strings.Contains(got, `"error.`) {
		t.Fatalf("successful activation log contains error fields: %s", got)
	}
}

func TestSandboxActivationRunnerLogsRetryAndTerminalAttemptOutcomes(t *testing.T) {
	tests := []struct {
		name           string
		resolution     ProviderOutcome[ActivationResolution]
		wantTransition string
		wantLevel      string
		wantOutcome    string
		wantErrorCode  string
	}{
		{
			name: "retry", resolution: ProviderOutcome[ActivationResolution]{
				EffectBoundary: ProviderProvedNotStarted, Disposition: ProviderRetryable, ErrorKind: "provider_transition_in_progress",
			},
			wantTransition: "retry:qjob_activation:provider_transition_in_progress", wantLevel: "INFO", wantOutcome: "retry",
			wantErrorCode: "provider_transition_in_progress",
		},
		{
			name: "terminal failure", resolution: ProviderOutcome[ActivationResolution]{
				EffectBoundary: ProviderProvedNotStarted, Disposition: ProviderTerminal,
				ErrorKind: "provider_activation_rejected", SafeMessage: "provider rejected activation",
			},
			wantTransition: "dead:qjob_activation:provider_activation_rejected", wantLevel: "ERROR", wantOutcome: "terminal_failure",
			wantErrorCode: "provider_activation_rejected",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var logs bytes.Buffer
			queueClient := &recordingSandboxQueue{leased: []*queuev1.QueueJob{sandboxActivationQueueJob()}}
			store := &recordingSandboxLifecycleStore{activation: sandboxActivationTestWork(true), current: true}
			registry, err := NewProviderRegistry(map[string]ProviderAdapter{
				sandboxdriver.DaytonaProviderName: &recordingLifecycleAdapter{resolution: test.resolution},
			})
			if err != nil {
				t.Fatalf("NewProviderRegistry: %v", err)
			}
			runner := &SandboxActivationJobRunner{
				Queue: queueClient, Store: store, Providers: registry,
				Logger: slog.New(slog.NewJSONHandler(&logs, nil)),
				Config: SandboxLifecycleRunnerConfig{WorkspaceID: "ws_lifecycle", LeaseDuration: time.Minute, HeartbeatInterval: 15 * time.Second},
			}
			if err := runner.RunOnce(context.Background()); err != nil {
				t.Fatalf("RunOnce: %v", err)
			}
			if !reflect.DeepEqual(queueClient.transitions, []string{test.wantTransition}) {
				t.Fatalf("Queue transitions = %v; want %q", queueClient.transitions, test.wantTransition)
			}
			got := logs.String()
			for _, want := range []string{
				`"level":"` + test.wantLevel + `"`, `"event":"sandbox_activation_attempt_completed"`,
				`"outcome":"` + test.wantOutcome + `"`, `"queue.attempt.count":1`, `"queue.attempt.max":5`,
				`"error.class":"sandbox_activation_error"`, `"error.code":"` + test.wantErrorCode + `"`,
			} {
				if !strings.Contains(got, want) {
					t.Fatalf("attempt log missing %s: %s", want, got)
				}
			}
			wantSafeMessage := "sandbox activation will be retried"
			if test.wantOutcome == "terminal_failure" {
				wantSafeMessage = "sandbox activation failed"
			}
			if !strings.Contains(got, `"error.message_safe":"`+wantSafeMessage+`"`) {
				t.Fatalf("attempt log missing safe message %q: %s", wantSafeMessage, got)
			}
		})
	}
}

func TestSandboxActivationRunnerLogsTransactionAuthorityLossAtInfo(t *testing.T) {
	var logs bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&logs, nil))
	queueClient := &recordingSandboxQueue{leased: []*queuev1.QueueJob{sandboxActivationQueueJob()}}
	store := &recordingSandboxLifecycleStore{claimActivationErr: errQueueLeaseLost}
	registry, err := NewProviderRegistry(map[string]ProviderAdapter{sandboxdriver.DaytonaProviderName: &recordingLifecycleAdapter{}})
	if err != nil {
		t.Fatalf("NewProviderRegistry: %v", err)
	}
	runner := &SandboxActivationJobRunner{
		Queue: queueClient, Store: store, Providers: registry, Logger: logger,
		Config: SandboxLifecycleRunnerConfig{WorkspaceID: "ws_lifecycle", LeaseDuration: time.Minute, HeartbeatInterval: 15 * time.Second},
	}
	if err := runner.RunOnce(context.Background()); !errors.Is(err, errQueueLeaseLost) {
		t.Fatalf("RunOnce = %v; want Queue authority loss", err)
	}
	got := logs.String()
	for _, want := range []string{
		`"level":"INFO"`, `"event":"sandbox_queue_authority_lost"`, `"writer":"sandbox_activation_claim"`,
		`"queue.kind":"sandbox_activate"`, `"workspace.id":"ws_lifecycle"`, `"job.id":"qjob_activation"`,
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("authority-loss log missing %s: %s", want, got)
		}
	}
	if strings.Contains(got, "sandbox_activation_attempt_completed") || len(queueClient.transitions) != 0 {
		t.Fatalf("authority loss recorded a durable attempt outcome: logs=%s transitions=%v", got, queueClient.transitions)
	}
}

func TestSandboxActivationRunnerAdoptsBeforeCreateAndDoesNotReplayUnknownCreate(t *testing.T) {
	tests := []struct {
		name            string
		work            SandboxActivationWork
		resolution      ProviderOutcome[ActivationResolution]
		activation      ProviderOutcome[sandbox.ProviderHandle]
		inspection      ProviderOutcome[ExecutionReadiness]
		wantAdapter     []string
		wantStore       []string
		wantTransitions []string
	}{
		{
			name: "adopts stable name",
			work: sandboxActivationTestWork(true),
			resolution: ProviderOutcome[ActivationResolution]{Value: ActivationResolution{
				Found: true, Handle: sandbox.ProviderHandle{Provider: sandboxdriver.DaytonaProviderName, SandboxID: "provider_adopted"},
			}},
			inspection:  ProviderOutcome[ExecutionReadiness]{Value: ExecutionReady},
			wantAdapter: []string{"resolve", "inspect"}, wantStore: []string{"claim", "complete:provider_adopted"},
			wantTransitions: []string{"ack:qjob_activation"},
		},
		{
			name:       "creates after proved absence",
			work:       sandboxActivationTestWork(true),
			resolution: ProviderOutcome[ActivationResolution]{Value: ActivationResolution{Found: false}},
			activation: ProviderOutcome[sandbox.ProviderHandle]{Value: sandbox.ProviderHandle{
				Provider: sandboxdriver.DaytonaProviderName, SandboxID: "provider_created",
			}},
			inspection:  ProviderOutcome[ExecutionReadiness]{Value: ExecutionReady},
			wantAdapter: []string{"resolve", "activate:create", "inspect"}, wantStore: []string{"claim", "complete:provider_created"},
			wantTransitions: []string{"ack:qjob_activation"},
		},
		{
			name:       "unknown create enters observation only",
			work:       sandboxActivationTestWork(true),
			resolution: ProviderOutcome[ActivationResolution]{Value: ActivationResolution{Found: false}},
			activation: ProviderOutcome[sandbox.ProviderHandle]{
				EffectBoundary: ProviderOutcomeUnknown, Disposition: ProviderTerminal,
				ErrorKind: "provider_timeout",
			},
			wantAdapter: []string{"resolve", "activate:create"}, wantStore: []string{"claim", "observe:provider_timeout"},
			wantTransitions: []string{"retry:qjob_activation:provider_timeout"},
		},
		{
			name:        "observing retry waits for bounded negative probes",
			work:        sandboxActivationTestWork(false),
			resolution:  ProviderOutcome[ActivationResolution]{Value: ActivationResolution{Found: false}},
			wantAdapter: []string{"resolve"}, wantStore: []string{"claim"},
			wantTransitions: []string{"retry:qjob_activation:activation_observation_not_visible"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			queueClient := &recordingSandboxQueue{leased: []*queuev1.QueueJob{sandboxActivationQueueJob()}}
			store := &recordingSandboxLifecycleStore{activation: test.work, current: true}
			adapter := &recordingLifecycleAdapter{resolution: test.resolution, activation: test.activation, inspection: test.inspection}
			registry, err := NewProviderRegistry(map[string]ProviderAdapter{sandboxdriver.DaytonaProviderName: adapter})
			if err != nil {
				t.Fatalf("NewProviderRegistry: %v", err)
			}
			runner := &SandboxActivationJobRunner{
				Queue: queueClient, Store: store, Providers: registry,
				Config: SandboxLifecycleRunnerConfig{WorkspaceID: "ws_lifecycle", LeaseDuration: time.Minute, HeartbeatInterval: 15 * time.Second},
			}
			if err := runner.RunOnce(context.Background()); err != nil {
				t.Fatalf("RunOnce: %v", err)
			}
			if !reflect.DeepEqual(adapter.calls, test.wantAdapter) {
				t.Fatalf("adapter calls = %v; want %v", adapter.calls, test.wantAdapter)
			}
			if !reflect.DeepEqual(store.calls, test.wantStore) {
				t.Fatalf("store calls = %v; want %v", store.calls, test.wantStore)
			}
			if !reflect.DeepEqual(queueClient.transitions, test.wantTransitions) {
				t.Fatalf("queue transitions = %v; want %v", queueClient.transitions, test.wantTransitions)
			}
		})
	}
}

func TestSandboxReleaseRunnerCompletesAnAlreadyAbsentHandleWithoutReleaseCall(t *testing.T) {
	queueClient := &recordingSandboxQueue{leased: []*queuev1.QueueJob{sandboxReleaseQueueJob()}}
	store := &recordingSandboxLifecycleStore{
		release: SandboxReleaseWork{
			Job: SandboxLifecycleJob{
				JobID: "qjob_release", LeaseToken: "lease_release", WorkspaceID: "ws_lifecycle",
				SessionID: "sesn_lifecycle", LogicalSandboxID: "sbox_lifecycle", OperationID: "sop_release",
			},
			Provider: sandboxdriver.DaytonaProviderName,
			Handle:   sandbox.ProviderHandle{Provider: sandboxdriver.DaytonaProviderName, SandboxID: "provider_deleted"},
			Reason:   SandboxReleaseSessionDelete,
		},
		current: true,
	}
	adapter := &recordingLifecycleAdapter{releasePresenceSet: true, releasePresence: ProviderOutcome[bool]{Value: false}}
	registry, err := NewProviderRegistry(map[string]ProviderAdapter{sandboxdriver.DaytonaProviderName: adapter})
	if err != nil {
		t.Fatalf("NewProviderRegistry: %v", err)
	}
	runner := &SandboxReleaseJobRunner{
		Queue: queueClient, Store: store, Providers: registry,
		Config: SandboxLifecycleRunnerConfig{WorkspaceID: "ws_lifecycle", LeaseDuration: time.Minute, HeartbeatInterval: 15 * time.Second},
	}
	if err := runner.RunOnce(context.Background()); err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if !reflect.DeepEqual(adapter.calls, []string{"inspect-release"}) {
		t.Fatalf("adapter calls = %v; want inspection without duplicate release", adapter.calls)
	}
	if !reflect.DeepEqual(store.calls, []string{"claim-release", "complete-release"}) {
		t.Fatalf("store calls = %v; want durable release completion", store.calls)
	}
	if !reflect.DeepEqual(queueClient.transitions, []string{"ack:qjob_release"}) {
		t.Fatalf("queue transitions = %v; want release ACK", queueClient.transitions)
	}
}

func TestSandboxReleaseRunnerDeletesPresentNonExecutableHandle(t *testing.T) {
	queueClient := &recordingSandboxQueue{leased: []*queuev1.QueueJob{sandboxReleaseQueueJob()}}
	store := &recordingSandboxLifecycleStore{
		release: SandboxReleaseWork{
			Job: SandboxLifecycleJob{
				JobID: "qjob_release", LeaseToken: "lease_release", WorkspaceID: "ws_lifecycle",
				SessionID: "sesn_lifecycle", LogicalSandboxID: "sbox_lifecycle", OperationID: "sop_release",
			},
			Provider: sandboxdriver.DaytonaProviderName,
			Handle:   sandbox.ProviderHandle{Provider: sandboxdriver.DaytonaProviderName, SandboxID: "provider_destroyed"},
			Reason:   SandboxReleaseSessionDelete,
		},
		current: true,
	}
	adapter := &recordingLifecycleAdapter{releasePresenceSet: true, releasePresence: ProviderOutcome[bool]{Value: true}}
	registry, err := NewProviderRegistry(map[string]ProviderAdapter{sandboxdriver.DaytonaProviderName: adapter})
	if err != nil {
		t.Fatalf("NewProviderRegistry: %v", err)
	}
	runner := &SandboxReleaseJobRunner{
		Queue: queueClient, Store: store, Providers: registry,
		Config: SandboxLifecycleRunnerConfig{WorkspaceID: "ws_lifecycle", LeaseDuration: time.Minute, HeartbeatInterval: 15 * time.Second},
	}
	if err := runner.RunOnce(context.Background()); err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if !reflect.DeepEqual(adapter.calls, []string{"inspect-release", "release"}) {
		t.Fatalf("adapter calls = %v; want successful provider inspection followed by release", adapter.calls)
	}
	if !reflect.DeepEqual(store.calls, []string{"claim-release", "authorize-release", "complete-release"}) {
		t.Fatalf("store calls = %v; want authorized provider release", store.calls)
	}
	if !reflect.DeepEqual(queueClient.transitions, []string{"ack:qjob_release"}) {
		t.Fatalf("queue transitions = %v; want release ACK", queueClient.transitions)
	}
}

func TestSandboxReleaseRunnerParksBlockedReleaseWithoutSpendingRetryBudget(t *testing.T) {
	queueClient := &recordingSandboxQueue{leased: []*queuev1.QueueJob{sandboxReleaseQueueJob()}}
	store := &recordingSandboxLifecycleStore{claimReleaseErr: errSandboxReleaseBlocked}
	registry, err := NewProviderRegistry(map[string]ProviderAdapter{sandboxdriver.DaytonaProviderName: &recordingLifecycleAdapter{}})
	if err != nil {
		t.Fatalf("NewProviderRegistry: %v", err)
	}
	runner := &SandboxReleaseJobRunner{
		Queue: queueClient, Store: store, Providers: registry,
		Config: SandboxLifecycleRunnerConfig{WorkspaceID: "ws_lifecycle", LeaseDuration: time.Minute, HeartbeatInterval: 15 * time.Second},
	}
	if err := runner.RunOnce(context.Background()); err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if !reflect.DeepEqual(store.calls, []string{"claim-release", "park-release"}) {
		t.Fatalf("store calls = %v; want one transactional blocked-release park", store.calls)
	}
	if len(queueClient.transitions) != 0 {
		t.Fatalf("queue transitions = %v; blocked release ACK belongs to the store transaction", queueClient.transitions)
	}
}

func TestSandboxReleaseRunnerReclaimsReadyReleaseAfterLockedBlockedRecheck(t *testing.T) {
	runtimeDB, adminDB := newSandboxServiceTestDB(t)
	seedSandboxExecutionStoreFixture(t, adminDB)
	now := time.Now().UTC()
	seedReadySandboxBinding(t, adminDB, now)
	if _, err := adminDB.Exec(`UPDATE session_runtime_tool_results
		SET execution_state='running', authorized_binding_revision=1,
		    authorized_provider_resource_id='provider_execution_store', preparation_deadline=NULL
		WHERE workspace_id='ws_execution_store' AND tool_use_event_id='evt_execution_a'`); err != nil {
		t.Fatalf("seed release blocker: %v", err)
	}
	settlementCtx := sandboxTestQueueContext(t, runtimeDB)
	client := dbconnect.NewClientForTesting(runtimeDB)
	if err := client.WithWorkspaceTx(context.Background(), "ws_execution_store", "test.release.runner_recheck", func(tx *dbconnect.Tx) error {
		_, _, err := EnsureSandboxReleaseTx(
			context.Background(), tx, "ws_execution_store", "sesn_execution_store",
			SandboxReleaseSessionDelete, "provider_execution_store", now,
		)
		return err
	}); err != nil {
		t.Fatalf("EnsureSandboxReleaseTx: %v", err)
	}
	releaseJob := leaseReleaseJob(t, queue.NewPostgreSQLStore(client), now, time.Minute, "release-recheck")
	heartbeatObserved := make(chan struct{}, 1)
	queueClient := &releaseRecheckQueue{
		recordingSandboxQueue: recordingSandboxQueue{leased: []*queuev1.QueueJob{releaseJob.QueueJob}},
		heartbeatObserved:     heartbeatObserved,
	}
	realStore := NewPostgreSQLSandboxLifecycleStore(client, nil, 0)
	store := &releaseRecheckStore{
		SandboxLifecycleStore: realStore,
		coordinator:           NewPostgreSQLSandboxExecutionCoordinator(client, 30*time.Minute),
		settlementCtx:         settlementCtx,
		heartbeatObserved:     heartbeatObserved,
	}
	registry, err := NewProviderRegistry(map[string]ProviderAdapter{
		sandboxdriver.DaytonaProviderName: &recordingLifecycleAdapter{
			releasePresenceSet: true,
			releasePresence:    ProviderOutcome[bool]{Value: false},
		},
	})
	if err != nil {
		t.Fatalf("NewProviderRegistry: %v", err)
	}
	runner := &SandboxReleaseJobRunner{
		Queue: queueClient, Store: store, Providers: registry,
		Config: SandboxLifecycleRunnerConfig{
			WorkspaceID: "ws_execution_store", LeaseDuration: time.Minute, HeartbeatInterval: time.Millisecond,
		},
	}
	if err := runner.RunOnce(context.Background()); err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if store.claims != 2 || store.firstAuthority == nil || store.firstAuthority != store.secondAuthority {
		t.Fatalf("release claims = %d authorities %p/%p; want two claims under one live lease", store.claims, store.firstAuthority, store.secondAuthority)
	}
	if !store.heartbeatWasLive {
		t.Fatal("release heartbeat was not live during locked blocked-to-ready recheck")
	}
	var state string
	if err := adminDB.QueryRow(`SELECT state FROM sandbox_lifecycle_operations
		WHERE workspace_id='ws_execution_store' AND queue_job_id=$1`, releaseJob.JobID).Scan(&state); err != nil {
		t.Fatalf("read release after recheck: %v", err)
	}
	if state != "completed" {
		t.Fatalf("release state = %q; want completed", state)
	}
}

func TestSandboxReleaseRunnerReauthorizesAfterProviderProvesNoReleaseStarted(t *testing.T) {
	first := sandboxReleaseQueueJob()
	second := sandboxReleaseQueueJob()
	second.AttemptCount = 2
	queueClient := &recordingSandboxQueue{leased: []*queuev1.QueueJob{first, second}}
	store := &recordingSandboxLifecycleStore{
		release: SandboxReleaseWork{
			Job: SandboxLifecycleJob{
				JobID: "qjob_release", LeaseToken: "lease_release", WorkspaceID: "ws_lifecycle",
				SessionID: "sesn_lifecycle", LogicalSandboxID: "sbox_lifecycle", OperationID: "sop_release",
			},
			Provider: sandboxdriver.DaytonaProviderName,
			Handle:   sandbox.ProviderHandle{Provider: sandboxdriver.DaytonaProviderName, SandboxID: "provider_release"},
			Reason:   SandboxReleaseSessionDelete,
		},
		current: true,
	}
	adapter := &recordingLifecycleAdapter{
		releaseSequence: []ProviderOutcome[ReleaseResult]{
			{EffectBoundary: ProviderProvedNotStarted, Disposition: ProviderRetryable, ErrorKind: "provider_transition_in_progress"},
			{Value: ReleaseResult{}},
		},
	}
	registry, err := NewProviderRegistry(map[string]ProviderAdapter{sandboxdriver.DaytonaProviderName: adapter})
	if err != nil {
		t.Fatalf("NewProviderRegistry: %v", err)
	}
	runner := &SandboxReleaseJobRunner{
		Queue: queueClient, Store: store, Providers: registry,
		Config: SandboxLifecycleRunnerConfig{WorkspaceID: "ws_lifecycle", LeaseDuration: time.Minute, HeartbeatInterval: 15 * time.Second},
	}
	if err := runner.RunOnce(context.Background()); err != nil {
		t.Fatalf("first RunOnce: %v", err)
	}
	if err := runner.RunOnce(context.Background()); err != nil {
		t.Fatalf("second RunOnce: %v", err)
	}
	if !reflect.DeepEqual(store.calls, []string{
		"claim-release", "authorize-release", "rearm-release",
		"claim-release", "authorize-release", "complete-release",
	}) {
		t.Fatalf("store calls = %v; want release authorization on both deliveries", store.calls)
	}
	if !reflect.DeepEqual(adapter.calls, []string{"inspect-release", "release", "inspect-release", "release"}) {
		t.Fatalf("adapter calls = %v; want one provider call per authorized delivery", adapter.calls)
	}
	if !reflect.DeepEqual(queueClient.transitions, []string{
		"retry:qjob_release:provider_transition_in_progress", "ack:qjob_release",
	}) {
		t.Fatalf("queue transitions = %v", queueClient.transitions)
	}
}

func TestSandboxActivationRunnerResolvesMissingStartExactlyOnce(t *testing.T) {
	tests := []struct {
		name                   string
		replacementDisposition SandboxLifecycleDisposition
		replacement            SandboxActivationWork
		wantStore              []string
	}{
		{name: "replacement no longer applicable", replacementDisposition: SandboxLifecycleNotApplicable, wantStore: []string{"claim", "replace-missing"}},
		{name: "empty replacement", wantStore: []string{"claim", "replace-missing"}},
		{name: "replacement continues", replacement: func() SandboxActivationWork {
			work := sandboxActivationTestWork(true)
			work.Kind = ActivationReplace
			return work
		}(), wantStore: []string{"claim", "replace-missing", "complete:provider_replacement"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var logs bytes.Buffer
			queueClient := &recordingSandboxQueue{leased: []*queuev1.QueueJob{sandboxActivationQueueJob()}}
			work := sandboxActivationTestWork(true)
			work.Kind = ActivationStart
			work.CurrentHandle = sandbox.ProviderHandle{Provider: sandboxdriver.DaytonaProviderName, SandboxID: "provider_deleted"}
			store := &recordingSandboxLifecycleStore{activation: work, replacement: test.replacement, replacementDisposition: test.replacementDisposition, current: true}
			adapter := &recordingLifecycleAdapter{
				inspectionSequence: []ProviderOutcome[ExecutionReadiness]{{Value: ExecutionNeedsCreation}, {Value: ExecutionReady}},
				resolution:         ProviderOutcome[ActivationResolution]{Value: ActivationResolution{Found: false}},
				activation:         ProviderOutcome[sandbox.ProviderHandle]{Value: sandbox.ProviderHandle{Provider: sandboxdriver.DaytonaProviderName, SandboxID: "provider_replacement"}},
			}
			registry, err := NewProviderRegistry(map[string]ProviderAdapter{sandboxdriver.DaytonaProviderName: adapter})
			if err != nil {
				t.Fatalf("NewProviderRegistry: %v", err)
			}
			runner := &SandboxActivationJobRunner{
				Queue: queueClient, Store: store, Providers: registry,
				Logger: slog.New(slog.NewJSONHandler(&logs, nil)),
				Config: SandboxLifecycleRunnerConfig{WorkspaceID: "ws_lifecycle", LeaseDuration: time.Minute, HeartbeatInterval: 15 * time.Second},
			}
			if err := runner.RunOnce(context.Background()); err != nil {
				t.Fatalf("RunOnce: %v", err)
			}
			if !reflect.DeepEqual(store.calls, test.wantStore) || !reflect.DeepEqual(queueClient.transitions, []string{"ack:qjob_activation"}) {
				t.Fatalf("store=%v Queue=%v; want %v and ACK", store.calls, queueClient.transitions, test.wantStore)
			}
			got := logs.String()
			if strings.Count(got, `"event":"sandbox_activation_resolved"`) != 1 || strings.Count(got, `"event":"sandbox_activation_attempt_completed"`) != 1 || !strings.Contains(got, `"resolution":"absent"`) {
				t.Fatalf("missing-start lifecycle logs = %s; want one absent resolution and one completion", got)
			}
		})
	}
}

func TestSandboxActivationRunnerRetainsObservationAfterFinalSubmissionAttempt(t *testing.T) {
	queueJob := sandboxActivationQueueJob()
	queueJob.AttemptCount = sandboxActivationSubmissionMaxAttempts
	queueJob.MaxAttempts = sandboxActivationMaxAttempts
	queueClient := &recordingSandboxQueue{leased: []*queuev1.QueueJob{queueJob}}
	store := &recordingSandboxLifecycleStore{activation: sandboxActivationTestWork(true), current: true}
	adapter := &recordingLifecycleAdapter{
		resolution: ProviderOutcome[ActivationResolution]{Value: ActivationResolution{Found: false}},
		activation: ProviderOutcome[sandbox.ProviderHandle]{
			EffectBoundary: ProviderOutcomeUnknown, Disposition: ProviderTerminal, ErrorKind: "provider_timeout",
		},
	}
	registry, err := NewProviderRegistry(map[string]ProviderAdapter{sandboxdriver.DaytonaProviderName: adapter})
	if err != nil {
		t.Fatalf("NewProviderRegistry: %v", err)
	}
	runner := &SandboxActivationJobRunner{
		Queue: queueClient, Store: store, Providers: registry,
		Config: SandboxLifecycleRunnerConfig{WorkspaceID: "ws_lifecycle", LeaseDuration: time.Minute, HeartbeatInterval: 15 * time.Second},
	}
	if err := runner.RunOnce(context.Background()); err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if !reflect.DeepEqual(store.calls, []string{"claim", "observe:provider_timeout"}) ||
		!reflect.DeepEqual(queueClient.transitions, []string{"retry:qjob_activation:provider_timeout"}) {
		t.Fatalf("store=%v queue=%v; want durable observation retry", store.calls, queueClient.transitions)
	}
}

func TestSandboxActivationRunnerReinspectsAfterAmbiguousStart(t *testing.T) {
	queueClient := &recordingSandboxQueue{leased: []*queuev1.QueueJob{sandboxActivationQueueJob()}}
	work := sandboxActivationTestWork(true)
	work.Kind = ActivationStart
	work.CurrentHandle = sandbox.ProviderHandle{Provider: sandboxdriver.DaytonaProviderName, SandboxID: "provider_stopped"}
	store := &recordingSandboxLifecycleStore{activation: work, current: true}
	adapter := &recordingLifecycleAdapter{
		inspection: ProviderOutcome[ExecutionReadiness]{Value: ExecutionNeedsActivation},
		activation: ProviderOutcome[sandbox.ProviderHandle]{
			EffectBoundary: ProviderOutcomeUnknown, Disposition: ProviderTerminal,
			ErrorKind: "provider_timeout",
		},
	}
	registry, err := NewProviderRegistry(map[string]ProviderAdapter{sandboxdriver.DaytonaProviderName: adapter})
	if err != nil {
		t.Fatalf("NewProviderRegistry: %v", err)
	}
	runner := &SandboxActivationJobRunner{
		Queue: queueClient, Store: store, Providers: registry,
		Config: SandboxLifecycleRunnerConfig{WorkspaceID: "ws_lifecycle", LeaseDuration: time.Minute, HeartbeatInterval: 15 * time.Second},
	}
	if err := runner.RunOnce(context.Background()); err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if !reflect.DeepEqual(store.calls, []string{"claim"}) {
		t.Fatalf("store calls = %v; ambiguous Start must remain observable", store.calls)
	}
	if !reflect.DeepEqual(queueClient.transitions, []string{"retry:qjob_activation:provider_timeout"}) {
		t.Fatalf("queue transitions = %v; want observation retry", queueClient.transitions)
	}
}

func TestSandboxMaterializationRunnerRechecksReadinessBeforeProjection(t *testing.T) {
	tests := []struct {
		name            string
		inspection      ProviderOutcome[ExecutionReadiness]
		materialization ProviderOutcome[MaterializationResult]
		wantAdapter     []string
		wantStore       []string
		wantTransitions []string
	}{
		{
			name:       "ready projects resources",
			inspection: ProviderOutcome[ExecutionReadiness]{Value: ExecutionReady},
			materialization: ProviderOutcome[MaterializationResult]{Value: MaterializationResult{
				MaterializedEnvironmentGeneration: 1,
				MaterializedResourceRevision:      1,
				Resources:                         sandbox.ResourceSetup{ResourceRootsJSON: `[{"path":"/mnt/session/uploads/a","mode":"read"}]`},
			}},
			wantAdapter: []string{"inspect", "materialize"}, wantStore: []string{"claim-materialization", "complete-materialization"},
			wantTransitions: []string{"ack:qjob_materialization"},
		},
		{
			name:        "cold resource rejoins activation",
			inspection:  ProviderOutcome[ExecutionReadiness]{Value: ExecutionNeedsActivation},
			wantAdapter: []string{"inspect"}, wantStore: []string{"claim-materialization", "materialization-activation:needs_activation"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			queueClient := &recordingSandboxQueue{leased: []*queuev1.QueueJob{sandboxMaterializationQueueJob()}}
			store := &recordingSandboxLifecycleStore{materialization: sandboxMaterializationTestWork(), current: true}
			adapter := &recordingLifecycleAdapter{inspection: test.inspection, materialization: test.materialization}
			registry, err := NewProviderRegistry(map[string]ProviderAdapter{sandboxdriver.DaytonaProviderName: adapter})
			if err != nil {
				t.Fatalf("NewProviderRegistry: %v", err)
			}
			runner := &SandboxMaterializationJobRunner{
				Queue: queueClient, Store: store, Providers: registry,
				Config: SandboxLifecycleRunnerConfig{WorkspaceID: "ws_lifecycle", LeaseDuration: time.Minute, HeartbeatInterval: 15 * time.Second},
			}
			if err := runner.RunOnce(context.Background()); err != nil {
				t.Fatalf("RunOnce: %v", err)
			}
			if !reflect.DeepEqual(adapter.calls, test.wantAdapter) {
				t.Fatalf("adapter calls = %v; want %v", adapter.calls, test.wantAdapter)
			}
			if !reflect.DeepEqual(store.calls, test.wantStore) {
				t.Fatalf("store calls = %v; want %v", store.calls, test.wantStore)
			}
			if !reflect.DeepEqual(queueClient.transitions, test.wantTransitions) {
				t.Fatalf("queue transitions = %v; want %v", queueClient.transitions, test.wantTransitions)
			}
		})
	}
}

func TestSandboxMaterializationRunnerMakesNoQueueTransitionWhenWaitLosesAuthority(t *testing.T) {
	queueClient := &recordingSandboxQueue{leased: []*queuev1.QueueJob{sandboxMaterializationQueueJob()}}
	store := &recordingSandboxLifecycleStore{
		materialization: sandboxMaterializationTestWork(), current: true,
		waitDisposition: SandboxLifecycleLostAuthority,
	}
	adapter := &recordingLifecycleAdapter{inspection: ProviderOutcome[ExecutionReadiness]{Value: ExecutionNeedsActivation}}
	registry, err := NewProviderRegistry(map[string]ProviderAdapter{sandboxdriver.DaytonaProviderName: adapter})
	if err != nil {
		t.Fatalf("NewProviderRegistry: %v", err)
	}
	runner := &SandboxMaterializationJobRunner{
		Queue: queueClient, Store: store, Providers: registry,
		Config: SandboxLifecycleRunnerConfig{WorkspaceID: "ws_lifecycle", LeaseDuration: time.Minute, HeartbeatInterval: 15 * time.Second},
	}
	if err := runner.RunOnce(context.Background()); !errors.Is(err, errQueueLeaseLost) {
		t.Fatalf("RunOnce error = %v; want lost authority", err)
	}
	if len(queueClient.transitions) != 0 {
		t.Fatalf("Queue transitions = %v; want none after authority loss", queueClient.transitions)
	}
}

func TestSandboxLifecycleRunnerRejectsLeaseResponseAfterLocalAuthorityWindow(t *testing.T) {
	queueClient := &recordingSandboxQueue{
		leased:     []*queuev1.QueueJob{sandboxActivationQueueJob()},
		leaseDelay: 60 * time.Millisecond,
	}
	store := &recordingSandboxLifecycleStore{}
	registry, err := NewProviderRegistry(map[string]ProviderAdapter{sandboxdriver.DaytonaProviderName: &recordingLifecycleAdapter{}})
	if err != nil {
		t.Fatalf("NewProviderRegistry: %v", err)
	}
	runner := &SandboxActivationJobRunner{
		Queue: queueClient, Store: store, Providers: registry,
		Config: SandboxLifecycleRunnerConfig{
			WorkspaceID: "ws_lifecycle", LeaseDuration: 40 * time.Millisecond, HeartbeatInterval: 10 * time.Millisecond,
		},
	}
	if err := runner.RunOnce(context.Background()); !errors.Is(err, errQueueLeaseLost) {
		t.Fatalf("RunOnce error = %v; want local lease authority loss", err)
	}
	if len(store.calls) != 0 || len(queueClient.transitions) != 0 {
		t.Fatalf("delayed Lease response reached business/Queue work = %v/%v", store.calls, queueClient.transitions)
	}
}

func TestSandboxMaterializationRunnerDoesNotReplaySubmittedFailure(t *testing.T) {
	queueClient := &recordingSandboxQueue{leased: []*queuev1.QueueJob{sandboxMaterializationQueueJob()}}
	store := &recordingSandboxLifecycleStore{materialization: sandboxMaterializationTestWork(), current: true}
	adapter := &recordingLifecycleAdapter{
		inspection: ProviderOutcome[ExecutionReadiness]{Value: ExecutionReady},
		materialization: ProviderOutcome[MaterializationResult]{
			EffectBoundary: ProviderSubmitted, Disposition: ProviderRetryable,
			ErrorKind: "provider_transport_lost", SafeMessage: "materialization outcome is not replayable",
		},
	}
	registry, err := NewProviderRegistry(map[string]ProviderAdapter{sandboxdriver.DaytonaProviderName: adapter})
	if err != nil {
		t.Fatalf("NewProviderRegistry: %v", err)
	}
	runner := &SandboxMaterializationJobRunner{
		Queue: queueClient, Store: store, Providers: registry,
		Config: SandboxLifecycleRunnerConfig{WorkspaceID: "ws_lifecycle", LeaseDuration: time.Minute, HeartbeatInterval: 15 * time.Second},
	}
	if err := runner.RunOnce(context.Background()); err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if !reflect.DeepEqual(store.calls, []string{"claim-materialization", "fail-materialization:submitted:retryable:provider_transport_lost"}) {
		t.Fatalf("store calls = %v; want submitted failure receipt", store.calls)
	}
	if !reflect.DeepEqual(queueClient.transitions, []string{"dead:qjob_materialization:provider_transport_lost"}) {
		t.Fatalf("queue transitions = %v; submitted materialization must not be retried", queueClient.transitions)
	}
}

func TestSandboxLifecycleRunnersFinalizeExhaustedAttempts(t *testing.T) {
	tests := []struct {
		name      string
		kind      string
		queueJob  *queuev1.QueueJob
		run       func(*recordingSandboxQueue, *recordingSandboxLifecycleStore, *ProviderRegistry) error
		wantStore string
		wantDead  string
	}{
		{
			name: "activation", kind: queue.KindSandboxActivate, queueJob: sandboxActivationQueueJob(),
			run: func(queueClient *recordingSandboxQueue, store *recordingSandboxLifecycleStore, registry *ProviderRegistry) error {
				return (&SandboxActivationJobRunner{Queue: queueClient, Store: store, Providers: registry,
					Config: SandboxLifecycleRunnerConfig{WorkspaceID: "ws_lifecycle", LeaseDuration: time.Minute, HeartbeatInterval: 15 * time.Second}}).RunOnce(context.Background())
			},
			wantStore: "exhausted:sandbox_activate", wantDead: "dead:qjob_activation:sandbox_activation_attempts_exhausted",
		},
		{
			name: "materialization", kind: queue.KindSandboxMaterialize, queueJob: sandboxMaterializationQueueJob(),
			run: func(queueClient *recordingSandboxQueue, store *recordingSandboxLifecycleStore, registry *ProviderRegistry) error {
				return (&SandboxMaterializationJobRunner{Queue: queueClient, Store: store, Providers: registry,
					Config: SandboxLifecycleRunnerConfig{WorkspaceID: "ws_lifecycle", LeaseDuration: time.Minute, HeartbeatInterval: 15 * time.Second}}).RunOnce(context.Background())
			},
			wantStore: "exhausted:sandbox_materialize", wantDead: "dead:qjob_materialization:sandbox_materialization_attempts_exhausted",
		},
	}
	adapter := &recordingLifecycleAdapter{}
	registry, err := NewProviderRegistry(map[string]ProviderAdapter{sandboxdriver.DaytonaProviderName: adapter})
	if err != nil {
		t.Fatalf("NewProviderRegistry: %v", err)
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			test.queueJob.AttemptCount = test.queueJob.MaxAttempts + 1
			queueClient := &recordingSandboxQueue{leased: []*queuev1.QueueJob{test.queueJob}}
			store := &recordingSandboxLifecycleStore{}
			if err := test.run(queueClient, store, registry); err != nil {
				t.Fatalf("RunOnce: %v", err)
			}
			if !reflect.DeepEqual(store.calls, []string{test.wantStore}) {
				t.Fatalf("store calls = %v; want finalizer", store.calls)
			}
			if !reflect.DeepEqual(queueClient.transitions, []string{test.wantDead}) {
				t.Fatalf("queue transitions = %v; want dead letter", queueClient.transitions)
			}
		})
	}
}

func TestSandboxLifecycleRunnersCheckAttemptBudgetBeforePayloadDecode(t *testing.T) {
	tests := []struct {
		name      string
		kind      string
		queueJob  *queuev1.QueueJob
		run       func(*recordingSandboxQueue, *recordingSandboxLifecycleStore, *ProviderRegistry) error
		wantStore string
		wantDead  string
	}{
		{
			name: "activation", kind: queue.KindSandboxActivate, queueJob: sandboxActivationQueueJob(),
			run: func(queueClient *recordingSandboxQueue, store *recordingSandboxLifecycleStore, registry *ProviderRegistry) error {
				return (&SandboxActivationJobRunner{Queue: queueClient, Store: store, Providers: registry,
					Config: SandboxLifecycleRunnerConfig{WorkspaceID: "ws_lifecycle", LeaseDuration: time.Minute, HeartbeatInterval: 15 * time.Second}}).RunOnce(context.Background())
			},
			wantStore: "exhausted:sandbox_activate", wantDead: "dead:qjob_activation:sandbox_activation_attempts_exhausted",
		},
		{
			name: "materialization", kind: queue.KindSandboxMaterialize, queueJob: sandboxMaterializationQueueJob(),
			run: func(queueClient *recordingSandboxQueue, store *recordingSandboxLifecycleStore, registry *ProviderRegistry) error {
				return (&SandboxMaterializationJobRunner{Queue: queueClient, Store: store, Providers: registry,
					Config: SandboxLifecycleRunnerConfig{WorkspaceID: "ws_lifecycle", LeaseDuration: time.Minute, HeartbeatInterval: 15 * time.Second}}).RunOnce(context.Background())
			},
			wantStore: "exhausted:sandbox_materialize", wantDead: "dead:qjob_materialization:sandbox_materialization_attempts_exhausted",
		},
	}
	adapter := &recordingLifecycleAdapter{}
	registry, err := NewProviderRegistry(map[string]ProviderAdapter{sandboxdriver.DaytonaProviderName: adapter})
	if err != nil {
		t.Fatalf("NewProviderRegistry: %v", err)
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			test.queueJob.AttemptCount = test.queueJob.MaxAttempts + 1
			test.queueJob.PayloadJson = `{not-json`
			queueClient := &recordingSandboxQueue{leased: []*queuev1.QueueJob{test.queueJob}}
			store := &recordingSandboxLifecycleStore{}
			if err := test.run(queueClient, store, registry); err != nil {
				t.Fatalf("RunOnce: %v", err)
			}
			if !reflect.DeepEqual(store.calls, []string{test.wantStore}) {
				t.Fatalf("store calls = %v; want exhaustion before payload decode", store.calls)
			}
			if !reflect.DeepEqual(queueClient.transitions, []string{test.wantDead}) {
				t.Fatalf("queue transitions = %v; want dead letter", queueClient.transitions)
			}
		})
	}
}

func TestSandboxReleaseRunnerTreatsMissingAttemptBudgetAsIntegrityFailure(t *testing.T) {
	job := sandboxReleaseQueueJob()
	job.MaxAttempts = 0
	queueClient := &recordingSandboxQueue{leased: []*queuev1.QueueJob{job}}
	store := &recordingSandboxLifecycleStore{}
	registry, err := NewProviderRegistry(map[string]ProviderAdapter{sandboxdriver.DaytonaProviderName: &recordingLifecycleAdapter{}})
	if err != nil {
		t.Fatalf("NewProviderRegistry: %v", err)
	}
	runner := &SandboxReleaseJobRunner{
		Queue: queueClient, Store: store, Providers: registry,
		Config: SandboxLifecycleRunnerConfig{WorkspaceID: "ws_lifecycle", LeaseDuration: time.Minute, HeartbeatInterval: 15 * time.Second},
	}

	if err := runner.RunOnce(context.Background()); err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if !reflect.DeepEqual(store.calls, []string{"invalid:sandbox_release"}) {
		t.Fatalf("store calls = %v; want invalid lifecycle finalization", store.calls)
	}
	if !reflect.DeepEqual(queueClient.transitions, []string{"dead:qjob_release:sandbox_queue_integrity_error"}) {
		t.Fatalf("queue transitions = %v; want integrity dead letter", queueClient.transitions)
	}
}

func TestSandboxLifecycleRunnersSettleBusinessStateBeforeDeadLetteringInvalidPayload(t *testing.T) {
	tests := []struct {
		name     string
		kind     string
		queueJob *queuev1.QueueJob
		run      func(*recordingSandboxQueue, *recordingSandboxLifecycleStore, *ProviderRegistry) error
		wantDead string
	}{
		{
			name: "activation", kind: queue.KindSandboxActivate, queueJob: sandboxActivationQueueJob(),
			run: func(queueClient *recordingSandboxQueue, store *recordingSandboxLifecycleStore, registry *ProviderRegistry) error {
				return (&SandboxActivationJobRunner{Queue: queueClient, Store: store, Providers: registry,
					Config: SandboxLifecycleRunnerConfig{WorkspaceID: "ws_lifecycle", LeaseDuration: time.Minute, HeartbeatInterval: 15 * time.Second}}).RunOnce(context.Background())
			},
			wantDead: "dead:qjob_activation:invalid_sandbox_activate_payload",
		},
		{
			name: "materialization", kind: queue.KindSandboxMaterialize, queueJob: sandboxMaterializationQueueJob(),
			run: func(queueClient *recordingSandboxQueue, store *recordingSandboxLifecycleStore, registry *ProviderRegistry) error {
				return (&SandboxMaterializationJobRunner{Queue: queueClient, Store: store, Providers: registry,
					Config: SandboxLifecycleRunnerConfig{WorkspaceID: "ws_lifecycle", LeaseDuration: time.Minute, HeartbeatInterval: 15 * time.Second}}).RunOnce(context.Background())
			},
			wantDead: "dead:qjob_materialization:invalid_sandbox_materialize_payload",
		},
	}
	registry, err := NewProviderRegistry(map[string]ProviderAdapter{sandboxdriver.DaytonaProviderName: &recordingLifecycleAdapter{}})
	if err != nil {
		t.Fatalf("NewProviderRegistry: %v", err)
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			test.queueJob.PayloadJson = `{not-json`
			queueClient := &recordingSandboxQueue{leased: []*queuev1.QueueJob{test.queueJob}}
			store := &recordingSandboxLifecycleStore{}
			if err := test.run(queueClient, store, registry); err != nil {
				t.Fatalf("RunOnce: %v", err)
			}
			if !reflect.DeepEqual(store.calls, []string{"invalid:" + test.kind}) {
				t.Fatalf("store calls = %v; want business settlement before transport closure", store.calls)
			}
			if !reflect.DeepEqual(queueClient.transitions, []string{test.wantDead}) {
				t.Fatalf("queue transitions = %v; want invalid payload dead letter", queueClient.transitions)
			}
		})
	}
}

func TestSandboxLifecycleRunnersExecuteTheLastPermittedAttempt(t *testing.T) {
	tests := []struct {
		name     string
		queueJob *queuev1.QueueJob
		run      func(*recordingSandboxQueue, *recordingSandboxLifecycleStore, *ProviderRegistry) error
		wantCall string
		wantAck  string
	}{
		{
			name: "activation", queueJob: sandboxActivationQueueJob(), wantCall: "claim", wantAck: "ack:qjob_activation",
			run: func(queueClient *recordingSandboxQueue, store *recordingSandboxLifecycleStore, registry *ProviderRegistry) error {
				return (&SandboxActivationJobRunner{Queue: queueClient, Store: store, Providers: registry,
					Config: SandboxLifecycleRunnerConfig{WorkspaceID: "ws_lifecycle", LeaseDuration: time.Minute, HeartbeatInterval: 15 * time.Second}}).RunOnce(context.Background())
			},
		},
		{
			name: "materialization", queueJob: sandboxMaterializationQueueJob(), wantCall: "claim-materialization", wantAck: "ack:qjob_materialization",
			run: func(queueClient *recordingSandboxQueue, store *recordingSandboxLifecycleStore, registry *ProviderRegistry) error {
				return (&SandboxMaterializationJobRunner{Queue: queueClient, Store: store, Providers: registry,
					Config: SandboxLifecycleRunnerConfig{WorkspaceID: "ws_lifecycle", LeaseDuration: time.Minute, HeartbeatInterval: 15 * time.Second}}).RunOnce(context.Background())
			},
		},
	}
	registry, err := NewProviderRegistry(map[string]ProviderAdapter{sandboxdriver.DaytonaProviderName: &recordingLifecycleAdapter{}})
	if err != nil {
		t.Fatalf("NewProviderRegistry: %v", err)
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			test.queueJob.AttemptCount = test.queueJob.MaxAttempts
			queueClient := &recordingSandboxQueue{leased: []*queuev1.QueueJob{test.queueJob}}
			store := &recordingSandboxLifecycleStore{current: false}
			if err := test.run(queueClient, store, registry); err != nil {
				t.Fatalf("RunOnce: %v", err)
			}
			if !reflect.DeepEqual(store.calls, []string{test.wantCall}) {
				t.Fatalf("store calls = %v; want handler at exact budget", store.calls)
			}
			if !reflect.DeepEqual(queueClient.transitions, []string{test.wantAck}) {
				t.Fatalf("queue transitions = %v; want ack", queueClient.transitions)
			}
		})
	}
}

func TestSandboxLifecycleRunnersFinalizeRetryFromLastPermittedAttempt(t *testing.T) {
	tests := []struct {
		name     string
		kind     string
		queueJob *queuev1.QueueJob
		run      func(*recordingSandboxQueue, *recordingSandboxLifecycleStore, *ProviderRegistry) error
		wantDead string
	}{
		{
			name: "activation", kind: queue.KindSandboxActivate, queueJob: sandboxActivationQueueJob(),
			run: func(queueClient *recordingSandboxQueue, store *recordingSandboxLifecycleStore, registry *ProviderRegistry) error {
				return (&SandboxActivationJobRunner{Queue: queueClient, Store: store, Providers: registry,
					Config: SandboxLifecycleRunnerConfig{WorkspaceID: "ws_lifecycle", LeaseDuration: time.Minute, HeartbeatInterval: 10 * time.Second}}).RunOnce(context.Background())
			},
			wantDead: "dead:qjob_activation:sandbox_activation_attempts_exhausted",
		},
		{
			name: "materialization", kind: queue.KindSandboxMaterialize, queueJob: sandboxMaterializationQueueJob(),
			run: func(queueClient *recordingSandboxQueue, store *recordingSandboxLifecycleStore, registry *ProviderRegistry) error {
				return (&SandboxMaterializationJobRunner{Queue: queueClient, Store: store, Providers: registry,
					Config: SandboxLifecycleRunnerConfig{WorkspaceID: "ws_lifecycle", LeaseDuration: time.Minute, HeartbeatInterval: 10 * time.Second}}).RunOnce(context.Background())
			},
			wantDead: "dead:qjob_materialization:sandbox_materialization_attempts_exhausted",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			test.queueJob.AttemptCount = test.queueJob.MaxAttempts
			queueClient := &recordingSandboxQueue{leased: []*queuev1.QueueJob{test.queueJob}}
			store := &recordingSandboxLifecycleStore{activation: sandboxActivationTestWork(false), materialization: sandboxMaterializationTestWork(), current: true}
			adapter := &recordingLifecycleAdapter{
				resolution: ProviderOutcome[ActivationResolution]{EffectBoundary: ProviderProvedNotStarted, Disposition: ProviderRetryable, ErrorKind: "provider_transition_in_progress"},
				inspection: ProviderOutcome[ExecutionReadiness]{EffectBoundary: ProviderProvedNotStarted, Disposition: ProviderRetryable, ErrorKind: "provider_transition_in_progress"},
			}
			registry, err := NewProviderRegistry(map[string]ProviderAdapter{sandboxdriver.DaytonaProviderName: adapter})
			if err != nil {
				t.Fatalf("NewProviderRegistry: %v", err)
			}
			if err := test.run(queueClient, store, registry); err != nil {
				t.Fatalf("RunOnce: %v", err)
			}
			wantStore := "exhausted:" + test.kind
			if store.calls[len(store.calls)-1] != wantStore {
				t.Fatalf("store calls = %v; want final call %q", store.calls, wantStore)
			}
			if !reflect.DeepEqual(queueClient.transitions, []string{test.wantDead}) {
				t.Fatalf("queue transitions = %v; want dead letter after business finalization", queueClient.transitions)
			}
		})
	}
}

func TestSandboxActivationRunnerMakesNoQueueTransitionAfterFinalizerLosesAuthority(t *testing.T) {
	cases := map[string]func(*queuev1.QueueJob, *recordingSandboxLifecycleStore){
		"invalid_budget": func(job *queuev1.QueueJob, _ *recordingSandboxLifecycleStore) {
			job.MaxAttempts = 0
		},
		"over_budget": func(job *queuev1.QueueJob, _ *recordingSandboxLifecycleStore) {
			job.AttemptCount = job.MaxAttempts + 1
		},
		"retry_at_budget": func(job *queuev1.QueueJob, store *recordingSandboxLifecycleStore) {
			job.AttemptCount = job.MaxAttempts
			store.current = true
			store.activation = sandboxActivationTestWork(false)
		},
	}
	for name, arrange := range cases {
		t.Run(name, func(t *testing.T) {
			job := sandboxActivationQueueJob()
			store := &recordingSandboxLifecycleStore{finalizer: SandboxLifecycleLostAuthority}
			arrange(job, store)
			queueClient := &recordingSandboxQueue{leased: []*queuev1.QueueJob{job}}
			adapter := &recordingLifecycleAdapter{
				resolution: ProviderOutcome[ActivationResolution]{EffectBoundary: ProviderProvedNotStarted, Disposition: ProviderRetryable, ErrorKind: "provider_transition_in_progress"},
			}
			registry, err := NewProviderRegistry(map[string]ProviderAdapter{sandboxdriver.DaytonaProviderName: adapter})
			if err != nil {
				t.Fatalf("NewProviderRegistry: %v", err)
			}
			runner := &SandboxActivationJobRunner{
				Queue: queueClient, Store: store, Providers: registry,
				Config: SandboxLifecycleRunnerConfig{WorkspaceID: "ws_lifecycle", LeaseDuration: time.Minute, HeartbeatInterval: 10 * time.Second},
			}
			if err := runner.RunOnce(context.Background()); !errors.Is(err, errQueueLeaseLost) {
				t.Fatalf("RunOnce error = %v; want lost authority", err)
			}
			if len(queueClient.transitions) != 0 {
				t.Fatalf("Queue transitions = %v; want none after authority loss", queueClient.transitions)
			}
			if name != "retry_at_budget" && len(adapter.calls) != 0 {
				t.Fatalf("provider calls = %v; want none before rejected finalizer", adapter.calls)
			}
		})
	}
}

func sandboxActivationTestWork(mayCreate bool) SandboxActivationWork {
	return SandboxActivationWork{
		Job:  SandboxLifecycleJob{WorkspaceID: "ws_lifecycle", SessionID: "sesn_lifecycle", LogicalSandboxID: "sbox_lifecycle", OperationID: "sop_activation"},
		Kind: ActivationCreate, Provider: sandboxdriver.DaytonaProviderName,
		StableName: "sbox_lifecycle", Labels: map[string]string{"tetral.workspace_id": "ws_lifecycle"},
		MayCreate: mayCreate,
		Setup:     sandbox.SandboxSetup{WorkspaceID: "ws_lifecycle", SessionID: "sesn_lifecycle", SandboxID: "sbox_lifecycle", LifecycleOperationID: "sop_activation", EnvironmentID: "env_lifecycle", ProviderArtifactRef: "artifact_lifecycle"},
	}
}

func sandboxMaterializationTestWork() SandboxMaterializationWork {
	return SandboxMaterializationWork{
		Job:             SandboxLifecycleJob{WorkspaceID: "ws_lifecycle", SessionID: "sesn_lifecycle", LogicalSandboxID: "sbox_lifecycle", OperationID: "sop_materialization"},
		Provider:        sandboxdriver.DaytonaProviderName,
		Handle:          sandbox.ProviderHandle{Provider: sandboxdriver.DaytonaProviderName, SandboxID: "provider_lifecycle"},
		BindingRevision: 1, TargetEnvironmentGeneration: 1, TargetResourceRevision: 1,
		Setup: sandbox.SandboxSetup{WorkspaceID: "ws_lifecycle", SessionID: "sesn_lifecycle", SandboxID: "sbox_lifecycle",
			LifecycleOperationID: "sop_materialization", EnvironmentID: "env_lifecycle", EnvironmentGeneration: 1, ResourceRevision: 1},
	}
}

func sandboxActivationQueueJob() *queuev1.QueueJob {
	return &queuev1.QueueJob{
		Id: "qjob_activation", WorkspaceId: "ws_lifecycle", Kind: queue.KindSandboxActivate,
		PartitionKey: queue.FormatSandboxLifecyclePartitionKey("ws_lifecycle", "sbox_lifecycle"),
		DedupeKey:    queue.FormatSandboxLifecycleDedupeKey(queue.KindSandboxActivate, "ws_lifecycle", "sbox_lifecycle", "sop_activation"),
		PayloadJson:  `{"workspace_id":"ws_lifecycle","session_id":"sesn_lifecycle","logical_sandbox_id":"sbox_lifecycle","operation_id":"sop_activation"}`,
		LeaseToken:   "lease_activation", AttemptCount: 1, MaxAttempts: 5,
	}
}

func sandboxMaterializationQueueJob() *queuev1.QueueJob {
	return &queuev1.QueueJob{
		Id: "qjob_materialization", WorkspaceId: "ws_lifecycle", Kind: queue.KindSandboxMaterialize,
		PartitionKey: queue.FormatSandboxLifecyclePartitionKey("ws_lifecycle", "sbox_lifecycle"),
		DedupeKey:    queue.FormatSandboxLifecycleDedupeKey(queue.KindSandboxMaterialize, "ws_lifecycle", "sbox_lifecycle", "sop_materialization"),
		PayloadJson:  `{"workspace_id":"ws_lifecycle","session_id":"sesn_lifecycle","logical_sandbox_id":"sbox_lifecycle","operation_id":"sop_materialization"}`,
		LeaseToken:   "lease_materialization", AttemptCount: 1, MaxAttempts: 5,
	}
}

func sandboxReleaseQueueJob() *queuev1.QueueJob {
	return &queuev1.QueueJob{
		Id: "qjob_release", WorkspaceId: "ws_lifecycle", Kind: queue.KindSandboxRelease,
		PartitionKey: queue.FormatSandboxLifecyclePartitionKey("ws_lifecycle", "sbox_lifecycle"),
		DedupeKey:    queue.FormatSandboxLifecycleDedupeKey(queue.KindSandboxRelease, "ws_lifecycle", "sbox_lifecycle", "sop_release"),
		PayloadJson:  `{"workspace_id":"ws_lifecycle","session_id":"sesn_lifecycle","logical_sandbox_id":"sbox_lifecycle","operation_id":"sop_release"}`,
		LeaseToken:   "lease_release", AttemptCount: 1, MaxAttempts: 5,
	}
}

type recordingSandboxLifecycleStore struct {
	activation             SandboxActivationWork
	replacement            SandboxActivationWork
	replacementDisposition SandboxLifecycleDisposition
	materialization        SandboxMaterializationWork
	release                SandboxReleaseWork
	current                bool
	disposition            SandboxLifecycleDisposition
	finalizer              SandboxLifecycleDisposition
	waitDisposition        SandboxLifecycleDisposition
	claimReleaseErr        error
	claimActivationErr     error
	calls                  []string
}

type releaseRecheckQueue struct {
	recordingSandboxQueue
	heartbeatObserved chan<- struct{}
}

func (q *releaseRecheckQueue) Heartbeat(ctx context.Context, request *queuev1.HeartbeatRequest) (*queuev1.HeartbeatResponse, error) {
	select {
	case q.heartbeatObserved <- struct{}{}:
	default:
	}
	return q.recordingSandboxQueue.Heartbeat(ctx, request)
}

type releaseRecheckStore struct {
	SandboxLifecycleStore
	coordinator       *PostgreSQLSandboxExecutionCoordinator
	settlementCtx     context.Context
	heartbeatObserved <-chan struct{}
	claims            int
	firstAuthority    *sandboxQueueAuthority
	secondAuthority   *sandboxQueueAuthority
	heartbeatWasLive  bool
}

func (s *releaseRecheckStore) ClaimRelease(ctx context.Context, job SandboxLifecycleJob, now time.Time) (SandboxReleaseWork, SandboxLifecycleDisposition, error) {
	s.claims++
	switch s.claims {
	case 1:
		s.firstAuthority = sandboxQueueAuthorityFromContext(ctx)
	case 2:
		s.secondAuthority = sandboxQueueAuthorityFromContext(ctx)
	}
	return s.SandboxLifecycleStore.ClaimRelease(ctx, job, now)
}

func (s *releaseRecheckStore) ParkBlockedRelease(ctx context.Context, job SandboxLifecycleJob, now time.Time) (SandboxLifecycleDisposition, error) {
	select {
	case <-s.heartbeatObserved:
		s.heartbeatWasLive = true
	case <-time.After(time.Second):
		return SandboxLifecycleNotApplicable, errors.New("release heartbeat was not observed before blocked recheck")
	}
	if err := s.coordinator.SettleExecution(s.settlementCtx, SandboxExecutionWork{
		Ref: SandboxExecutionRef{
			WorkspaceID: "ws_execution_store", SessionID: "sesn_execution_store",
			SessionThreadID: "thr_execution_store", ToolUseEventID: "evt_execution_a",
		},
		AttemptGeneration: 1,
	}, SandboxExecutionSettlement{
		Kind: SandboxExecutionFailed, ErrorKind: "cancelled", SafeMessage: "sandbox execution was cancelled",
	}); err != nil {
		return SandboxLifecycleNotApplicable, err
	}
	return s.SandboxLifecycleStore.ParkBlockedRelease(ctx, job, now)
}

func (s *recordingSandboxLifecycleStore) finalizerDisposition() SandboxLifecycleDisposition {
	if s.finalizer != "" {
		return s.finalizer
	}
	return s.lifecycleDisposition()
}

func (s *recordingSandboxLifecycleStore) lifecycleDisposition() SandboxLifecycleDisposition {
	if s.disposition != "" {
		return s.disposition
	}
	if s.current {
		return SandboxLifecycleApplied
	}
	return SandboxLifecycleNotApplicable
}

func (s *recordingSandboxLifecycleStore) ClaimActivation(context.Context, SandboxLifecycleJob, time.Time) (SandboxActivationWork, SandboxLifecycleDisposition, error) {
	s.calls = append(s.calls, "claim")
	return s.activation, s.lifecycleDisposition(), s.claimActivationErr
}
func (s *recordingSandboxLifecycleStore) CompleteActivation(_ context.Context, _ SandboxActivationWork, handle sandbox.ProviderHandle, _ time.Time) (SandboxLifecycleDisposition, error) {
	s.calls = append(s.calls, "complete:"+handle.SandboxID)
	return s.lifecycleDisposition(), nil
}
func (s *recordingSandboxLifecycleStore) ReplaceMissingActivation(_ context.Context, _ SandboxActivationWork, _ time.Time) (SandboxActivationWork, SandboxLifecycleDisposition, error) {
	s.calls = append(s.calls, "replace-missing")
	if s.replacementDisposition != "" {
		return s.replacement, s.replacementDisposition, nil
	}
	return s.replacement, s.lifecycleDisposition(), nil
}
func (s *recordingSandboxLifecycleStore) ObserveUnknownActivation(_ context.Context, _ SandboxActivationWork, kind string, _ time.Time) (SandboxLifecycleDisposition, error) {
	s.calls = append(s.calls, "observe:"+kind)
	return s.lifecycleDisposition(), nil
}
func (s *recordingSandboxLifecycleStore) FailActivation(_ context.Context, _ SandboxActivationWork, boundary ProviderEffectBoundary, _ ProviderDisposition, kind string, _ string, _ time.Time) (SandboxLifecycleDisposition, error) {
	s.calls = append(s.calls, "fail:"+string(boundary)+":"+kind)
	return s.lifecycleDisposition(), nil
}
func (s *recordingSandboxLifecycleStore) ClaimMaterialization(context.Context, SandboxLifecycleJob, time.Time) (SandboxMaterializationWork, SandboxLifecycleDisposition, error) {
	s.calls = append(s.calls, "claim-materialization")
	return s.materialization, s.lifecycleDisposition(), nil
}
func (s *recordingSandboxLifecycleStore) WaitMaterializationForActivation(_ context.Context, _ SandboxMaterializationWork, readiness ExecutionReadiness, _ time.Time) (SandboxLifecycleDisposition, error) {
	s.calls = append(s.calls, "materialization-activation:"+string(readiness))
	if s.waitDisposition != "" {
		return s.waitDisposition, nil
	}
	return s.lifecycleDisposition(), nil
}
func (s *recordingSandboxLifecycleStore) CompleteMaterialization(_ context.Context, _ SandboxMaterializationWork, _ MaterializationResult, _ time.Time) error {
	s.calls = append(s.calls, "complete-materialization")
	return nil
}
func (s *recordingSandboxLifecycleStore) FailMaterialization(_ context.Context, _ SandboxMaterializationWork, boundary ProviderEffectBoundary, disposition ProviderDisposition, kind string, _ string, _ time.Time) error {
	s.calls = append(s.calls, "fail-materialization:"+string(boundary)+":"+string(disposition)+":"+kind)
	return nil
}
func (s *recordingSandboxLifecycleStore) ClaimRelease(context.Context, SandboxLifecycleJob, time.Time) (SandboxReleaseWork, SandboxLifecycleDisposition, error) {
	s.calls = append(s.calls, "claim-release")
	return s.release, s.lifecycleDisposition(), s.claimReleaseErr
}
func (s *recordingSandboxLifecycleStore) ParkBlockedRelease(context.Context, SandboxLifecycleJob, time.Time) (SandboxLifecycleDisposition, error) {
	s.calls = append(s.calls, "park-release")
	return SandboxLifecycleApplied, nil
}
func (s *recordingSandboxLifecycleStore) AuthorizeRelease(context.Context, SandboxReleaseWork, time.Time) (bool, error) {
	s.calls = append(s.calls, "authorize-release")
	return true, nil
}
func (s *recordingSandboxLifecycleStore) RearmRelease(context.Context, SandboxReleaseWork, time.Time) error {
	s.calls = append(s.calls, "rearm-release")
	return nil
}
func (s *recordingSandboxLifecycleStore) CompleteRelease(context.Context, SandboxReleaseWork, time.Time) error {
	s.calls = append(s.calls, "complete-release")
	return nil
}
func (s *recordingSandboxLifecycleStore) ObserveUnknownRelease(context.Context, SandboxReleaseWork, string, time.Time) error {
	s.calls = append(s.calls, "observe-release")
	return nil
}
func (s *recordingSandboxLifecycleStore) FailRelease(_ context.Context, _ SandboxReleaseWork, boundary ProviderEffectBoundary, _ ProviderDisposition, kind string, _ string, _ time.Time) error {
	s.calls = append(s.calls, "fail-release:"+string(boundary)+":"+kind)
	return nil
}
func (s *recordingSandboxLifecycleStore) FinalizeExhaustedLifecycle(_ context.Context, _ *queuev1.QueueJob, kind string, _ time.Time) (SandboxLifecycleDisposition, error) {
	s.calls = append(s.calls, "exhausted:"+kind)
	return s.finalizerDisposition(), nil
}
func (s *recordingSandboxLifecycleStore) FinalizeInvalidLifecycle(_ context.Context, _ *queuev1.QueueJob, kind string, _ time.Time) (SandboxLifecycleDisposition, error) {
	s.calls = append(s.calls, "invalid:"+kind)
	return s.finalizerDisposition(), nil
}

type recordingLifecycleAdapter struct {
	resolution         ProviderOutcome[ActivationResolution]
	activation         ProviderOutcome[sandbox.ProviderHandle]
	inspection         ProviderOutcome[ExecutionReadiness]
	inspectionSequence []ProviderOutcome[ExecutionReadiness]
	releasePresence    ProviderOutcome[bool]
	releasePresenceSet bool
	materialization    ProviderOutcome[MaterializationResult]
	releaseSequence    []ProviderOutcome[ReleaseResult]
	calls              []string
}

func (a *recordingLifecycleAdapter) ResolveActivation(context.Context, ActivationResolutionRequest) ProviderOutcome[ActivationResolution] {
	a.calls = append(a.calls, "resolve")
	return a.resolution
}
func (a *recordingLifecycleAdapter) Activate(_ context.Context, request ActivationRequest) ProviderOutcome[sandbox.ProviderHandle] {
	a.calls = append(a.calls, "activate:"+string(request.Kind))
	return a.activation
}
func (a *recordingLifecycleAdapter) InspectForExecution(context.Context, string) ProviderOutcome[ExecutionReadiness] {
	a.calls = append(a.calls, "inspect")
	if len(a.inspectionSequence) > 0 {
		outcome := a.inspectionSequence[0]
		a.inspectionSequence = a.inspectionSequence[1:]
		return outcome
	}
	return a.inspection
}
func (a *recordingLifecycleAdapter) InspectForRelease(context.Context, string) ProviderOutcome[bool] {
	a.calls = append(a.calls, "inspect-release")
	if !a.releasePresenceSet {
		return ProviderOutcome[bool]{Value: true}
	}
	return a.releasePresence
}
func (a *recordingLifecycleAdapter) MaterializeResources(context.Context, MaterializationRequest) ProviderOutcome[MaterializationResult] {
	a.calls = append(a.calls, "materialize")
	return a.materialization
}
func (a *recordingLifecycleAdapter) PrepareTool(context.Context, ToolExecutionRequest) ProviderOutcome[ToolPreparationResult] {
	return ProviderOutcome[ToolPreparationResult]{Value: ToolPreparationResult{Prepared: recordingPreparedTool{}}}
}
func (a *recordingLifecycleAdapter) ExecuteTool(context.Context, ToolExecutionRequest) ProviderOutcome[sandboxdriver.ToolExecution] {
	return ProviderOutcome[sandboxdriver.ToolExecution]{}
}
func (a *recordingLifecycleAdapter) ObserveTool(context.Context, sandboxdriver.ForegroundCommandObservation) ProviderOutcome[sandboxdriver.ToolExecution] {
	return ProviderOutcome[sandboxdriver.ToolExecution]{}
}
func (a *recordingLifecycleAdapter) Release(context.Context, ReleaseRequest) ProviderOutcome[ReleaseResult] {
	a.calls = append(a.calls, "release")
	if len(a.releaseSequence) > 0 {
		outcome := a.releaseSequence[0]
		a.releaseSequence = a.releaseSequence[1:]
		return outcome
	}
	return ProviderOutcome[ReleaseResult]{}
}
