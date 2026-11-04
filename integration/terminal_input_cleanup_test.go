package integration

import (
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/queue"
	"github.com/tetral-ai/tetral/internal/runtimecontrol"
	"github.com/tetral-ai/tetral/internal/sessionevent"
	"github.com/tetral-ai/tetral/internal/workspace"
	agentruntimev1 "github.com/tetral-ai/tetral/services/agent-runtime/gen/tetral/agent_runtime/v1"
	agentruntimebridge "github.com/tetral-ai/tetral/services/bridge"
	tetralcleanup "github.com/tetral-ai/tetral/services/cleanup"
	jobrunner "github.com/tetral-ai/tetral/services/job-runner"
)

func TestTerminalInputSettlementRestoresIdleCleanup(t *testing.T) {
	for _, outcome := range []string{"discovery failure", "context load failure", "delivery exhausted", "idle interrupt", "delivery failure after consumed mail", "message then interrupt", "accepted message then interrupt", "fresh Session delivery exhausted"} {
		t.Run(outcome, func(t *testing.T) {
			f := newInputRedeliveryFixture(t, "sesn_terminal_cleanup")
			var binding runtimecontrol.Binding
			fresh := outcome == "fresh Session delivery exhausted"
			if !fresh {
				binding = f.bind("bind_terminal_cleanup")
				setInputSessionIdle(t, f)
			}
			if outcome == "delivery failure after consumed mail" {
				seedAcceptedCleanupMail(f, true)
			}
			api := dbconnect.NewClientForTesting(f.workloads.OpenWorkload(t, "api", nil))
			events := sessionevent.NewService(sessionevent.NewPostgreSQLStore(api))
			input := sessionevent.IncomingEvent{Type: sessionevent.EventTypeUserMessage, Content: []sessionevent.ContentBlock{{Type: sessionevent.ContentBlockTypeText, Text: "hello"}}}
			if outcome == "idle interrupt" {
				input = sessionevent.IncomingEvent{Type: sessionevent.EventTypeUserInterrupt}
			}
			appended, err := events.AppendClientEvents(f.ctx, workspace.DefaultID, f.sessionID, "terminal-input", sessionevent.AppendRequest{Events: []sessionevent.IncomingEvent{input}})
			if err != nil {
				t.Fatal(err)
			}
			cancelMessage := outcome == "message then interrupt" || outcome == "accepted message then interrupt"
			if cancelMessage {
				if outcome == "accepted message then interrupt" {
					if got := f.deliverAll(); got != 1 {
						t.Fatalf("message delivery = %d; want 1", got)
					}
				}
				if _, err := events.AppendClientEvents(f.ctx, workspace.DefaultID, f.sessionID, "terminal-interrupt", sessionevent.AppendRequest{Events: []sessionevent.IncomingEvent{{Type: sessionevent.EventTypeUserInterrupt}}}); err != nil {
					t.Fatal(err)
				}
			}
			var cleared bool
			if err := f.admin.QueryRowContext(f.ctx, `SELECT cleanup_after IS NULL FROM session_runtime_status WHERE workspace_id='default' AND session_id=$1`, f.sessionID).Scan(&cleared); err != nil || !cleared {
				t.Fatalf("admission cleared timer = %t/%v", cleared, err)
			}
			if outcome == "idle interrupt" || cancelMessage {
				bridgeStore := agentruntimebridge.NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(f.workloads.OpenWorkload(t, "bridge", nil)))
				bridgeStore.RuntimeBindingTokenHMACKey = []byte("idle-interrupt-cleanup-signing-key")
				f.runner.Deliverer = jobrunner.RuntimePodDirectDeliverer{Store: f.store, Sender: &committingInterruptRuntimeSender{recordingRuntimeCommandSender: f.sender, bridge: bridgeStore}}
				f.deliverAll()
			} else if fresh {
				f.exec(`UPDATE queue_jobs SET max_attempts=1 WHERE workspace_id='default' AND payload_json::jsonb->>'session_id'=$1 AND kind='runtime_input'`, f.sessionID)
				f.sender.result = jobrunner.RuntimeDeliveryResult{Status: jobrunner.RuntimeDeliveryRejected, Retryable: true, ErrorKind: "runtime_busy"}
				if got := f.deliverAll(); got != 1 {
					t.Fatalf("first delivery = %d; want 1", got)
				}
				f.sender.result = jobrunner.RuntimeDeliveryResult{Status: jobrunner.RuntimeDeliveryAccepted}
			} else {
				leased, err := f.queue.Lease(f.ctx, queue.LeaseRequest{WorkspaceID: workspace.DefaultID, Kinds: []string{queue.KindRuntimeInput}, LeaseOwner: "terminal-cleanup", MaxJobs: 1, LeaseDuration: time.Minute})
				if err != nil || len(leased) != 1 {
					t.Fatalf("lease input = %v/%v", leased, err)
				}
				job, err := jobrunner.DecodeRuntimeJob(queueJobProto(leased[0]))
				if err != nil {
					t.Fatal(err)
				}
				if outcome == "discovery failure" {
					f.exec(`UPDATE sessions SET installed_tools_json=$2 WHERE workspace_id='default' AND id=$1`, f.sessionID,
						`{"tools":[{"type":"tetral_agent_toolset","family":"claude"},{"type":"mcp_toolset","mcp_server_name":"configured"}],"mcp_servers":[{"type":"url","name":"configured","url":"https://example.test/mcp"}]}`)
					f.store.MCPManifestLister = &recordingMCPManifestLister{err: errors.New("discovery unavailable")}
					_, err = f.store.PrepareRuntimeCommand(f.ctx, job)
					var preparation runtimecontrol.PreparationError
					if !errors.As(err, &preparation) || preparation.Retryable {
						t.Fatalf("discovery result = %v; want terminal failure", err)
					}
				} else {
					if _, err := f.store.PrepareRuntimeCommand(f.ctx, job); err != nil {
						t.Fatal(err)
					}
					result := retryableExhaustionResultForBinding(binding.BindingID, 1, binding.PodUID)
					if outcome == "context load failure" {
						result.Retryable = false
						result.ErrorKind = "context_load_failed"
					}
					finalized, err := f.store.FinalizeRuntimeDelivery(f.ctx, job, result)
					if err != nil || finalized.Retryable {
						t.Fatalf("terminal delivery = %v/%v", finalized, err)
					}
				}
				// Manual finalization stops before Job Runner's Queue transition.
				settled, err := f.queue.DeadLetter(f.ctx, queue.DeadLetterRequest{WorkspaceID: workspace.DefaultID, JobID: job.JobID, LeaseToken: job.LeaseToken, ErrorKind: "runtime_delivery_exhausted", ErrorMessage: "terminal fixture delivery"})
				if err != nil || !settled {
					t.Fatalf("terminal Queue settlement = %t/%v", settled, err)
				}
			}
			if cancelMessage {
				assertInputProcessedRevision(t, f, appended.Data[0].ID, true)
			}
			var cleanupAfter sql.NullTime
			var idleEvents int
			if err := f.admin.QueryRowContext(f.ctx, `SELECT cleanup_after,
				(SELECT count(*) FROM session_events WHERE workspace_id='default' AND session_id=$1 AND session_thread_id IS NULL AND type='session.status_idle')
				FROM session_runtime_status WHERE workspace_id='default' AND session_id=$1`, f.sessionID).Scan(&cleanupAfter, &idleEvents); err != nil {
				t.Fatal(err)
			}
			if !cleanupAfter.Valid || idleEvents != 1 {
				t.Fatalf("terminal cleanup timer %v, aggregate idle events %d; want timer and exactly one aggregate idle", cleanupAfter, idleEvents)
			}
			if fresh {
				var stopReason string
				var afterError bool
				if err := f.admin.QueryRowContext(f.ctx, `SELECT idle.payload_json::jsonb->'stop_reason'->>'type',
					idle.insert_stream_position > (SELECT max(insert_stream_position) FROM session_events WHERE workspace_id='default' AND session_id=$1 AND type='session.error')
					FROM session_events idle WHERE idle.workspace_id='default' AND idle.session_id=$1 AND idle.session_thread_id IS NULL AND idle.type='session.status_idle'`, f.sessionID).Scan(&stopReason, &afterError); err != nil {
					t.Fatal(err)
				}
				if stopReason != "end_turn" || !afterError {
					t.Fatalf("first idle = %s after error %t", stopReason, afterError)
				}
			}
			// Repeated settlement preserves the deadline and the one idle fence.
			if err := f.store.Client.WithWorkspaceTx(f.ctx, "default", "test.repeat_cleanup_settlement", func(tx *dbconnect.Tx) error {
				if err := runtimecontrol.LockRuntimeMutationSessionTx(f.ctx, tx, "default", f.sessionID); err != nil {
					return err
				}
				return runtimecontrol.EnsureIdleCleanupScheduledTx(f.ctx, tx, "default", f.sessionID, time.Now().Add(time.Minute))
			}); err != nil {
				t.Fatal(err)
			}
			var unchanged bool
			if err := f.admin.QueryRowContext(f.ctx, `SELECT cleanup_after=$2 AND
				(SELECT count(*) FROM session_events WHERE workspace_id='default' AND session_id=$1 AND session_thread_id IS NULL AND type='session.status_idle')=1
				FROM session_runtime_status WHERE workspace_id='default' AND session_id=$1`, f.sessionID, cleanupAfter.Time).Scan(&unchanged); err != nil || !unchanged {
				t.Fatalf("repeat settlement unchanged=%t/%v", unchanged, err)
			}
			// Advance the persisted deadline instead of waiting for the idle interval.
			f.exec(`UPDATE session_runtime_status SET cleanup_after=clock_timestamp()-interval '1 second' WHERE workspace_id='default' AND session_id=$1`, f.sessionID)
			cleanup := dbconnect.NewClientForTesting(f.workloads.OpenWorkload(t, "cleanup", nil))
			result, err := tetralcleanup.NewScheduler(cleanup, 10).RunSchedulingPhase(f.ctx)
			if err != nil || result.Claimed != 1 {
				t.Fatalf("cleanup scheduling = %+v/%v; want one claim", result, err)
			}
			var jobs int
			if err := f.admin.QueryRowContext(f.ctx, `SELECT count(*) FROM session_runtime_status s JOIN queue_jobs j
				ON j.workspace_id=s.workspace_id AND j.dedupe_key='cleanup_session:default:'||s.session_id||':'||s.cleanup_job_id
				WHERE s.workspace_id='default' AND s.session_id=$1 AND j.status='pending'`, f.sessionID).Scan(&jobs); err != nil || jobs != 1 {
				t.Fatalf("scheduled cleanup jobs = %d/%v", jobs, err)
			}
			for i := 0; i < 8 && f.deliverAll() > 0; i++ {
			}
			var bindings int
			if err := f.admin.QueryRowContext(f.ctx, `SELECT count(*) FROM session_runtime_bindings WHERE workspace_id='default' AND session_id=$1`, f.sessionID).Scan(&bindings); err != nil || bindings != 0 {
				t.Fatalf("bindings after cleanup = %d/%v; want zero", bindings, err)
			}

		})
	}
}

func setInputSessionIdle(t *testing.T, f *inputRedeliveryFixture) {
	t.Helper()
	for _, state := range []string{"running", "idle"} {
		if err := f.store.Client.WithWorkspaceTx(f.ctx, "default", "test.idle_transition", func(tx *dbconnect.Tx) error {
			if err := runtimecontrol.LockRuntimeMutationSessionTx(f.ctx, tx, "default", f.sessionID); err != nil {
				return err
			}
			if _, err := tx.Exec(f.ctx, `UPDATE session_threads SET status=$3 WHERE workspace_id='default' AND session_id=$1 AND id=$2`, f.sessionID, f.mainID, state); err != nil {
				return err
			}
			return runtimecontrol.RefreshSessionStatusTx(f.ctx, tx, "default", f.sessionID, time.Now().Add(-time.Hour))
		}); err != nil {
			t.Fatal(err)
		}
	}
}

func TestIdleCleanupSchedulingPreservesExistingOwnership(t *testing.T) {
	for _, state := range []string{"existing deadline", "claimed cleanup", "new message", "unconsumed mail"} {
		t.Run(state, func(t *testing.T) {
			f := newInputRedeliveryFixture(t, "sesn_cleanup_ownership")
			f.bind("bind_cleanup_ownership")
			setInputSessionIdle(t, f)
			if state == "unconsumed mail" {
				seedAcceptedCleanupMail(f, false)
				f.exec(`UPDATE session_runtime_status SET cleanup_after=NULL WHERE workspace_id='default' AND session_id=$1`, f.sessionID)
			}
			if state == "claimed cleanup" {
				f.exec(`UPDATE session_runtime_status SET cleanup_after=NULL,cleanup_job_id='cleanup_claimed',cleanup_enqueued_at=clock_timestamp(),cleanup_claimed_at=clock_timestamp() WHERE workspace_id='default' AND session_id=$1`, f.sessionID)
			}
			if state == "new message" {
				events := sessionevent.NewService(sessionevent.NewPostgreSQLStore(dbconnect.NewClientForTesting(f.workloads.OpenWorkload(t, "api", nil))))
				if _, err := events.AppendClientEvents(f.ctx, workspace.DefaultID, f.sessionID, "new-input", sessionevent.AppendRequest{Events: []sessionevent.IncomingEvent{{Type: sessionevent.EventTypeUserMessage, Content: []sessionevent.ContentBlock{{Type: sessionevent.ContentBlockTypeText, Text: "next"}}}}}); err != nil {
					t.Fatal(err)
				}
			}
			read := func() string {
				t.Helper()
				var value string
				if err := f.admin.QueryRowContext(f.ctx, `SELECT json_build_array(cleanup_after,cleanup_job_id,cleanup_enqueued_at,cleanup_claimed_at)::text FROM session_runtime_status WHERE workspace_id='default' AND session_id=$1`, f.sessionID).Scan(&value); err != nil {
					t.Fatal(err)
				}
				return value
			}
			before := read()
			if err := f.store.Client.WithWorkspaceTx(f.ctx, "default", "test.cleanup_timer", func(tx *dbconnect.Tx) error {
				if err := runtimecontrol.LockRuntimeMutationSessionTx(f.ctx, tx, "default", f.sessionID); err != nil {
					return err
				}
				return runtimecontrol.EnsureIdleCleanupScheduledTx(f.ctx, tx, "default", f.sessionID, time.Now())
			}); err != nil {
				t.Fatal(err)
			}
			if after := read(); before != after {
				t.Fatalf("cleanup ownership changed: %s -> %s", before, after)
			}
		})
	}
}

func seedAcceptedCleanupMail(f *inputRedeliveryFixture, consumed bool) {
	f.t.Helper()
	f.exec(`INSERT INTO session_runtime_inbox
		(workspace_id,session_id,session_thread_id,runtime_input_id,input_kind,event_ids_json,status,binding_id,binding_generation,target_pod_uid,created_at,updated_at,committed_at)
		SELECT 'default',$1,$2,'agent_mail:previous','agent_mail','[]','accepted',binding_id,binding_generation,agent_runtime_pod_uid,clock_timestamp(),clock_timestamp(),
		CASE WHEN $3 THEN clock_timestamp() END FROM session_runtime_bindings WHERE workspace_id='default' AND session_id=$1`, f.sessionID, f.mainID, consumed)
}

// Assert the authoritative feed revision, not only the cleanup predicate.
func assertInputProcessedRevision(t *testing.T, f *inputRedeliveryFixture, eventID string, wantProcessed bool) {
	t.Helper()
	var processed bool
	var revision, changes int
	if err := f.admin.QueryRowContext(f.ctx, `SELECT processed_at IS NOT NULL, revision,
		(SELECT count(*) FROM session_event_stream_changes WHERE workspace_id='default' AND session_id=$1 AND event_id=$2)
		FROM session_events WHERE workspace_id='default' AND session_id=$1 AND event_id=$2`, f.sessionID, eventID).Scan(&processed, &revision, &changes); err != nil {
		t.Fatal(err)
	}
	wantRevision := 1
	if wantProcessed {
		wantRevision = 2
	}
	if processed != wantProcessed || revision != wantRevision || changes != wantRevision {
		t.Fatalf("event %s processed=%t revision=%d changes=%d; want %t/%d/%d", eventID, processed, revision, changes, wantProcessed, wantRevision, wantRevision)
	}
}

func TestInterruptCancellationProcessesOnlySelectedEvents(t *testing.T) {
	for _, accepted := range []bool{false, true} {
		name := "queued"
		if accepted {
			name = "accepted"
		}
		t.Run(name, func(t *testing.T) {
			f := newInputRedeliveryFixture(t, "sesn_interrupt_event_settlement")
			f.bind("bind_interrupt_event_settlement")
			setInputSessionIdle(t, f)
			events := sessionevent.NewService(sessionevent.NewPostgreSQLStore(dbconnect.NewClientForTesting(f.workloads.OpenWorkload(t, "api", nil))))
			appendEvent := func(key string, event sessionevent.IncomingEvent) string {
				t.Helper()
				result, err := events.AppendClientEvents(f.ctx, workspace.DefaultID, f.sessionID, key, sessionevent.AppendRequest{Events: []sessionevent.IncomingEvent{event}})
				if err != nil {
					t.Fatal(err)
				}
				return result.Data[0].ID
			}
			message := sessionevent.IncomingEvent{Type: sessionevent.EventTypeUserMessage, Content: []sessionevent.ContentBlock{{Type: sessionevent.ContentBlockTypeText, Text: "message"}}}
			older := appendEvent("older", message)
			if accepted {
				f.deliverAll()
			}
			appendEvent("interrupt", sessionevent.IncomingEvent{Type: sessionevent.EventTypeUserInterrupt})
			newer := appendEvent("newer", message)
			leased, err := f.queue.Lease(f.ctx, queue.LeaseRequest{WorkspaceID: workspace.DefaultID, Kinds: []string{queue.KindRuntimeInput}, LeaseOwner: "interrupt-events", MaxJobs: 1, LeaseDuration: time.Minute})
			if err != nil || len(leased) != 1 {
				t.Fatalf("lease interrupt = %v/%v", leased, err)
			}
			job, err := jobrunner.DecodeRuntimeJob(queueJobProto(leased[0]))
			if err != nil || job.InputKind != "interrupt_control" {
				t.Fatalf("interrupt job = %+v/%v", job, err)
			}
			bridgeStore := agentruntimebridge.NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(f.workloads.OpenWorkload(t, "bridge", nil)))
			bridgeStore.RuntimeBindingTokenHMACKey = []byte("interrupt-events-signing-key")
			sender := &committingInterruptRuntimeSender{recordingRuntimeCommandSender: f.sender, bridge: bridgeStore}
			direct := jobrunner.RuntimePodDirectDeliverer{Store: f.store, Sender: sender}
			for attempt := 0; attempt < 2; attempt++ {
				result, found, err := f.store.ReplayRuntimeDeliveryFinalization(f.ctx, job)
				if err != nil {
					t.Fatalf("replay interrupt settlement: %v", err)
				}
				if !found {
					result, err = direct.DeliverRuntimeJob(f.ctx, job)
				}
				if err != nil || (result.Status != jobrunner.RuntimeDeliveryAccepted && result.Status != jobrunner.RuntimeDeliveryDuplicate) {
					t.Fatalf("interrupt attempt %d = %+v/%v", attempt, result, err)
				}
				assertInputProcessedRevision(t, f, older, true)
				assertInputProcessedRevision(t, f, newer, false)
			}
			// Replay Bridge's commit as well as Job Runner's delivery receipt.
			for _, request := range sender.requests {
				if interrupt, ok := request.(*agentruntimev1.InterruptRequest); ok {
					if _, err := sender.Interrupt(f.ctx, jobrunner.RuntimePodTarget{}, interrupt); err != nil {
						t.Fatal(err)
					}
					break
				}
			}
			assertInputProcessedRevision(t, f, older, true)
			assertInputProcessedRevision(t, f, newer, false)
		})
	}
}

func TestFirstIdleCleanupWaitsForOpenInput(t *testing.T) {
	f := newInputRedeliveryFixture(t, "sesn_fresh_cleanup_open_input")
	events := sessionevent.NewService(sessionevent.NewPostgreSQLStore(dbconnect.NewClientForTesting(f.workloads.OpenWorkload(t, "api", nil))))
	if _, err := events.AppendClientEvents(f.ctx, workspace.DefaultID, f.sessionID, "open-input", sessionevent.AppendRequest{Events: []sessionevent.IncomingEvent{{Type: sessionevent.EventTypeUserMessage, Content: []sessionevent.ContentBlock{{Type: sessionevent.ContentBlockTypeText, Text: "pending"}}}}}); err != nil {
		t.Fatal(err)
	}
	leased, err := f.queue.Lease(f.ctx, queue.LeaseRequest{WorkspaceID: workspace.DefaultID, Kinds: []string{queue.KindRuntimeInput}, LeaseOwner: "first-idle", MaxJobs: 1, LeaseDuration: time.Minute})
	if err != nil || len(leased) != 1 {
		t.Fatalf("lease first input = %v/%v", leased, err)
	}
	job, err := jobrunner.DecodeRuntimeJob(queueJobProto(leased[0]))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.store.PrepareRuntimeCommand(f.ctx, job); err != nil {
		t.Fatal(err)
	}
	if err := f.store.Client.WithWorkspaceTx(f.ctx, "default", "test.open_input_cleanup", func(tx *dbconnect.Tx) error {
		if err := runtimecontrol.LockRuntimeMutationSessionTx(f.ctx, tx, "default", f.sessionID); err != nil {
			return err
		}
		return runtimecontrol.EnsureIdleCleanupScheduledTx(f.ctx, tx, "default", f.sessionID, time.Now())
	}); err != nil {
		t.Fatal(err)
	}
	var unchanged bool
	if err := f.admin.QueryRowContext(f.ctx, `SELECT s.status='idle' AND s.status_event_id IS NULL AND s.cleanup_after IS NULL
		AND EXISTS(SELECT 1 FROM session_runtime_bindings WHERE workspace_id='default' AND session_id=$1)
		AND NOT EXISTS(SELECT 1 FROM session_events WHERE workspace_id='default' AND session_id=$1 AND type='session.status_idle')
		FROM session_runtime_status s WHERE s.workspace_id='default' AND s.session_id=$1`, f.sessionID).Scan(&unchanged); err != nil || !unchanged {
		t.Fatalf("open first input preserved without idle fence = %t/%v", unchanged, err)
	}
}
