package integration

import (
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/queue"
	"github.com/tetral-ai/tetral/internal/sessionevent"
	"github.com/tetral-ai/tetral/internal/workspace"
	tetralcleanup "github.com/tetral-ai/tetral/services/cleanup"
	jobrunner "github.com/tetral-ai/tetral/services/job-runner"
)

func TestIdleInterruptDoesNotPlaceRuntime(t *testing.T) {
	for _, cleaned := range []bool{false, true} {
		name := "fresh session"
		if cleaned {
			name = "after idle cleanup"
		}
		t.Run(name, func(t *testing.T) {
			f := newInputRedeliveryFixture(t, "sesn_idle_interrupt")
			if cleaned {
				f.bind("bind_before_cleanup")
				setInputSessionIdle(t, f)
				cleanup := dbconnect.NewClientForTesting(f.workloads.OpenWorkload(t, "cleanup", nil))
				result, err := tetralcleanup.NewScheduler(cleanup, 10).RunSchedulingPhase(f.ctx)
				if err != nil || result.Claimed != 1 {
					t.Fatalf("schedule idle cleanup = %+v/%v", result, err)
				}
				f.deliverAll()
				f.sender.requests = nil
			}
			events := sessionevent.NewService(sessionevent.NewPostgreSQLStore(dbconnect.NewClientForTesting(f.workloads.OpenWorkload(t, "api", nil))))
			appended, err := events.AppendClientEvents(f.ctx, workspace.DefaultID, f.sessionID, "idle-interrupt", sessionevent.AppendRequest{Events: []sessionevent.IncomingEvent{{Type: sessionevent.EventTypeUserInterrupt}}})
			if err != nil {
				t.Fatal(err)
			}
			f.deliverAll()
			var bindings int
			var processed bool
			var inbox, job string
			if err := f.admin.QueryRowContext(f.ctx, `SELECT
				(SELECT count(*) FROM session_runtime_bindings WHERE workspace_id='default' AND session_id=$1),
				(SELECT processed_at IS NOT NULL FROM session_events WHERE workspace_id='default' AND event_id=$2),
				i.status,j.status FROM session_runtime_inbox i JOIN queue_jobs j ON j.workspace_id=i.workspace_id AND j.dedupe_key='runtime_input:default:'||i.session_id||':'||i.runtime_input_id
				WHERE i.workspace_id='default' AND i.session_id=$1 AND i.input_kind='interrupt_control'`, f.sessionID, appended.Data[0].ID).
				Scan(&bindings, &processed, &inbox, &job); err != nil {
				t.Fatal(err)
			}
			if bindings != 0 || !processed || inbox != "cancelled" || job != "cancelled" || len(f.sender.requests) != 0 {
				t.Fatalf("idle interrupt: bindings %d processed %t inbox %s job %s RPCs %d", bindings, processed, inbox, job, len(f.sender.requests))
			}
		})
	}
}

func TestUnboundInterruptAwaitingApprovalUsesRuntime(t *testing.T) {
	f := newInputRedeliveryFixture(t, "sesn_interrupt_approval")
	seedBridgeAPIPendingApproval(t, f.admin, "default", f.sessionID, f.mainID, "evt_pending_approval", f.nextSequence())
	f.exec(`UPDATE session_threads SET status='requires_action' WHERE workspace_id='default' AND session_id=$1 AND id=$2`, f.sessionID, f.mainID)
	events := sessionevent.NewService(sessionevent.NewPostgreSQLStore(dbconnect.NewClientForTesting(f.workloads.OpenWorkload(t, "api", nil))))
	appended, err := events.AppendClientEvents(f.ctx, workspace.DefaultID, f.sessionID, "interrupt-approval", sessionevent.AppendRequest{Events: []sessionevent.IncomingEvent{{Type: sessionevent.EventTypeUserInterrupt}}})
	if err != nil {
		t.Fatal(err)
	}
	leased, err := f.queue.Lease(f.ctx, queue.LeaseRequest{WorkspaceID: workspace.DefaultID, Kinds: []string{queue.KindRuntimeInput}, LeaseOwner: "approval-interrupt", MaxJobs: 1, LeaseDuration: time.Minute})
	if err != nil || len(leased) != 1 {
		t.Fatalf("lease approval interrupt = %v/%v", leased, err)
	}
	job, err := jobrunner.DecodeRuntimeJob(queueJobProto(leased[0]))
	if err != nil {
		t.Fatal(err)
	}
	plan, err := f.store.PrepareRuntimeCommand(f.ctx, job)
	if err != nil {
		t.Fatal(err)
	}
	if plan.StaleAccepted || plan.Interrupt == nil || plan.Interrupt.GetBindingId() == "" {
		t.Fatalf("approval interrupt plan = %+v; want bound Runtime interrupt", plan)
	}
	var processed bool
	var inbox string
	if err := f.admin.QueryRowContext(f.ctx, `SELECT e.processed_at IS NOT NULL,i.status FROM session_events e JOIN session_runtime_inbox i
		ON i.workspace_id=e.workspace_id AND i.session_id=e.session_id AND i.event_ids_json::jsonb ? e.event_id
		WHERE e.workspace_id='default' AND e.session_id=$1 AND e.event_id=$2`, f.sessionID, appended.Data[0].ID).Scan(&processed, &inbox); err != nil {
		t.Fatal(err)
	}
	if processed || inbox != "delivering" {
		t.Fatalf("approval interrupt processed=%t inbox=%s; want unprocessed Runtime delivery", processed, inbox)
	}
}
