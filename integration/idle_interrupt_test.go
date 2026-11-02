package integration

import (
	"testing"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/sessionevent"
	"github.com/tetral-ai/tetral/internal/workspace"
	tetralcleanup "github.com/tetral-ai/tetral/services/cleanup"
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
