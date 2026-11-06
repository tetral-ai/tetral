package joint

import (
	"context"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/protocol"
	bridgev1 "github.com/tetral-ai/tetral/services/bridge/gen/tetral/bridge/v1"
)

// TestPoliciesAreCheckedBeforeAnyEffect calls a machine whose platform
// isolation is unavailable. Admission refuses a call with no policy, one with
// an unknown field and one of an unsupported version, and ends one whose
// network mode the machine cannot enforce, none reaching the machine. The
// CLI rejects the calls it gets with sandbox_unavailable, and the Session
// gets one computer_sandbox_error per connection.
func TestPoliciesAreCheckedBeforeAnyEffect(t *testing.T) {
	in := requireInputs(t, needs{platforms: []string{"linux"}})
	record := newAcceptanceRecord(t, in, platformOf(in))
	record.boundary("Linux only: isolation is made unavailable by an App Server PATH with no bwrap; macOS has no unprivileged way to take sandbox-exec away")
	record.boundary("the CLI's own rejection of a missing, unknown-field, unsupported-version or unenforceable policy is not reached: Engine refuses or ends those calls before it sends any tool.execute")
	e := startEngine(t, record, engineOptions{})
	set := newMachines(t, in, needs{}, e, record)
	m := unisolatedMachine(t, e, set, "unisolated")
	session, computers, rs := driveSession(t, e, "policies", nil, m)
	ctx := t.Context()
	for name, policy := range map[string]string{
		"missing":     "",
		"unknown":     `{"schema_version":1,"filesystem":{"rules":[]},"network":{"mode":"disabled"},"mode":"workspace"}`,
		"unsupported": `{"schema_version":2,"filesystem":{"rules":[]},"network":{"mode":"disabled"}}`,
	} {
		use := rs.use(protocol.ToolExecCommand, `{"cmd":"true"}`, "machine_tool")
		_, err := rs.r.client.AdmitToolCall(ctx, &bridgev1.AdmitToolCallRequest{Scope: rs.scope, ToolUseEventId: use, ComputerId: computers[0],
			Tool: protocol.ToolExecCommand, InputJson: `{"cmd":"true"}`, SandboxPolicyJson: policy})
		if status.Code(err) != codes.InvalidArgument {
			t.Fatalf("admission with the %s policy answered %v; want InvalidArgument", name, err)
		}
	}
	managed := `{"schema_version":1,"filesystem":{"rules":[]},"network":{"mode":"managed","rules":{"allow":[{"destination":{"kind":"host","value":"example.com"},"transport":"tcp","ports":[{"from":443,"to":443}]}],"deny":[],"listen":[]}}}`
	use, response := rs.admit(computers[0], protocol.ToolExecCommand, `{"cmd":"true"}`, managed)
	unenforceable := rs.await(use, time.Minute)
	if response.GetAdmitted() == nil || unenforceable.Settlement == "report" {
		t.Fatalf("the call with a managed network answered %v and settled %+v; want admitted and ended without a report", response, unenforceable)
	}
	if n := toolInstructions(t, e, session.ID); n != 0 {
		t.Fatalf("%d tool.execute instructions were written; want none", n)
	}
	record.observe("missing, unknown-field and unsupported-version policies were refused at admission; the managed-network call settled %s %s %q; no tool.execute was written", unenforceable.Settlement, unenforceable.Code, unenforceable.Message)

	rs.requireRejected(rs.exec(computers[0], "true"), "sandbox_unavailable")
	rs.requireRejected(rs.exec(computers[0], "true"), "sandbox_unavailable")
	if errs := errorsOfType(e, session.ID, "computer_sandbox_error"); len(errs) != 1 {
		t.Fatalf("%d computer_sandbox_error events on the first connection; want 1", len(errs))
	}
	developer := e.developer()
	first := developer.waitComputer(ctx, session.ID, computers[0], time.Minute, "ready", func(c computerObject) bool { return c.ExecutionState == "ready" })
	m.server.stop()
	m.waitOffline(e)
	m.server = m.serveUnisolated()
	developer.waitComputer(ctx, session.ID, computers[0], 2*time.Minute, "ready on the new connection", func(c computerObject) bool {
		return c.ExecutionState == "ready" && c.BindingID != nil && *c.BindingID != *first.BindingID
	})
	rs.requireRejected(rs.exec(computers[0], "true"), "sandbox_unavailable")
	if errs := errorsOfType(e, session.ID, "computer_sandbox_error"); len(errs) != 2 {
		t.Fatalf("%d computer_sandbox_error events after a second connection; want 2", len(errs))
	}
	record.observe("two sandbox_unavailable rejections on the first connection wrote one computer_sandbox_error; one on the next connection wrote the second")
}

// toolInstructions counts the tool.execute instructions written for the
// Session's bindings.
func toolInstructions(t *testing.T, e *engine, sessionID string) int {
	t.Helper()
	var n int
	e.ownerTx(func(ctx context.Context, tx *dbconnect.Tx) error {
		return tx.QueryRow(ctx, `SELECT count(*) FROM connection_instructions i JOIN session_tool_dispatches d ON d.workspace_id=i.workspace_id AND d.message_id=i.message_id
			WHERE i.workspace_id=$1 AND d.session_id=$2 AND i.type='tool.execute'`, harnessWorkspace, sessionID).Scan(&n)
	})
	return n
}
