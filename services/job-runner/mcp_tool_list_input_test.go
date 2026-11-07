package jobrunner

import (
	"testing"

	"github.com/tetral-ai/tetral/internal/queue"
)

// An input to a Session whose user computer's instance never reported a
// local MCP tool list starts its run: the placement's plan accepts the input
// in the same attempt, with no MCP configuration before it.
func TestAnInputStartsItsRunWithNoToolListReported(t *testing.T) {
	f := newMachineMessageFixture(t, "sesn_no_tool_list")
	f.exec(`INSERT INTO instances (workspace_id,id,kind,name,registration_state) VALUES ('default','inst_no_tool_list','user','laptop','active')`)
	f.exec(`INSERT INTO computers
		(workspace_id,session_id,id,kind,instance_id,current_instance_id,working_directory,networking,drive_exclude,association_state,execution_state)
		VALUES ('default',$1,'comp_user_no_tool_list','user','inst_no_tool_list','inst_no_tool_list','/work','{"type":"unrestricted"}','[]','attached','waiting')`, f.sessionID)
	sequence := f.nextSequence()
	seedBridgeAPIEvent(t, f.admin, "default", f.sessionID, f.mainID, "sevt_no_tool_list", sequence, "user.message", `{"content":[{"type":"text","text":"hello"}]}`)
	input := RuntimeJob{
		JobID: "qjob_no_tool_list", LeaseToken: "lease_no_tool_list", Kind: queue.KindRuntimeInput,
		WorkspaceID: "default", SessionID: f.sessionID, SessionThreadID: f.mainID, RuntimeInputID: "rin_no_tool_list",
		EventIDs: []string{"sevt_no_tool_list"}, SequenceFrom: sequence, SequenceTo: sequence, InputKind: "messages", PayloadJSON: `{"type":"messages"}`,
	}
	seedRuntimeInboxBirthForJob(t, f.admin, input)
	plan, err := f.store.PrepareRuntimeCommand(f.ctx, input)
	if err != nil || plan.AcceptInput == nil || plan.AcceptInput.GetRuntimeInputId() != input.RuntimeInputID || len(plan.MCPBeforeInput) != 0 {
		t.Fatalf("plan = %+v, %v; want the input accepted in the placement's attempt with no MCP configuration before it", plan, err)
	}
	var lists int
	if err := f.admin.QueryRowContext(f.ctx, `SELECT count(*) FROM instance_mcp_tool_lists`).Scan(&lists); err != nil || lists != 0 {
		t.Fatalf("tool lists = %d, %v; want none", lists, err)
	}
}
