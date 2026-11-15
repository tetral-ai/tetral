package agentruntimebridge

import (
	"context"
	"reflect"
	"slices"
	"testing"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/runtimecontrol"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	"github.com/tetral-ai/tetral/internal/storage/storagetest/sessionfixture"
	bridgev1 "github.com/tetral-ai/tetral/services/bridge/gen/tetral/bridge/v1"
)

// Every RPC is classified. A mutation or current RPC refuses a retired
// process; a receipt replay by a retired process is answered before the
// fence, and the retirement tests hold those replays.
var runtimeProcessCallerInventory = map[string]string{
	"RegisterRuntimeProcess": "registry", "ReportRuntimeProcess": "registry", "ReleaseRuntimeBinding": "handoff", "McpManifestChanged": "service",
	"LoadContext": "current", "RefreshRuntimeBindingToken": "current", "CommitInputs": "mutation", "CommitTaskNotificationResult": "mutation", "WriteEvent": "mutation", "SettleToolResult": "mutation", "WriteRequestEnd": "mutation", "FinishIdle": "mutation", "CreateSubagentThread": "mutation", "EnsureApprovalReviewerTrunk": "mutation", "EnsureApprovalReviewerSidecar": "mutation", "AdmitApprovalReviewInput": "mutation", "ResolveChildThread": "read", "ListChildThreads": "read", "DeliverInterAgentMail": "mutation", "ReadAgentMail": "read", "AdmitChildInterrupt": "mutation", "AwaitChildInterrupt": "read", "CloseChildControl": "mutation", "CloseApprovalReviewer": "mutation", "MarkChildThreadActive": "mutation", "AcceptSandboxExecution": "mutation", "AwaitSandboxExecution": "receipt-read", "AdmitToolCall": "mutation", "AdmitToolControl": "mutation", "CancelToolCall": "mutation", "AwaitToolCall": "receipt-read", "ReadCommandResult": "mutation", "SendCommandInput": "mutation", "CancelCommand": "mutation", "AuthorizeWebToolExecution": "current", "RunMemory": "mutation", "ResolveTransientAttachment": "read", "ResolveAttachment": "read", "ResolveFileAttachmentMetadata": "read", "ReadFileAttachmentChunk": "read", "ClaimMcpToolResult": "mutation", "CommitMcpToolResult": "mutation", "RelinquishMcpToolResult": "mutation", "CommitInternalToolRepair": "mutation", "CommitRuntimeTermination": "mutation",
}

func TestEachRPCIsClassified(t *testing.T) {
	var names []string
	for _, method := range bridgev1.AgentRuntimeBridgeService_ServiceDesc.Methods {
		names = append(names, method.MethodName)
		if _, ok := runtimeProcessCallerInventory[method.MethodName]; !ok {
			t.Errorf("unclassified RPC %s", method.MethodName)
		}
	}
	for name := range runtimeProcessCallerInventory {
		if !slices.Contains(names, name) {
			t.Errorf("the inventory names %s, which is no RPC", name)
		}
	}
}

// After a same-Pod promotion the binding still names the retired process.
// Each mutation and current RPC is sent new work that its lookups accept;
// the answer is stale, as a typed outcome or a superseded scope, and no
// tenant row changes.
func TestPostgreSQLRetiredProcessIsRefusedNewWork(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	ctx := context.Background()
	const (
		sessionID = "sesn_retired_fence"
		threadID  = "thr_retired_fence"
		bindingID = "bind_retired_fence"
		podUID    = "pod_retired_fence"
		request   = "mreq_retired_fence"
	)
	sessionfixture.SeedBridgeAPISession(t, admin, "default", sessionID, threadID)
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, bindingID, 1, podUID)
	scope := sessionfixture.BridgeAPIScope(sessionID, threadID, bindingID, 1, podUID)
	running, err := store.WriteEvent(ctx, closeoutWriteEventRequest(scope, "rwrite_retired_fence_running"))
	if err != nil || running.GetCommitted() == nil {
		t.Fatalf("open the durable turn = %v/%v", running, err)
	}
	seedBridgeAPIRequestStart(t, store, scope, "rwrite_retired_fence_start", request, "agent_provider_request", 0)
	// The close's interrupt control is leased as the only Runtime input job,
	// so it comes before any other work that queues one.
	const closingChild = "thr_retired_fence_child"
	sessionfixture.SeedBridgeAPIChildThread(t, admin, "default", sessionID, threadID, closingChild)
	closeControl := awaitDriveSyncChildClose(t, driveSyncChild{ctx: ctx, runtime: runtime, admin: admin, sessionID: sessionID, mainID: threadID, childID: closingChild, bindingID: bindingID, podUID: podUID, store: store})
	declare := func(callID string, declaration *bridgev1.RuntimeToolDeclaration) string {
		t.Helper()
		response, err := store.WriteEvent(ctx, &bridgev1.WriteEventRequest{Scope: scope, RuntimeWriteId: "rwrite_" + callID, ModelRequestId: request, ToolDeclaration: declaration})
		if err != nil || response.GetCommitted() == nil {
			t.Fatalf("declare %s = %v/%v", callID, response, err)
		}
		return response.GetCommitted().GetEventId()
	}
	ordinary := func(callID, name, input string) string {
		return declare(callID, sessionfixture.BridgeToolDeclarationWithRouteForTest(callID, name, input, "allow"))
	}
	must := func(what string, ok bool, response any, err error) {
		t.Helper()
		if err != nil || !ok {
			t.Fatalf("%s = %v/%v", what, response, err)
		}
	}

	execTool := ordinary("call_fence_exec", "exec_command", `{"cmd":"printf fresh"}`)
	settledTool := ordinary("call_fence_settled", "exec_command", `{"cmd":"printf settled"}`)
	accepted, err := store.AcceptSandboxExecution(ctx, &bridgev1.AcceptSandboxExecutionRequest{Scope: scope, ToolUseEventId: settledTool})
	must("accept the settled execution", accepted.GetCommitted() != nil, accepted, err)
	commitAwaitExecutionSettlement(t, admin, scope, settledTool, `{"status":"success","result":{"stdout":"settled"}}`, false)
	webTool := ordinary("call_fence_web", "web_search", `{"query":"fence"}`)
	spawnTool := ordinary("call_fence_spawn", "spawn_agent", `{"task_name":"fence-worker","agent_type":"general","fork_turns":"none","prompt":"work"}`)
	newSpawnTool := ordinary("call_fence_spawn_new", "spawn_agent", `{"task_name":"fence-new","agent_type":"general","fork_turns":"none","prompt":"work"}`)
	created, err := store.CreateSubagentThread(ctx, &bridgev1.CreateSubagentThreadRequest{Scope: scope, SourceToolUseEventId: spawnTool, TaskName: "fence-worker", AgentType: "worker", InitialPrompt: "work"})
	must("create the child", created.GetCommitted() != nil, created, err)
	child := created.GetCommitted().GetChildThreadId()
	mailTool := ordinary("call_fence_mail", "send_message", `{"task_name":"fence-worker","message":"hello"}`)
	resumeTool := ordinary("call_fence_resume", "resume_agent", `{"task_name":"fence-worker"}`)
	interruptTool := seedBridgeAPIChildLifecycleToolSource(t, admin, sessionID, threadID, "evt_retired_fence_interrupt")
	trunk, err := store.EnsureApprovalReviewerTrunk(ctx, &bridgev1.EnsureApprovalReviewerTrunkRequest{Scope: scope, EnsureOperationId: "ensure_fence_trunk"})
	must("ensure the reviewer trunk", trunk.GetCommitted() != nil, trunk, err)
	reviewer, err := store.EnsureApprovalReviewerSidecar(ctx, &bridgev1.EnsureApprovalReviewerSidecarRequest{Scope: scope, ReviewId: "review_fence"})
	must("ensure the reviewer", reviewer.GetCommitted() != nil, reviewer, err)
	reviewerThread := reviewer.GetCommitted().GetReviewerThreadId()
	seedBridgeAPIEvent(t, admin, "default", sessionID, reviewerThread, "evt_retired_fence_review_failure", sessionfixture.NextBridgeAPIEventSequenceForTest(t, admin, sessionID, reviewerThread), "approval_review.failure", `{"type":"approval_review.failure"}`)
	if _, err := admin.ExecContext(ctx, `UPDATE session_events SET runtime_write_id='rwrite_review_fence_failure' WHERE workspace_id='default' AND event_id='evt_retired_fence_review_failure'`); err != nil {
		t.Fatal(err)
	}
	mcpClaimTool := declare("call_fence_mcp_claim", bridgeMCPToolDeclarationForTest("call_fence_mcp_claim", "create_issue", "github", `{"title":"Bug","body":"claim"}`, "allow"))
	mcpHeldTool := declare("call_fence_mcp_held", bridgeMCPToolDeclarationForTest("call_fence_mcp_held", "create_issue", "github", `{"title":"Bug","body":"held"}`, "allow"))
	mcpReleasedTool := declare("call_fence_mcp_released", bridgeMCPToolDeclarationForTest("call_fence_mcp_released", "create_issue", "github", `{"title":"Bug","body":"released"}`, "allow"))
	for tool, claim := range map[string]string{mcpHeldTool: "claim_fence_held", mcpReleasedTool: "claim_fence_released"} {
		held, err := store.ClaimMcpToolResult(ctx, &bridgev1.ClaimMcpToolResultRequest{Scope: scope, ToolUseEventId: tool, ClaimId: claim})
		must("hold the MCP claim "+claim, held.GetAcquired() != nil, held, err)
	}
	// FinishIdle closes a turn with no open request, so it has a Session of
	// its own on the same Pod.
	idleScope := sessionfixture.BridgeAPIScope("sesn_retired_fence_idle", "thr_retired_fence_idle", "bind_retired_fence_idle", 1, podUID)
	sessionfixture.SeedBridgeAPISession(t, admin, "default", idleScope.SessionId, idleScope.SessionThreadId)
	seedBridgeAPIRuntimeBinding(t, admin, "default", idleScope.SessionId, idleScope.Binding.BindingId, 1, podUID)
	seedBridgeAPIOpenDurableTurn(t, admin, idleScope, "evt_retired_fence_idle_turn")
	const task = "task_retired_fence"
	stdinTool := "evt_retired_fence_stdin"
	seedBridgeAPIEvent(t, admin, "default", sessionID, threadID, stdinTool, sessionfixture.NextBridgeAPIEventSequenceForTest(t, admin, sessionID, threadID), "agent.tool_use", `{"name":"write_stdin","input":{"session_id":"task_retired_fence","chars":"once"},"evaluated_permission":"allow"}`)
	seedBridgeAPIToolDeclarationProjection(t, admin, "default", sessionID, threadID, stdinTool, "call_retired_fence_stdin", "write_stdin", `{"session_id":"task_retired_fence","chars":"once"}`, "background_command")
	sessionfixture.SeedBridgeAPIAllowedToolRoute(t, admin, "default", sessionID, threadID, stdinTool)
	seedBridgeAPIBackgroundTask(t, admin, "default", sessionID, threadID, bindingID, task, "evt_retired_fence_task_source")
	// A cancel names the poll it interrupts.
	pollTool := "evt_retired_fence_poll"
	seedBridgeAPIEvent(t, admin, "default", sessionID, threadID, pollTool, sessionfixture.NextBridgeAPIEventSequenceForTest(t, admin, sessionID, threadID), "agent.tool_use", `{"name":"write_stdin","input":{"session_id":"task_retired_fence","chars":""},"evaluated_permission":"allow"}`)
	seedBridgeAPIToolDeclarationProjection(t, admin, "default", sessionID, threadID, pollTool, "call_retired_fence_poll", "write_stdin", `{"session_id":"task_retired_fence","chars":""}`, "background_command")
	sessionfixture.SeedBridgeAPIAllowedToolRoute(t, admin, "default", sessionID, threadID, pollTool)
	if _, err := admin.ExecContext(ctx, `INSERT INTO session_runtime_tool_results (
		workspace_id, session_id, session_thread_id, tool_use_event_id, tool_kind,
		normalized_input_hash, tool_name, input_json, ack_status,
		background_operation_kind, background_operation_state, background_request_id,
		background_task_id, background_max_output_tokens, created_at, updated_at
	) VALUES ('default',$1,$2,$3,'sandbox_background','poll_hash','write_stdin','{}','committed',
		'poll','pending','req_retired_fence_poll',$4,0,now(),now())`, sessionID, threadID, pollTool, task); err != nil {
		t.Fatal(err)
	}
	sessionfixture.SeedBridgeAPIWritableMemoryStore(t, admin, "default", sessionID, "memory_retired_fence")
	memory := durableMemoryRequestForTest(t, admin, scope, "evt_retired_fence_memory", `{"action":"create","path":"notes/fence.md","content":"new"}`)
	inputSequence := sessionfixture.NextBridgeAPIEventSequenceForTest(t, admin, sessionID, threadID)
	seedBridgeAPIEvent(t, admin, "default", sessionID, threadID, "evt_retired_fence_input", inputSequence, "user.message", `{"content":[{"type":"text","text":"input"}]}`)
	sessionfixture.SeedBridgeAPIRuntimeInbox(t, admin, "default", sessionID, threadID, "rin_retired_fence", "messages", `["evt_retired_fence_input"]`, "accepted", bindingID, podUID, inputSequence, inputSequence)
	seedBridgeAPINotifiableBackgroundTask(t, admin, "default", sessionID, threadID, bindingID, "task_retired_fence_notify", "evt_retired_fence_notify_source")
	settleBridgeAPIBackgroundTask(t, admin, sessionID, "task_retired_fence_notify", "expired", `{"task_id":"task_retired_fence_notify","source_tool_use_event_id":"evt_retired_fence_notify_source","status":"expired","stdout":{"text":"","truncated":false},"stderr":{"text":"","truncated":false},"exit_code":null}`)
	sessionfixture.SeedBridgeAPITaskNotificationInbox(t, admin, "default", sessionID, threadID, "task_notification:task_retired_fence_notify", bindingID, podUID)

	newWork := map[string]proto.Message{
		"LoadContext":                   &bridgev1.LoadContextRequest{Scope: scope},
		"RefreshRuntimeBindingToken":    &bridgev1.RefreshRuntimeBindingTokenRequest{Scope: scope},
		"CommitInputs":                  &bridgev1.CommitInputsRequest{Scope: scope, RuntimeInputId: "rin_retired_fence"},
		"CommitTaskNotificationResult":  &bridgev1.CommitTaskNotificationResultRequest{Scope: scope, RuntimeInputId: "task_notification:task_retired_fence_notify"},
		"WriteEvent":                    &bridgev1.WriteEventRequest{Scope: scope, RuntimeWriteId: "rwrite_retired_fence_message", ModelRequestId: request, PreallocatedEventId: bridgeString("evt_00000000000000000000000000000003"), EventType: "agent.message", PayloadJson: `{"type":"agent.message","content":[{"type":"text","text":"new"}]}`, AssistantContextDelta: sessionfixture.BridgeTextContextDeltaForTest("new")},
		"SettleToolResult":              sessionfixture.BridgeToolSettlementRequestForTest(scope, sessionfixture.BridgeCompletedToolSettlementForTest(settledTool, "settled")),
		"WriteRequestEnd":               &bridgev1.WriteRequestEndRequest{Scope: scope, RuntimeWriteId: "rwrite_retired_fence_end", ModelRequestId: request, FinishReason: "stop", UsageJson: `{}`, ProviderContextRetention: &bridgev1.ProviderContextRetention{Disposition: "completed"}},
		"FinishIdle":                    &bridgev1.FinishIdleRequest{Scope: idleScope, DurableTurnId: "evt_retired_fence_idle_turn", StopReasonJson: `{"type":"end_turn"}`},
		"CreateSubagentThread":          &bridgev1.CreateSubagentThreadRequest{Scope: scope, SourceToolUseEventId: newSpawnTool, TaskName: "fence-new", AgentType: "worker", InitialPrompt: "work"},
		"EnsureApprovalReviewerTrunk":   &bridgev1.EnsureApprovalReviewerTrunkRequest{Scope: scope, EnsureOperationId: "ensure_retired_fence"},
		"EnsureApprovalReviewerSidecar": &bridgev1.EnsureApprovalReviewerSidecarRequest{Scope: scope, ReviewId: "review_retired_fence_new"},
		"AdmitApprovalReviewInput":      &bridgev1.AdmitApprovalReviewInputRequest{Scope: scope, ReviewerThreadId: reviewerThread, ReviewId: "review_fence"},
		"DeliverInterAgentMail":         &bridgev1.DeliverInterAgentMailRequest{Scope: scope, DeliveryId: runtimecontrol.AgentMailDeliveryID(mailTool, child), TargetThreadId: child, SourceToolUseEventId: mailTool, Content: "hello"},
		"AdmitChildInterrupt":           &bridgev1.AdmitChildInterruptRequest{Scope: scope, SourceToolUseEventId: interruptTool, TargetChildThreadId: closingChild, Action: bridgev1.ChildControlAction_CHILD_CONTROL_ACTION_INTERRUPT},
		"CloseChildControl":             &bridgev1.CloseChildControlRequest{Scope: scope, ControlOperationId: closeControl},
		"CloseApprovalReviewer":         &bridgev1.CloseApprovalReviewerRequest{Scope: scope, ReviewerThreadId: reviewerThread, ReviewId: "review_fence", SettlementKind: bridgev1.ApprovalReviewerCloseSettlementKind_APPROVAL_REVIEWER_CLOSE_SETTLEMENT_KIND_FAILURE, SettlementEventId: "evt_retired_fence_review_failure"},
		"MarkChildThreadActive":         &bridgev1.MarkChildThreadActiveRequest{Scope: scope, SourceToolUseEventId: resumeTool, TargetChildThreadId: child},
		"AcceptSandboxExecution":        &bridgev1.AcceptSandboxExecutionRequest{Scope: scope, ToolUseEventId: execTool},
		"AdmitToolCall":                 &bridgev1.AdmitToolCallRequest{Scope: scope, ToolUseEventId: execTool, ComputerId: "computer_retired_fence", Tool: "exec_command", InputJson: `{"cmd":"printf fresh"}`, SandboxPolicyJson: `{"schema_version":1,"filesystem":{"rules":[]},"network":{"mode":"disabled"}}`},
		"AdmitToolControl":              &bridgev1.AdmitToolControlRequest{Scope: scope, ToolUseEventId: stdinTool, TaskId: task, Chars: "once"},
		"CancelToolCall":                &bridgev1.CancelToolCallRequest{Scope: scope, ToolUseEventId: execTool},
		"ReadCommandResult":             &bridgev1.ReadCommandResultRequest{Scope: scope, TaskId: task, ToolUseEventId: stdinTool, OperationId: "op_retired_fence_poll"},
		"SendCommandInput":              &bridgev1.SendCommandInputRequest{Scope: scope, TaskId: task, ToolUseEventId: stdinTool, OperationId: "op_retired_fence_stdin"},
		"CancelCommand":                 &bridgev1.CancelCommandRequest{Scope: scope, TaskId: task, ToolUseEventId: pollTool, OperationId: "op_retired_fence_cancel", Reason: "fence"},
		"AuthorizeWebToolExecution":     &bridgev1.AuthorizeWebToolExecutionRequest{Scope: scope, ToolUseEventId: webTool},
		"RunMemory":                     memory,
		"ClaimMcpToolResult":            &bridgev1.ClaimMcpToolResultRequest{Scope: scope, ToolUseEventId: mcpClaimTool, ClaimId: "claim_retired_fence"},
		"CommitMcpToolResult":           &bridgev1.CommitMcpToolResultRequest{Scope: scope, ToolUseEventId: mcpHeldTool, ClaimId: "claim_fence_held", ResultJson: `{"response":{"status":1,"result_text":"created","attachments":[]},"content_items":1,"refresh_triggered":false}`},
		"RelinquishMcpToolResult":       &bridgev1.RelinquishMcpToolResultRequest{Scope: scope, ToolUseEventId: mcpReleasedTool, ClaimId: "claim_fence_released"},
		"CommitInternalToolRepair":      &bridgev1.CommitInternalToolRepairRequest{Scope: scope, ModelRequestId: request, ModelToolCallId: "call_retired_fence_repair", ToolName: "unknown_tool", RepairKey: internalToolRepairKey(request, "call_retired_fence_repair", "unknown_tool"), CanonicalInputJson: `{"q":"x"}`, Error: &bridgev1.RuntimeToolError{ErrorJson: `{"type":"provider_tool_protocol_error","message":"invalid tool","retryable":false}`}},
		"CommitRuntimeTermination":      &bridgev1.CommitRuntimeTerminationRequest{Scope: scope, RuntimeWriteId: running.GetCommitted().GetEventId(), FailureJson: `{"type":"runtime","code":"runtime_invalid_sequence","message":"Runtime operation failed.","retryable":false,"fatal":true,"retryStatus":{"type":"terminal"},"reason":"runtime_contract_validation"}`},
	}

	identity := runtimecontrol.ProcessIdentity{Namespace: "tetral-agent-runtime", PodUID: podUID, ID: "process_retired_fence_replacement"}
	replacement, err := runtimecontrol.RegisterProcess(ctx, dbconnect.NewClientForTesting(admin), identity)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := runtimecontrol.ReportProcess(ctx, dbconnect.NewClientForTesting(admin), identity, replacement.RegistrationReceipt, runtimecontrol.ProcessAccepting); err != nil {
		t.Fatal(err)
	}
	before := receiptTenantSnapshot(t, admin)
	server := NewBridgeAPIServer(store)
	for _, method := range bridgev1.AgentRuntimeBridgeService_ServiceDesc.Methods {
		class := runtimeProcessCallerInventory[method.MethodName]
		request, ok := newWork[method.MethodName]
		if (class == "mutation" || class == "current") != ok {
			t.Errorf("%s is a %s RPC; new work for it is %t", method.MethodName, class, ok)
			continue
		}
		if !ok {
			continue
		}
		response, err := method.Handler(server, ctx, func(into any) error {
			proto.Merge(into.(proto.Message), request)
			return nil
		}, nil)
		if err != nil {
			if !runtimecontrol.IsScopeSupersededError(err) || !slices.ContainsFunc(status.Convert(err).Details(), func(detail any) bool {
				info, ok := detail.(*errdetails.ErrorInfo)
				return ok && info.Reason == "RUNTIME_PROCESS_STALE"
			}) {
				t.Errorf("%s answers %v; want a stale process", method.MethodName, err)
			}
			continue
		}
		outcome := response.(proto.Message).ProtoReflect()
		stale := outcome.Descriptor().Fields().ByName("stale")
		if stale == nil || !outcome.Has(stale) {
			t.Errorf("%s answers %v; want stale", method.MethodName, response)
		}
	}
	if after := receiptTenantSnapshot(t, admin); !reflect.DeepEqual(before, after) {
		for table, rows := range before {
			if after[table] != rows {
				t.Errorf("a refused request changed %s", table)
			}
		}
	}
}

// The generated base type answers Unimplemented for an RPC the server does
// not define. Without a store every defined RPC refuses before it reads its
// request, so any other answer shows the server's own method ran.
func TestBridgeAPIServerDefinesEveryRPC(t *testing.T) {
	for _, method := range bridgev1.AgentRuntimeBridgeService_ServiceDesc.Methods {
		_, err := method.Handler(NewBridgeAPIServer(nil), t.Context(), func(any) error { return nil }, nil)
		if code := status.Code(err); code != codes.FailedPrecondition {
			t.Errorf("%s without a store answers %s, not the server's refusal", method.MethodName, code)
		}
	}
}
