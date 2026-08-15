package static

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestInternalProtoLayoutAndPackages(t *testing.T) {
	root := finalArchitectureEngineRoot(t)
	assertNoProtoSources(t, filepath.Join(root, "services", "agent-runtime", "packages", "protocol"))
}

func TestBridgeServiceLocalProtoLayoutAndPackages(t *testing.T) {
	root := finalArchitectureEngineRoot(t)
	source := "services/bridge/proto/tetral/bridge/v1/bridge.proto"
	body, err := os.ReadFile(filepath.Join(root, source)) //nolint:gosec // source tree path
	if err != nil {
		t.Fatalf("read bridge proto: %v", err)
	}
	text := string(body)
	for _, required := range []string{
		"package tetral.bridge.v1;",
		"github.com/tetral-ai/tetral/services/bridge/gen/tetral/bridge/v1;bridgev1",
		"service AgentRuntimeBridgeService",
		"rpc LoadContext(LoadContextRequest) returns (LoadContextResponse);",
		"rpc CommitInputs(CommitInputsRequest) returns (CommitInputsResponse);",
		"rpc CommitTaskNotificationResult(CommitTaskNotificationResultRequest) returns (CommitTaskNotificationResultResponse);",
		"rpc WriteEvent(WriteEventRequest) returns (WriteEventResponse);",
		"rpc SettleToolResult(SettleToolResultRequest) returns (SettleToolResultResponse);",
		"rpc WriteRequestEnd(WriteRequestEndRequest) returns (WriteRequestEndResponse);",
		"rpc FinishIdle(FinishIdleRequest) returns (FinishIdleResponse);",
		"rpc CommitRuntimeTermination(CommitRuntimeTerminationRequest) returns (CommitRuntimeTerminationResponse);",
		"rpc CreateSubagentThread(CreateSubagentThreadRequest) returns (CreateSubagentThreadResponse);",
		"rpc EnsureApprovalReviewerTrunk(EnsureApprovalReviewerTrunkRequest) returns (EnsureApprovalReviewerTrunkResponse);",
		"rpc EnsureApprovalReviewerSidecar(EnsureApprovalReviewerSidecarRequest) returns (EnsureApprovalReviewerSidecarResponse);",
		"rpc AdmitApprovalReviewInput(AdmitApprovalReviewInputRequest) returns (AdmitApprovalReviewInputResponse);",
		"rpc ResolveChildThread(ResolveChildThreadRequest) returns (ResolveChildThreadResponse);",
		"rpc ListChildThreads(ListChildThreadsRequest) returns (ListChildThreadsResponse);",
		"rpc DeliverInterAgentMail(DeliverInterAgentMailRequest) returns (DeliverInterAgentMailResponse);",
		"rpc ReadAgentMail(ReadAgentMailRequest) returns (ReadAgentMailResponse);",
		"rpc AdmitChildInterrupt(AdmitChildInterruptRequest) returns (AdmitChildInterruptResponse);",
		"rpc AwaitChildInterrupt(AwaitChildInterruptRequest) returns (AwaitChildInterruptResponse);",
		"message ChildThreadFact",
		"message DeliverInterAgentMailCommitted {}",
		"message AdmitChildInterruptCommitted",
		"string control_operation_id = 1;",
		"message MarkChildThreadActiveCommitted",
		"ChildLifecycleDisposition disposition = 1;",
		"rpc CloseChildControl(CloseChildControlRequest) returns (CloseChildControlResponse);",
		"rpc CloseApprovalReviewer(CloseApprovalReviewerRequest) returns (CloseApprovalReviewerResponse);",
		"rpc MarkChildThreadActive(MarkChildThreadActiveRequest) returns (MarkChildThreadActiveResponse);",
		"rpc AcceptSandboxExecution(AcceptSandboxExecutionRequest) returns (AcceptSandboxExecutionResponse);",
		"rpc AwaitSandboxExecution(AwaitSandboxExecutionRequest) returns (AwaitSandboxExecutionResponse);",
		"rpc ReadCommandResult(ReadCommandResultRequest) returns (ReadCommandResultResponse);",
		"rpc SendCommandInput(SendCommandInputRequest) returns (SendCommandInputResponse);",
		"rpc CancelCommand(CancelCommandRequest) returns (CancelCommandResponse);",
		"rpc RunMemory(RunMemoryRequest) returns (RunMemoryResponse);",
		"message AcceptSandboxExecutionRequest",
		"string tool_use_event_id = 2;",
		"message AcceptSandboxExecutionResponse",
		"message SandboxExecutionCommitted {}",
		"message AwaitSandboxExecutionResponse",
		"message RunMemoryRequest",
		"message MemoryRunCommitted",
		"message ClaimMcpToolResultRequest",
		"string claim_id = 3;",
		"message McpToolClaimAcquired",
		"message CommitMcpToolResultRequest",
		"message McpToolCommitCommitted {}",
		"rpc RelinquishMcpToolResult(RelinquishMcpToolResultRequest) returns (RelinquishMcpToolResultResponse);",
		"message RelinquishMcpToolResultRequest",
		"message McpToolRelinquishRelinquished {}",
		"string operation_id = 6;",
		"message RuntimeScope",
		"message RuntimeBindingRef",
		"message ServerToolUseUsage",
		"message SettleToolResultRequest",
		"RuntimeToolSettlement settlement = 2;",
		"message SettleToolResultResponse",
		"ToolResultCommitted committed = 1;",
		"ToolResultDuplicate duplicate = 2;",
		"ToolResultStale stale = 3;",
		"message RuntimeTerminationCommitted",
		"message RuntimeTerminationDuplicate",
		"message RuntimeTerminationStale {}",
		"int64 web_search_requests = 1;",
		"int64 web_fetch_requests = 2;",
	} {
		if !strings.Contains(text, required) {
			t.Fatalf("bridge proto missing %q", required)
		}
	}
	for _, forbidden := range []string{
		"string memory" + "_store_id",
		"memory" + "_store_id =",
		"rpc CreateChildThread(",
		"rpc ResolveInterAgentDelivery(",
		"message ChildLifecycleSource",
		"string child_thread_json",
	} {
		if strings.Contains(text, forbidden) {
			t.Fatalf("bridge proto exposes forbidden Runtime boundary field %q", forbidden)
		}
	}
	for _, generated := range []string{
		"services/bridge/gen/tetral/bridge/v1/bridge.pb.go",
		"services/bridge/gen/tetral/bridge/v1/bridge_grpc.pb.go",
		"services/agent-runtime/packages/protocol/src/gen-bridge/tetral/bridge/v1/bridge.ts",
		"services/gateway/packages/protocol/src/gen-bridge/tetral/bridge/v1/bridge.ts",
	} {
		if _, err := os.Stat(filepath.Join(root, generated)); err != nil {
			t.Fatalf("expected bridge generated file %s: %v", generated, err)
		}
	}
}

func TestBridgeStableReasoningIsDerivedFromDurableAssistantMembers(t *testing.T) {
	root := finalArchitectureEngineRoot(t)
	path := filepath.Join(root, "services/bridge/proto/tetral/bridge/v1/bridge.proto")
	body, err := os.ReadFile(path) //nolint:gosec // repository-local protocol source.
	if err != nil {
		t.Fatalf("read bridge proto: %v", err)
	}
	text := string(body)
	if strings.Contains(text, "StableReasoningPart") || strings.Contains(text, "stable_reasoning_parts") {
		t.Fatal("bridge proto retained Runtime-authored stable reasoning transport")
	}
	if forbidden := "CommitStable" + "ReasoningPart"; strings.Contains(text, forbidden) {
		t.Fatalf("bridge proto retained standalone stable reasoning surface %q", forbidden)
	}
}

func TestRuntimePodServiceLocalProtoLayoutAndPackages(t *testing.T) {
	root := finalArchitectureEngineRoot(t)
	source := "services/agent-runtime/proto/tetral/agent_runtime/v1/agent_runtime.proto"
	body, err := os.ReadFile(filepath.Join(root, source)) //nolint:gosec // source tree path
	if err != nil {
		t.Fatalf("read runtime pod proto: %v", err)
	}
	text := string(body)
	for _, required := range []string{
		"package tetral.agent_runtime.v1;",
		"github.com/tetral-ai/tetral/services/agent-runtime/gen/tetral/agent_runtime/v1;agentruntimev1",
		"rpc AcceptInput(AcceptInputRequest) returns (AcceptInputResponse);",
		"rpc AcceptAgentMail(AcceptAgentMailRequest) returns (AcceptAgentMailResponse);",
		"rpc AcceptTaskNotification(AcceptTaskNotificationRequest) returns (AcceptTaskNotificationResponse);",
		"rpc Interrupt(InterruptRequest) returns (InterruptResponse);",
		"rpc ResolveToolConfirmation(ResolveToolConfirmationRequest) returns (ResolveToolConfirmationResponse);",
		"rpc ApplyRuntimeConfig(ApplyRuntimeConfigRequest) returns (ApplyRuntimeConfigResponse);",
		"rpc CleanupSession(CleanupSessionRequest) returns (CleanupSessionResponse);",
		"int64 input_order = 8;",
		"string delivery_id = 8;",
		"string tool_use_event_id = 8;",
		"string cleanup_operation_id = 6;",
	} {
		if !strings.Contains(text, required) {
			t.Fatalf("runtime pod proto missing %q", required)
		}
	}
	for _, forbidden := range []string{"RuntimeInput" + "CommandRequest", "RuntimeInput" + "CommandResponse", "RuntimeCommand" + "Kind", "command_kind", "event_ids"} {
		if strings.Contains(text, forbidden) {
			t.Fatalf("runtime pod proto retained universal command member %q", forbidden)
		}
	}
	for _, generated := range []string{
		"services/agent-runtime/gen/tetral/agent_runtime/v1/agent_runtime.pb.go",
		"services/agent-runtime/gen/tetral/agent_runtime/v1/agent_runtime_grpc.pb.go",
		"services/agent-runtime/packages/protocol/src/gen/tetral/agent_runtime/v1/agent_runtime.ts",
	} {
		if _, err := os.Stat(filepath.Join(root, generated)); err != nil {
			t.Fatalf("expected runtime pod generated file %s: %v", generated, err)
		}
	}
}

func TestGatewayServiceLocalProtoLayoutAndPackages(t *testing.T) {
	root := finalArchitectureEngineRoot(t)
	source := "services/gateway/proto/tetral/provider_gateway/v1/provider_gateway.proto"
	body, err := os.ReadFile(filepath.Join(root, source)) //nolint:gosec // source tree path
	if err != nil {
		t.Fatalf("read gateway proto: %v", err)
	}
	text := string(body)
	for _, required := range []string{
		"package tetral.provider_gateway.v1;",
		"github.com/tetral-ai/tetral/services/gateway/gen/tetral/provider_gateway/v1;providergatewayv1",
		"service ProviderGatewayService",
		"rpc StreamProviderRequest(ProviderRequest) returns (stream ProviderStreamEvent);",
		"rpc RunWeb(RunWebRequest) returns (RunWebResponse);",
		"PROVIDER_REQUEST_KIND_AGENT_PROVIDER_REQUEST",
		"PROVIDER_REQUEST_KIND_COMPACTION_SUMMARY",
		"PROVIDER_REQUEST_KIND_APPROVAL_REVIEWER",
		"string request_id = 1;",
		"string model_request_id = 2;",
		"ProviderRequestKind request_kind = 3;",
		"string runtime_binding_token = 10;",
		"repeated SystemSegment system = 12;",
		"repeated ProviderContextEntry context = 13;",
		"message ProviderContextEntry",
		"repeated ProviderContextItem content = 2;",
		"message ProviderToolCall",
		"string model_tool_call_id = 1;",
		"repeated RuntimeToolDefinition tools = 14;",
		"repeated ProviderRequestAttachment attachments = 15;",
		"ProviderRequestLimits limits = 16;",
		"PROVIDER_STREAM_EVENT_TYPE_TEXT_START",
		"PROVIDER_STREAM_EVENT_TYPE_TEXT_DELTA",
		"PROVIDER_STREAM_EVENT_TYPE_TEXT_END",
		"PROVIDER_STREAM_EVENT_TYPE_REASONING_START",
		"PROVIDER_STREAM_EVENT_TYPE_REASONING_DELTA",
		"PROVIDER_STREAM_EVENT_TYPE_REASONING_END",
		"PROVIDER_STREAM_EVENT_TYPE_TOOL_INPUT_START",
		"PROVIDER_STREAM_EVENT_TYPE_TOOL_INPUT_DELTA",
		"PROVIDER_STREAM_EVENT_TYPE_TOOL_INPUT_END",
		"PROVIDER_STREAM_EVENT_TYPE_TOOL_CALL",
		"PROVIDER_STREAM_EVENT_TYPE_FINISH",
		"PROVIDER_STREAM_EVENT_TYPE_PROVIDER_ERROR",
		"int64 input_total_tokens = 1;",
		"int64 input_uncached_tokens = 2;",
		"optional int64 input_cache_read_tokens = 3;",
		"optional int64 input_cache_write_tokens = 4;",
		"int64 output_total_tokens = 5;",
		"optional int64 output_reasoning_tokens = 6;",
		"optional int64 total_tokens = 7;",
		"string provider_usage_json = 8;",
		"ProviderFinishPayload finish = 8;",
		"ProviderErrorPayload provider_error = 9;",
		"WebToolInput input = 5;",
		"optional int32 next_lineno = 4;",
		"repeated WebSearchQuery search_query = 1;",
		"repeated WebOpenRequest open = 2;",
		"repeated WebFindRequest find = 3;",
		"optional string title = 3;",
		"optional int32 line_start = 4;",
		"optional int32 line_end = 5;",
		"optional int32 total_lines = 6;",
	} {
		if !strings.Contains(text, required) {
			t.Fatalf("gateway proto missing %q", required)
		}
	}
	for _, retired := range []string{
		"optional string tool_use_event_id = 3;",
		"string source_tool_use_event_id",
	} {
		if strings.Contains(text, retired) {
			t.Fatalf("Gateway provider carrier retained Runtime-owned member %q", retired)
		}
	}
	for _, generated := range []string{
		"services/gateway/gen/tetral/provider_gateway/v1/provider_gateway.pb.go",
		"services/gateway/gen/tetral/provider_gateway/v1/provider_gateway_grpc.pb.go",
		"services/gateway/packages/protocol/src/gen/tetral/provider_gateway/v1/provider_gateway.ts",
	} {
		if _, err := os.Stat(filepath.Join(root, generated)); err != nil {
			t.Fatalf("expected gateway generated file %s: %v", generated, err)
		}
	}
}

func TestQueueServiceLocalProtoLayoutAndPackages(t *testing.T) {
	root := finalArchitectureEngineRoot(t)
	source := "services/queue/proto/tetral/queue/v1/queue.proto"
	body, err := os.ReadFile(filepath.Join(root, source)) //nolint:gosec // source tree path
	if err != nil {
		t.Fatalf("read queue proto: %v", err)
	}
	text := string(body)
	for _, required := range []string{
		"package tetral.queue.v1;",
		"github.com/tetral-ai/tetral/services/queue/gen/tetral/queue/v1;queuev1",
		"rpc Lease(LeaseRequest) returns (LeaseResponse);",
		"rpc Heartbeat(HeartbeatRequest) returns (HeartbeatResponse);",
		"rpc Ack(AckRequest) returns (TransitionResponse);",
		"rpc Retry(RetryRequest) returns (TransitionResponse);",
		"rpc Defer(DeferRequest) returns (TransitionResponse);",
		"rpc DeadLetter(DeadLetterRequest) returns (TransitionResponse);",
		"rpc Cancel(CancelRequest) returns (CancelResponse);",
		"repeated string kinds = 2;",
		"int32 cancelled_count = 1;",
	} {
		if !strings.Contains(text, required) {
			t.Fatalf("queue proto missing %q", required)
		}
	}
	for _, generated := range []string{
		"services/queue/gen/tetral/queue/v1/queue.pb.go",
		"services/queue/gen/tetral/queue/v1/queue_grpc.pb.go",
	} {
		if _, err := os.Stat(filepath.Join(root, generated)); err != nil {
			t.Fatalf("expected queue generated file %s: %v", generated, err)
		}
	}
}

func TestGeneratedTypeScriptProtocolUsesSafeNumericScalars(t *testing.T) {
	root := finalArchitectureEngineRoot(t)
	agentRuntimeBody, err := os.ReadFile(filepath.Join(root, "services/agent-runtime/packages/protocol/src/gen/tetral/agent_runtime/v1/agent_runtime.ts")) //nolint:gosec // generated source path.
	if err != nil {
		t.Fatalf("read generated runtime pod protocol: %v", err)
	}
	text := string(agentRuntimeBody)
	for _, forbidden := range []string{"bindingGeneration: bigint", "bindingGeneration: string", "inputOrder: bigint", "inputOrder: string"} {
		if strings.Contains(text, forbidden) {
			t.Fatalf("generated Runtime Pod protocol contains forbidden scalar %q", forbidden)
		}
	}
	for _, required := range []string{"bindingGeneration: number", "inputOrder: number"} {
		if !strings.Contains(text, required) {
			t.Fatalf("generated Runtime Pod protocol missing TypeScript number scalar %q", required)
		}
	}
}

func TestRootBufWorkspaceListsOnlyServiceLocalProtoModules(t *testing.T) {
	root := finalArchitectureEngineRoot(t)
	body, err := os.ReadFile(filepath.Join(root, "buf.yaml")) //nolint:gosec // repository-local config path.
	if err != nil {
		t.Fatalf("read root buf.yaml: %v", err)
	}
	text := string(body)
	for _, required := range []string{
		"path: services/bridge/proto",
		"path: services/agent-runtime/proto",
		"path: services/gateway/proto",
		"path: services/queue/proto",
	} {
		if !strings.Contains(text, required) {
			t.Fatalf("root buf.yaml missing service-local module %q", required)
		}
	}
}

func TestQueueServiceLocalBufGenerationFreshness(t *testing.T) {
	root := finalArchitectureEngineRoot(t)
	cmd := exec.Command("buf", "generate", "--template", "services/queue/buf.gen.yaml", "services/queue/proto") //nolint:gosec // repository-local generation command.
	cmd.Dir = root
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("queue buf generate failed: %v\n%s", err, output)
	}
	assertGeneratedOutputClean(t, root, "services/queue/gen")
}

func TestRuntimePodServiceLocalBufGenerationFreshness(t *testing.T) {
	root := finalArchitectureEngineRoot(t)
	cmd := exec.Command("buf", "generate", "--template", "services/agent-runtime/buf.gen.yaml") //nolint:gosec // repository-local generation command.
	cmd.Dir = root
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("runtime pod buf generate failed: %v\n%s", err, output)
	}
	assertGeneratedOutputClean(t, root, "services/agent-runtime/gen", "services/agent-runtime/packages/protocol/src/gen")
}

func TestGatewayServiceLocalBufGenerationFreshness(t *testing.T) {
	root := finalArchitectureEngineRoot(t)
	cmd := exec.Command("buf", "generate", "--template", "services/gateway/buf.gen.yaml") //nolint:gosec // repository-local generation command.
	cmd.Dir = root
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("gateway buf generate failed: %v\n%s", err, output)
	}
	assertGeneratedOutputClean(t, root, "services/gateway/gen", "services/gateway/packages/protocol/src/gen")
}

func TestBridgeServiceLocalBufGenerationFreshness(t *testing.T) {
	root := finalArchitectureEngineRoot(t)
	cmd := exec.Command("buf", "generate", "--template", "services/bridge/buf.gen.yaml") //nolint:gosec // repository-local generation command.
	cmd.Dir = root
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("bridge buf generate failed: %v\n%s", err, output)
	}
	assertGeneratedOutputClean(t, root, "services/bridge/gen", "services/agent-runtime/packages/protocol/src/gen-bridge", "services/gateway/packages/protocol/src/gen-bridge")
}

func assertNoProtoSources(t *testing.T, root string) {
	t.Helper()
	if _, err := os.Stat(root); os.IsNotExist(err) {
		return
	} else if err != nil {
		t.Fatalf("stat %s: %v", root, err)
	}
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			return nil
		}
		if filepath.Ext(path) == ".proto" {
			rel, relErr := filepath.Rel(finalArchitectureEngineRoot(t), path)
			if relErr != nil {
				return relErr
			}
			t.Fatalf("unexpected proto source outside service-local protocol roots: %s", filepath.ToSlash(rel))
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk %s: %v", root, err)
	}
}

func assertGeneratedOutputClean(t *testing.T, root string, paths ...string) {
	t.Helper()
	diffArgs := append([]string{"diff", "--quiet", "--"}, paths...)
	diff := exec.Command("git", diffArgs...) //nolint:gosec // repository-local generated-output test pathspecs.
	diff.Dir = root
	if output, err := diff.CombinedOutput(); err != nil {
		t.Fatalf("generated proto output is dirty after buf generate: %v\n%s", err, output)
	}
	otherArgs := append([]string{"ls-files", "--others", "--exclude-standard", "--"}, paths...)
	others := exec.Command("git", otherArgs...) //nolint:gosec // repository-local generated-output test pathspecs.
	others.Dir = root
	output, err := others.CombinedOutput()
	if err != nil {
		t.Fatalf("list untracked generated proto output: %v\n%s", err, output)
	}
	if strings.TrimSpace(string(output)) != "" {
		t.Fatalf("generated proto output has untracked files after buf generate:\n%s", output)
	}
}
