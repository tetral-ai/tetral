package enginetest

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/kubernetes"
	"github.com/tetral-ai/tetral/internal/queue"
	"github.com/tetral-ai/tetral/internal/runtimecontrol"
	bridgev1 "github.com/tetral-ai/tetral/services/bridge/gen/tetral/bridge/v1"
	jobrunner "github.com/tetral-ai/tetral/services/job-runner"
)

// RuntimePod is the Kubernetes Pod a Runtime client stands in for.
type RuntimePod struct {
	Namespace string
	Name      string
	UID       string
}

// Runtime is a client in one Runtime pod's place. It calls Bridge through a
// client whose connection authenticates as the Runtime's service account, and it
// takes Session bindings through Job Runner's delivery store, which sees this
// client as its only Runtime pod and reads its placement load from an
// endpoint the client serves. It runs no SQL of its own; the delivery store
// runs Job Runner's.
type Runtime struct {
	Bridge    bridgev1.AgentRuntimeBridgeServiceClient
	Pod       RuntimePod
	ProcessID string

	delivery  *jobrunner.PostgreSQLRuntimeDeliveryStore
	placement *httptest.Server
	receipt   string
}

// NewRuntime returns a client for pod whose process is processID. runner is
// Job Runner's database client. Close stops the placement endpoint.
func NewRuntime(bridge bridgev1.AgentRuntimeBridgeServiceClient, runner *dbconnect.Client, pod RuntimePod, processID string) (*Runtime, error) {
	r := &Runtime{Bridge: bridge, Pod: pod, ProcessID: processID}
	r.placement = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		if request.URL.Path != "/metrics/placement" {
			http.NotFound(w, request)
			return
		}
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		_, _ = io.WriteString(w, "runtimepod_active_sessions 0\nruntimepod_session_capacity 64\nruntimepod_container_memory_usage_bytes 1\nruntimepod_container_memory_limit_bytes 1000\nruntimepod_ready 1\nruntimepod_accepting_commands 1\n")
	}))
	address := strings.TrimPrefix(r.placement.URL, "http://")
	// The store dials the Pod IP; every dial reaches this client's endpoint.
	load := &http.Client{Transport: &http.Transport{DialContext: func(ctx context.Context, network, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, network, address)
	}}}
	r.delivery = jobrunner.NewJobRunnerRuntimeDeliveryStore(runner, nil, jobrunner.JobRunnerConfig{AgentRuntimeGRPCPort: 9090}, func() kubernetes.BindingVisibilitySnapshot {
		return kubernetes.NewBindingVisibilitySnapshotForTest(true, []kubernetes.BindingCandidate{{Namespace: pod.Namespace, PodName: pod.Name, PodUID: pod.UID, PodIP: "127.0.0.1"}})
	})
	resolver, ok := r.delivery.TargetResolver.(jobrunner.KubernetesRuntimeTargetResolver)
	if !ok {
		r.placement.Close()
		return nil, fmt.Errorf("delivery store resolves targets with %T, not Job Runner's Kubernetes resolver", r.delivery.TargetResolver)
	}
	resolver.LoadClient = load
	r.delivery.TargetResolver = resolver
	return r, nil
}

// Close stops the placement endpoint.
func (r *Runtime) Close() { r.placement.Close() }

// Register registers the client's process with RegisterRuntimeProcess and
// reports it accepting, so it becomes the Pod's current process.
func (r *Runtime) Register(ctx context.Context) error {
	registered, err := r.Bridge.RegisterRuntimeProcess(ctx, &bridgev1.RegisterRuntimeProcessRequest{RuntimeProcessId: r.ProcessID})
	if err != nil {
		return fmt.Errorf("register Runtime process: %w", err)
	}
	r.receipt = registered.GetRegistrationReceipt()
	reported, err := r.Bridge.ReportRuntimeProcess(ctx, &bridgev1.ReportRuntimeProcessRequest{RuntimeProcessId: r.ProcessID, RegistrationReceipt: r.receipt, Phase: bridgev1.RuntimeProcessPhase_RUNTIME_PROCESS_PHASE_ACCEPTING})
	if err != nil {
		return fmt.Errorf("report Runtime process accepting: %w", err)
	}
	if !reported.GetCurrent() {
		return fmt.Errorf("runtime process %s is not current after its accepting report", r.ProcessID)
	}
	return nil
}

// Bind takes a binding of the Session's thread through Job Runner's delivery
// store with a configuration update, as Job Runner does before it delivers to
// a Pod, and loads it with LoadContext. It returns the binding's scope.
func (r *Runtime) Bind(ctx context.Context, workspaceID, sessionID, threadID string) (*bridgev1.RuntimeScope, error) {
	job := jobrunner.RuntimeJob{Kind: queue.KindRuntimeConfigUpdate, WorkspaceID: workspaceID, SessionID: sessionID, SessionThreadID: threadID, ConfigGeneration: "1", RuntimeInputID: "runtime_config_update:" + sessionID + ":1"}
	plan, err := r.delivery.PrepareRuntimeCommand(ctx, job)
	if err != nil {
		return nil, fmt.Errorf("take a Runtime binding: %w", err)
	}
	attempt := plan.AttemptedBinding
	if attempt.BindingID == "" || attempt.TargetPodUID != r.Pod.UID {
		return nil, fmt.Errorf("runtime binding %+v does not target this client's Pod %s", attempt, r.Pod.UID)
	}
	scope := &bridgev1.RuntimeScope{WorkspaceId: workspaceID, SessionId: sessionID, SessionThreadId: threadID, Binding: &bridgev1.RuntimeBindingRef{BindingId: attempt.BindingID, BindingGeneration: attempt.Generation, TargetPodUid: attempt.TargetPodUID, RuntimeProcessId: attempt.RuntimeProcessID}}
	if _, err := r.Bridge.LoadContext(ctx, &bridgev1.LoadContextRequest{Scope: scope}); err != nil {
		return nil, fmt.Errorf("load the bound context: %w", err)
	}
	return scope, nil
}

// StartModelRequest writes the span.model_request_start of a model request
// under writeID, as the Runtime does before the model's first output.
func (r *Runtime) StartModelRequest(ctx context.Context, scope *bridgev1.RuntimeScope, writeID, modelRequestID string) error {
	boundary := int64(0)
	_, err := r.Bridge.WriteEvent(ctx, &bridgev1.WriteEventRequest{
		Scope:                         scope,
		RuntimeWriteId:                writeID,
		ModelRequestId:                modelRequestID,
		EventType:                     "span.model_request_start",
		PayloadJson:                   fmt.Sprintf(`{"type":"span.model_request_start","model_request_id":%q}`, modelRequestID),
		ContextThroughMessageSequence: &boundary,
		RequestKind:                   runtimecontrol.RequestKindAgentProviderRequest,
	})
	if err != nil {
		return fmt.Errorf("start model request: %w", err)
	}
	return nil
}

// ToolUse is one assistant tool-use event of a model request.
type ToolUse struct {
	WriteID         string
	ModelRequestID  string
	ModelToolCallID string
	Tool            string
	InputJSON       string
	// RouteCapability is machine_tool, or machine_control for write_stdin.
	RouteCapability string
}

// DeclareToolUse writes the tool-use event with decision allow, which leaves
// its route resolving, and returns the event's ID.
func (r *Runtime) DeclareToolUse(ctx context.Context, scope *bridgev1.RuntimeScope, use ToolUse) (string, error) {
	response, err := r.Bridge.WriteEvent(ctx, &bridgev1.WriteEventRequest{
		Scope:          scope,
		RuntimeWriteId: use.WriteID,
		ModelRequestId: use.ModelRequestID,
		ToolDeclaration: &bridgev1.RuntimeToolDeclaration{
			EventKind:                bridgev1.RuntimeToolEventKind_RUNTIME_TOOL_EVENT_KIND_TOOL,
			ModelToolCallId:          use.ModelToolCallID,
			ToolName:                 use.Tool,
			PublicExecutionInputJson: use.InputJSON,
			EvaluatedPermission:      "allow",
			RouteCapability:          use.RouteCapability,
		},
	})
	if err != nil {
		return "", fmt.Errorf("declare tool use: %w", err)
	}
	if response.GetCommitted() == nil {
		return "", fmt.Errorf("tool use declaration not committed: %v", response)
	}
	return response.GetCommitted().GetEventId(), nil
}

// AdmitToolCall sends request to Bridge's AdmitToolCall.
func (r *Runtime) AdmitToolCall(ctx context.Context, request *bridgev1.AdmitToolCallRequest) (*bridgev1.AdmitToolCallResponse, error) {
	return r.Bridge.AdmitToolCall(ctx, request)
}

// AdmitToolControl sends request to Bridge's AdmitToolControl.
func (r *Runtime) AdmitToolControl(ctx context.Context, request *bridgev1.AdmitToolControlRequest) (*bridgev1.AdmitToolControlResponse, error) {
	return r.Bridge.AdmitToolControl(ctx, request)
}

// CancelToolCall sends request to Bridge's CancelToolCall.
func (r *Runtime) CancelToolCall(ctx context.Context, request *bridgev1.CancelToolCallRequest) (*bridgev1.CancelToolCallResponse, error) {
	return r.Bridge.CancelToolCall(ctx, request)
}

// AwaitToolCall sends request to Bridge's AwaitToolCall.
func (r *Runtime) AwaitToolCall(ctx context.Context, request *bridgev1.AwaitToolCallRequest) (*bridgev1.AwaitToolCallResponse, error) {
	return r.Bridge.AwaitToolCall(ctx, request)
}
