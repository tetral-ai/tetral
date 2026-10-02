package agentruntimebridge

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"strings"
	"testing"

	"google.golang.org/protobuf/encoding/protojson"

	"github.com/tetral-ai/tetral/internal/runtimecontrol"
	agentruntimev1 "github.com/tetral-ai/tetral/services/agent-runtime/gen/tetral/agent_runtime/v1"
	bridgev1 "github.com/tetral-ai/tetral/services/bridge/gen/tetral/bridge/v1"
)

type taskNotificationRuntimeCompositionOutput struct {
	Declaration         json.RawMessage   `json:"declaration"`
	AcceptResult        json.RawMessage   `json:"acceptResult"`
	CommitResult        json.RawMessage   `json:"commitResult"`
	ProviderInvocations int               `json:"providerInvocations"`
	RequestEndCount     int               `json:"requestEndCount"`
	ProviderContexts    []json.RawMessage `json:"providerContexts"`
}

func TestTaskNotificationCanonicalShapesCrossRuntimeDeclarationBoundary(t *testing.T) {
	cases := []struct {
		name           string
		taskID         string
		terminalStatus string
		storedResult   string
		wantStatus     string
	}{
		{name: "completed empty", taskID: "task_shape", terminalStatus: "completed", storedResult: `{"status":"completed","exit_code":null,"stdout":{"text":"","truncated":false},"stderr":{"text":"","truncated":false}}`, wantStatus: "completed"},
		{name: "failed escaped Unicode", taskID: "task_<>&\u2028paragraph\u2029", terminalStatus: "failed", storedResult: "{\"status\":\"failed\",\"exit_code\":-1,\"stdout\":{\"text\":\"<line>&\\\\n雪\\u2028paragraph\\u2029\",\"truncated\":false},\"stderr\":{\"text\":\"failed\",\"truncated\":true,\"total_bytes\":24,\"total_lines\":2}}", wantStatus: "failed"},
		{name: "cancelled safe maximum", taskID: "task_shape", terminalStatus: "cancelled", storedResult: `{"status":"cancelled","exit_code":9007199254740991,"stdout":{"text":"cancelled","truncated":false,"total_bytes":9},"stderr":{"text":"","truncated":false}}`, wantStatus: "cancelled"},
		{name: "expired safe metadata", taskID: "task_shape", terminalStatus: "expired", storedResult: `{"status":"expired","stdout":{"text":"","truncated":false},"stderr":{"text":"expired","truncated":true,"original_bytes":9007199254740991,"original_lines":0}}`, wantStatus: "expired"},
		{name: "unknown lowered", taskID: "task_shape", terminalStatus: "unknown_outcome", storedResult: `{"status":"unknown_outcome","stdout":{"text":"","truncated":false},"stderr":{"text":"unknown","truncated":false}}`, wantStatus: "failed"},
		{name: "large output fitted", taskID: "task_large_shape", terminalStatus: "completed", storedResult: `{"status":"completed","exit_code":0,"stdout":{"text":"` + strings.Repeat("a", 40*1024) + `","truncated":false},"stderr":{"text":"` + strings.Repeat("b", 40*1024) + `","truncated":false}}`, wantStatus: "completed"},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			payload, err := runtimecontrol.CanonicalTaskNotificationPayloadJSON(testCase.taskID, "sevt_tool_shape", testCase.terminalStatus, testCase.storedResult)
			if err != nil {
				t.Fatalf("build canonical shape: %v", err)
			}
			request := &agentruntimev1.AcceptTaskNotificationRequest{
				WorkspaceId: "default", SessionId: "sesn_task_shape", SessionThreadId: "thr_task_shape",
				BindingId: "bind_task_shape", BindingGeneration: 1, TargetPodUid: "pod_task_shape",
				RuntimeInputId: "task_notification:" + testCase.taskID, InputOrder: 0,
				NotificationJson: payload,
			}
			composed, err := runTaskNotificationRuntimeComposition(context.Background(), t.TempDir()+"/shape.json", request, nil)
			if err != nil {
				t.Fatalf("run Runtime shape composition: %v", err)
			}
			declaration := &bridgev1.CommitTaskNotificationResultRequest{}
			if err := protojson.Unmarshal(composed.Declaration, declaration); err != nil {
				t.Fatalf("decode Runtime declaration: %v", err)
			}
			if declaration.GetRuntimeInputId() != request.GetRuntimeInputId() {
				t.Fatalf("Runtime declaration = %#v; want exact input target", declaration)
			}
			if len([]byte(payload)) > runtimecontrol.RuntimeTaskNotificationPayloadMaxBytes {
				t.Fatalf("Runtime declaration payload bytes = %d; want <= %d", len([]byte(payload)), runtimecontrol.RuntimeTaskNotificationPayloadMaxBytes)
			}
			var payloadObject map[string]any
			if err := json.Unmarshal([]byte(payload), &payloadObject); err != nil || payloadObject["status"] != testCase.wantStatus {
				t.Fatalf("canonical status = %#v/%v; want %s", payloadObject["status"], err, testCase.wantStatus)
			}
			declarationJSON := string(composed.Declaration)
			if strings.Contains(declarationJSON, "notificationJson") || strings.Contains(declarationJSON, "resultJson") || strings.Contains(declarationJSON, "stdout") || strings.Contains(declarationJSON, "stderr") {
				t.Fatalf("Runtime declaration echoed canonical task payload: %s", declarationJSON)
			}
		})
	}
}

func runTaskNotificationRuntimeComposition(
	ctx context.Context,
	inputPath string,
	request *agentruntimev1.AcceptTaskNotificationRequest,
	commitResponse *bridgev1.CommitTaskNotificationResultResponse,
) (taskNotificationRuntimeCompositionOutput, error) {
	input := map[string]any{
		"notificationJson":  request.GetNotificationJson(),
		"workspaceId":       request.GetWorkspaceId(),
		"sessionId":         request.GetSessionId(),
		"sessionThreadId":   request.GetSessionThreadId(),
		"bindingId":         request.GetBindingId(),
		"bindingGeneration": request.GetBindingGeneration(),
		"targetPodUid":      request.GetTargetPodUid(),
		"runtimeInputId":    request.GetRuntimeInputId(),
		"inputOrder":        request.GetInputOrder(),
	}
	if commitResponse != nil {
		var outcome map[string]any
		switch {
		case commitResponse.GetCommitted() != nil:
			outcome = map[string]any{"committed": assignedContextSequencesForComposition(commitResponse.GetCommitted().GetAssignedContextSequences())}
		case commitResponse.GetStale() != nil:
			outcome = map[string]any{"stale": map[string]any{}}
		case commitResponse.GetParked() != nil:
			outcome = map[string]any{"parked": map[string]any{}}
		case commitResponse.GetRejected() != nil:
			outcome = map[string]any{"rejected": map[string]any{"reason": commitResponse.GetRejected().GetReason()}}
		default:
			return taskNotificationRuntimeCompositionOutput{}, errors.New("commit response has no outcome")
		}
		input["commitResponse"] = outcome
	}
	rawInput, err := json.Marshal(input)
	if err != nil {
		return taskNotificationRuntimeCompositionOutput{}, err
	}
	if err := os.WriteFile(inputPath, rawInput, 0o600); err != nil {
		return taskNotificationRuntimeCompositionOutput{}, err
	}
	command := exec.CommandContext(ctx, "bun", "packages/runtime-pod/test/fixtures/task-notification-composition.ts", inputPath) //nolint:gosec // Fixed repository fixture and test-owned input.
	command.Dir = "../agent-runtime"
	output, err := command.CombinedOutput()
	if err != nil {
		return taskNotificationRuntimeCompositionOutput{}, errors.New(string(output))
	}
	var composed taskNotificationRuntimeCompositionOutput
	if err := json.Unmarshal(output, &composed); err != nil {
		return taskNotificationRuntimeCompositionOutput{}, err
	}
	return composed, nil
}

func assignedContextSequencesForComposition(sequences []int64) map[string]any {
	return map[string]any{"assignedContextSequences": sequences}
}
