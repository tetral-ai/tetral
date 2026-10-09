package agentruntimebridge

import (
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"testing"
	"time"
)

type providerRescheduleRecoveryComposition struct {
	ResultType                    string          `json:"resultType"`
	ProviderInvocations           int             `json:"providerInvocations"`
	ExecutorInvocations           int             `json:"executorInvocations"`
	SandboxAcceptanceInvocations  int             `json:"sandboxAcceptanceInvocations"`
	SandboxObservationInvocations int             `json:"sandboxObservationInvocations"`
	WaitedMS                      []int64         `json:"waitedMs"`
	AcceptedInputBarrierEntered   bool            `json:"acceptedInputCommitBarrierEntered"`
	AcceptedInputBarrierReleased  bool            `json:"acceptedInputCommitBarrierReleased"`
	ProviderContext               json.RawMessage `json:"providerContext"`
	RecoveredTurnEvents           []string        `json:"recoveredTurnEventIds"`
	PreloadResult                 json.RawMessage `json:"preloadResult"`
	LastSnapshot                  json.RawMessage `json:"lastSnapshot"`
	TerminationResults            json.RawMessage `json:"terminationResults"`
	Command                       struct {
		WorkspaceID       string `json:"workspaceId"`
		SessionID         string `json:"sessionId"`
		SessionThreadID   string `json:"sessionThreadId"`
		BindingID         string `json:"bindingId"`
		BindingGeneration int64  `json:"bindingGeneration"`
		TargetPodUID      string `json:"targetPodUid"`
		SourceEventID     string `json:"sourceEventId"`
	} `json:"command"`
}

func runProviderRescheduleRecoveryComposition(t *testing.T, input map[string]any) providerRescheduleRecoveryComposition {
	t.Helper()
	inputJSON, err := json.Marshal(input)
	if err != nil {
		t.Fatalf("encode provider reschedule recovery input: %v", err)
	}
	inputPath := t.TempDir() + "/input.json"
	if err := os.WriteFile(inputPath, inputJSON, 0o600); err != nil {
		t.Fatalf("write provider reschedule recovery input: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, "bun", "packages/runtime-pod/test/fixtures/provider-reschedule-recovery-composition.ts", inputPath) //nolint:gosec // Fixed Runtime composition fixture and test-owned input.
	command.Dir = "../agent-runtime"
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("run provider reschedule recovery composition: %v: %s", err, output)
	}
	var result providerRescheduleRecoveryComposition
	if err := json.Unmarshal(output, &result); err != nil {
		t.Fatalf("decode provider reschedule recovery composition: %v: %s", err, output)
	}
	return result
}
