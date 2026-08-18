//go:build daytona_release_smoke

package tetralsandbox

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/daytonaio/daytona/libs/sdk-go/pkg/daytona"
	"github.com/daytonaio/daytona/libs/sdk-go/pkg/types"

	"github.com/tetral-ai/tetral/internal/id"
	"github.com/tetral-ai/tetral/internal/sandbox"
	"github.com/tetral-ai/tetral/internal/sandbox/driver"
)

const (
	envDaytonaReleaseSmoke         = "TETRAL_DAYTONA_RELEASE_SMOKE"
	envDaytonaReleaseArtifactRef   = "TETRAL_DAYTONA_RELEASE_SMOKE_ARTIFACT_REF"
	daytonaReleaseCommandTimeout   = 2 * time.Minute
	daytonaReleaseLifecycleTimeout = 15 * time.Minute
)

// TestDaytonaPublishedImageProductionAdapterSmoke is an operator-triggered
// release gate. It deliberately never skips: the workflow selects it only
// after an immutable published Sandbox image and Daytona credentials exist.
func TestDaytonaPublishedImageProductionAdapterSmoke(t *testing.T) {
	if os.Getenv(envDaytonaReleaseSmoke) != "1" {
		t.Fatalf("%s=1 is required; the Daytona release smoke never skips", envDaytonaReleaseSmoke)
	}
	for _, name := range []string{EnvDaytonaAPIURL, EnvDaytonaAPIKey, envDaytonaReleaseArtifactRef} {
		if strings.TrimSpace(os.Getenv(name)) == "" {
			t.Fatalf("%s is required; the Daytona release smoke never skips", name)
		}
	}
	artifactRef := strings.TrimSpace(os.Getenv(envDaytonaReleaseArtifactRef))
	if !isImmutablePublishedImageRef(artifactRef) {
		t.Fatalf("%s must select a published image by sha256 digest", envDaytonaReleaseArtifactRef)
	}

	ctx, cancel := context.WithTimeout(context.Background(), daytonaReleaseLifecycleTimeout)
	defer cancel()
	cfg := driver.Config{
		DaytonaAPIURL:  os.Getenv(EnvDaytonaAPIURL),
		DaytonaTarget:  os.Getenv(EnvDaytonaTarget),
		DaytonaAPIKey:  os.Getenv(EnvDaytonaAPIKey),
		CommandTimeout: daytonaReleaseCommandTimeout,
	}
	client, err := daytona.NewClientWithConfig(&types.DaytonaConfig{
		APIKey: cfg.DaytonaAPIKey, APIUrl: cfg.DaytonaAPIURL, Target: cfg.DaytonaTarget,
	})
	if err != nil {
		t.Fatalf("create Daytona SDK client: %v", err)
	}
	lifecycle, err := driver.NewDaytonaLifecycleProviderForSDKClient(client, cfg)
	if err != nil {
		t.Fatalf("create production Daytona lifecycle: %v", err)
	}
	helper, err := driver.NewDaytonaHelperExecutorForSDKClient(client, cfg.CommandTimeout)
	if err != nil {
		t.Fatalf("create production Daytona Helper executor: %v", err)
	}

	var logBuffer bytes.Buffer
	payloadCanary := "daytona-release-payload-" + strings.ToLower(id.New("cnry_"))
	stdinCanary := "daytona-release-stdin-" + strings.ToLower(id.New("cnry_"))
	commandOutputCanary := "daytona-release-output-" + strings.ToLower(id.New("cnry_"))
	providerResponseCanary := "daytona-release-response-" + strings.ToLower(id.New("cnry_"))
	adapter := &DaytonaAdapter{
		Lifecycle: lifecycle,
		Tools:     helper,
		Logger:    slog.New(slog.NewTextHandler(&logBuffer, nil)),
	}
	logProviderOutcomeCompletion(context.Background(), adapter.Logger, "sandbox.provider.release_smoke_log_policy",
		providerOperationIdentity{}, 0, ProviderOutcome[struct{}]{
			Disposition: ProviderTerminal, ErrorKind: "provider_test_response",
			ProviderSafeMessage: "Authorization: Bearer " + providerResponseCanary,
		})
	defer func() {
		registered := []struct {
			name  string
			value string
		}{
			{name: "credential", value: cfg.DaytonaAPIKey},
			{name: "payload", value: payloadCanary},
			{name: "stdin", value: stdinCanary},
			{name: "command output", value: commandOutputCanary},
			{name: "provider response", value: providerResponseCanary},
		}
		for _, secret := range registered {
			if strings.Contains(logBuffer.String(), secret.value) {
				t.Errorf("Daytona release smoke logged registered %s content", secret.name)
			}
		}
	}()

	sandboxID := "release-" + strings.ToLower(id.New("sbx_"))
	setup := sandbox.SandboxSetup{
		SandboxID:           sandboxID,
		ProviderArtifactRef: artifactRef,
		Network:             sandbox.NetworkSetup{Type: "unrestricted"},
	}
	activated := adapter.Activate(ctx, ActivationRequest{Kind: ActivationCreate, Setup: setup})
	if activated.Failed() || activated.Value.SandboxID == "" {
		t.Fatalf("create published-image Daytona Sandbox: kind=%s disposition=%s", activated.ErrorKind, activated.Disposition)
	}
	handle := activated.Value
	defer func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cleanupCancel()
		released := adapter.Release(cleanupCtx, ReleaseRequest{Handle: handle})
		if released.Failed() || !released.Value.Released {
			t.Errorf("delete Daytona release-smoke Sandbox: kind=%s disposition=%s", released.ErrorKind, released.Disposition)
		}
	}()

	target := driver.ToolTarget{
		WorkspaceID: "default", SessionID: "sesn_daytona_release_smoke",
		SessionThreadID: "thr_daytona_release_smoke", BindingID: "bind_daytona_release_smoke",
		BindingGeneration: 1, SandboxID: sandboxID, ProviderSandboxID: handle.SandboxID,
	}
	assertDaytonaReleaseTool(t, ctx, adapter, handle, driver.ToolInvocation{
		Target: target, ToolUseEventID: "sevt_daytona_release_write", ToolName: "Write",
		InputJSON: fmt.Sprintf(`{"file_path":"/workspace/release-smoke/note.txt","content":%q}`, payloadCanary+"\n"),
	})
	identityCommand := fmt.Sprintf("set -eu; test \"$(id -u)\" -ne 0; printf 'user=%%s\\nhome=%%s\\nshell=%%s\\n' \"$(id -un)\" \"$HOME\" \"$(getent passwd \"$(id -u)\" | cut -d: -f7)\"; printf 'command-output=%%s\\n' %q; git -C /workspace/release-smoke init -q; git -C /workspace/release-smoke config user.name 'Tetral Smoke'; git -C /workspace/release-smoke config user.email smoke@tetral.invalid; git -C /workspace/release-smoke add note.txt; git -C /workspace/release-smoke status --short", commandOutputCanary)
	identityAndGit := assertDaytonaReleaseTool(t, ctx, adapter, handle, driver.ToolInvocation{
		Target: target, ToolUseEventID: "sevt_daytona_release_git", ToolName: "exec_command",
		InputJSON: fmt.Sprintf(`{"cmd":%q,"yield_time_ms":30000,"max_output_tokens":1000}`, identityCommand),
	})
	for _, marker := range []string{"user=daytona", "home=/home/daytona", "shell=/bin/bash", "command-output=" + commandOutputCanary, "A  note.txt"} {
		if !strings.Contains(identityAndGit, marker) {
			t.Fatalf("published Sandbox identity/Git result is missing %q", marker)
		}
	}

	detached := assertDaytonaReleaseToolExecution(t, ctx, adapter, handle, driver.ToolInvocation{
		Target: target, ToolUseEventID: "sevt_daytona_release_detached", ToolName: "exec_command",
		InputJSON: `{"cmd":"IFS= read -r line; printf 'stdin-received\\n'; while :; do sleep 1; done","yield_time_ms":250,"max_output_tokens":100}`,
	})
	if detached.BackgroundTask == nil {
		t.Fatal("detached Daytona Helper execution returned no background task")
	}
	reference := driver.CommandReference{
		Target: target, Task: *detached.BackgroundTask,
		ToolUseEventID: "sevt_daytona_release_detached", MaxOutputTokens: 100,
	}
	input := adapter.SendBackgroundInput(ctx, driver.CommandInput{
		CommandReference: reference,
		InputJSON:        fmt.Sprintf(`{"chars":%q,"yield_time_ms":250,"write_seq":1,"max_output_tokens":100}`, stdinCanary+"\n"),
	})
	if input.Failed() {
		t.Fatalf("fresh Daytona Helper stdin failed: kind=%s disposition=%s", input.ErrorKind, input.Disposition)
	}
	poll := adapter.PollBackground(ctx, reference)
	if poll.Failed() {
		t.Fatalf("fresh Daytona Helper poll failed: kind=%s disposition=%s", poll.ErrorKind, poll.Disposition)
	}
	cancelled := adapter.CancelBackground(ctx, driver.CommandCancel{CommandReference: reference, Reason: "release smoke complete"})
	if cancelled.Failed() || cancelled.Value.TerminalStatus == "" {
		t.Fatalf("fresh Daytona Helper cancel failed: kind=%s disposition=%s", cancelled.ErrorKind, cancelled.Disposition)
	}
}

func assertDaytonaReleaseTool(t *testing.T, ctx context.Context, adapter *DaytonaAdapter, handle sandbox.ProviderHandle, invocation driver.ToolInvocation) string {
	t.Helper()
	result := assertDaytonaReleaseToolExecution(t, ctx, adapter, handle, invocation)
	if result.BackgroundTask != nil || result.ForegroundObservation != nil {
		t.Fatal("release-smoke foreground tool unexpectedly detached")
	}
	if !json.Valid([]byte(result.ResultJSON)) || !strings.Contains(result.ResultJSON, `"status":"success"`) {
		t.Fatal("release-smoke foreground tool did not return a successful Helper envelope")
	}
	return result.ResultJSON
}

func assertDaytonaReleaseToolExecution(t *testing.T, ctx context.Context, adapter *DaytonaAdapter, handle sandbox.ProviderHandle, invocation driver.ToolInvocation) driver.ToolExecution {
	t.Helper()
	prepared := adapter.PrepareTool(ctx, ToolExecutionRequest{Handle: handle, Invocation: invocation})
	if prepared.Failed() || prepared.Value.ImmediateResult != nil {
		t.Fatalf("prepare Daytona Helper tool: kind=%s disposition=%s", prepared.ErrorKind, prepared.Disposition)
	}
	executed := adapter.ExecuteTool(ctx, ToolExecutionRequest{Handle: handle, Invocation: invocation, Prepared: prepared.Value.Prepared})
	if executed.Failed() {
		t.Fatalf("execute Daytona Helper tool: kind=%s disposition=%s", executed.ErrorKind, executed.Disposition)
	}
	return executed.Value
}

func isImmutablePublishedImageRef(value string) bool {
	_, digest, ok := strings.Cut(value, "@sha256:")
	if !ok || len(digest) != 64 {
		return false
	}
	for _, r := range digest {
		if !strings.ContainsRune("0123456789abcdef", r) {
			return false
		}
	}
	return true
}
