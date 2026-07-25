//go:build daytona_behavioral

package tetralsandbox

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/id"
	"github.com/tetral-ai/tetral/internal/sandbox"
	"github.com/tetral-ai/tetral/internal/sandbox/driver"
)

func TestDaytonaPreparationTimeoutKillsDelayedRemoteMutation(t *testing.T) {
	required := []string{EnvDaytonaAPIURL, EnvDaytonaAPIKey, envResourceProjectionLiveArtifactRef}
	for _, name := range required {
		if strings.TrimSpace(os.Getenv(name)) == "" {
			t.Fatalf("%s is required; this behavioral test never skips", name)
		}
	}
	cfg := driver.Config{
		DaytonaAPIURL:             os.Getenv(EnvDaytonaAPIURL),
		DaytonaTarget:             os.Getenv(EnvDaytonaTarget),
		DaytonaAPIKey:             os.Getenv(EnvDaytonaAPIKey),
		PreparationCommandTimeout: time.Second,
	}
	provider, err := driver.NewDaytonaLifecycleProvider(cfg)
	if err != nil {
		t.Fatalf("NewDaytonaLifecycleProvider: %v", err)
	}
	executor, err := driver.NewDaytonaHelperExecutor(cfg)
	if err != nil {
		t.Fatalf("NewDaytonaHelperExecutor: %v", err)
	}
	sandboxID := "timeout-" + strings.ToLower(id.New("sbx_"))
	handle, err := provider.CreateSandbox(context.Background(), sandbox.CreateSandboxRequest{Setup: sandbox.SandboxSetup{
		SandboxID:           sandboxID,
		ProviderArtifactRef: os.Getenv(envResourceProjectionLiveArtifactRef),
		Network:             sandbox.NetworkSetup{Type: "unrestricted"},
	}})
	if err != nil {
		t.Fatalf("CreateSandbox: %v", err)
	}
	defer func() {
		if releaseErr := provider.ReleaseSandbox(context.Background(), handle, sandbox.ReleaseReasonDelete); releaseErr != nil {
			t.Errorf("delete behavioral sandbox: %v", releaseErr)
		}
	}()
	target := driver.PreparationCommandTarget{ProviderSandboxID: handle.SandboxID}
	marker := "/tmp/tetral-timeout-marker-" + sandboxID
	delayed := fmt.Sprintf("rm -f -- %q; sleep 2; printf late > %q", marker, marker)
	if err := executor.RunPreparationCommand(context.Background(), target, delayed, nil, time.Second); err == nil {
		t.Fatal("delayed command succeeded; want Daytona server-side timeout")
	}
	time.Sleep(3 * time.Second)
	if err := executor.RunPreparationCommand(context.Background(), target, fmt.Sprintf("test ! -e %q", marker), nil, time.Second); err != nil {
		t.Fatalf("delayed marker exists after timeout plus kill grace: %v", err)
	}
}
