//go:build daytona_behavioral

package tetralsandbox

import (
	"context"
	"fmt"
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

func TestDaytonaPreparationTimeoutKillsDelayedRemoteMutation(t *testing.T) {
	required := []string{EnvDaytonaAPIURL, EnvDaytonaAPIKey, envResourceProjectionLiveArtifactRef}
	for _, name := range required {
		if strings.TrimSpace(os.Getenv(name)) == "" {
			t.Fatalf("%s is required; this behavioral test never skips", name)
		}
	}
	cfg := driver.Config{
		DaytonaAPIURL:  os.Getenv(EnvDaytonaAPIURL),
		DaytonaTarget:  os.Getenv(EnvDaytonaTarget),
		DaytonaAPIKey:  os.Getenv(EnvDaytonaAPIKey),
		CommandTimeout: time.Second,
	}
	client, err := daytona.NewClientWithConfig(&types.DaytonaConfig{
		APIKey: cfg.DaytonaAPIKey, APIUrl: cfg.DaytonaAPIURL, Target: cfg.DaytonaTarget,
	})
	if err != nil {
		t.Fatalf("create Daytona SDK client: %v", err)
	}
	provider, err := driver.NewDaytonaLifecycleProviderForSDKClient(client, cfg)
	if err != nil {
		t.Fatalf("NewDaytonaLifecycleProviderForSDKClient: %v", err)
	}
	executor, err := driver.NewDaytonaHelperExecutorForSDKClient(client, cfg.CommandTimeout)
	if err != nil {
		t.Fatalf("NewDaytonaHelperExecutorForSDKClient: %v", err)
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
		if releaseErr := provider.ReleaseSandbox(context.Background(), handle); releaseErr != nil {
			t.Errorf("delete behavioral sandbox: %v", releaseErr)
		}
	}()
	target := driver.DaytonaCommandTarget{ProviderSandboxID: handle.SandboxID}
	marker := "/tmp/tetral-timeout-marker-" + sandboxID
	delayed := fmt.Sprintf("rm -f -- %q; sleep 2; printf late > %q", marker, marker)
	if err := executor.RunDaytonaCommand(context.Background(), target, delayed, nil, time.Second); err == nil {
		t.Fatal("delayed command succeeded; want Daytona server-side timeout")
	}
	time.Sleep(3 * time.Second)
	if err := executor.RunDaytonaCommand(context.Background(), target, fmt.Sprintf("test ! -e %q", marker), nil, time.Second); err != nil {
		t.Fatalf("delayed marker exists after timeout plus kill grace: %v", err)
	}
}
