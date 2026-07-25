package agentruntimebridge

import (
	"math"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/queue"
)

type configTestEnv map[string]string

func (e configTestEnv) Getenv(key string) string {
	return e[key]
}

func validJobRunnerConfigEnv() configTestEnv {
	return configTestEnv{ //nolint:gosec // Test env values are fixture paths/DSNs, not secrets.
		EnvQueueGRPCAddress:                 "queue:9090",
		EnvDatabaseURL:                      "postgres://bridge@example.invalid/tetral",
		EnvKubernetesNamespace:              "tetral-agent-runtime",
		EnvAgentRuntimeLabelSelector:        "app.kubernetes.io/name=agent-runtime",
		EnvRuntimePodServiceTokenPath:       "/var/run/secrets/tetral-internal-grpc/agent-runtime/token",
		EnvSandboxServiceGRPCAddress:        "sandbox:9090",
		EnvSandboxServiceTokenPath:          "/var/run/secrets/tetral-internal-grpc/sandbox/token",
		EnvJobRunnerMCPConnectorGRPCAddress: "gateway.tetral-system.svc.cluster.local:9091",
		EnvJobRunnerGatewayTokenPath:        "/var/run/secrets/tetral-internal-grpc/gateway/token",
	}
}

func TestResourceCredentialRefreshMarginFromEnv(t *testing.T) {
	t.Run("default", func(t *testing.T) {
		margin, err := ResourceCredentialRefreshMarginFromEnv(configTestEnv{})
		if err != nil {
			t.Fatalf("ResourceCredentialRefreshMarginFromEnv: %v", err)
		}
		if margin != 30*time.Minute {
			t.Fatalf("margin = %s; want 30m", margin)
		}
	})

	t.Run("configured", func(t *testing.T) {
		margin, err := ResourceCredentialRefreshMarginFromEnv(configTestEnv{EnvResourceCredRefreshMargin: "45m"})
		if err != nil {
			t.Fatalf("ResourceCredentialRefreshMarginFromEnv: %v", err)
		}
		if margin != 45*time.Minute {
			t.Fatalf("margin = %s; want 45m", margin)
		}
	})

	t.Run("rejects nonpositive", func(t *testing.T) {
		_, err := ResourceCredentialRefreshMarginFromEnv(configTestEnv{EnvResourceCredRefreshMargin: "0s"})
		if err == nil || !strings.Contains(err.Error(), EnvResourceCredRefreshMargin+" must be a positive duration") {
			t.Fatalf("ResourceCredentialRefreshMarginFromEnv error = %v; want positive duration validation", err)
		}
	})
}

func TestSandboxStatusFreshnessWindowFromEnv(t *testing.T) {
	t.Run("default", func(t *testing.T) {
		window, err := SandboxStatusFreshnessWindowFromEnv(configTestEnv{})
		if err != nil {
			t.Fatalf("SandboxStatusFreshnessWindowFromEnv: %v", err)
		}
		if window != time.Minute {
			t.Fatalf("window = %s; want 60s", window)
		}
	})

	t.Run("configured", func(t *testing.T) {
		window, err := SandboxStatusFreshnessWindowFromEnv(configTestEnv{EnvSandboxStatusFreshnessWindow: "90s"})
		if err != nil {
			t.Fatalf("SandboxStatusFreshnessWindowFromEnv: %v", err)
		}
		if window != 90*time.Second {
			t.Fatalf("window = %s; want 90s", window)
		}
	})

	t.Run("rejects nonpositive", func(t *testing.T) {
		_, err := SandboxStatusFreshnessWindowFromEnv(configTestEnv{EnvSandboxStatusFreshnessWindow: "0s"})
		if err == nil || !strings.Contains(err.Error(), EnvSandboxStatusFreshnessWindow+" must be a positive duration") {
			t.Fatalf("SandboxStatusFreshnessWindowFromEnv error = %v; want positive duration validation", err)
		}
	})
}

func TestMemoryProjectionPushTimeoutFromEnv(t *testing.T) {
	t.Run("default", func(t *testing.T) {
		timeout, err := MemoryProjectionPushTimeoutFromEnv(configTestEnv{})
		if err != nil {
			t.Fatalf("MemoryProjectionPushTimeoutFromEnv: %v", err)
		}
		if timeout != 30*time.Second {
			t.Fatalf("timeout = %s; want 30s", timeout)
		}
	})

	t.Run("configured", func(t *testing.T) {
		timeout, err := MemoryProjectionPushTimeoutFromEnv(configTestEnv{EnvMemoryProjectionPushTimeout: "45s"})
		if err != nil {
			t.Fatalf("MemoryProjectionPushTimeoutFromEnv: %v", err)
		}
		if timeout != 45*time.Second {
			t.Fatalf("timeout = %s; want 45s", timeout)
		}
	})

	t.Run("rejects nonpositive", func(t *testing.T) {
		_, err := MemoryProjectionPushTimeoutFromEnv(configTestEnv{EnvMemoryProjectionPushTimeout: "0s"})
		if err == nil || !strings.Contains(err.Error(), EnvMemoryProjectionPushTimeout+" must be a positive duration") {
			t.Fatalf("MemoryProjectionPushTimeoutFromEnv error = %v; want positive duration validation", err)
		}
	})
}

func TestBridgeAPIConfigRequiresMCPConnectorRoute(t *testing.T) {
	env := configTestEnv{
		EnvBridgeMCPConnectorGRPCAddr: "gateway.tetral-system.svc.cluster.local:9091",
		EnvBridgeGatewayTokenPath:     "/var/run/secrets/tetral-internal-grpc/gateway/token",
	}
	cfg, err := BridgeAPIConfigFromEnv(env)
	if err != nil {
		t.Fatalf("BridgeAPIConfigFromEnv: %v", err)
	}
	if cfg.MCPConnectorGRPCAddress != env[EnvBridgeMCPConnectorGRPCAddr] || cfg.GatewayTokenPath != env[EnvBridgeGatewayTokenPath] {
		t.Fatalf("BridgeAPIConfigFromEnv = %#v; want env projection", cfg)
	}
	if cfg.ProviderRescheduleBudget != 3 || cfg.CompactionRescheduleBudget != 2 {
		t.Fatalf("BridgeAPIConfigFromEnv retry budgets = %d/%d; want 3/2", cfg.ProviderRescheduleBudget, cfg.CompactionRescheduleBudget)
	}

	for _, missing := range []string{EnvBridgeMCPConnectorGRPCAddr, EnvBridgeGatewayTokenPath} {
		t.Run(missing, func(t *testing.T) {
			missingEnv := configTestEnv{
				EnvBridgeMCPConnectorGRPCAddr: env[EnvBridgeMCPConnectorGRPCAddr],
				EnvBridgeGatewayTokenPath:     env[EnvBridgeGatewayTokenPath],
			}
			delete(missingEnv, missing)
			if _, err := BridgeAPIConfigFromEnv(missingEnv); err == nil || !strings.Contains(err.Error(), missing+" is required") {
				t.Fatalf("BridgeAPIConfigFromEnv missing %s error = %v; want required validation", missing, err)
			}
		})
	}

	for _, test := range []struct {
		name  string
		key   string
		value string
	}{
		{name: "provider negative", key: EnvProviderRescheduleBudget, value: "-1"},
		{name: "provider too large", key: EnvProviderRescheduleBudget, value: "11"},
		{name: "compaction malformed", key: EnvCompactionRescheduleBudget, value: "two"},
	} {
		t.Run(test.name, func(t *testing.T) {
			invalid := configTestEnv{
				EnvBridgeMCPConnectorGRPCAddr: env[EnvBridgeMCPConnectorGRPCAddr],
				EnvBridgeGatewayTokenPath:     env[EnvBridgeGatewayTokenPath],
				test.key:                      test.value,
			}
			if _, err := BridgeAPIConfigFromEnv(invalid); err == nil || !strings.Contains(err.Error(), test.key+" must be an integer between 0 and 10") {
				t.Fatalf("BridgeAPIConfigFromEnv %s error = %v; want bounded integer validation", test.key, err)
			}
		})
	}
}

func TestJobRunnerConfigCarriesResourceCredentialRefreshMargin(t *testing.T) {
	t.Run("default", func(t *testing.T) {
		cfg, err := JobRunnerConfigFromEnv(validJobRunnerConfigEnv())
		if err != nil {
			t.Fatalf("JobRunnerConfigFromEnv: %v", err)
		}
		if cfg.SandboxStatusFreshnessWindow != time.Minute {
			t.Fatalf("SandboxStatusFreshnessWindow = %s; want 60s", cfg.SandboxStatusFreshnessWindow)
		}
		if cfg.ResourceCredentialRefreshMargin != 30*time.Minute {
			t.Fatalf("ResourceCredentialRefreshMargin = %s; want 30m", cfg.ResourceCredentialRefreshMargin)
		}
	})

	t.Run("configured", func(t *testing.T) {
		env := validJobRunnerConfigEnv()
		env[EnvResourceCredRefreshMargin] = "15m"
		env[EnvSandboxStatusFreshnessWindow] = "45s"
		cfg, err := JobRunnerConfigFromEnv(env)
		if err != nil {
			t.Fatalf("JobRunnerConfigFromEnv: %v", err)
		}
		if cfg.SandboxStatusFreshnessWindow != 45*time.Second {
			t.Fatalf("SandboxStatusFreshnessWindow = %s; want 45s", cfg.SandboxStatusFreshnessWindow)
		}
		if cfg.ResourceCredentialRefreshMargin != 15*time.Minute {
			t.Fatalf("ResourceCredentialRefreshMargin = %s; want 15m", cfg.ResourceCredentialRefreshMargin)
		}
	})
}

func TestJobRunnerConfigRequiresDeliveryDependencies(t *testing.T) {
	for _, missing := range []string{
		EnvQueueGRPCAddress,
		EnvDatabaseURL,
		EnvKubernetesNamespace,
		EnvAgentRuntimeLabelSelector,
		EnvRuntimePodServiceTokenPath,
		EnvSandboxServiceGRPCAddress,
		EnvSandboxServiceTokenPath,
		EnvJobRunnerMCPConnectorGRPCAddress,
		EnvJobRunnerGatewayTokenPath,
	} {
		t.Run(missing, func(t *testing.T) {
			env := validJobRunnerConfigEnv()
			delete(env, missing)
			if _, err := JobRunnerConfigFromEnv(env); err == nil || !strings.Contains(err.Error(), missing+" is required") {
				t.Fatalf("JobRunnerConfigFromEnv missing %s error = %v; want required validation", missing, err)
			}
		})
	}
	cfg, err := JobRunnerConfigFromEnv(validJobRunnerConfigEnv())
	if err != nil {
		t.Fatalf("JobRunnerConfigFromEnv: %v", err)
	}
	if cfg.MCPConnectorGRPCAddress != "gateway.tetral-system.svc.cluster.local:9091" ||
		cfg.GatewayTokenPath != "/var/run/secrets/tetral-internal-grpc/gateway/token" {
		t.Fatalf("JobRunnerConfigFromEnv MCP route = %q / %q; want env projection", cfg.MCPConnectorGRPCAddress, cfg.GatewayTokenPath)
	}
}

func TestJobRunnerConfigDerivesAndValidatesHeartbeatInterval(t *testing.T) {
	env := validJobRunnerConfigEnv()
	env[EnvJobRunnerLeaseDurationMS] = "300"
	cfg, err := JobRunnerConfigFromEnv(env)
	if err != nil {
		t.Fatalf("JobRunnerConfigFromEnv: %v", err)
	}
	if cfg.HeartbeatInterval != 100*time.Millisecond {
		t.Fatalf("HeartbeatInterval = %s; want lease/3", cfg.HeartbeatInterval)
	}

	env[EnvJobRunnerHeartbeatIntervalMS] = "300"
	if _, err := JobRunnerConfigFromEnv(env); err == nil || !strings.Contains(err.Error(), "must be less than") {
		t.Fatalf("JobRunnerConfigFromEnv heartbeat >= lease error = %v; want validation", err)
	}
}

func TestJobRunnerConfigRejectsMillisecondDurationOverflow(t *testing.T) {
	maxSafeMillis := int64(math.MaxInt64) / int64(time.Millisecond)
	for _, key := range []string{EnvJobRunnerLeaseDurationMS, EnvJobRunnerHeartbeatIntervalMS, EnvJobRunnerPollIntervalMS} {
		env := validJobRunnerConfigEnv()
		env[key] = strconv.FormatInt(maxSafeMillis+1, 10)
		if _, err := JobRunnerConfigFromEnv(env); err == nil || !strings.Contains(err.Error(), key+" is too large") {
			t.Fatalf("JobRunnerConfigFromEnv %s overflow error = %v; want bounded millisecond validation", key, err)
		}
	}
}

func TestJobRunnerConfigRejectsLeaseBatchAboveTransportCapacity(t *testing.T) {
	env := validJobRunnerConfigEnv()
	env[EnvJobRunnerMaxJobs] = strconv.Itoa(queue.MaxQueueLeaseJobs() + 1)
	if _, err := JobRunnerConfigFromEnv(env); err == nil || !strings.Contains(err.Error(), EnvJobRunnerMaxJobs) {
		t.Fatalf("JobRunnerConfigFromEnv oversized batch error = %v; want knob-owned startup validation", err)
	}
}

func TestJobRunnerConfigRejectsLeaseOwnerAboveTransportBound(t *testing.T) {
	env := validJobRunnerConfigEnv()
	env[EnvJobRunnerLeaseOwner] = strings.Repeat("l", queue.MaxQueueLeaseOwnerBytes+1)
	if _, err := JobRunnerConfigFromEnv(env); err == nil || !strings.Contains(err.Error(), EnvJobRunnerLeaseOwner) {
		t.Fatalf("JobRunnerConfigFromEnv oversized lease owner error = %v; want knob-owned startup validation", err)
	}
}
