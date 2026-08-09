package agentruntimebridge

import (
	"math"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/dbconnect"
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
		EnvJobRunnerMCPConnectorGRPCAddress: "gateway.tetral-system.svc.cluster.local:9091",
		EnvJobRunnerGatewayTokenPath:        "/var/run/secrets/tetral-internal-grpc/gateway/token",
	}
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

func TestJobRunnerConfigRequiresDeliveryDependencies(t *testing.T) {
	for _, missing := range []string{
		EnvQueueGRPCAddress,
		EnvDatabaseURL,
		EnvKubernetesNamespace,
		EnvAgentRuntimeLabelSelector,
		EnvRuntimePodServiceTokenPath,
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

func TestJobRunnerConfigRequiresTwoDatabaseConnections(t *testing.T) {
	for _, test := range []struct {
		name    string
		maxOpen string
		wantErr bool
	}{
		{name: "unchanged default"},
		{name: "minimum", maxOpen: "2"},
		{name: "listener would consume only connection", maxOpen: "1", wantErr: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			env := validJobRunnerConfigEnv()
			if test.maxOpen != "" {
				env[dbconnect.EnvDBMaxOpenConns] = test.maxOpen
			}
			_, err := JobRunnerConfigFromEnv(env)
			if test.wantErr {
				if err == nil || !strings.Contains(err.Error(), dbconnect.EnvDBMaxOpenConns+" must be at least 2") {
					t.Fatalf("JobRunnerConfigFromEnv max open %q error = %v; want minimum validation", test.maxOpen, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("JobRunnerConfigFromEnv max open %q: %v", test.maxOpen, err)
			}
		})
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
