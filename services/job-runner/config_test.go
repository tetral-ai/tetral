package jobrunner

import (
	"errors"
	"math"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/queue"
	"github.com/tetral-ai/tetral/internal/workload"
)

type configTestEnv map[string]string

func (e configTestEnv) Getenv(key string) string {
	return e[key]
}

func TestJobRunnerConfigRejectsNilEnvironment(t *testing.T) {
	cfg, err := JobRunnerConfigFromEnv(nil)
	var configErr *workload.ConfigError
	if !errors.As(err, &configErr) || configErr.Message != "environment is required" {
		t.Fatalf("nil environment error = %v; want typed configuration error requiring environment", err)
	}
	if cfg != (JobRunnerConfig{}) {
		t.Fatalf("nil environment configuration = %#v; want zero configuration", cfg)
	}
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
	env[EnvJobRunnerLeaseDurationMS] = "30000"
	cfg, err := JobRunnerConfigFromEnv(env)
	if err != nil {
		t.Fatalf("JobRunnerConfigFromEnv: %v", err)
	}
	if cfg.HeartbeatInterval != 10*time.Second {
		t.Fatalf("HeartbeatInterval = %s; want lease/3", cfg.HeartbeatInterval)
	}

	env[EnvJobRunnerHeartbeatIntervalMS] = "30000"
	if _, err := JobRunnerConfigFromEnv(env); err == nil || !strings.Contains(err.Error(), "must be less than") {
		t.Fatalf("JobRunnerConfigFromEnv heartbeat >= lease error = %v; want validation", err)
	}
}

func TestJobRunnerConfigRejectsMillisecondDurationOverflow(t *testing.T) {
	maxSafeMillis := int64(math.MaxInt64) / int64(time.Millisecond)
	for _, key := range []string{EnvJobRunnerLeaseDurationMS, EnvJobRunnerHeartbeatIntervalMS} {
		env := validJobRunnerConfigEnv()
		env[key] = strconv.FormatInt(maxSafeMillis+1, 10)
		if _, err := JobRunnerConfigFromEnv(env); err == nil || !strings.Contains(err.Error(), key+" is too large") {
			t.Fatalf("JobRunnerConfigFromEnv %s overflow error = %v; want bounded millisecond validation", key, err)
		}
	}
}

func TestJobRunnerConfigRejectsLeaseBatchAboveTransportCapacity(t *testing.T) {
	env := validJobRunnerConfigEnv()
	env[EnvJobRunnerMaxJobs] = strconv.Itoa(queue.MaxJobRunnerLeaseJobs())
	if cfg, err := JobRunnerConfigFromEnv(env); err != nil || cfg.MaxJobs != queue.MaxJobRunnerLeaseJobs() {
		t.Fatalf("JobRunnerConfigFromEnv maximum batch = %+v/%v; want accepted", cfg.MaxJobs, err)
	}
	env[EnvJobRunnerMaxJobs] = strconv.Itoa(queue.MaxJobRunnerLeaseJobs() + 1)
	if _, err := JobRunnerConfigFromEnv(env); err == nil || !strings.Contains(err.Error(), EnvJobRunnerMaxJobs) {
		t.Fatalf("JobRunnerConfigFromEnv oversized batch error = %v; want knob-owned startup validation", err)
	}
}

func TestJobRunnerConfigBoundsLeaseDurationToDirectLeaseRange(t *testing.T) {
	cfg, err := JobRunnerConfigFromEnv(validJobRunnerConfigEnv())
	if err != nil || cfg.LeaseDuration != 30*time.Second {
		t.Fatalf("default lease duration = %s/%v; want 30000 ms", cfg.LeaseDuration, err)
	}
	for _, accepted := range []string{"5000", "300000"} {
		env := validJobRunnerConfigEnv()
		env[EnvJobRunnerLeaseDurationMS] = accepted
		if _, err := JobRunnerConfigFromEnv(env); err != nil {
			t.Fatalf("lease duration %s ms rejected: %v", accepted, err)
		}
	}
	for _, rejected := range []string{"4999", "300001"} {
		env := validJobRunnerConfigEnv()
		env[EnvJobRunnerLeaseDurationMS] = rejected
		if _, err := JobRunnerConfigFromEnv(env); err == nil || !strings.Contains(err.Error(), EnvJobRunnerLeaseDurationMS+" must be between 5000 and 300000") {
			t.Fatalf("lease duration %s ms error = %v; want startup rejection", rejected, err)
		}
	}
}

func TestJobRunnerConfigRejectsLeaseOwnerAboveTransportBound(t *testing.T) {
	env := validJobRunnerConfigEnv()
	env[EnvJobRunnerLeaseOwner] = strings.Repeat("l", queue.MaxQueueLeaseOwnerBytes+1)
	if _, err := JobRunnerConfigFromEnv(env); err == nil || !strings.Contains(err.Error(), EnvJobRunnerLeaseOwner) {
		t.Fatalf("JobRunnerConfigFromEnv oversized lease owner error = %v; want knob-owned startup validation", err)
	}
}

func TestLifecycleConfigFitsPodApplicationAllocation(t *testing.T) {
	for _, tc := range []struct {
		drain, join string
		valid       bool
	}{
		{"2000", "3000", true}, {"30000", "5000", true}, {"30001", "5000", false}, {"120000", "5000", false}, {"9223372036854", "9223372036854", false},
	} {
		env := validJobRunnerConfigEnv()
		env["TETRAL_DRAIN_TIMEOUT_MS"], env["TETRAL_CANCEL_JOIN_TIMEOUT_MS"] = tc.drain, tc.join
		cfg, err := JobRunnerConfigFromEnv(env)
		if (err == nil) != tc.valid {
			t.Fatalf("drain=%s join=%s error=%v", tc.drain, tc.join, err)
		}
		if tc.valid && cfg.DrainTimeout+cfg.CancelJoinTimeout > 35000*time.Millisecond {
			t.Fatal("invalid application allocation")
		}
	}
}

func TestJobRunnerConfigPinsRuntimeCommandTimeoutKeys(t *testing.T) {
	for key, method := range runtimeCommandTimeoutKeys {
		t.Run(key, func(t *testing.T) {
			env := validJobRunnerConfigEnv()
			env[key] = "1234"
			cfg, err := JobRunnerConfigFromEnv(env)
			if err != nil {
				t.Fatal(err)
			}
			for _, other := range runtimeCommandTimeoutKeys {
				got, err := cfg.CommandPolicy.timeout(other)
				want := 30 * time.Second
				if other == method {
					want = 1234 * time.Millisecond
				}
				if err != nil || got != want {
					t.Fatalf("%s set %s timeout=%s/%v want %s", key, other, got, err, want)
				}
			}
			for _, invalid := range []string{"0", "-1", "abc"} {
				env[key] = invalid
				if _, err := JobRunnerConfigFromEnv(env); err == nil || !strings.Contains(err.Error(), key) {
					t.Fatalf("%s=%q error=%v; want rejection naming the key", key, invalid, err)
				}
			}
		})
	}
}

func TestJobRunnerConfigPinsRuntimePlacementKeys(t *testing.T) {
	env := validJobRunnerConfigEnv()
	env["TETRAL_RUNTIME_LOAD_PROBE_TIMEOUT_MS"] = "500"
	env["TETRAL_RUNTIME_PLACEMENT_TIMEOUT_MS"] = "1500"
	env["TETRAL_RUNTIME_PLACEMENT_ROUNDS"] = "1"
	env["TETRAL_RUNTIME_LOAD_MAX_BYTES"] = "131072"
	env["TETRAL_RUNTIME_PLACEMENT_MEMORY_CUTOFF"] = "0.75"
	cfg, err := JobRunnerConfigFromEnv(env)
	if err != nil {
		t.Fatal(err)
	}
	want := RuntimePlacementPolicy{ProbeTimeout: 500 * time.Millisecond, ProbeBudget: 1500 * time.Millisecond, Rounds: 1, MaxResponseBytes: 131072, MemoryCutoff: 0.75}
	if cfg.PlacementPolicy != want {
		t.Fatalf("placement policy=%+v want %+v", cfg.PlacementPolicy, want)
	}
	for _, invalid := range []map[string]string{
		{"TETRAL_RUNTIME_PLACEMENT_ROUNDS": "0"},
		{"TETRAL_RUNTIME_PLACEMENT_ROUNDS": "3"},
		{"TETRAL_RUNTIME_PLACEMENT_ROUNDS": "x"},
		{"TETRAL_RUNTIME_LOAD_MAX_BYTES": "0"},
		{"TETRAL_RUNTIME_LOAD_MAX_BYTES": "262145"},
		{"TETRAL_RUNTIME_PLACEMENT_MEMORY_CUTOFF": "0"},
		{"TETRAL_RUNTIME_PLACEMENT_MEMORY_CUTOFF": "1.01"},
		{"TETRAL_RUNTIME_PLACEMENT_MEMORY_CUTOFF": "NaN"},
		{"TETRAL_RUNTIME_PLACEMENT_MEMORY_CUTOFF": "+Inf"},
		{"TETRAL_RUNTIME_LOAD_PROBE_TIMEOUT_MS": "1500", "TETRAL_RUNTIME_PLACEMENT_TIMEOUT_MS": "1000"},
	} {
		env := validJobRunnerConfigEnv()
		for key, value := range invalid {
			env[key] = value
		}
		if _, err := JobRunnerConfigFromEnv(env); err == nil {
			t.Fatalf("invalid placement settings %v accepted", invalid)
		}
	}
}
