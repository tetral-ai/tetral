package helm_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/auth"
	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/queue"
	"github.com/tetral-ai/tetral/internal/runtimecontrol"
	"github.com/tetral-ai/tetral/internal/workload"
	api "github.com/tetral-ai/tetral/services/api"
	authservice "github.com/tetral-ai/tetral/services/auth"
	bridge "github.com/tetral-ai/tetral/services/bridge"
	cleanup "github.com/tetral-ai/tetral/services/cleanup"
	gitproxy "github.com/tetral-ai/tetral/services/git-proxy"
	runner "github.com/tetral-ai/tetral/services/job-runner"
	queueservice "github.com/tetral-ai/tetral/services/queue"
	sandbox "github.com/tetral-ai/tetral/services/sandbox"
	webconnector "github.com/tetral-ai/tetral/services/web-connector"
)

func configurationMap(value any) map[string]any { result, _ := value.(map[string]any); return result }
func configurationList(value any) []any         { result, _ := value.([]any); return result }
func configurationAt(value any, keys ...string) any {
	for _, key := range keys {
		value = configurationMap(value)[key]
	}
	return value
}

type projectionEnv map[string]string

func (e projectionEnv) Getenv(key string) string { return e[key] }

// Resolve actual literal/ConfigMap wiring. Secret and downward API values are
// supplied separately by this startup-only fixture; no file or network is opened.
func configurationEnvironments(t *testing.T, objects map[string]map[string]any) map[string]projectionEnv {
	t.Helper()
	result := map[string]projectionEnv{}
	for _, object := range objects {
		kind, _ := object["kind"].(string)
		if kind != "Deployment" && kind != "CronJob" {
			continue
		}
		pod := configurationMap(configurationAt(object, "spec", "template", "spec"))
		if kind == "CronJob" {
			pod = configurationMap(configurationAt(object, "spec", "jobTemplate", "spec", "template", "spec"))
		}
		namespace := fmt.Sprint(configurationAt(object, "metadata", "namespace"))
		for _, raw := range configurationList(pod["containers"]) {
			c := configurationMap(raw)
			role := fmt.Sprint(configurationAt(object, "metadata", "name"))
			env := projectionEnv{}
			for _, raw := range configurationList(c["env"]) {
				item := configurationMap(raw)
				key := fmt.Sprint(item["name"])
				if _, exists := env[key]; exists {
					t.Fatalf("%s duplicate env %s", role, key)
				}
				value, literal := item["value"]
				if literal {
					env[key] = fmt.Sprint(value)
					continue
				}
				source := configurationMap(item["valueFrom"])
				if ref := configurationMap(source["configMapKeyRef"]); ref != nil {
					cm := objects["v1|ConfigMap|"+namespace+"|"+fmt.Sprint(ref["name"])]
					if cm == nil {
						t.Fatalf("%s missing referenced ConfigMap", role)
					}
					value, exists := configurationMap(cm["data"])[fmt.Sprint(ref["key"])]
					if !exists {
						t.Fatalf("%s missing ConfigMap key %s", role, key)
					}
					env[key] = fmt.Sprint(value)
				} else if ref := configurationMap(source["fieldRef"]); ref != nil {
					switch fmt.Sprint(ref["fieldPath"]) {
					case "metadata.namespace":
						env[key] = namespace
					case "metadata.name":
						env[key] = role + "-fixture"
					case "metadata.uid":
						env[key] = "pod-fixture-uid"
					case "status.podIP":
						env[key] = "127.0.0.1"
					default:
						t.Fatalf("unhandled downward API field: %v", ref)
					}
				} else if configurationMap(source["secretKeyRef"]) != nil {
					// Valid test material only; actual deployment leaves these unresolved.
					switch key {
					case "ENGINE_API_KEY":
						env[key] = strings.Repeat("x", 32)
					case "TETRAL_AUTH_INTERNAL_PRINCIPAL_PRIVATE_KEY_B64":
						private, err := auth.GenerateEd25519PrivateKeyBase64()
						if err != nil {
							t.Fatal(err)
						}
						env[key] = private
					case "TETRAL_WEB_API_KEYS":
						env[key] = `["fixture-web-key"]`
					case "TETRAL_BLOB_ENDPOINT":
						env[key] = "https://blob.fixture.invalid"
					case "ENGINE_VAULT_KEY":
						env[key] = strings.Repeat("a", 64)
					case "TETRAL_RUNTIME_BINDING_TOKEN_HMAC_KEY":
						env[key] = strings.Repeat("h", 32)
					case "TETRAL_DATABASE_URL", "TETRAL_POSTGRES_DSN", "TETRAL_EVENT_STREAM_DATABASE_URL":
						env[key] = "postgres://fixture.invalid/tetral"
					default:
						env[key] = "fixture-secret"
					}
				} else {
					t.Fatalf("%s unknown env wiring for %s", role, key)
				}
			}
			result[role] = env
		}
	}
	return result
}

func queueOperational(cfg queueservice.Config) map[string]int64 {
	return map[string]int64{"reclaim_seconds": int64(cfg.LeaseReclaimInterval / time.Second), "reclaim_limit": int64(cfg.LeaseReclaimBatchLimit), "base_ms": int64(cfg.RetryBaseDelay / time.Millisecond), "cap_ms": int64(cfg.RetryMaxDelay / time.Millisecond), "attempts": int64(cfg.RetryMaxAttempts)}
}
func runnerOperational(cfg runner.JobRunnerConfig) map[string]int64 {
	return map[string]int64{"lease_ms": int64(cfg.LeaseDuration / time.Millisecond), "heartbeat_ms": int64(cfg.HeartbeatInterval / time.Millisecond), "jobs": int64(cfg.MaxJobs), "poll_ms": int64(cfg.PollInterval / time.Millisecond)}
}
func requireProjection(actual, expected any) error {
	if !reflect.DeepEqual(actual, expected) {
		return fmt.Errorf("parsed projection differs: got %v, want %v", actual, expected)
	}
	return nil
}

func TestConfigurationOperationalProjectionUsesOwningParsers(t *testing.T) {
	helm := requireHelm(t)
	chart := filepath.Join(engineRoot(t), "deploy/helm/tetral")
	for _, profile := range []string{"standard-routed", "hardened"} {
		for _, configured := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/configured=%v", profile, configured), func(t *testing.T) {
				values := []string{"transport.profile=" + profile}
				qwant := map[string]int64{"reclaim_seconds": 30, "reclaim_limit": 100, "base_ms": 1000, "cap_ms": 60000, "attempts": 10}
				rwant := map[string]int64{"lease_ms": 30000, "heartbeat_ms": 10000, "jobs": 8, "poll_ms": 1000}
				dwant := workload.DefaultDiagnosticConfig()
				if configured {
					values = append(values, "queue.leaseReclaimIntervalSeconds=17", "queue.leaseReclaimLimit=23", "queue.retryBaseMs=700", "queue.retryCapMs=9000", "queue.retryMaxAttempts=4", "jobRunner.leaseDurationMs=24000", "jobRunner.heartbeatIntervalMs=6000", "jobRunner.maxJobs=3", "jobRunner.pollIntervalMs=250", "observability.logLevel=warn", "observability.logMaxRecordBytes=8192", "observability.logSummaryIntervalMs=7000", "observability.logBurst=3", "observability.deploymentEnvironment=projection-test", "observability.serviceVersion=projection-version", "lifecycle.providerDrainMs=200", "lifecycle.providerJoinMs=1000", "lifecycle.mcpDrainMs=250", "lifecycle.mcpJoinMs=1200")
					qwant = map[string]int64{"reclaim_seconds": 17, "reclaim_limit": 23, "base_ms": 700, "cap_ms": 9000, "attempts": 4}
					rwant = map[string]int64{"lease_ms": 24000, "heartbeat_ms": 6000, "jobs": 3, "poll_ms": 250}
					dwant.Level = 4
					dwant.MaxRecordBytes = 8192
					dwant.SummaryInterval = 7 * time.Second
					dwant.Burst = 3
				}
				envs := configurationEnvironments(t, uniqueObjects(t, renderChart(t, helm, chart, values...)))
				q, err := queueservice.ConfigFromEnv(envs["queue"])
				if err != nil {
					t.Fatal(err)
				}
				if err := requireProjection(queueOperational(q), qwant); err != nil {
					t.Fatal(err)
				}
				r, err := runner.JobRunnerConfigFromEnv(envs["job-runner"])
				if err != nil {
					t.Fatal(err)
				}
				if err := requireProjection(runnerOperational(r), rwant); err != nil {
					t.Fatal(err)
				}
				for role, env := range envs {
					d, err := workload.DiagnosticConfigFromEnv(env.Getenv)
					if err != nil {
						t.Fatalf("%s: %v", role, err)
					}
					if err := requireProjection(d, dwant); err != nil {
						t.Fatalf("%s: %v", role, err)
					}
					for _, key := range []string{"TETRAL_LOG_LEVEL", "TETRAL_LOG_MAX_RECORD_BYTES", "TETRAL_LOG_SUMMARY_INTERVAL_MS", "TETRAL_LOG_BURST"} {
						if env[key] == "" {
							t.Fatalf("%s missing %s", role, key)
						}
					}
					resource := workload.ResourceConfigFromEnv(env.Getenv)
					expected := workload.ResourceConfig{DeploymentEnvironment: "local", ServiceVersion: "dev"}
					if configured {
						expected = workload.ResourceConfig{DeploymentEnvironment: "projection-test", ServiceVersion: "projection-version"}
					}
					if err := requireProjection(resource, expected); err != nil {
						t.Fatalf("%s: %v", role, err)
					}
					if _, present := env[dbconnect.EnvDBMaxOpenConns]; present {
						pool, err := dbconnect.PoolConfigFromEnv(env.Getenv)
						if err != nil || pool.MaxOpenConns != 20 {
							t.Fatalf("%s pool: %+v %v", role, pool, err)
						}
					}
				}
				c, err := cleanup.ConfigFromEnv(envs["cleanup"])
				if err != nil || c.ClaimLimit != 100 {
					t.Fatalf("cleanup: %+v %v", c, err)
				}
				for _, role := range []string{"bridge", "job-runner"} {
					policy, err := runtimecontrol.ProcessPolicyFromEnv(envs[role].Getenv)
					if err != nil {
						t.Fatal(err)
					}
					if err := requireProjection(policy, runtimecontrol.DefaultProcessPolicy()); err != nil {
						t.Fatalf("%s: %v", role, err)
					}
				}
				b, err := bridge.BridgeAPIConfigFromEnv(envs["bridge"])
				if err != nil || b.ProviderRescheduleBudget != 3 || b.CompactionRescheduleBudget != 2 {
					t.Fatalf("Bridge: %+v %v", b, err)
				}
				checkGoOwningParsers(t, envs)
				checkTypeScriptProjection(t, envs, configured)
			})
		}
	}
}

func checkTypeScriptProjection(t *testing.T, envs map[string]projectionEnv, configured bool) {
	t.Helper()
	bun, err := exec.LookPath("bun")
	if err != nil {
		t.Fatal("Bun is required for actual TypeScript startup parser projection")
	}
	type row struct {
		Role string        `json:"role"`
		Env  projectionEnv `json:"env"`
	}
	rows := []row{}
	for _, role := range []string{"provider-gateway", "mcp-connector", "agent-runtime"} {
		rows = append(rows, row{role, envs[role]})
	}
	input, _ := json.Marshal(rows)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, bun, "run", filepath.Join(engineRoot(t), "deploy/helm/testdata/configuration-parsers.ts"))
	cmd.Stdin = bytes.NewReader(input)
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("actual TypeScript parsers: %v\n%s", err, output)
	}
	var results []struct {
		Role        string
		OK          bool
		Diagnostics struct {
			Level                                    string
			MaxRecordBytes, SummaryIntervalMs, Burst int
		}
		DeploymentEnvironment, ServiceVersion string
		DatabasePool                          map[string]int
		Lifecycle                             map[string]int
		BridgePolicy                          bridgePolicyEvidence
		DrainTimeoutMs, CancelJoinTimeoutMs   int
	}
	if err := json.Unmarshal(output, &results); err != nil {
		t.Fatalf("parser output: %v", err)
	}
	if len(results) != 3 {
		t.Fatal("missing owning parser result")
	}
	for _, result := range results {
		if !result.OK {
			t.Fatalf("%s actual startup parser rejected rendered config", result.Role)
		}
		want := struct {
			Level                                    string
			MaxRecordBytes, SummaryIntervalMs, Burst int
		}{"info", 16384, 30000, 1}
		environment, version := "local", "dev"
		if configured {
			want = struct {
				Level                                    string
				MaxRecordBytes, SummaryIntervalMs, Burst int
			}{"warn", 8192, 7000, 3}
			environment, version = "projection-test", "projection-version"
		}
		if err := requireProjection(result.Diagnostics, want); err != nil {
			t.Fatalf("%s: %v", result.Role, err)
		}
		if result.DeploymentEnvironment != environment || result.ServiceVersion != version {
			t.Fatalf("%s TS metadata differs", result.Role)
		}
		if result.Role != "agent-runtime" {
			if err := requireProjection(result.DatabasePool, map[string]int{"max": 10, "idleTimeout": 30, "maxLifetime": 1800, "connectionTimeout": 30, "statementTimeoutMs": 30000}); err != nil {
				t.Fatalf("%s: %v", result.Role, err)
			}
			// The rendered drain and join reach the owning parser, including
			// distinct non-default values for each Gateway service.
			drain, join := 30000, 5000
			if configured && result.Role == "provider-gateway" {
				drain, join = 200, 1000
			} else if configured {
				drain, join = 250, 1200
			}
			if result.DrainTimeoutMs != drain || result.CancelJoinTimeoutMs != join {
				t.Fatalf("%s parsed drain/join=%d/%d want %d/%d", result.Role, result.DrainTimeoutMs, result.CancelJoinTimeoutMs, drain, join)
			}
		} else {
			if result.Lifecycle["reportIntervalMs"] != 2000 || result.Lifecycle["processFreshnessMs"] != 10000 {
				t.Fatal("Runtime process custody disagrees with Go default projection")
			}
			checkBridgePolicyEvidence(t, result.BridgePolicy)
		}
	}
}

type bridgeMethodInventoryPolicy struct {
	Kind           string `json:"kind"`
	EnvironmentKey string `json:"environment_key"`
	TimeoutMS      int    `json:"timeout_ms,omitempty"`
}

type bridgeParsedPolicy struct {
	Kind      string
	TimeoutMs int
}

type bridgePolicyEvidence struct {
	Defaults                                                                          map[string]bridgeMethodInventoryPolicy
	EnvironmentKeys                                                                   []string
	DefaultPolicies                                                                   map[string]bridgeParsedPolicy
	MinimumOverride, MaximumOverride                                                  bridgeParsedPolicy
	InvalidFixedRejected, RemainingOverridesRejected, MissingProviderDeadlineRejected bool
	FixedDeadline, ClippedDeadline, ProviderDeadline                                  int
}

// Compare the complete census and derived keys to actual owner exports, rather
// than relying on a literal-source scan that cannot see generated names.
func bridgePolicyInventoryMatches(actual, declared map[string]bridgeMethodInventoryPolicy, actualKeys, declaredKeys []string) error {
	if err := requireProjection(declared, actual); err != nil {
		return fmt.Errorf("Bridge method policies: %w", err)
	}
	keys := map[string]bool{}
	for _, key := range declaredKeys {
		if keys[key] {
			return fmt.Errorf("duplicate Bridge environment key %s", key)
		}
		keys[key] = true
	}
	if len(keys) != len(actualKeys) {
		return fmt.Errorf("Bridge derived environment key coverage differs")
	}
	for _, key := range actualKeys {
		if !keys[key] {
			return fmt.Errorf("missing Bridge environment key %s", key)
		}
	}
	return nil
}

func checkBridgePolicyEvidence(t *testing.T, evidence bridgePolicyEvidence) {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(engineRoot(t), "deploy/managed/configuration-inventory.json"))
	if err != nil {
		t.Fatal(err)
	}
	// Other families have scalar defaults; decode only the selected family.
	var catalog struct{ Families []json.RawMessage }
	if err := json.Unmarshal(data, &catalog); err != nil {
		t.Fatal(err)
	}
	for _, raw := range catalog.Families {
		var identity struct{ ID string }
		if err := json.Unmarshal(raw, &identity); err != nil {
			t.Fatal(err)
		}
		if identity.ID == "runtime-bridge-rpc-deadlines" {
			var selected struct {
				ID                     string
				Defaults               map[string]bridgeMethodInventoryPolicy
				DerivedEnvironmentKeys []string `json:"derived_environment_keys"`
			}
			if err := json.Unmarshal(raw, &selected); err != nil {
				t.Fatal(err)
			}
			if err := bridgePolicyInventoryMatches(evidence.Defaults, selected.Defaults, evidence.EnvironmentKeys, selected.DerivedEnvironmentKeys); err != nil {
				t.Fatal(err)
			}
			// Owner-valid policy drift and missing policy/key controls must fail the census guard.
			missing := make(map[string]bridgeMethodInventoryPolicy)
			for method, policy := range selected.Defaults {
				missing[method] = policy
			}
			delete(missing, "loadContext")
			if bridgePolicyInventoryMatches(evidence.Defaults, missing, evidence.EnvironmentKeys, selected.DerivedEnvironmentKeys) == nil {
				t.Fatal("missing RPC policy was not detected")
			}
			missing["loadContext"] = selected.Defaults["loadContext"]
			drift := missing["writeEvent"]
			drift.TimeoutMS = 7000
			missing["writeEvent"] = drift
			if bridgePolicyInventoryMatches(evidence.Defaults, missing, evidence.EnvironmentKeys, selected.DerivedEnvironmentKeys) == nil {
				t.Fatal("valid changed RPC policy was not detected")
			}
			if bridgePolicyInventoryMatches(evidence.Defaults, selected.Defaults, evidence.EnvironmentKeys, selected.DerivedEnvironmentKeys[1:]) == nil {
				t.Fatal("missing derived RPC key was not detected")
			}
			checkBridgePolicyBoundaries(t, evidence)
			return
		}
	}
	t.Fatal("missing runtime-bridge-rpc-deadlines family")
}

func checkBridgePolicyBoundaries(t *testing.T, evidence bridgePolicyEvidence) {
	t.Helper()
	fixed, remaining := 0, 0
	for method, policy := range evidence.Defaults {
		switch policy.Kind {
		case "fixed":
			fixed++
		case "remaining_provider_budget":
			remaining++
		default:
			t.Fatalf("unknown Bridge policy %s", policy.Kind)
		}
		want := bridgeParsedPolicy{policy.Kind, policy.TimeoutMS}
		if evidence.DefaultPolicies[method] != want {
			t.Fatalf("%s parsed default differs from its owner", method)
		}
	}
	if fixed != 37 || remaining != 3 || len(evidence.DefaultPolicies) != 40 {
		t.Fatal("Bridge policy semantic coverage differs")
	}
	if evidence.Defaults["registerRuntimeProcess"].EnvironmentKey != "TETRAL_RUNTIME_REGISTER_TIMEOUT_MS" || evidence.Defaults["reportRuntimeProcess"].EnvironmentKey != "TETRAL_RUNTIME_REPORT_TIMEOUT_MS" {
		t.Fatal("Runtime alias keys changed")
	}
	if evidence.MinimumOverride != (bridgeParsedPolicy{"fixed", 1}) || evidence.MaximumOverride != (bridgeParsedPolicy{"fixed", 2147483647}) || !evidence.InvalidFixedRejected || !evidence.RemainingOverridesRejected {
		t.Fatal("actual Bridge policy override constraints differ")
	}
	if evidence.FixedDeadline != 31000 || evidence.ClippedDeadline != 1100 || evidence.ProviderDeadline != 1100 || !evidence.MissingProviderDeadlineRejected {
		t.Fatal("actual Bridge caller-budget constraints differ")
	}
}

func TestConfigurationProjectionDetectsValidMirrorDriftAndInvalidCombinations(t *testing.T) {
	helm := requireHelm(t)
	chart := filepath.Join(engineRoot(t), "deploy/helm/tetral")
	envs := configurationEnvironments(t, uniqueObjects(t, renderChart(t, helm, chart)))
	qenv := envs["queue"]
	qenv[queueservice.EnvRetryBaseMS] = "42"
	cfg, err := queueservice.ConfigFromEnv(qenv)
	if err != nil {
		t.Fatal("drift control must remain valid to the owner")
	}
	if requireProjection(queueOperational(cfg), map[string]int64{"reclaim_seconds": 30, "reclaim_limit": 100, "base_ms": 1000, "cap_ms": 60000, "attempts": 10}) == nil {
		t.Fatal("valid changed mirror was not detected")
	}
	for _, tc := range []struct{ role, key, value string }{
		{"queue", queueservice.EnvRetryCapMS, "1"}, {"queue", queueservice.EnvRetryMaxAttempts, "0"},
		{"job-runner", runner.EnvJobRunnerHeartbeatIntervalMS, "30000"},
		{"job-runner", runner.EnvJobRunnerMaxJobs, strconv.Itoa(queue.MaxQueueLeaseJobs() + 1)},
		{"job-runner", "TETRAL_DRAIN_TIMEOUT_MS", "35000"},
	} {
		env := projectionEnv{}
		for k, v := range envs[tc.role] {
			env[k] = v
		}
		env[tc.key] = tc.value
		var err error
		if tc.role == "queue" {
			_, err = queueservice.ConfigFromEnv(env)
		} else {
			_, err = runner.JobRunnerConfigFromEnv(env)
		}
		if err == nil {
			t.Fatalf("owning parser accepted %s %s invalid combination", tc.role, tc.key)
		}
	}
	for _, values := range [][]string{{"queue.retryCapMs=1"}, {"queue.retryBaseMs=0"}, {"jobRunner.heartbeatIntervalMs=30000"}, {"jobRunner.maxJobs=0"}, {"observability.logLevel=trace"}, {"observability.logBurst=01"}, {"observability.logMaxRecordBytes=65537"}, {"observability.logSummaryIntervalMs=99"}} {
		if _, err := renderChartFailure(t, helm, chart, values...); err == nil {
			t.Fatalf("chart accepted invalid %v", values)
		}
	}
}

func TestConfigurationInventoryHasCurrentOwnerSources(t *testing.T) {
	root := engineRoot(t)
	data, err := os.ReadFile(filepath.Join(root, "deploy/managed/configuration-inventory.json"))
	if err != nil {
		t.Fatal(err)
	}
	var catalog struct {
		SchemaVersion int `json:"schema_version"`
		Families      []struct {
			ID, Owner, Units, UnsetZero, Constraints, Projection string
			Sources, Proofs                                      []string
		}
	}
	// Explicit tags retain the public snake_case field contract.
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(raw["schema_version"], &catalog.SchemaVersion); err != nil {
		t.Fatal(err)
	}
	var families []map[string]json.RawMessage
	if err := json.Unmarshal(raw["families"], &families); err != nil {
		t.Fatal(err)
	}
	seen := map[string]bool{}
	for _, family := range families {
		var id string
		_ = json.Unmarshal(family["id"], &id)
		if id == "" || seen[id] {
			t.Fatal("empty/duplicate configuration family")
		}
		seen[id] = true
		for _, key := range []string{"owner", "units", "unset_zero", "constraints", "projection"} {
			var value string
			_ = json.Unmarshal(family[key], &value)
			if value == "" {
				t.Fatalf("%s missing %s disposition", id, key)
			}
		}
		var sourcePaths, declaredKeys []string
		_ = json.Unmarshal(family["sources"], &sourcePaths)
		_ = json.Unmarshal(family["environment_keys"], &declaredKeys)
		actualKeys := map[string]bool{}
		pattern := regexp.MustCompile(`\b(?:TETRAL_[A-Z0-9_]+|ENGINE_[A-Z0-9_]+|DAYTONA_[A-Z0-9_]+)\b`)
		for _, path := range sourcePaths {
			source, err := os.ReadFile(filepath.Join(root, path))
			if err != nil {
				t.Fatal(err)
			}
			for _, key := range pattern.FindAllString(string(source), -1) {
				if !strings.HasSuffix(key, "_") {
					actualKeys[key] = true
				}
			}
		}
		if len(actualKeys) != len(declaredKeys) {
			t.Fatalf("%s environment census differs from owning sources", id)
		}
		for _, key := range declaredKeys {
			if !actualKeys[key] {
				t.Fatalf("%s obsolete/invented env key %s", id, key)
			}
		}
		for _, key := range []string{"sources", "proofs"} {
			var paths []string
			if err := json.Unmarshal(family[key], &paths); err != nil {
				t.Fatal(err)
			}
			for _, path := range paths {
				if filepath.IsAbs(path) || strings.Contains(path, "..") {
					t.Fatal("inventory path escapes repository")
				}
				if _, err := os.Stat(filepath.Join(root, path)); err != nil {
					t.Fatal(err)
				}
			}
		}
	}
	expected := strings.Fields("go-database-pool bun-database-pool resource-metadata diagnostics auth-listeners-and-exchange internal-authentication runtime-process-custody job-runner bridge queue cleanup sandbox blob-transport provider-gateway mcp-connector runtime-pod runtime-bridge-rpc-deadlines event-stream git-proxy web-connector api-and-bootstrap kubernetes-visibility runtime-installed-configuration domain-protocol-storage deployment-policy build-release-verification documentation-workflow preview-broker transport-security")
	if catalog.SchemaVersion != 1 || len(seen) != len(expected) {
		t.Fatal("configuration semantic coverage differs")
	}
	for _, id := range expected {
		if !seen[id] {
			t.Fatalf("missing semantic family %s", id)
		}
	}
	checkedConfigurationDefaults(t, families)

}

// Catalog defaults are checked against actual owners. The independent literal
// oracles and override/constraint tests remain in the owning parser suites.
func checkedConfigurationDefaults(t *testing.T, families []map[string]json.RawMessage) {
	t.Helper()
	empty := projectionEnv{}
	pool, err := dbconnect.PoolConfigFromEnv(empty.Getenv)
	if err != nil {
		t.Fatal(err)
	}
	diagnostic := workload.DefaultDiagnosticConfig()
	q, err := queueservice.ConfigFromEnv(empty)
	if err != nil {
		t.Fatal(err)
	}
	process := runtimecontrol.DefaultProcessPolicy()
	c, err := cleanup.ConfigFromEnv(empty)
	if err != nil {
		t.Fatal(err)
	}
	expected := map[string]map[string]any{
		"go-database-pool":        {"max_open": pool.MaxOpenConns, "max_idle": pool.MaxIdleConns, "max_lifetime": pool.ConnMaxLifetime.String(), "max_idle_time": pool.ConnMaxIdleTime.String(), "statement_timeout": pool.StatementTimeout.String()},
		"diagnostics":             {"level": strings.ToLower(diagnostic.Level.String()), "max_record_bytes": diagnostic.MaxRecordBytes, "summary_interval_ms": diagnostic.SummaryInterval.Milliseconds(), "burst": diagnostic.Burst},
		"runtime-process-custody": {"registration_ms": process.RegistrationTimeout.Milliseconds(), "report_timeout_ms": process.ReportTimeout.Milliseconds(), "report_interval_ms": process.ReportInterval.Milliseconds(), "freshness_ms": process.Freshness.Milliseconds()},
		"queue":                   {"reclaim_interval_seconds": int64(q.LeaseReclaimInterval / time.Second), "reclaim_limit": q.LeaseReclaimBatchLimit, "retry_base_ms": q.RetryBaseDelay.Milliseconds(), "retry_cap_ms": q.RetryMaxDelay.Milliseconds(), "retry_max_attempts": q.RetryMaxAttempts, "drain_ms": q.DrainTimeout.Milliseconds(), "cancel_join_ms": q.CancelJoinTimeout.Milliseconds()},
		"cleanup":                 {"claim_limit": c.ClaimLimit, "metrics_export_timeout": c.MetricsExportTimeout.String()},
	}
	for _, family := range families {
		var id string
		_ = json.Unmarshal(family["id"], &id)
		defaults, check := expected[id]
		if !check {
			continue
		}
		actualJSON, _ := json.Marshal(defaults)
		var actual, declared map[string]any
		_ = json.Unmarshal(actualJSON, &actual)
		_ = json.Unmarshal(family["defaults"], &declared)
		// Go duration formatting is canonical; inventory permits conventional minute abbreviations.
		if id == "go-database-pool" {
			for _, key := range []string{"max_lifetime", "max_idle_time", "statement_timeout"} {
				value, err := time.ParseDuration(fmt.Sprint(declared[key]))
				if err != nil {
					t.Fatal(err)
				}
				declared[key] = value.String()
			}
		}
		if err := requireProjection(declared, actual); err != nil {
			t.Fatalf("%s catalog defaults: %v", id, err)
		}
	}
}

func TestLoggingInventoryMapsSafeFieldsAndDisabledProxy(t *testing.T) {
	root := engineRoot(t)
	var inventory struct {
		SchemaVersion     int                         `json:"schema_version"`
		ApplicationRecord struct{ Required []string } `json:"application_record"`
		Families          []struct {
			ID              string
			Sources, Proofs []string
			OptionalFields  []string `json:"optional_fields"`
		} `json:"families"`
	}
	data, err := os.ReadFile(filepath.Join(root, "deploy/managed/logging-inventory.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(data, &inventory); err != nil {
		t.Fatal(err)
	}
	var vocabulary []string
	data, err = os.ReadFile(filepath.Join(root, "internal/ts-observability/src/fields.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(data, &vocabulary); err != nil {
		t.Fatal(err)
	}
	approved := map[string]bool{}
	for _, field := range vocabulary {
		approved[field] = true
	}
	for _, family := range inventory.Families {
		sourceBytes := []byte{}
		for _, path := range family.Sources {
			source, err := os.ReadFile(filepath.Join(root, path))
			if err != nil {
				t.Fatal(err)
			}
			sourceBytes = append(sourceBytes, source...)
		}
		for _, field := range family.OptionalFields {
			if !bytes.Contains(sourceBytes, []byte(strconv.Quote(field))) {
				t.Fatalf("%s lacks a producer reference for %s", family.ID, field)
			}
			if !approved[field] {
				t.Fatalf("%s invents unsupported structured field %s", family.ID, field)
			}
		}
		for _, path := range append(family.Sources, family.Proofs...) {
			if _, err := os.Stat(filepath.Join(root, path)); err != nil {
				t.Fatal(err)
			}
		}
	}
	var record bytes.Buffer
	owner := workload.NewProcessLogger(&record, "inventory-probe", "projection-test", "projection-version", workload.DefaultDiagnosticConfig())
	owner.Logger.Info("inventory.probe", "workspace.id", "workspace-fixture", "session.id", "session-fixture")
	owner.CloseWithBudget()
	var actual map[string]any
	if err := json.Unmarshal(bytes.TrimSpace(record.Bytes()), &actual); err != nil {
		t.Fatal(err)
	}
	for _, field := range inventory.ApplicationRecord.Required {
		if _, exists := actual[field]; !exists {
			t.Fatalf("actual Go process record lacks catalog required %s", field)
		}
	}
	for _, field := range []string{"workspace.id", "session.id"} {
		if actual[field] == nil {
			t.Fatalf("actual scoped record lost %s", field)
		}
	}
	objects := uniqueObjects(t, renderChart(t, requireHelm(t), filepath.Join(root, "deploy/helm/tetral"), "edge.enabled=true"))
	proxyCount := 0
	for _, object := range objects {
		if object["kind"] != "EnvoyProxy" {
			continue
		}
		proxyCount++
		if configurationAt(object, "spec", "telemetry", "accessLog", "disable") != true {
			t.Fatal("public proxy access logs must remain disabled")
		}
	}
	if inventory.SchemaVersion != 1 || proxyCount != 1 {
		t.Fatal("logging/proxy contract missing")
	}
}

// Other service owners retain fixed business defaults. Passing actual rendered
// environment through those parsers detects a valid-looking deployment mirror
// that violates the service's required/typed startup contract.
func checkGoOwningParsers(t *testing.T, envs map[string]projectionEnv) {
	t.Helper()
	for _, role := range strings.Fields("api auth bridge job-runner queue sandbox cleanup event-stream git-proxy web-connector provider-gateway mcp-connector agent-runtime") {
		if len(envs[role]) == 0 {
			t.Fatalf("missing configured application process %s", role)
		}
	}
	a, err := api.ConfigFromEnv(envs["api"])
	if err != nil {
		t.Fatalf("API parser: %v", err)
	}
	if a.ListenAddress != ":8080" || a.MetricsAddress != ":8081" {
		t.Fatal("API listener projection differs")
	}
	au, err := authservice.ConfigFromEnv(envs["auth"])
	if err != nil {
		t.Fatalf("Auth parser: %v", err)
	}
	if au.InternalPrincipalTTL != 60*time.Second || au.JWKSCacheTTL != 600*time.Second || au.ExchangeLimits.BodyBytes != 32768 {
		t.Fatal("Auth fixed policy projection differs")
	}
	g, err := gitproxy.ConfigFromEnv(envs["git-proxy"])
	if err != nil {
		t.Fatalf("Git parser: %v", err)
	}
	if g.DrainGrace != 1800*time.Second {
		t.Fatal("Git drain/rotation mirror differs")
	}
	s, err := sandbox.ConfigFromEnv(envs["sandbox"])
	if err != nil {
		t.Fatalf("Sandbox parser: %v", err)
	}
	if s.LeaseHeartbeatInterval != 15*time.Second || s.JobLeaseDuration != 120*time.Second || s.ProviderCommandTimeout != 45*time.Second || s.LateCommandMargin != 30*time.Second {
		t.Fatal("Sandbox fixed lease fence projection differs")
	}
	w, err := webconnector.LoadConfig(envs["web-connector"])
	if err != nil {
		t.Fatalf("Web parser: %v", err)
	}
	if w.DrainTimeout != 10*time.Second || w.CancelJoinTimeout != 5*time.Second {
		t.Fatal("Web join projection differs")
	}
}

func TestConfigurationHardenedDefaultsMatchRawProfile(t *testing.T) {
	root := engineRoot(t)
	rendered := renderChart(t, requireHelm(t), filepath.Join(root, "deploy/helm/tetral"), "transport.profile=hardened")
	canonical := readManifestObjects(t, []string{filepath.Join(root, "deploy/kubernetes/profiles/hardened/workloads.yaml")})
	requireObjectSetsEqual(t, rendered, canonical)
}
