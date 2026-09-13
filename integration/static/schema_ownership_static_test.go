package static_test

import (
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"

	"github.com/tetral-ai/tetral/internal/schemaidentity"
)

var databaseEnvironmentPattern = regexp.MustCompile(`(?m)^\s*- name: (?:TETRAL_DATABASE_URL|TETRAL_POSTGRES_DSN|TETRAL_EVENT_STREAM_DATABASE_URL)\s*$`)
var databaseConfigurationSourcePattern = regexp.MustCompile(`(?mi)^\s+name:\s*[A-Za-z0-9_.-]*(?:database|postgres)[A-Za-z0-9_.-]*\s*$`)
var yamlNamePattern = regexp.MustCompile(`^(\s*)- name: ([A-Za-z0-9_.-]+)\s*$`)

func TestSchemaOwnershipManifestDiscoveryClassifiesEveryDatabaseContainer(t *testing.T) {
	root := schemaOwnershipEngineRoot(t)
	localFiles, err := filepath.Glob(filepath.Join(root, "services", "*", "k8s", "*.yaml"))
	if err != nil {
		t.Fatalf("glob service manifests: %v", err)
	}
	topRoot := filepath.Join(root, "deploy", "kubernetes")
	if override := os.Getenv("TETRAL_SCHEMA_OWNERSHIP_TOP_MANIFESTS_ROOT"); override != "" {
		topRoot = override
	}
	topFiles, err := filepath.Glob(filepath.Join(topRoot, "*.yaml"))
	if err != nil {
		t.Fatalf("glob top-level manifests: %v", err)
	}

	local := discoverDatabaseContainers(t, localFiles)
	top := discoverDatabaseContainers(t, topFiles)
	want := []string{
		"api=verify",
		"auth=verify",
		"bridge-api=verify",
		"cleanup=verify",
		"event-stream=verify",
		"git-proxy=verify",
		"job-runner=verify",
		"mcp-connector=verify",
		"provider-gateway=verify",
		"queue=verify",
		"sandbox=verify",
	}
	if strings.Join(local, "\n") != strings.Join(want, "\n") {
		t.Fatalf("service-local DB container census = %v, want %v", local, want)
	}
	if strings.Join(top, "\n") != strings.Join(want, "\n") {
		t.Fatalf("top-level DB container census = %v, want %v", top, want)
	}
}

func TestSchemaOwnershipServingProcessesOnlyVerify(t *testing.T) {
	root := schemaOwnershipEngineRoot(t)
	entrypoints := map[string]struct {
		path     string
		gate     string
		roleGate string
	}{
		"api":              {path: "services/api/tetralapi.go", gate: ".VerifySchema(ctx)", roleGate: ".VerifyRuntimeRole(ctx)"},
		"auth":             {path: "services/auth/wiring.go", gate: ".VerifySchema(ctx)", roleGate: ".VerifyRuntimeRole(ctx)"},
		"queue":            {path: "services/queue/cmd/tetral-queue/main.go", gate: "verifySchema(ctx", roleGate: ".VerifyRuntimeRole(ctx)"},
		"sandbox":          {path: "services/sandbox/cmd/tetral-sandbox/main.go", gate: "verifySchema(ctx", roleGate: ".VerifyRuntimeRole(ctx)"},
		"bridge-api":       {path: "services/bridge/cmd/bridge-api/main.go", gate: "verifySchema(ctx", roleGate: ".VerifyRuntimeRole(ctx)"},
		"job-runner":       {path: "services/bridge/cmd/job-runner/main.go", gate: "verifySchema(ctx", roleGate: ".VerifyRuntimeRole(ctx)"},
		"event-stream":     {path: "services/event-stream/cmd/event-stream/main.go", gate: ".VerifySchema(ctx)", roleGate: ".VerifyRuntimeRole(ctx)"},
		"cleanup":          {path: "services/cleanup/cmd/tetral-cleanup/main.go", gate: "verifySchema(ctx", roleGate: ".VerifyRuntimeRole(ctx)"},
		"git-proxy":        {path: "services/git-proxy/cmd/git-proxy/main.go", gate: "verifySchema(ctx", roleGate: ".VerifyRuntimeRole(ctx)"},
		"provider-gateway": {path: "services/gateway/packages/provider-gateway/src/command.ts", gate: "verifyPostgreSQLReadiness", roleGate: "verifyPostgreSQLReadiness"},
		"mcp-connector":    {path: "services/gateway/packages/mcp-connector/src/command.ts", gate: "verifyPostgreSQLReadiness", roleGate: "verifyPostgreSQLReadiness"},
	}
	for name, entrypoint := range entrypoints {
		text := readSchemaOwnershipFile(t, filepath.Join(root, entrypoint.path))
		if !strings.Contains(text, entrypoint.gate) {
			t.Errorf("%s startup missing schema gate %q in %s", name, entrypoint.gate, entrypoint.path)
		}
		if !strings.Contains(text, entrypoint.roleGate) {
			t.Errorf("%s startup missing runtime-role gate %q in %s", name, entrypoint.roleGate, entrypoint.path)
		}
		if strings.Index(text, entrypoint.roleGate) < strings.Index(text, entrypoint.gate) {
			t.Errorf("%s startup checks runtime role before schema in %s", name, entrypoint.path)
		}
		if strings.Contains(text, "MigrateSchema") {
			t.Errorf("non-owner %s references MigrateSchema", name)
		}
	}

	forbiddenDDL := []string{"CREATE TABLE", "ALTER TABLE", "DROP TABLE", "CREATE INDEX", "CREATE POLICY", "MigrateSchema(", "ApplyRoleContract("}
	err := filepath.WalkDir(filepath.Join(root, "services"), func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() && (entry.Name() == "node_modules" || entry.Name() == "dist") {
			return filepath.SkipDir
		}
		if entry.IsDir() || strings.Contains(filepath.ToSlash(path), "/test/") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		if !strings.HasSuffix(path, ".go") && !strings.HasSuffix(path, ".ts") {
			return nil
		}
		text := readSchemaOwnershipFile(t, path)
		if strings.Contains(text, "InitializeSchema(") {
			t.Errorf("production startup retains legacy schema initializer: %s", path)
		}
		for _, token := range forbiddenDDL {
			if strings.Contains(text, token) {
				t.Errorf("production service source contains forbidden DDL token %q: %s", token, path)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk production services: %v", err)
	}
}

func TestSchemaOwnershipJobRunnerUsesProductionRuntimeDeliveryAssembly(t *testing.T) {
	root := schemaOwnershipEngineRoot(t)
	const path = "services/bridge/cmd/job-runner/main.go"
	text := readSchemaOwnershipFile(t, filepath.Join(root, path))
	for _, required := range []string{
		"deliveryStore := agentruntimebridge.NewJobRunnerRuntimeDeliveryStore(",
		"agentruntimebridge.JobRunner{",
		"Deliverer: agentruntimebridge.RuntimePodDirectDeliverer{",
		"Store: deliveryStore,",
	} {
		if !strings.Contains(text, required) {
			t.Fatalf("job-runner startup missing production runtime-delivery wiring %q in %s", required, path)
		}
	}
	// The deliverer must be a field of the JobRunner literal, not a detached
	// value: pin the ordering so the assignment itself is proven.
	runnerAt := strings.Index(text, "agentruntimebridge.JobRunner{")
	delivererAt := strings.Index(text, "Deliverer: agentruntimebridge.RuntimePodDirectDeliverer{")
	if runnerAt >= delivererAt {
		t.Fatalf("job-runner startup does not assign the deliverer inside the JobRunner literal in %s (JobRunner at %d, Deliverer at %d)", path, runnerAt, delivererAt)
	}
}

func TestSchemaOwnershipGatewayChecksumsMatchGoRegistry(t *testing.T) {
	root := schemaOwnershipEngineRoot(t)
	gatewaySource := readSchemaOwnershipFile(t, filepath.Join(root, "services/gateway/packages/schema/src/verify.ts"))
	identities := schemaidentity.History()
	declarationPattern := regexp.MustCompile(`(PostgreSQLSchemaVersion[A-Za-z]+Checksum)\s*=\s*\n?\s*"([0-9a-f]{64})"`)
	declarations := declarationPattern.FindAllStringSubmatch(gatewaySource, -1)
	if len(identities) == 0 || len(declarations) != len(identities) {
		t.Fatalf("Gateway declarations = %d, shared identities = %d", len(declarations), len(identities))
	}
	checksums := make(map[string]string, len(declarations))
	for _, declaration := range declarations {
		if _, exists := checksums[declaration[1]]; exists {
			t.Fatalf("duplicate Gateway checksum declaration %s", declaration[1])
		}
		checksums[declaration[1]] = declaration[2]
	}
	registryPattern := regexp.MustCompile(`(?s)const PostgreSQLSchemaRegistry = \[(.*?)\] as const`)
	registryMatch := registryPattern.FindStringSubmatch(gatewaySource)
	if len(registryMatch) != 2 {
		t.Fatal("could not locate Gateway PostgreSQLSchemaRegistry")
	}
	var entries []string
	for _, raw := range strings.Split(registryMatch[1], ",") {
		if entry := strings.TrimSpace(raw); entry != "" {
			entries = append(entries, entry)
		}
	}
	if len(entries) != len(identities) {
		t.Fatalf("Gateway registry length = %d, shared identities = %d", len(entries), len(identities))
	}
	for i, identity := range identities {
		if identity.Version != int64(i+1) || checksums[entries[i]] != identity.Checksum {
			t.Fatalf("Gateway schema version %d checksum = %q; shared identity = %+v", i+1, checksums[entries[i]], identity)
		}
	}
	// Storage's TestPostgreSQLMigrationChecksumsMatchExactOrderedPayloads proves
	// that every shared identity is bound to its exact executable migration DDL.
}

func discoverDatabaseContainers(t *testing.T, files []string) []string {
	t.Helper()
	classified := map[string]string{}
	for _, path := range files {
		text := readSchemaOwnershipFile(t, path)
		lines := strings.Split(text, "\n")
		for index, line := range lines {
			match := yamlNamePattern.FindStringSubmatch(line)
			if match == nil || strings.HasPrefix(match[2], "TETRAL_") {
				continue
			}
			indent := len(match[1])
			end := len(lines)
			for next := index + 1; next < len(lines); next++ {
				nextMatch := yamlNamePattern.FindStringSubmatch(lines[next])
				if nextMatch != nil && len(nextMatch[1]) == indent {
					end = next
					break
				}
			}
			block := strings.Join(lines[index:end], "\n")
			if !strings.Contains(block, "\n"+strings.Repeat(" ", indent+2)+"image:") ||
				(!databaseEnvironmentPattern.MatchString(block) && !databaseConfigurationSourcePattern.MatchString(block)) {
				continue
			}
			mode := schemaModeFromContainerBlock(block)
			if mode == "" {
				t.Errorf("DB-connected container %s in %s has no literal TETRAL_SCHEMA_MODE", match[2], path)
				mode = "<missing>"
			}
			if previous, duplicate := classified[match[2]]; duplicate && previous != mode {
				t.Errorf("container %s mode drift: %s vs %s", match[2], previous, mode)
			}
			classified[match[2]] = mode
		}
	}
	var result []string
	for name, mode := range classified {
		result = append(result, name+"="+mode)
	}
	sort.Strings(result)
	return result
}

func schemaModeFromContainerBlock(block string) string {
	lines := strings.Split(block, "\n")
	for index, line := range lines {
		if strings.TrimSpace(line) != "- name: TETRAL_SCHEMA_MODE" {
			continue
		}
		for next := index + 1; next < len(lines) && next <= index+3; next++ {
			trimmed := strings.TrimSpace(lines[next])
			if strings.HasPrefix(trimmed, "value:") {
				return strings.Trim(strings.TrimSpace(strings.TrimPrefix(trimmed, "value:")), `"'`)
			}
		}
	}
	return ""
}

func schemaOwnershipEngineRoot(t *testing.T) string {
	t.Helper()
	root, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatalf("resolve engine root: %v", err)
	}
	return root
}

func readSchemaOwnershipFile(t *testing.T, path string) string {
	t.Helper()
	content, err := os.ReadFile(path) //nolint:gosec // test-owned manifest root path.
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return string(content)
}
