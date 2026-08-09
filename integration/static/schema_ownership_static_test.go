package static_test

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"
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
		"api=migrate",
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

func TestSchemaOwnershipProductionWiringHasOneMigratorAndNoOtherDDL(t *testing.T) {
	root := schemaOwnershipEngineRoot(t)
	entrypoints := map[string]struct {
		path string
		gate string
	}{
		"api":              {path: "services/api/tetralapi.go", gate: ".MigrateSchema(ctx)"},
		"auth":             {path: "services/auth/wiring.go", gate: ".VerifySchema(ctx)"},
		"queue":            {path: "services/queue/cmd/tetral-queue/main.go", gate: "verifySchema(ctx"},
		"sandbox":          {path: "services/sandbox/cmd/tetral-sandbox/main.go", gate: "verifySchema(ctx"},
		"bridge-api":       {path: "services/bridge/cmd/bridge-api/main.go", gate: "verifySchema(ctx"},
		"job-runner":       {path: "services/bridge/cmd/job-runner/main.go", gate: "verifySchema(ctx"},
		"event-stream":     {path: "services/event-stream/cmd/event-stream/main.go", gate: ".VerifySchema(ctx)"},
		"cleanup":          {path: "services/cleanup/cmd/tetral-cleanup/main.go", gate: "verifySchema(ctx"},
		"git-proxy":        {path: "services/git-proxy/cmd/git-proxy/main.go", gate: "verifySchema(ctx"},
		"provider-gateway": {path: "services/gateway/packages/provider-gateway/src/command.ts", gate: "verifyPostgreSQLSchema"},
		"mcp-connector":    {path: "services/gateway/packages/mcp-connector/src/command.ts", gate: "verifyPostgreSQLSchema"},
	}
	for name, entrypoint := range entrypoints {
		text := readSchemaOwnershipFile(t, filepath.Join(root, entrypoint.path))
		if !strings.Contains(text, entrypoint.gate) {
			t.Errorf("%s startup missing schema gate %q in %s", name, entrypoint.gate, entrypoint.path)
		}
		if name != "api" && strings.Contains(text, "MigrateSchema") {
			t.Errorf("non-owner %s references MigrateSchema", name)
		}
	}

	forbiddenDDL := []string{"CREATE TABLE", "ALTER TABLE", "DROP TABLE", "CREATE INDEX", "CREATE POLICY"}
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
	goSource := readSchemaOwnershipFile(t, filepath.Join(root, "internal/storage/postgresql_migrator.go"))
	gatewaySource := readSchemaOwnershipFile(t, filepath.Join(root, "services/gateway/packages/schema/src/verify.ts"))
	declarationPattern := regexp.MustCompile(`PostgreSQLSchemaVersion([A-Za-z]+)Checksum\s*=\s*\n?\s*"([0-9a-f]{64})"`)
	goChecksums := declarationPattern.FindAllStringSubmatch(goSource, -1)
	gatewayChecksums := declarationPattern.FindAllStringSubmatch(gatewaySource, -1)
	if len(goChecksums) == 0 || len(goChecksums) != len(gatewayChecksums) {
		t.Fatalf("Go/Gateway schema checksum declaration counts = %d/%d; want one Gateway checksum per Go migration", len(goChecksums), len(gatewayChecksums))
	}
	for index := range goChecksums {
		if goChecksums[index][1] != gatewayChecksums[index][1] ||
			goChecksums[index][2] != gatewayChecksums[index][2] {
			t.Fatalf(
				"Gateway schema checksum %d = v%s/%s; want Go v%s/%s",
				index+1,
				strings.ToLower(gatewayChecksums[index][1]),
				gatewayChecksums[index][2],
				strings.ToLower(goChecksums[index][1]),
				goChecksums[index][2],
			)
		}
	}

	goFile, err := parser.ParseFile(token.NewFileSet(), "postgresql_migrator.go", goSource, 0)
	if err != nil {
		t.Fatalf("parse Go schema registry source: %v", err)
	}
	var goRegistryLiteral *ast.CompositeLit
	for _, declaration := range goFile.Decls {
		function, ok := declaration.(*ast.FuncDecl)
		if !ok || function.Name.Name != "postgresqlMigrationRegistry" || function.Body == nil {
			continue
		}
		if len(function.Body.List) != 1 {
			t.Fatalf("Go postgresqlMigrationRegistry body has %d statements; want one direct return", len(function.Body.List))
		}
		returnStatement, ok := function.Body.List[0].(*ast.ReturnStmt)
		if !ok || len(returnStatement.Results) != 1 {
			t.Fatal("Go postgresqlMigrationRegistry must directly return one composite literal")
		}
		goRegistryLiteral, _ = returnStatement.Results[0].(*ast.CompositeLit)
	}
	if goRegistryLiteral == nil {
		t.Fatal("could not locate Go postgresqlMigrationRegistry composite literal")
	}
	type goRegistryEntry struct {
		version  string
		checksum string
		steps    string
	}
	goRegistryEntries := make([]goRegistryEntry, 0, len(goRegistryLiteral.Elts))
	for index, rawEntry := range goRegistryLiteral.Elts {
		entryLiteral, ok := rawEntry.(*ast.CompositeLit)
		if !ok {
			t.Fatalf("Go executable schema registry entry %d is not a composite literal", index+1)
		}
		entry := goRegistryEntry{}
		for _, rawField := range entryLiteral.Elts {
			field, ok := rawField.(*ast.KeyValueExpr)
			if !ok {
				continue
			}
			key, ok := field.Key.(*ast.Ident)
			if !ok {
				continue
			}
			switch key.Name {
			case "version":
				value, ok := field.Value.(*ast.BasicLit)
				if !ok || value.Kind != token.INT {
					t.Fatalf("Go executable schema registry entry %d has a non-integer version", index+1)
				}
				entry.version = value.Value
			case "checksum":
				value, ok := field.Value.(*ast.Ident)
				if !ok {
					t.Fatalf("Go executable schema registry entry %d has a non-identifier checksum", index+1)
				}
				entry.checksum = value.Name
			case "steps":
				call, ok := field.Value.(*ast.CallExpr)
				if !ok || len(call.Args) != 0 {
					t.Fatalf("Go executable schema registry entry %d has a non-zero-argument steps call", index+1)
				}
				value, ok := call.Fun.(*ast.Ident)
				if !ok {
					t.Fatalf("Go executable schema registry entry %d has a non-identifier steps function", index+1)
				}
				entry.steps = value.Name
			}
		}
		if entry.version == "" || entry.checksum == "" || entry.steps == "" {
			t.Fatalf("Go executable schema registry entry %d omits version, checksum, or steps", index+1)
		}
		goRegistryEntries = append(goRegistryEntries, entry)
	}
	if len(goRegistryEntries) != len(goChecksums) {
		t.Fatalf("Go executable schema registry length = %d; want %d checksum declarations", len(goRegistryEntries), len(goChecksums))
	}
	wantRegistry := []struct {
		checksumVersion string
		steps           string
	}{
		{checksumVersion: "One", steps: "postgresqlBaselineSteps"},
	}
	if len(wantRegistry) != len(goChecksums) {
		t.Fatalf("Go executable schema registry expectation count = %d; want %d checksum declarations", len(wantRegistry), len(goChecksums))
	}
	for index, checksumDeclaration := range goChecksums {
		if len(checksumDeclaration) < 2 || index >= len(wantRegistry) || index >= len(goRegistryEntries) {
			t.Fatalf("schema registry row %d is short: checksum fields=%d registry=%d expectations=%d", index+1, len(checksumDeclaration), len(goRegistryEntries), len(wantRegistry))
		}
		//nolint:gosec // G602: index bound is asserted on the line above; gosec cannot follow it.
		want := wantRegistry[index]
		entry := goRegistryEntries[index]
		wantVersion := strconv.Itoa(index + 1)
		wantChecksum := "PostgreSQLSchemaVersion" + want.checksumVersion + "Checksum"
		if checksumDeclaration[1] != want.checksumVersion {
			t.Fatalf(
				"Go schema checksum declaration %d names version %s; want %s",
				index+1,
				checksumDeclaration[1],
				want.checksumVersion,
			)
		}
		if entry.version != wantVersion || entry.checksum != wantChecksum || entry.steps != want.steps {
			t.Fatalf(
				"Go executable schema registry entry %d = version %s/%s/%s; want version %s/%s/%s",
				index+1,
				entry.version,
				entry.checksum,
				entry.steps,
				wantVersion,
				wantChecksum,
				want.steps,
			)
		}
	}

	registryPattern := regexp.MustCompile(`(?s)const PostgreSQLSchemaRegistry = \[(.*?)\] as const`)
	registryMatch := registryPattern.FindStringSubmatch(gatewaySource)
	if len(registryMatch) != 2 {
		t.Fatal("could not locate Gateway PostgreSQLSchemaRegistry")
	}
	var registryEntries []string
	for _, raw := range strings.Split(registryMatch[1], ",") {
		if entry := strings.TrimSpace(raw); entry != "" {
			registryEntries = append(registryEntries, entry)
		}
	}
	if len(registryEntries) != len(goChecksums) {
		t.Fatalf("Gateway schema registry length = %d; want %d Go migrations", len(registryEntries), len(goChecksums))
	}
	for index := range goChecksums {
		wantEntry := "PostgreSQLSchemaVersion" + goChecksums[index][1] + "Checksum"
		if registryEntries[index] != wantEntry {
			t.Fatalf(
				"Gateway schema registry entry %d = %q; want %q",
				index+1,
				registryEntries[index],
				wantEntry,
			)
		}
	}
}

func TestSchemaOwnershipRolloutWaitsForAPIBeforeEveryNonOwner(t *testing.T) {
	root := schemaOwnershipEngineRoot(t)
	text := readSchemaOwnershipFile(t, filepath.Join(root, "deploy", "kubernetes", "rollout-schema-ordered.sh"))
	apiApply := strings.Index(text, "api.yaml")
	wait := strings.Index(text, "rollout status deployment/api")
	if apiApply < 0 || wait < 0 || apiApply >= wait {
		t.Fatalf("rollout must apply api then wait for its rollout; script was:\n%s", text)
	}
	for _, manifest := range []string{
		"auth.yaml",
		"queue.yaml",
		"sandbox.yaml",
		"bridge.yaml",
		"event-stream.yaml",
		"cleanup.yaml",
		"git-proxy.yaml",
		"gateway.yaml",
	} {
		index := strings.Index(text, manifest)
		if index < 0 {
			t.Errorf("rollout missing non-owner manifest %s", manifest)
		} else if index <= wait {
			t.Errorf("rollout applies %s before api wait", manifest)
		}
	}
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
