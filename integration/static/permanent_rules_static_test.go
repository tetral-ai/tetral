package static

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"testing"
)

func TestFinalArchitectureHasNoTopLevelPkgDirectory(t *testing.T) {
	engineRoot := finalArchitectureEngineRoot(t)
	if _, err := os.Stat(filepath.Join(engineRoot, "pkg")); err == nil {
		t.Fatal("engine/pkg must not exist; Engine ownership stays under internal packages")
	} else if !os.IsNotExist(err) {
		t.Fatalf("stat engine/pkg: %v", err)
	}
}

func TestFinalArchitectureGRPCAndProtobufImportsAreConfined(t *testing.T) {
	engineRoot := finalArchitectureEngineRoot(t)
	var violations []string
	err := filepath.WalkDir(engineRoot, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			return finalArchitectureSkipDir(entry)
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		rel := finalArchitectureRel(t, engineRoot, path)
		file, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.ImportsOnly)
		if err != nil {
			return err
		}
		for _, imported := range file.Imports {
			value := strings.Trim(imported.Path.Value, `"`)
			if finalArchitectureIsGRPCOrProtobufImport(value) && !finalArchitectureAllowsGRPCOrProtobuf(rel) {
				violations = append(violations, rel+" imports "+value)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("scan gRPC/protobuf imports: %v", err)
	}
	if len(violations) > 0 {
		t.Fatalf("gRPC/protobuf imports outside allowed internal surfaces:\n%s", strings.Join(violations, "\n"))
	}
}

func TestFinalArchitectureGeneratedPackagesDoNotImportPublicAPI(t *testing.T) {
	engineRoot := finalArchitectureEngineRoot(t)
	for _, packageDir := range []string{
		"internal/gen",
	} {
		scanGeneratedPackageImports(t, engineRoot, packageDir, []string{
			"github.com/tetral-ai/tetral/internal/httpapi",
			"github.com/tetral-ai/tetral/internal/tetralapi",
			"github.com/tetral-ai/tetral/services/api",
			"github.com/tetral-ai/tetral/internal/session",
			"github.com/tetral-ai/tetral/internal/sessionevent",
			"github.com/tetral-ai/tetral/internal/kubernetes",
		})
	}
}

func TestQueueJobInsertsStayBehindTheCanonicalQueueBoundary(t *testing.T) {
	engineRoot := finalArchitectureEngineRoot(t)
	var violations []string
	err := filepath.WalkDir(engineRoot, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			return finalArchitectureSkipDir(entry)
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		rel := finalArchitectureRel(t, engineRoot, path)
		if strings.HasPrefix(rel, "internal/queue/") {
			return nil
		}
		body, err := os.ReadFile(path) //nolint:gosec // repository-local static test path.
		if err != nil {
			return err
		}
		containsInsert, err := goSourceContainsQueueJobInsert(path, body)
		if err != nil {
			return err
		}
		if containsInsert {
			violations = append(violations, rel)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("scan queue insert sites: %v", err)
	}
	if len(violations) > 0 {
		t.Fatalf("queue job insert bypasses internal/queue.EnqueueTx:\n%s", strings.Join(violations, "\n"))
	}
}

func TestQueueJobInsertDetectorRejectsCaseWhitespaceAndLiteralSplits(t *testing.T) {
	for _, source := range []string{
		"package sample\nvar query = `insert into queue_jobs (id) values ('x')`\n",
		"package sample\nvar query = `INSERT\nINTO queue_jobs (id) VALUES ('x')`\n",
		"package sample\nvar query = \"INSERT \" + \"INTO queue_jobs (id) VALUES ('x')\"\n",
		"package sample\nconst table = \"queue_jobs\"\nvar query = \"INSERT INTO \" + table + \" (id) VALUES ('x')\"\n",
		"package sample\nfunc insert() { const verb = \"INSERT INTO \"; const table = \"queue_jobs\"; _ = verb + table + \" (id) VALUES ('x')\" }\n",
		"package sample\nfunc insert() { const verb = \"INSERT INTO \"; const table = \"queue_jobs\"; _ = verb + table + \" (id) VALUES ('x')\" }\nfunc mask() { const table = \"other\"; _ = table }\n",
		"package sample\nvar query = `INSERT INTO \"queue_jobs\" (id) VALUES ('x')`\n",
		"package sample\nvar query = `INSERT INTO public.queue_jobs (id) VALUES ('x')`\n",
	} {
		matched, err := goSourceContainsQueueJobInsert("fixture.go", []byte(source))
		if err != nil {
			t.Fatalf("parse detector fixture: %v", err)
		}
		if !matched {
			t.Fatalf("queue INSERT detector missed fixture:\n%s", source)
		}
	}
}

const queueJobInsertPattern = `(?i)\binsert\s+into\s+(?:(?:"?[a-z_][a-z0-9_]*"?)\s*\.\s*)?"?queue_jobs"?(?:\s|\(|$)`

func goSourceContainsQueueJobInsert(filename string, source []byte) (bool, error) {
	file, err := parser.ParseFile(token.NewFileSet(), filename, source, 0)
	if err != nil {
		return false, err
	}
	pattern := regexp.MustCompile(queueJobInsertPattern)
	found := false
	ast.Inspect(file, func(node ast.Node) bool {
		if found {
			return false
		}
		expression, ok := node.(ast.Expr)
		if !ok {
			return true
		}
		value, ok := constantGoString(expression, nil)
		if ok && pattern.MatchString(value) {
			found = true
			return false
		}
		return true
	})
	return found, nil
}

// This walker resolves file-local string constants for concatenation checks, so
// the go/ast identifier-object graph is sufficient and a full go/types pass would
// buy nothing. ast.Object is deprecated for cross-file resolution, which this
// never attempts.
//
//nolint:staticcheck // SA1019: file-local constant resolution only; see comment above.
func constantGoString(expression ast.Expr, resolving map[*ast.Object]bool) (string, bool) {
	switch value := expression.(type) {
	case *ast.BasicLit:
		if value.Kind != token.STRING {
			return "", false
		}
		decoded, err := strconv.Unquote(value.Value)
		return decoded, err == nil
	case *ast.BinaryExpr:
		if value.Op != token.ADD {
			return "", false
		}
		left, leftOK := constantGoString(value.X, resolving)
		right, rightOK := constantGoString(value.Y, resolving)
		return left + right, leftOK && rightOK
	case *ast.ParenExpr:
		return constantGoString(value.X, resolving)
	case *ast.Ident:
		object := value.Obj
		if object == nil || object.Kind != ast.Con {
			return "", false
		}
		if resolving == nil {
			resolving = map[*ast.Object]bool{}
		}
		if resolving[object] {
			return "", false
		}
		declaration, ok := object.Decl.(*ast.ValueSpec)
		if !ok {
			return "", false
		}
		index := -1
		for candidateIndex, name := range declaration.Names {
			if name.Obj == object {
				index = candidateIndex
				break
			}
		}
		if index < 0 || index >= len(declaration.Values) {
			return "", false
		}
		resolving[object] = true
		result, resolved := constantGoString(declaration.Values[index], resolving)
		delete(resolving, object)
		return result, resolved
	default:
		return "", false
	}
}

func TestRuntimeCommandDataMarshalSitesAreExplicitAndComplete(t *testing.T) {
	engineRoot := finalArchitectureEngineRoot(t)
	want := map[string]bool{
		"services/bridge/bridge_api_events.go:userMessageDataJSON":                 true,
		"services/bridge/bridge_api_mcp.go:runtimeMCPManifestCommandPayload":       true,
		"services/bridge/bridge_api_settlement.go:validateStableReasoningBudget":   true,
		"services/bridge/runtime_declaration.go:stableReasoningLedgerTx":           true,
		"services/bridge/runtime_delivery.go:acceptedMessageCommandPayloadTx":      true,
		"services/bridge/runtime_delivery.go:runtimeCommandPayloadForJobTx":        true,
		"services/bridge/runtime_delivery.go:runtimeSessionConfigCommandPayloadTx": true,
	}
	got := map[string]bool{}
	root := filepath.Join(engineRoot, "services", "bridge")
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		file, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
		if err != nil {
			return err
		}
		rel := finalArchitectureRel(t, engineRoot, path)
		for _, declaration := range file.Decls {
			function, ok := declaration.(*ast.FuncDecl)
			if !ok || function.Body == nil {
				continue
			}
			ast.Inspect(function.Body, func(node ast.Node) bool {
				call, ok := node.(*ast.CallExpr)
				if !ok {
					return true
				}
				identifier, ok := call.Fun.(*ast.Ident)
				if ok && identifier.Name == "marshalBridgeDataJSON" {
					got[rel+":"+function.Name.Name] = true
				}
				return true
			})
		}
		return nil
	})
	if err != nil {
		t.Fatalf("scan runtime data marshal sites: %v", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("runtime data marshal sites = %#v; want %#v", got, want)
	}
}

func TestFinalArchitectureAgentRuntimeServiceLayoutAndProtocolSource(t *testing.T) {
	engineRoot := finalArchitectureEngineRoot(t)
	agentRuntimeRoot := filepath.Join(engineRoot, "services", "agent-runtime")
	for _, required := range []string{
		"CLAUDE.md",
		"package.json",
		"bun.lock",
		"tsconfig.json",
		filepath.Join("packages", "core", "package.json"),
		filepath.Join("packages", "core", "src"),
		filepath.Join("packages", "protocol", "package.json"),
		filepath.Join("packages", "protocol", "src", "gen"),
		filepath.Join("packages", "runtime-pod", "package.json"),
		filepath.Join("packages", "runtime-pod", "src"),
	} {
		if _, err := os.Stat(filepath.Join(agentRuntimeRoot, required)); err != nil {
			t.Fatalf("active Agent Runtime package is missing %s: %v", required, err)
		}
	}
	assertNoGoImportsAgentRuntimeService(t, engineRoot)
	assertNoTypeScriptImportsEngineInternal(t, agentRuntimeRoot)
	assertProtoSourcesAreServiceLocalContracts(t, engineRoot)
	// The facts formerly asserted as CLAUDE.md keyword matches are guarded
	// structurally above: the package layout os.Stat list and the
	// service-local proto-source assertion. Docs are not proof surfaces.
}

func TestFinalArchitectureGatewayServiceLayoutAndProtocolSource(t *testing.T) {
	engineRoot := finalArchitectureEngineRoot(t)
	gatewayRoot := filepath.Join(engineRoot, "services", "gateway")
	for _, required := range []string{
		"Dockerfile",
		"package.json",
		"bun.lock",
		"tsconfig.json",
		"buf.gen.yaml",
		filepath.Join("proto", "tetral", "provider_gateway", "v1", "provider_gateway.proto"),
		filepath.Join("packages", "protocol", "package.json"),
		filepath.Join("packages", "protocol", "src", "gen", "tetral", "provider_gateway", "v1", "provider_gateway.ts"),
		filepath.Join("packages", "lowering", "package.json"),
		filepath.Join("packages", "provider-gateway", "package.json"),
		filepath.Join("packages", "provider-gateway", "src"),
	} {
		if _, err := os.Stat(filepath.Join(gatewayRoot, required)); err != nil {
			t.Fatalf("Gateway service package is missing %s: %v", required, err)
		}
	}
	assertNoTypeScriptImportsEngineInternal(t, gatewayRoot)
	assertProtoSourcesAreServiceLocalContracts(t, engineRoot)
	// The Gateway-boundary fact is guarded structurally by the layout
	// os.Stat list above; docs are not proof surfaces.
}

func TestFinalArchitectureServiceLocalMetricsSurfacesStayInternal(t *testing.T) {
	engineRoot := finalArchitectureEngineRoot(t)
	runtimeHTTP := finalArchitectureReadText(t, filepath.Join(engineRoot, "services", "agent-runtime", "packages", "runtime-pod", "src", "http-server.ts"))
	for _, required := range []string{
		`path === "/metrics"`,
		`text/plain; version=0.0.4; charset=utf-8`,
		"runtimeMetrics?: RuntimePodMetricsSource",
		"runtimePodMetricsText(lifecycle, runtimeMetrics)",
	} {
		if !strings.Contains(runtimeHTTP, required) {
			t.Fatalf("Runtime Pod HTTP ops plane missing metrics guard token %q", required)
		}
	}
	runtimeService := finalArchitectureReadText(t, filepath.Join(engineRoot, "services", "agent-runtime", "k8s", "service.yaml"))
	for _, required := range []string{
		"- name: http",
		"targetPort: http",
	} {
		if !strings.Contains(runtimeService, required) {
			t.Fatalf("Runtime Pod service must expose metrics only on its service-local HTTP ops port; missing %q", required)
		}
	}

	gatewayHTTP := finalArchitectureReadText(t, filepath.Join(engineRoot, "services", "gateway", "packages", "provider-gateway", "src", "http-server.ts"))
	for _, required := range []string{
		`path === "/metrics"`,
		`text/plain; version=0.0.4; charset=utf-8`,
		"state.metricsText()",
	} {
		if !strings.Contains(gatewayHTTP, required) {
			t.Fatalf("Provider Gateway HTTP ops plane missing metrics guard token %q", required)
		}
	}
	gatewayService := finalArchitectureReadText(t, filepath.Join(engineRoot, "services", "gateway", "k8s", "service.yaml"))
	for _, required := range []string{
		"- name: http",
		"targetPort: http",
		"- name: provider-grpc",
		"targetPort: provider-grpc",
	} {
		if !strings.Contains(gatewayService, required) {
			t.Fatalf("Gateway service must keep metrics on the HTTP ops port and separate from provider gRPC; missing %q", required)
		}
	}
}

func TestFinalArchitecturePublicAuthServiceBoundaryManifests(t *testing.T) {
	engineRoot := finalArchitectureEngineRoot(t)
	authRoot := filepath.Join(engineRoot, "services", "auth")
	for _, required := range []string{
		filepath.Join("cmd", "tetral-auth", "main.go"),
		"config.go",
		"wiring.go",
		"routes.go",
		filepath.Join("k8s", "configmap.yaml"),
		filepath.Join("k8s", "deployment.yaml"),
		filepath.Join("k8s", "networkpolicy.yaml"),
		filepath.Join("k8s", "secret.example.yaml"),
		filepath.Join("k8s", "service.yaml"),
		filepath.Join("k8s", "serviceaccount.yaml"),
	} {
		if _, err := os.Stat(filepath.Join(authRoot, required)); err != nil {
			t.Fatalf("auth service-local boundary is missing %s: %v", required, err)
		}
	}
	apiRoot := filepath.Join(engineRoot, "services", "api")
	for _, required := range []string{
		"bootstrap.go",
		filepath.Join("cmd", "tetral-api", "main.go"),
		"config.go",
		"routes.go",
		"wiring.go",
		filepath.Join("k8s", "configmap.yaml"),
		filepath.Join("k8s", "deployment.yaml"),
		filepath.Join("k8s", "networkpolicy.yaml"),
		filepath.Join("k8s", "secret.example.yaml"),
		filepath.Join("k8s", "service.yaml"),
		filepath.Join("k8s", "serviceaccount.yaml"),
	} {
		if _, err := os.Stat(filepath.Join(apiRoot, required)); err != nil {
			t.Fatalf("api service-local boundary is missing %s: %v", required, err)
		}
	}
	authDeployment := finalArchitectureReadText(t, filepath.Join(authRoot, "k8s", "deployment.yaml"))
	for _, required := range []string{
		"app.kubernetes.io/name: auth",
		"app.kubernetes.io/component: auth",
		"app.kubernetes.io/managed-by: kustomize",
		"name: ENGINE_API_KEY",
		"name: ENGINE_BOOTSTRAP_WORKSPACE_ID",
		"name: TETRAL_AUTH_INTERNAL_PRINCIPAL_PRIVATE_KEY_B64",
		"name: TETRAL_AUTH_INTERNAL_PRINCIPAL_TTL_SECONDS",
		"ephemeral-storage:",
		"automountServiceAccountToken: false",
	} {
		if !strings.Contains(authDeployment, required) {
			t.Fatalf("auth deployment missing auth boundary token %q", required)
		}
	}
	for _, target := range []struct {
		service string
		file    string
	}{
		{service: "api", file: filepath.Join(engineRoot, "services", "api", "k8s", "deployment.yaml")},
		{service: "event-stream", file: filepath.Join(engineRoot, "services", "event-stream", "k8s", "deployment.yaml")},
	} {
		text := finalArchitectureReadText(t, target.file)
		if !strings.Contains(text, "name: TETRAL_AUTH_INTERNAL_PRINCIPAL_PUBLIC_KEY_B64") ||
			!strings.Contains(text, "key: public_key_b64") {
			t.Fatalf("%s deployment must consume only the auth verifier public key", target.service)
		}
		for _, forbidden := range []string{
			"ENGINE_API_KEY",
			"TETRAL_AUTH_INTERNAL_PRINCIPAL_PRIVATE_KEY_B64",
			"auth-bootstrap",
			"api-bootstrap",
			"key: private_key_b64",
		} {
			if strings.Contains(text, forbidden) {
				t.Fatalf("%s deployment still carries raw auth/signing material %q", target.service, forbidden)
			}
		}
		if !strings.Contains(text, "ephemeral-storage:") {
			t.Fatalf("%s deployment must declare resource bounds including ephemeral-storage", target.service)
		}
	}
	apiSecretExample := finalArchitectureReadText(t, filepath.Join(apiRoot, "k8s", "secret.example.yaml"))
	for _, required := range []string{
		"name: api-secrets",
		"name: api-database",
		"public_key_b64",
	} {
		if !strings.Contains(apiSecretExample, required) {
			t.Fatalf("api secret.example.yaml missing %q", required)
		}
	}
	for _, forbidden := range []string{
		"engine-api-key:",
		"private_key_b64:",
	} {
		if strings.Contains(apiSecretExample, forbidden) {
			t.Fatalf("api secret.example.yaml contains forbidden auth owner material %q", forbidden)
		}
	}
}

func TestFinalArchitectureBridgeServiceLayoutAndProtocolSource(t *testing.T) {
	engineRoot := finalArchitectureEngineRoot(t)
	bridgeRoot := filepath.Join(engineRoot, "services", "bridge")
	for _, required := range []string{
		"buf.gen.yaml",
		filepath.Join("proto", "tetral", "bridge", "v1", "bridge.proto"),
		filepath.Join("gen", "tetral", "bridge", "v1", "bridge.pb.go"),
		filepath.Join("gen", "tetral", "bridge", "v1", "bridge_grpc.pb.go"),
		filepath.Join("cmd", "bridge-api", "main.go"),
		filepath.Join("cmd", "job-runner", "main.go"),
		filepath.Join("k8s", "configmap.yaml"),
		filepath.Join("k8s", "deployment.yaml"),
		filepath.Join("k8s", "networkpolicy.yaml"),
		"api.go",
		"job_runner.go",
	} {
		if _, err := os.Stat(filepath.Join(bridgeRoot, required)); err != nil {
			t.Fatalf("Bridge service package is missing %s: %v", required, err)
		}
	}
	protoBody, err := os.ReadFile(filepath.Join(bridgeRoot, "proto", "tetral", "bridge", "v1", "bridge.proto")) //nolint:gosec // repository-local static test path.
	if err != nil {
		t.Fatalf("read Bridge proto: %v", err)
	}
	for _, required := range []string{
		"message AcceptSandboxExecutionRequest",
		"string tool_use_event_id = 2;",
		"message SandboxExecutionCommitted {}",
		"message ClaimMcpToolResultRequest",
		"string claim_id = 3;",
		"message McpToolClaimAcquired",
		"message CommitMcpToolResultRequest",
		"message RunMemoryRequest",
		"message MemoryRunCommitted",
		"string operation_id = 6;",
	} {
		if !strings.Contains(string(protoBody), required) {
			t.Fatalf("Bridge tool protocols must expose narrow durable operation contract %q", required)
		}
	}
	apiBody, err := os.ReadFile(filepath.Join(bridgeRoot, "api.go")) //nolint:gosec // repository-local static test path.
	if err != nil {
		t.Fatalf("read Bridge api.go: %v", err)
	}
	for _, forbidden := range []string{
		"codes.Unimplemented",
		"bridgeAPIUnimplemented",
		"method is not implemented",
	} {
		if strings.Contains(string(apiBody), forbidden) {
			t.Fatalf("Bridge API server must not preserve unimplemented RPC fallback %q", forbidden)
		}
	}
	assertProtoSourcesAreServiceLocalContracts(t, engineRoot)
}

func TestFinalArchitectureTypeScriptProtocolGenerationIsServiceLocal(t *testing.T) {
	engineRoot := finalArchitectureEngineRoot(t)
	body, err := os.ReadFile(filepath.Join(engineRoot, "services", "agent-runtime", "buf.gen.yaml")) //nolint:gosec // repository-local static test path.
	if err != nil {
		t.Fatalf("read Runtime Pod buf.gen.yaml: %v", err)
	}
	text := string(body)
	for _, required := range []string{
		"clean: true",
		"directory: services/agent-runtime/proto",
		"out: services/agent-runtime/gen",
		"out: services/agent-runtime/packages/protocol/src/gen",
		"forceLong=number",
	} {
		if !strings.Contains(text, required) {
			t.Fatalf("Runtime Pod service-local buf.gen.yaml missing %q", required)
		}
	}

	gatewayBody, err := os.ReadFile(filepath.Join(engineRoot, "services", "gateway", "buf.gen.yaml")) //nolint:gosec // repository-local static test path.
	if err != nil {
		t.Fatalf("read Gateway buf.gen.yaml: %v", err)
	}
	gatewayText := string(gatewayBody)
	for _, required := range []string{
		"clean: true",
		"directory: services/gateway/proto",
		"out: services/gateway/gen",
		"out: services/gateway/packages/protocol/src/gen",
		"forceLong=number",
	} {
		if !strings.Contains(gatewayText, required) {
			t.Fatalf("Gateway service-local buf.gen.yaml missing %q", required)
		}
	}
	bridgeBody, err := os.ReadFile(filepath.Join(engineRoot, "services", "bridge", "buf.gen.yaml")) //nolint:gosec // repository-local static test path.
	if err != nil {
		t.Fatalf("read Bridge buf.gen.yaml: %v", err)
	}
	bridgeText := string(bridgeBody)
	for _, required := range []string{
		"clean: true",
		"directory: services/bridge/proto",
		"out: services/bridge/gen",
		"out: services/agent-runtime/packages/protocol/src/gen-bridge",
		"out: services/gateway/packages/protocol/src/gen-bridge",
	} {
		if !strings.Contains(bridgeText, required) {
			t.Fatalf("Bridge service-local buf.gen.yaml missing %q", required)
		}
	}
}

func TestProductionCodeDoesNotUseStandardLibraryPrintLogging(t *testing.T) {
	engineRoot := finalArchitectureEngineRoot(t)
	forbidden := []string{"log.Print(", "log.Printf(", "log.Println(", "log.Fatalf("}
	if err := filepath.WalkDir(engineRoot, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			if entry.Name() == "vendor" || entry.Name() == "node_modules" {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		body, err := os.ReadFile(path) //nolint:gosec // G304: path comes from repository-local WalkDir in a static test.
		if err != nil {
			return err
		}
		source := string(body)
		for _, token := range forbidden {
			if strings.Contains(source, token) {
				t.Fatalf("%s contains forbidden production logging token %q", path, token)
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestProductionCommandsDoNotEmitPlainStartupStderr(t *testing.T) {
	engineRoot := finalArchitectureEngineRoot(t)
	forbidden := []string{
		"fmt.Fprint(os.Stderr",
		"fmt.Fprintf(os.Stderr",
		"fmt.Fprintln(os.Stderr",
		"StartupFailureStderr(",
	}
	if err := filepath.WalkDir(engineRoot, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			if entry.Name() == "vendor" || entry.Name() == "node_modules" {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		rel := finalArchitectureRel(t, engineRoot, path)
		if !strings.Contains(rel, "/cmd/") && !strings.HasPrefix(rel, "internal/workload/") {
			return nil
		}
		body, err := os.ReadFile(path) //nolint:gosec // G304: path comes from repository-local WalkDir in a static test.
		if err != nil {
			return err
		}
		source := string(body)
		for _, token := range forbidden {
			if strings.Contains(source, token) {
				t.Fatalf("%s contains forbidden plain startup stderr token %q", rel, token)
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func finalArchitectureEngineRoot(t *testing.T) string {
	t.Helper()
	return filepath.Clean("../..")
}

func finalArchitectureRel(t *testing.T, root string, path string) string {
	t.Helper()
	rel, err := filepath.Rel(root, path)
	if err != nil {
		t.Fatalf("rel %s: %v", path, err)
	}
	return filepath.ToSlash(rel)
}

func finalArchitectureReadText(t *testing.T, path string) string {
	t.Helper()
	body, err := os.ReadFile(path) //nolint:gosec // repository-local static test path.
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return string(body)
}

func finalArchitectureSkipDir(entry os.DirEntry) error {
	switch entry.Name() {
	case ".git", ".wrangler", "dist", "node_modules", "vendor":
		return filepath.SkipDir
	default:
		return nil
	}
}

func finalArchitectureIsGRPCOrProtobufImport(importPath string) bool {
	return strings.HasPrefix(importPath, "google.golang.org/grpc") ||
		strings.HasPrefix(importPath, "google.golang.org/protobuf")
}

func finalArchitectureAllowsGRPCOrProtobuf(rel string) bool {
	for _, prefix := range []string{
		"internal/gen/",
		"internal/internalgrpc/",
	} {
		if strings.HasPrefix(rel, prefix) {
			return true
		}
	}
	return strings.HasPrefix(rel, "services/")
}

func TestFinalArchitectureObsoleteCentralRootsAreAbsent(t *testing.T) {
	engineRoot := finalArchitectureEngineRoot(t)
	for _, deleted := range []string{
		"cmd",
		filepath.Join("proto", "tetral"),
		filepath.Join("internal", "gen", "tetral"),
	} {
		if _, err := os.Stat(filepath.Join(engineRoot, deleted)); err == nil {
			t.Fatalf("obsolete central architecture root still exists: %s", deleted)
		} else if !os.IsNotExist(err) {
			t.Fatalf("stat obsolete central architecture root %s: %v", deleted, err)
		}
	}
}

func assertNoGoImportsAgentRuntimeService(t *testing.T, engineRoot string) {
	t.Helper()
	var violations []string
	err := filepath.WalkDir(engineRoot, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			return finalArchitectureSkipDir(entry)
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		rel := finalArchitectureRel(t, engineRoot, path)
		if strings.HasPrefix(rel, "integration/static/") {
			return nil
		}
		if strings.HasPrefix(rel, "services/agent-runtime/gen/") {
			return nil
		}
		file, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.ImportsOnly)
		if err != nil {
			return err
		}
		for _, imported := range file.Imports {
			value := strings.Trim(imported.Path.Value, `"`)
			if strings.HasPrefix(value, "github.com/tetral-ai/tetral/services/agent-runtime") &&
				!strings.HasPrefix(value, "github.com/tetral-ai/tetral/services/agent-runtime/gen/") {
				violations = append(violations, rel+" imports "+value)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("scan Go imports for Agent Runtime service boundary: %v", err)
	}
	if len(violations) > 0 {
		t.Fatalf("Go code imports Agent Runtime TypeScript service boundary:\n%s", strings.Join(violations, "\n"))
	}
}

func assertNoTypeScriptImportsEngineInternal(t *testing.T, agentRuntimeRoot string) {
	t.Helper()
	var violations []string
	err := filepath.WalkDir(agentRuntimeRoot, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			switch entry.Name() {
			case "dist", "node_modules":
				return filepath.SkipDir
			default:
				return nil
			}
		}
		if !strings.HasSuffix(path, ".ts") {
			return nil
		}
		body, err := os.ReadFile(path) //nolint:gosec // repository-local static test path.
		if err != nil {
			return err
		}
		if strings.Contains(string(body), "engine/internal") || strings.Contains(string(body), "../../internal") || strings.Contains(string(body), "../internal") {
			rel, relErr := filepath.Rel(agentRuntimeRoot, path)
			if relErr != nil {
				return relErr
			}
			violations = append(violations, filepath.ToSlash(rel))
		}
		return nil
	})
	if err != nil {
		t.Fatalf("scan TypeScript imports for engine/internal: %v", err)
	}
	if len(violations) > 0 {
		t.Fatalf("TypeScript code imports engine/internal:\n%s", strings.Join(violations, "\n"))
	}
}

func assertProtoSourcesAreServiceLocalContracts(t *testing.T, engineRoot string) {
	t.Helper()
	var violations []string
	err := filepath.WalkDir(engineRoot, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			return finalArchitectureSkipDir(entry)
		}
		if filepath.Ext(path) != ".proto" {
			return nil
		}
		rel := finalArchitectureRel(t, engineRoot, path)
		if !finalArchitectureAllowsProtoSource(rel) {
			violations = append(violations, rel)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("scan proto sources: %v", err)
	}
	if len(violations) > 0 {
		t.Fatalf("proto source exists outside service-local contract roots:\n%s", strings.Join(violations, "\n"))
	}
}

func finalArchitectureAllowsProtoSource(rel string) bool {
	if !strings.HasPrefix(rel, "services/") {
		return false
	}
	parts := strings.Split(rel, "/")
	return len(parts) >= 4 && parts[2] == "proto"
}

func scanGeneratedPackageImports(t *testing.T, engineRoot string, packageDir string, forbidden []string) {
	t.Helper()
	root := filepath.Join(engineRoot, packageDir)
	if _, err := os.Stat(root); err != nil {
		if os.IsNotExist(err) {
			return
		}
		t.Fatalf("stat generated package root %s: %v", packageDir, err)
	}
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		file, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.ImportsOnly)
		if err != nil {
			return err
		}
		for _, imported := range file.Imports {
			value := strings.Trim(imported.Path.Value, `"`)
			for _, forbiddenImport := range forbidden {
				if value == forbiddenImport {
					t.Fatalf("%s imports public/control-plane package %s", finalArchitectureRel(t, engineRoot, path), value)
				}
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("scan generated imports under %s: %v", packageDir, err)
	}
}
