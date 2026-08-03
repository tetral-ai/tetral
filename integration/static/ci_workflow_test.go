package static

import (
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

const (
	frozenForkSDKCommit     = "a8f5b9262e0d42fb571cb2ea8f902124bbb4f01d"
	supersededForkSDKCommit = "52103cbc6f820984f0f2f5cce82dfefb344c0c4a"
)

func TestEngineCIForkSDKCheckoutsPinFrozenCommit(t *testing.T) {
	repositoryRoot := finalArchitectureEngineRoot(t)
	body, err := os.ReadFile(filepath.Join(repositoryRoot, ".github", "workflows", "engine-ci.yml")) //nolint:gosec // repository-local static test path.
	if err != nil {
		t.Fatalf("read engine-ci workflow: %v", err)
	}
	text := string(body)
	tuple := regexp.MustCompile(`(?m)^      - name: Checkout fork SDK\n        # actions/checkout@v6\n        uses: actions/checkout@[0-9a-f]{40}\n        with:\n          repository: tetral-ai/tetral-sdk-typescript\n          ref: ` + frozenForkSDKCommit + `\n          path: tetral-sdk-typescript\n          persist-credentials: false$`)
	if got := len(tuple.FindAllString(text, -1)); got != 2 {
		t.Fatalf("engine-ci exact frozen fork SDK checkout tuples = %d; want 2", got)
	}
	if got := strings.Count(text, "ref: "+frozenForkSDKCommit); got != 2 {
		t.Fatalf("engine-ci frozen fork SDK ref count = %d; want 2", got)
	}
	if got := strings.Count(text, supersededForkSDKCommit); got != 0 {
		t.Fatalf("engine-ci superseded fork SDK ref count = %d; want 0", got)
	}
	for _, credentialField := range []string{"token:", "ssh-key:"} {
		if regexp.MustCompile(`(?m)^          ` + regexp.QuoteMeta(credentialField)).MatchString(text) {
			t.Fatalf("engine-ci checkout blocks must not carry %s credentials", strings.TrimSuffix(credentialField, ":"))
		}
	}
}

func TestFinalArchitectureEngineCIProtectsAgentRuntimeAndProtoGeneration(t *testing.T) {
	repositoryRoot := finalArchitectureEngineRoot(t)
	body, err := os.ReadFile(filepath.Join(repositoryRoot, ".github", "workflows", "engine-ci.yml")) //nolint:gosec // repository-local static test path.
	if err != nil {
		t.Fatalf("read engine-ci workflow: %v", err)
	}
	text := string(body)
	requiredGuard := "github.event_name != 'pull_request' || github.event.pull_request.head.repo.full_name == github.repository"
	if !strings.Contains(text, "permissions:\n  contents: read") {
		t.Fatal("engine-ci must run with read-only contents permission")
	}
	if got := strings.Count(text, "permissions:"); got != 1 {
		t.Fatalf("engine-ci permissions block count = %d; want one workflow-level block", got)
	}
	if !strings.Contains(text, "concurrency:\n  group: engine-ci-${{ github.event.pull_request.head.repo.full_name || github.repository }}-${{ github.event.pull_request.number || github.ref }}\n  cancel-in-progress: true") {
		t.Fatal("engine-ci must set workflow concurrency to prevent duplicate branch runs")
	}
	// Engine CI schedules every job on a GitHub-hosted runner. A public
	// repository cannot offer contributors a choice between executing their code
	// on a maintainer's machine and getting no checks at all.
	engineCIJobs := []string{
		"go-static:", "go-test:", "protocol:", "agent-runtime-ts:", "gateway-ts:",
		"k8s-manifests:", "helm-chart:", "security:", "helper-privilege:", "external-smoke:",
	}
	for _, jobName := range engineCIJobs {
		if !strings.Contains(workflowJobForStaticTest(t, text, jobName), "runs-on: ubuntu-latest") {
			t.Fatalf("engine-ci job %q must run on a GitHub-hosted runner", strings.TrimSuffix(jobName, ":"))
		}
	}
	// external-smoke reads every secret this workflow touches, so it gates on the
	// fork guard.
	if !strings.Contains(workflowJobForStaticTest(t, text, "external-smoke:"), requiredGuard) {
		t.Fatal("engine-ci external-smoke job must reject fork pull_request execution")
	}
	// A hosted runner starts empty, so each Go toolchain keys its cache on the
	// repository's module file.
	if got := strings.Count(text, "cache-dependency-path: go.sum"); got != 6 {
		t.Fatalf("engine-ci setup-go cache-dependency-path count = %d; want 6", got)
	}
	if strings.Count(text, "persist-credentials: false") != 12 {
		t.Fatal("each engine-ci checkout must disable credential persistence")
	}
	for fragment, want := range map[string]int{
		"repository: tetral-ai/tetral-sdk-typescript":                           2,
		"ref: " + frozenForkSDKCommit:                                           2,
		"path: tetral-sdk-typescript":                                           2,
		"name: Checkout fork SDK":                                               2,
		"TETRAL_ENGINE_SDK_ROOT: ${{ github.workspace }}/tetral-sdk-typescript": 2,
		"name: Fork SDK drift gates":                                            1,
	} {
		if got := strings.Count(text, fragment); got != want {
			t.Fatalf("engine-ci fork SDK fragment %q count = %d; want %d", fragment, got, want)
		}
	}
	requireWorkflowActionsUseFullSHAs(t, "engine-ci.yml", text)
	for _, jobName := range []string{
		"go-static:",
		"go-test:",
		"protocol:",
		"agent-runtime-ts:",
		"gateway-ts:",
		"k8s-manifests:",
		"helm-chart:",
		"security:",
		"external-smoke:",
	} {
		if !strings.Contains(text, "\n  "+jobName) {
			t.Fatalf("engine-ci must expose required gate job %q", strings.TrimSuffix(jobName, ":"))
		}
	}
	protocolJob := workflowJobForStaticTest(t, text, "protocol:")
	if !strings.Contains(protocolJob, "working-directory: services/agent-runtime") ||
		!strings.Contains(protocolJob, "working-directory: services/gateway") ||
		!strings.Contains(protocolJob, "Test.*BufGenerationFreshness") {
		t.Fatal("engine-ci protocol job must own both generated TS surfaces and run buf generation freshness")
	}
	goStaticJob := workflowJobForStaticTest(t, text, "go-static:")
	for _, fragment := range []string{
		"name: Checkout fork SDK",
		"name: Fork SDK drift gates",
		"TETRAL_ENGINE_SDK_ROOT: ${{ github.workspace }}/tetral-sdk-typescript",
	} {
		if !strings.Contains(goStaticJob, fragment) {
			t.Fatalf("engine-ci go-static job missing fork SDK drift-gate fragment %q", fragment)
		}
	}
	goTestJob := workflowJobForStaticTest(t, text, "go-test:")
	for _, fragment := range []string{
		"name: Checkout fork SDK",
		"name: Setup Bun\n        # oven-sh/setup-bun@v2\n        uses: oven-sh/setup-bun@0c5077e51419868618aeaa5fe8019c62421857d6",
		"TETRAL_ENGINE_SDK_ROOT: ${{ github.workspace }}/tetral-sdk-typescript",
	} {
		if !strings.Contains(goTestJob, fragment) {
			t.Fatalf("engine-ci go-test job missing fork SDK drift-gate fragment %q", fragment)
		}
	}
	agentRuntimeJob := workflowJobForStaticTest(t, text, "agent-runtime-ts:")
	if !strings.Contains(agentRuntimeJob, "working-directory: services/agent-runtime") {
		t.Fatal("engine-ci agent-runtime-ts job must own the Runtime Pod suite")
	}
	for _, command := range []string{
		"run: bun run typecheck",
		"name: Runtime Pod unit tests with coverage\n        run: bun run test",
		"run: bun run test:integration",
		"run: bun run build",
	} {
		if !strings.Contains(agentRuntimeJob, command) {
			t.Fatalf("engine-ci agent-runtime-ts job must own Runtime Pod TS gate command %q", command)
		}
	}
	gatewayJob := workflowJobForStaticTest(t, text, "gateway-ts:")
	if !strings.Contains(gatewayJob, "working-directory: services/gateway") {
		t.Fatal("engine-ci gateway-ts job must own the Gateway suite")
	}
	for _, fragment := range []string{
		"name: Gateway Service typecheck\n        run: bun run typecheck",
		"name: Gateway Service tests with coverage\n        run: bun run test",
		"name: Gateway Service build\n        run: bun run build",
	} {
		if !strings.Contains(gatewayJob, fragment) {
			t.Fatalf("engine-ci gateway-ts job must own Gateway protocol shell gate:\n%s", fragment)
		}
	}
	if !strings.Contains(protocolJob, "TETRAL_RUN_GO_BUN_GRPC_INTEROP: \"1\"") ||
		!strings.Contains(protocolJob, "run: go test -race -count=1 ./integration/grpcinterop") {
		t.Fatal("engine-ci protocol job must run the opt-in Go-to-Bun gRPC interop harness after Bun is installed")
	}
	securityJob := workflowJobForStaticTest(t, text, "security:")
	for _, fragment := range []string{
		"name: Agent Runtime dependency audit",
		"working-directory: services/agent-runtime\n        run: bun audit",
		"name: Gateway Service dependency audit",
		"working-directory: services/gateway\n        run: bun audit",
		"name: Secret and redaction static guards",
	} {
		if !strings.Contains(securityJob, fragment) {
			t.Fatalf("engine-ci security job missing fragment:\n%s", fragment)
		}
	}
	externalSmokeJob := workflowJobForStaticTest(t, text, "external-smoke:")
	if !strings.Contains(externalSmokeJob, "github.event_name == 'workflow_dispatch'") ||
		!strings.Contains(externalSmokeJob, "github.event_name == 'schedule'") ||
		!strings.Contains(externalSmokeJob, "contains(github.event.pull_request.labels.*.name, 'external-smoke')") {
		t.Fatal("engine-ci external-smoke job must be manual, scheduled, or label-triggered only")
	}
	for _, jobName := range []string{"go-static:", "go-test:", "protocol:", "agent-runtime-ts:", "gateway-ts:", "k8s-manifests:", "security:"} {
		if strings.Contains(workflowJobForStaticTest(t, text, jobName), "${{ secrets.") {
			t.Fatalf("default engine-ci job %q must not consume production secrets", strings.TrimSuffix(jobName, ":"))
		}
	}
}

func requireWorkflowActionsUseFullSHAs(t *testing.T, name string, text string) {
	t.Helper()
	actionRef := regexp.MustCompile(`^\s*(?:-\s*)?uses:\s*([^@\s#]+)(?:@([^\s#]+))?`)
	shaRef := regexp.MustCompile(`^[0-9a-fA-F]{40}$`)
	for index, line := range strings.Split(text, "\n") {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" || strings.HasPrefix(trimmed, "#") {
			continue
		}
		matches := actionRef.FindStringSubmatch(line)
		if matches == nil {
			continue
		}
		action := matches[1]
		ref := matches[2]
		if strings.HasPrefix(action, "./") || strings.HasPrefix(action, "../") {
			continue
		}
		if ref == "" {
			t.Fatalf("%s:%d action %q must pin an explicit ref", name, index+1, action)
		}
		if !shaRef.MatchString(ref) {
			t.Fatalf("%s:%d action %q ref %q must be a full commit SHA", name, index+1, action, ref)
		}
	}
}

// TestEngineCIWorkflowRunsFoundationSuites pins the Engine CI gate topology.
// The Engine go-test job must:
//   - run a real PostgreSQL service;
//   - expose TETRAL_TEST_DATABASE_URL to the test step;
//   - handle PostgreSQL readiness before the go test command;
//   - run the full Engine test suite, including PostgreSQL-backed integration
//     tests in the normal test gate.
//
// This test scans the workflow file at the repository root rather than
// parsing YAML structurally — the assertions below are tight enough to
// catch the CI posture requirements without pulling in a YAML dependency
// for a single CI proof.
func TestEngineCIWorkflowRunsFoundationSuites(t *testing.T) {
	repoRoot := finalArchitectureEngineRoot(t)
	workflowPath := filepath.Join(repoRoot, ".github", "workflows", "engine-ci.yml")

	content, err := os.ReadFile(workflowPath) //nolint:gosec // test-only static read of repo workflow
	if err != nil {
		t.Fatalf("read %s: %v", workflowPath, err)
	}
	text := string(content)
	requiredJobs := []string{
		"go-static:",
		"go-test:",
		"protocol:",
		"agent-runtime-ts:",
		"gateway-ts:",
		"k8s-manifests:",
		"security:",
		"helper-privilege:",
		"external-smoke:",
	}
	for _, jobName := range requiredJobs {
		t.Run("job_"+strings.TrimSuffix(jobName, ":"), func(t *testing.T) {
			if !strings.Contains(text, "\n  "+jobName) {
				t.Fatalf("engine-ci.yml must contain required gate job %q", strings.TrimSuffix(jobName, ":"))
			}
		})
	}
	goTestJob := workflowJobForStaticTest(t, text, "go-test:")

	// Required tokens, each tied to a specific CI requirement.
	required := []struct {
		token string
		why   string
	}{
		{"ghcr.io/tetral-ai/mirror/postgres:18-alpine", "the test job requires the Tetral-owned PostgreSQL mirror"},
		{"ghcr.io/tetral-ai/mirror/minio:RELEASE.2025-09-07T16-13-09Z", "the test job requires the Tetral-owned MinIO mirror"},
		{"TETRAL_TEST_DATABASE_URL", "the test step must export the PostgreSQL test DSN to Engine tests"},
		{"go list ./...", "shards must derive their package set from the module, so no package can be silently dropped"},
		{"go test -race -coverprofile=coverage.out $packages", "the shard test command must keep the race detector and coverage"},
		{"pg_isready", "an explicit readiness probe must run before go test so go test is not itself the readiness probe"},
	}
	for _, r := range required {
		t.Run(strings.ReplaceAll(r.token, " ", "_"), func(t *testing.T) {
			if !strings.Contains(goTestJob, r.token) {
				t.Errorf("engine-ci.yml must contain %q (%s)", r.token, r.why)
			}
		})
	}
	goStaticJob := workflowJobForStaticTest(t, text, "go-static:")
	for _, token := range []string{
		"run: test -z \"$(gofmt -l .)\"",
		"run: go vet ./...",
		"name: golangci-lint",
		"--config .golangci.yml",
		"./...",
		"name: Architecture static guards",
		"name: SDK compatibility traceability gate",
		"run: ./scripts/check-sdk-compatibility-traceability.sh",
	} {
		t.Run("go_static_"+strings.ReplaceAll(token, " ", "_"), func(t *testing.T) {
			if !strings.Contains(goStaticJob, token) {
				t.Fatalf("go-static gate missing %q; job was:\n%s", token, goStaticJob)
			}
		})
	}
	k8sJob := workflowJobForStaticTest(t, text, "k8s-manifests:")
	for _, token := range []string{
		"./integration/static",
		"./deploy/kubernetes",
	} {
		t.Run("k8s_manifests_"+strings.ReplaceAll(token, "/", "_"), func(t *testing.T) {
			if !strings.Contains(k8sJob, token) {
				t.Fatalf("k8s-manifests gate missing %q; job was:\n%s", token, k8sJob)
			}
		})
	}
	helmJob := workflowJobForStaticTest(t, text, "helm-chart:")
	for _, token := range []string{
		"version: v4.2.0",
		"helm lint deploy/helm/tetral",
		"go test -count=1 ./deploy/helm/...",
	} {
		t.Run("helm_chart_"+strings.ReplaceAll(token, " ", "_"), func(t *testing.T) {
			if !strings.Contains(helmJob, token) {
				t.Fatalf("helm-chart gate missing %q; job was:\n%s", token, helmJob)
			}
		})
	}

	if strings.Contains(goTestJob, "continue-on-error: true") {
		t.Fatal("the PostgreSQL-backed full-suite gate must not continue on error")
	}
	if got := strings.Count(goTestJob, "ghcr.io/tetral-ai/mirror/postgres:18-alpine"); got != 3 {
		t.Fatalf("go-test PostgreSQL mirror reference count = %d; want pull, error, and run references", got)
	}
	if got := strings.Count(goTestJob, "postgres:18-alpine"); got != 3 {
		t.Fatalf("go-test PostgreSQL image reference count = %d; want only the three GHCR mirror references", got)
	}
	for _, thirdParty := range []string{
		"public.ecr.aws/docker/library/postgres:18-alpine",
		"docker.io/library/postgres:18-alpine",
	} {
		if strings.Contains(goTestJob, thirdParty) {
			t.Fatalf("go-test must not pull PostgreSQL from shared third-party registry %q", thirdParty)
		}
	}
	if got := strings.Count(goTestJob, "ghcr.io/tetral-ai/mirror/minio:RELEASE.2025-09-07T16-13-09Z"); got != 3 {
		t.Fatalf("go-test MinIO mirror reference count = %d; want pull, error, and run references", got)
	}
	if got := strings.Count(goTestJob, "minio:RELEASE.2025-09-07T16-13-09Z"); got != 3 {
		t.Fatalf("go-test MinIO image reference count = %d; want only the three GHCR mirror references", got)
	}
	if strings.Contains(goTestJob, "quay.io/minio/minio:RELEASE.2025-09-07T16-13-09Z") {
		t.Fatal("go-test must not pull MinIO directly from quay.io")
	}
	for _, retryToken := range []string{
		"for attempt in 1 2 3 4 5; do",
		`if [ "$attempt" = 5 ]; then`,
		"sleep $((attempt * 5))",
	} {
		if !strings.Contains(goTestJob, retryToken) {
			t.Fatalf("go-test must preserve the bounded PostgreSQL pull retry lifecycle; missing %q", retryToken)
		}
	}
	if got := strings.Count(goTestJob, "for attempt in 1 2 3 4 5; do"); got != 2 {
		t.Fatalf("go-test bounded image-pull retry loops = %d; want PostgreSQL and MinIO", got)
	}
	if regexp.MustCompile(`(?m)(docker\s+login\b|uses:\s*docker/login-action@)`).MatchString(goTestJob) {
		t.Fatal("go-test must use anonymous pulls for the public GHCR mirror")
	}

	// Ordering proof: the readiness probe must run before the test
	// step, otherwise CI may pass through go test running before the
	// PostgreSQL container accepts connections.
	t.Run("readiness_before_go_test", func(t *testing.T) {
		// Anchor on the LAST occurrence of pg_isready: the workflow may
		// also mention it inside a comment or in the service
		// --health-cmd. The explicit standalone readiness step appears
		// last and is the load-bearing step.
		readinessIdx := strings.LastIndex(goTestJob, "pg_isready")
		testCmdIdx := strings.Index(goTestJob, "go test -race -coverprofile=coverage.out $packages")
		if readinessIdx < 0 || testCmdIdx < 0 {
			t.Fatal("readiness-order proof is missing its probe or sharded go-test command")
		}
		if readinessIdx > testCmdIdx {
			t.Errorf("pg_isready readiness probe must appear before the go test step in engine-ci.yml")
		}
	})

	t.Run("golangci_lint_covers_all_engine_packages", func(t *testing.T) {
		lintStep := workflowStepForStaticTest(t, goStaticJob, "- name: golangci-lint")
		token := "./..."
		if !strings.Contains(lintStep, token) {
			t.Fatalf("golangci-lint step must use %q; step was:\n%s", token, lintStep)
		}
	})
}

func TestEngineReleasePublishesChartOnlyAfterAllImages(t *testing.T) {
	repoRoot := finalArchitectureEngineRoot(t)
	workflowPath := filepath.Join(repoRoot, ".github", "workflows", "engine-release.yml")
	content, err := os.ReadFile(workflowPath) //nolint:gosec // test-only static read of repo workflow
	if err != nil {
		t.Fatalf("read %s: %v", workflowPath, err)
	}
	text := string(content)
	versionJob := workflowJobForStaticTest(t, text, "version:")
	imagesJob := workflowJobForStaticTest(t, text, "images:")
	chartJob := workflowJobForStaticTest(t, text, "chart:")

	for _, token := range []string{
		"outputs:",
		"version: ${{ steps.resolve.outputs.version }}",
		"SemVer 2",
		"without build metadata",
		"${#version} > 128",
		"OCI image tags are limited to 128 characters",
	} {
		if !strings.Contains(versionJob, token) {
			t.Errorf("release version job missing %q", token)
		}
	}
	for _, token := range []string{
		"needs: version",
		"${{ needs.version.outputs.version }}",
	} {
		if !strings.Contains(imagesJob, token) {
			t.Errorf("release image job missing %q", token)
		}
	}
	for _, token := range []string{
		"needs: [version, images]",
		"version: v4.2.0",
		`helm package deploy/helm/tetral --version "$VERSION" --app-version "$VERSION"`,
		"helm push",
		"oci://ghcr.io/tetral-ai/charts",
	} {
		if !strings.Contains(chartJob, token) {
			t.Errorf("release chart job missing %q", token)
		}
	}
}

func TestEngineCIWorkflowIsolatesRootHelperPrivilegeGate(t *testing.T) {
	engineRoot := finalArchitectureEngineRoot(t)
	workflowPath := filepath.Join(engineRoot, ".github", "workflows", "engine-ci.yml")
	body, err := os.ReadFile(workflowPath) //nolint:gosec // repository-local workflow.
	if err != nil {
		t.Fatalf("read workflow: %v", err)
	}
	workflow := string(body)
	if count := strings.Count(workflow, "\n  helper-privilege:"); count != 1 {
		t.Fatalf("helper-privilege job count = %d; want exactly one", count)
	}
	job := workflowJobForStaticTest(t, workflow, "helper-privilege:")
	for _, token := range []string{
		"runs-on: ubuntu-latest",
		"persist-credentials: false",
		"run: ./scripts/run-helper-privilege-ci.sh",
	} {
		if !strings.Contains(job, token) {
			t.Fatalf("helper-privilege job missing %q; job was:\n%s", token, job)
		}
	}
	for _, forbidden := range []string{"go test ./...", "make test", "bun test", "TETRAL_TEST_DATABASE_URL"} {
		if strings.Contains(job, forbidden) {
			t.Fatalf("helper-privilege job widens root execution with %q", forbidden)
		}
	}
	if regexp.MustCompile(`(?m)(docker\s+login\b|uses:\s*docker/login-action@)`).MatchString(job) {
		t.Fatal("helper-privilege job must use an anonymous pull for the public GHCR mirror")
	}

	scriptPath := filepath.Join(engineRoot, "scripts", "run-helper-privilege-ci.sh")
	scriptBody, err := os.ReadFile(scriptPath) //nolint:gosec // repository-local CI command.
	if err != nil {
		t.Fatalf("read helper privilege script: %v", err)
	}
	script := string(scriptBody)
	for _, token := range []string{
		"docker run --rm",
		"--user 0:0",
		"ghcr.io/tetral-ai/mirror/golang:1.25.12",
		`test "$(id -u)" -eq 0`,
		"go test ./internal/sandbox/helper",
		"TestSupervisorKeepsDetachedTaskAuthorizationAfterPrivilegeDrop",
		"--- SKIP:",
	} {
		if !strings.Contains(script, token) {
			t.Fatalf("helper privilege script missing %q", token)
		}
	}
	if strings.Contains(script, "go test ./...") {
		t.Fatal("helper privilege script must not run the full suite as root")
	}
	for _, thirdParty := range []string{
		"public.ecr.aws/docker/library/golang:1.25.12",
		"docker.io/library/golang:1.25.12",
	} {
		if strings.Contains(script, thirdParty) {
			t.Fatalf("helper privilege script must not pull Go from shared third-party registry %q", thirdParty)
		}
	}
	if got := strings.Count(script, "golang:1.25.12"); got != 1 {
		t.Fatalf("helper privilege Go image reference count = %d; want only the GHCR mirror reference", got)
	}
	if regexp.MustCompile(`(?m)docker\s+login\b`).MatchString(script) {
		t.Fatal("helper privilege script must use an anonymous pull for the public GHCR mirror")
	}
}

func TestHelperPrivilegeCIGuardFailsOnSkip(t *testing.T) {
	script := filepath.Join(finalArchitectureEngineRoot(t), "scripts", "run-helper-privilege-ci.sh")
	command := exec.Command("bash", script, "--verify-output")
	command.Stdin = bytes.NewBufferString("--- SKIP: TestSupervisorKeepsDetachedTaskAuthorizationAfterPrivilegeDrop (0.00s)\n")
	if err := command.Run(); err == nil {
		t.Fatal("helper privilege output guard accepted a skipped proof")
	}

	command = exec.Command("bash", script, "--verify-output")
	command.Stdin = bytes.NewBufferString("--- PASS: TestSupervisorKeepsDetachedTaskAuthorizationAfterPrivilegeDrop (0.01s)\n")
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("helper privilege output guard rejected a passing proof: %v\n%s", err, output)
	}
}

func TestBaseImageMirrorWorkflowOwnsManualGHCRCopies(t *testing.T) {
	repoRoot := finalArchitectureEngineRoot(t)
	workflowPath := filepath.Join(repoRoot, ".github", "workflows", "mirror-base-images.yml")
	body, err := os.ReadFile(workflowPath) //nolint:gosec // Repository-local workflow definition.
	if err != nil {
		t.Fatalf("read base-image mirror workflow: %v", err)
	}
	workflow := string(body)
	const workflowPreamble = `name: mirror-base-images

on:
  workflow_dispatch:

permissions:
  contents: read
  packages: write

`
	if !strings.HasPrefix(workflow, workflowPreamble) {
		t.Fatalf("base-image mirror workflow must be dispatch-only with workflow-level read/package-write permissions")
	}
	const loginBlock = `      - name: Log in to GitHub Container Registry
        # docker/login-action@v4.5.1
        uses: docker/login-action@abd2ef45e78c5afb21d64d4ca52ee8550d9572c7
        with:
          registry: ghcr.io
          username: ${{ github.actor }}
          password: ${{ secrets.GITHUB_TOKEN }}
`
	if !strings.Contains(workflow, loginBlock) {
		t.Fatal("base-image mirror workflow must authenticate its GHCR push with the workflow GITHUB_TOKEN")
	}
	const matrixEnvironment = `        env:
          PRIMARY: ${{ matrix.primary }}
          FALLBACK: ${{ matrix.fallback }}
          TARGET: ${{ matrix.target }}
`
	if !strings.Contains(workflow, matrixEnvironment) {
		t.Fatal("base-image mirror workflow must wire each matrix source tuple into the matching shell variables")
	}
	for _, matrixEntry := range []string{
		`          - primary: public.ecr.aws/docker/library/golang:1.25.12
            fallback: docker.io/library/golang:1.25.12
            target: ghcr.io/tetral-ai/mirror/golang:1.25.12`,
		`          - primary: public.ecr.aws/docker/library/postgres:18-alpine
            fallback: docker.io/library/postgres:18-alpine
            target: ghcr.io/tetral-ai/mirror/postgres:18-alpine`,
		`          - primary: quay.io/minio/minio:RELEASE.2025-09-07T16-13-09Z
            fallback: ""
            target: ghcr.io/tetral-ai/mirror/minio:RELEASE.2025-09-07T16-13-09Z`,
	} {
		if !strings.Contains(workflow, matrixEntry) {
			t.Fatalf("base-image mirror workflow has a missing or mismatched source tuple:\n%s", matrixEntry)
		}
	}
	const fallbackFlow = `          if docker pull "$PRIMARY"; then
            selected_source="$PRIMARY"
          elif [ -n "$FALLBACK" ]; then
            echo "primary mirror unavailable; falling back to Docker Hub"
            docker pull "$FALLBACK"
            selected_source="$FALLBACK"
          else
            echo "primary image unavailable and no fallback is configured" >&2
            exit 1
          fi
`
	if !strings.Contains(workflow, fallbackFlow) {
		t.Fatal("base-image mirror workflow must use a configured fallback and reject a failed source when no fallback exists")
	}
	for _, digestProof := range []string{
		`push_output="$(docker push "$TARGET" 2>&1)"`,
		`digest="$(sed -n 's/.*digest: \(sha256:[0-9a-f]\{64\}\).*/\1/p' <<<"$push_output" | tail -n 1)"`,
	} {
		if !strings.Contains(workflow, digestProof) {
			t.Fatalf("base-image mirror workflow must verify and report the pushed digest; missing %q", digestProof)
		}
	}
	const emptyDigestFailure = `          if [ -z "$digest" ]; then
            echo "mirror push did not report an image digest" >&2
            exit 1
          fi
`
	if !strings.Contains(workflow, emptyDigestFailure) {
		t.Fatal("base-image mirror workflow must reject a push that does not report a digest")
	}
	const summaryBlock = "          {\n" +
		"            echo \"### $TARGET\"\n" +
		"            echo\n" +
		"            echo '```'\n" +
		"            echo \"$TARGET\"\n" +
		"            echo \"$TARGET@$digest\"\n" +
		"            echo '```'\n" +
		"          } >> \"$GITHUB_STEP_SUMMARY\"\n"
	if !strings.Contains(workflow, summaryBlock) {
		t.Fatal("base-image mirror workflow must append the target and pushed digest to the job summary")
	}
	requireWorkflowActionsUseFullSHAs(t, "mirror-base-images.yml", workflow)
}

// govulncheckCommand is the exact, whole invocation the merge-gating
// govulncheck step must run. The Makefile owns the tool version so local and CI
// vulnerability scans cannot drift.
const govulncheckCommand = "make vulncheck"

// TestEngineCIWorkflowGatesOnGovulncheck pins the merge-blocking vulnerability
// gate. govulncheck must run inside the already-required
// `security` job, with its exit status propagated by STRICT EQUALITY of the final
// executed line — not a forbidden-token blacklist. The govulncheck step's run
// scalar must end with exactly the govulncheck command as a standalone
// top-level command that nothing can append to, background, expand,
// short-circuit, or absorb.
func TestEngineCIWorkflowGatesOnGovulncheck(t *testing.T) {
	repoRoot := finalArchitectureEngineRoot(t)
	workflowPath := filepath.Join(repoRoot, ".github", "workflows", "engine-vulncheck.yml")

	content, err := os.ReadFile(workflowPath) //nolint:gosec // test-only static read of repo workflow
	if err != nil {
		t.Fatalf("read %s: %v", workflowPath, err)
	}
	text := string(content)
	for _, trigger := range []string{"schedule:", "cron:", "workflow_dispatch:"} {
		if !strings.Contains(text, trigger) {
			t.Errorf("engine-vulncheck must declare %s so the scan runs on a schedule and on demand", trigger)
		}
	}
	makefilePath := filepath.Join(finalArchitectureEngineRoot(t), "Makefile")
	makefileContent, err := os.ReadFile(makefilePath) //nolint:gosec // test-only static read of repository Makefile
	if err != nil {
		t.Fatalf("read %s: %v", makefilePath, err)
	}
	makefileText := string(makefileContent)

	// Scope to the gating job FIRST (the `security` job is in the required gate
	// ledger), THEN to the govulncheck step within it. A whole-file step
	// scan would let a last-in-job govulncheck step bleed into the next job.
	job := workflowJobForStaticTest(t, text, "govulncheck:")

	// (b) the govulncheck step lives inside the gating job.
	step, found := workflowStepRunningGovulncheck(job)
	if !found {
		t.Fatalf("the govulncheck job must contain a step whose run: scalar invokes govulncheck; job was:\n%s", job)
	}

	// (c) the gating job and the step are not under continue-on-error.
	if strings.Contains(job, "continue-on-error: true") {
		t.Errorf("the govulncheck job (or a step in it) must not set continue-on-error: true; job was:\n%s", job)
	}

	// (d) exit status propagates by STRICT EQUALITY: the step's run scalar must
	// end with exactly the govulncheck command as a standalone top-level
	// command, with no trailing tokens and no preceding-line continuation,
	// operator, compound, subshell, or non-pipefail shell that could neuter it.
	scalar, ok := extractRunScalar(step)
	if !ok {
		t.Fatalf("could not extract the run: scalar from the govulncheck step; step was:\n%s", step)
	}
	if scalar != govulncheckCommand {
		t.Errorf("govulncheck step run scalar = %q; want exactly %q", scalar, govulncheckCommand)
	}
	shell := extractStepShell(step)
	if reason := govulncheckRunPropagatesExit(scalar, shell); reason != "" {
		t.Errorf("govulncheck gate does not propagate its exit status: %s\nrun scalar was:\n%s", reason, scalar)
	}

	// (a) go.mod pins the govulncheck version through its tool directive, so the
	// scanner is resolved and checksummed exactly like every other dependency.
	// The Makefile routes through that declaration and the workflow delegates to
	// the Makefile, leaving one place where the version can change.
	moduleFile, err := os.ReadFile(filepath.Join(finalArchitectureEngineRoot(t), "go.mod")) //nolint:gosec // repository-local static test path.
	if err != nil {
		t.Fatalf("read engine go.mod: %v", err)
	}
	if !strings.Contains(string(moduleFile), "tool golang.org/x/vuln/cmd/govulncheck") {
		t.Error("engine go.mod must declare govulncheck as a tool dependency so its version is pinned and checksummed")
	}
	const vulncheckTarget = "vulncheck:\n\tgo tool govulncheck ./..."
	if !strings.Contains(makefileText, vulncheckTarget) {
		t.Errorf("Makefile must route vulncheck through the module tool over ./...; want:\n%s", vulncheckTarget)
	}
	if count := strings.Count(text, govulncheckCommand); count != 1 {
		t.Errorf("engine-ci workflow %q invocation count = %d; want exactly one", govulncheckCommand, count)
	}
}

// TestGovulncheckRunPropagatesExitRejectsNeuteredForms proves the structural
// strict-equality check actually rejects the known gate-neutering forms and
// accepts only the bare standalone command.
// This is the load-bearing proof that (d) is structural, not a finite operator
// blacklist.
func TestGovulncheckRunPropagatesExitRejectsNeuteredForms(t *testing.T) {
	accepted := []struct {
		name   string
		scalar string
		shell  string
	}{
		{
			name:   "bare_inline_command",
			scalar: govulncheckCommand,
		},
		{
			name:   "bare_block_command_with_leading_step",
			scalar: "go env GOFLAGS\n" + govulncheckCommand,
		},
		{
			name:   "bare_block_command_with_bash_pipefail_shell",
			scalar: govulncheckCommand,
			shell:  "bash",
		},
		{
			// A trailing comment cannot neuter the gate: govulncheck remains the
			// final EXECUTED command and its exit still propagates.
			name:   "trailing_comment_after_command",
			scalar: govulncheckCommand + "\n# vuln gate",
		},
	}
	for _, testCase := range accepted {
		t.Run("accept_"+testCase.name, func(t *testing.T) {
			if reason := govulncheckRunPropagatesExit(testCase.scalar, testCase.shell); reason != "" {
				t.Errorf("expected the bare standalone govulncheck command to be accepted, got rejection: %s", reason)
			}
		})
	}

	rejected := []struct {
		name   string
		scalar string
		shell  string
	}{
		{
			name:   "trailing_exit_zero",
			scalar: govulncheckCommand + " ; exit 0",
		},
		{
			name:   "backgrounded",
			scalar: govulncheckCommand + " &",
		},
		{
			name:   "github_expansion_appended",
			scalar: govulncheckCommand + " ${{ env.X }}",
		},
		{
			name:   "if_then_swallows_exit",
			scalar: "if " + govulncheckCommand + "; then :; fi",
		},
		{
			name:   "preceding_line_short_circuits_via_continuation",
			scalar: "true || \\\n" + govulncheckCommand,
		},
		{
			name:   "preceding_line_dangling_or_operator",
			scalar: "true ||\n" + govulncheckCommand,
		},
		{
			name:   "pipe_to_truthy",
			scalar: govulncheckCommand + " | cat",
		},
		{
			name:   "non_pipefail_shell",
			scalar: govulncheckCommand,
			shell:  "sh",
		},
		{
			name:   "custom_bash_without_pipefail",
			scalar: govulncheckCommand,
			shell:  "bash --noprofile --norc -e {0}",
		},
	}
	for _, testCase := range rejected {
		t.Run("reject_"+testCase.name, func(t *testing.T) {
			if reason := govulncheckRunPropagatesExit(testCase.scalar, testCase.shell); reason == "" {
				t.Errorf("expected the neutered form to be rejected, but it was accepted:\n%s", testCase.scalar)
			}
		})
	}
}

func workflowStepForStaticTest(t *testing.T, text string, stepName string) string {
	t.Helper()
	start := strings.Index(text, stepName)
	if start < 0 {
		t.Fatalf("workflow missing step %q", stepName)
	}
	rest := text[start+len(stepName):]
	nextStep := strings.Index(rest, "\n      - name:")
	if nextStep < 0 {
		return text[start:]
	}
	return text[start : start+len(stepName)+nextStep]
}

func workflowJobForStaticTest(t *testing.T, text string, jobName string) string {
	t.Helper()
	start := strings.Index(text, "\n  "+jobName)
	if start < 0 {
		t.Fatalf("workflow missing job %q", jobName)
	}
	lines := strings.Split(text[start+1:], "\n")
	var jobLines []string
	for index, line := range lines {
		if index > 0 && strings.HasPrefix(line, "  ") && len(line) > 2 && line[2] != ' ' {
			break
		}
		jobLines = append(jobLines, line)
	}
	return strings.Join(jobLines, "\n")
}

// workflowStepRunningGovulncheck returns, scoped to a single job block, the
// `- name:` step whose body invokes govulncheck, plus whether one was found.
// Splitting on the step boundary keeps a last-in-job govulncheck step from
// bleeding into the next step's body.
func workflowStepRunningGovulncheck(job string) (string, bool) {
	const stepBoundary = "\n      - name:"
	for _, step := range splitJobIntoSteps(job, stepBoundary) {
		if strings.Contains(step, "name: Govulncheck") {
			return step, true
		}
	}
	return "", false
}

// splitJobIntoSteps slices a job block into its individual `- name:` steps.
func splitJobIntoSteps(job string, stepBoundary string) []string {
	indices := allIndices(job, stepBoundary)
	if len(indices) == 0 {
		return nil
	}
	var steps []string
	for position, start := range indices {
		end := len(job)
		if position+1 < len(indices) {
			end = indices[position+1]
		}
		steps = append(steps, job[start:end])
	}
	return steps
}

func allIndices(haystack string, needle string) []int {
	var indices []int
	offset := 0
	for {
		next := strings.Index(haystack[offset:], needle)
		if next < 0 {
			return indices
		}
		indices = append(indices, offset+next)
		offset += next + len(needle)
	}
}

// extractRunScalar pulls the shell program from a step block's `run:` key,
// handling both the inline (`run: <cmd>`) and block-scalar (`run: |`,
// `run: >-`, etc.) forms. The returned scalar is dedented to the indentation
// of its first content line so downstream structural checks see the raw shell.
func extractRunScalar(step string) (string, bool) {
	lines := strings.Split(step, "\n")
	for index, line := range lines {
		trimmed := strings.TrimSpace(line)
		if !strings.HasPrefix(trimmed, "run:") {
			continue
		}
		afterKey := strings.TrimSpace(strings.TrimPrefix(trimmed, "run:"))
		if afterKey != "" && !isBlockScalarIndicator(afterKey) {
			// Inline form: `run: go run ... ./...`.
			return afterKey, true
		}
		// Block form: collect the indented lines that follow, more indented
		// than the `run:` key itself.
		keyIndent := indentWidth(line)
		var body []string
		bodyIndent := -1
		for _, bodyLine := range lines[index+1:] {
			if strings.TrimSpace(bodyLine) == "" {
				body = append(body, "")
				continue
			}
			if indentWidth(bodyLine) <= keyIndent {
				break
			}
			if bodyIndent < 0 {
				bodyIndent = indentWidth(bodyLine)
			}
			body = append(body, dedent(bodyLine, bodyIndent))
		}
		return strings.Join(body, "\n"), true
	}
	return "", false
}

// isBlockScalarIndicator reports whether the text after `run:` is a YAML block
// scalar header (`|`, `>`, with optional chomping/indentation indicators and
// trailing comment) rather than an inline command.
func isBlockScalarIndicator(after string) bool {
	if after == "" {
		return false
	}
	// Strip a trailing comment.
	if hash := strings.Index(after, " #"); hash >= 0 {
		after = strings.TrimSpace(after[:hash])
	}
	if after == "" {
		return false
	}
	if after[0] != '|' && after[0] != '>' {
		return false
	}
	for _, char := range after[1:] {
		if char != '-' && char != '+' && (char < '0' || char > '9') {
			return false
		}
	}
	return true
}

func indentWidth(line string) int {
	return len(line) - len(strings.TrimLeft(line, " "))
}

func dedent(line string, width int) string {
	if indentWidth(line) < width {
		return strings.TrimLeft(line, " ")
	}
	return line[width:]
}

// extractStepShell returns the value of a step's `shell:` key, or "" when the
// step relies on the default shell.
func extractStepShell(step string) string {
	for _, line := range strings.Split(step, "\n") {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "shell:") {
			return strings.TrimSpace(strings.TrimPrefix(trimmed, "shell:"))
		}
	}
	return ""
}

// govulncheckRunPropagatesExit implements the structural strict-equality
// check. It returns "" when the run scalar ends with the govulncheck
// command as a standalone top-level command whose non-zero exit cannot be
// suppressed, and a human-readable reason otherwise. This is deliberately NOT a
// blacklist of forbidden operators: the load-bearing assertion is that the
// final executed line IS exactly the govulncheck command and that nothing
// before it leaves the command in a position to be appended to, backgrounded,
// expanded, short-circuited, or absorbed.
func govulncheckRunPropagatesExit(scalar string, shell string) string {
	// A non-default shell must be bash with pipefail (a non-pipefail shell lets
	// a pipe-to-truthy swallow govulncheck's failure). GitHub's `bash` keyword
	// already runs with `set -eo pipefail`, so the bare value `bash` is safe; a
	// custom shell command line must name both `bash` and `pipefail`; any other
	// interpreter (sh, pwsh, python, …) is rejected.
	if shell != "" {
		normalizedShell := strings.TrimSpace(shell)
		safeShell := normalizedShell == "bash" ||
			(strings.Contains(normalizedShell, "bash") && strings.Contains(normalizedShell, "pipefail"))
		if !safeShell {
			return "non-default shell must be bash with pipefail, got: " + shell
		}
	}

	rawLines := strings.Split(scalar, "\n")

	// Determine the final executed line: the last line that is neither blank nor
	// a pure comment. A trailing comment must NOT displace the command from the
	// final-line position.
	finalIndex := -1
	for index := len(rawLines) - 1; index >= 0; index-- {
		trimmed := strings.TrimSpace(rawLines[index])
		if trimmed == "" || strings.HasPrefix(trimmed, "#") {
			continue
		}
		finalIndex = index
		break
	}
	if finalIndex < 0 {
		return "run scalar has no executable command"
	}

	finalLine := strings.TrimSpace(rawLines[finalIndex])
	if finalLine != govulncheckCommand {
		return "final executed line must be exactly " + govulncheckCommand + ", got: " + finalLine
	}

	// The preceding non-empty line must not leave a dangling continuation or
	// operator that would absorb the final command into a compound expression.
	for index := finalIndex - 1; index >= 0; index-- {
		trimmed := strings.TrimSpace(rawLines[index])
		if trimmed == "" || strings.HasPrefix(trimmed, "#") {
			continue
		}
		if strings.HasSuffix(trimmed, "\\") {
			return "preceding line ends with a continuation `\\` that absorbs the govulncheck command: " + trimmed
		}
		if strings.HasSuffix(trimmed, "||") || strings.HasSuffix(trimmed, "&&") || strings.HasSuffix(trimmed, "|") {
			return "preceding line ends with a dangling operator that short-circuits the govulncheck command: " + trimmed
		}
		break
	}

	// No line in the scalar may open an unterminated compound (if/while/until/
	// for ... do|then, or an unmatched `(`/`{`) that encloses the final command,
	// and the final line must itself be a bare command (no leading keyword, no
	// trailing operator/expansion/redirect).
	if reason := finalLineIsBareCommand(finalLine); reason != "" {
		return reason
	}
	if reason := noEnclosingCompound(rawLines[:finalIndex]); reason != "" {
		return reason
	}
	return ""
}

// finalLineIsBareCommand verifies the final line is exactly the govulncheck
// command with no leading control keyword and no trailing token of any kind.
// Because finalLine has already been asserted equal to govulncheckCommand, this
// is a defensive restatement that keeps the meta-test honest if the equality
// check is ever loosened.
func finalLineIsBareCommand(finalLine string) string {
	if finalLine != govulncheckCommand {
		return "final line is not the bare govulncheck command: " + finalLine
	}
	leadingKeywords := []string{"if ", "while ", "until ", "for ", "case ", "{ ", "( "}
	for _, keyword := range leadingKeywords {
		if strings.HasPrefix(finalLine, keyword) {
			return "final command is wrapped in a control keyword: " + finalLine
		}
	}
	return ""
}

// noEnclosingCompound reports whether the lines preceding the final command
// open a compound (if/while/until/for, or unmatched `(`/`{`) that the final
// command would run inside, where the compound's own exit (not govulncheck's)
// becomes the step result.
func noEnclosingCompound(precedingLines []string) string {
	openCompounds := 0
	parenDepth := 0
	braceDepth := 0
	for _, line := range precedingLines {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" || strings.HasPrefix(trimmed, "#") {
			continue
		}
		fields := strings.Fields(trimmed)
		if len(fields) > 0 {
			switch fields[0] {
			case "if", "while", "until", "for", "case":
				openCompounds++
			case "fi", "done", "esac":
				if openCompounds > 0 {
					openCompounds--
				}
			}
		}
		parenDepth += strings.Count(trimmed, "(") - strings.Count(trimmed, ")")
		braceDepth += strings.Count(trimmed, "{") - strings.Count(trimmed, "}")
	}
	if openCompounds > 0 {
		return "the govulncheck command runs inside an unterminated if/while/until/for/case compound"
	}
	if parenDepth > 0 {
		return "the govulncheck command runs inside an unterminated subshell `(`"
	}
	if braceDepth > 0 {
		return "the govulncheck command runs inside an unterminated brace group `{`"
	}
	return ""
}
