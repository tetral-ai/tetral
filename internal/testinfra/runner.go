package testinfra

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

type RunOptions struct {
	Root       string
	OutputDir  string
	PlanOnly   bool
	MaxWorkers int
}

func Execute(ctx context.Context, plan Plan, options RunOptions) (Result, error) {
	result := Result{Plan: plan, Execution: executionEnvelopeFromEnvironment(), StartedAt: time.Now().UTC(), Status: "pass"}
	if options.PlanOnly {
		result.FinishedAt = time.Now().UTC()
		return result, nil
	}
	if options.MaxWorkers <= 0 {
		options.MaxWorkers = max(1, min(runtime.NumCPU(), 4))
	}
	if options.OutputDir == "" {
		options.OutputDir = filepath.Join(options.Root, ".test-results", result.StartedAt.Format("20060102T150405.000000000Z"))
	}
	if err := os.MkdirAll(options.OutputDir, 0o700); err != nil {
		return result, err
	}

	setupStart := time.Now()
	dependencies, err := startDependencies(ctx, plan.Dependencies, options.Root)
	result.Setup = time.Since(setupStart)
	if err != nil {
		result.Status = "apparatus-failed"
		result.FinishedAt = time.Now().UTC()
		_ = writeJSON(filepath.Join(options.OutputDir, "result.json"), result)
		return result, err
	}
	result.Dependencies = append(result.Dependencies, dependencies.evidence...)

	var runErr error
	goSelections := make([]Selection, 0)
	for _, selection := range plan.Selections {
		if selection.Group == "go" && len(selection.Packages) == 1 {
			goSelections = append(goSelections, selection)
			continue
		}
		steps, stepErr := executeSelection(ctx, plan, selection, options, dependencies)
		result.Steps = append(result.Steps, steps...)
		if stepErr != nil {
			runErr = errors.Join(runErr, stepErr)
			break
		}
	}
	if runErr == nil && len(goSelections) > 0 {
		steps, stepErr := executeGoSelections(ctx, plan.Profile, goSelections, options, dependencies)
		result.Steps = append(result.Steps, steps...)
		runErr = errors.Join(runErr, stepErr)
	}

	teardownStart := time.Now()
	stopErr := dependencies.stopBounded()
	result.Teardown = time.Since(teardownStart)
	if stopErr != nil {
		result.Status = "apparatus-failed"
		runErr = errors.Join(runErr, stopErr)
	} else if runErr != nil {
		result.Status = "fail"
		for _, step := range result.Steps {
			if step.Status == "apparatus-failed" {
				result.Status = "apparatus-failed"
				break
			}
		}
	}
	result.FinishedAt = time.Now().UTC()
	if writeErr := writeJSON(filepath.Join(options.OutputDir, "result.json"), result); writeErr != nil {
		result.Status = "apparatus-failed"
		runErr = errors.Join(runErr, writeErr)
	}
	return result, runErr
}

func executionEnvelopeFromEnvironment() ExecutionEnvelope {
	return ExecutionEnvelope{
		Repository:        os.Getenv("TETRAL_CI_REPOSITORY"),
		EventHeadSHA:      os.Getenv("TETRAL_CI_EVENT_HEAD_SHA"),
		EventBaseSHA:      os.Getenv("TETRAL_CI_EVENT_BASE_SHA"),
		TestMergeSHA:      os.Getenv("TETRAL_CI_TEST_MERGE_SHA"),
		RequiredCheckSHA:  os.Getenv("TETRAL_CI_REQUIRED_CHECK_SHA"),
		WorkflowSourceSHA: os.Getenv("TETRAL_CI_WORKFLOW_SOURCE_SHA"),
		Workflow:          os.Getenv("TETRAL_CI_WORKFLOW"),
		RunID:             os.Getenv("TETRAL_CI_RUN_ID"),
		RunAttempt:        os.Getenv("TETRAL_CI_RUN_ATTEMPT"),
		Job:               os.Getenv("TETRAL_CI_JOB"),
		Producer:          os.Getenv("TETRAL_CI_PRODUCER"),
	}
}

func executeSelection(ctx context.Context, plan Plan, selection Selection, options RunOptions, dependencies *dependencyManager) ([]StepResult, error) {
	commands, err := commandsForSelection(plan, selection, options.Root, options.OutputDir)
	if err != nil {
		return nil, err
	}
	var results []StepResult
	for _, command := range commands {
		step, stepErr := runStep(ctx, options.Root, selection.Group, command, dependencies, options.OutputDir)
		results = append(results, step)
		if stepErr != nil {
			return results, stepErr
		}
	}
	return results, nil
}

func executeGoSelections(ctx context.Context, profile Profile, selections []Selection, options RunOptions, dependencies *dependencyManager) ([]StepResult, error) {
	type outcome struct {
		index int
		step  StepResult
		err   error
	}
	jobs := make(chan int)
	outcomes := make(chan outcome, len(selections))
	workers := min(options.MaxWorkers, len(selections))
	var wait sync.WaitGroup
	for worker := 0; worker < workers; worker++ {
		wait.Add(1)
		go func() {
			defer wait.Done()
			for index := range jobs {
				selection := selections[index]
				arguments := []string{"go", "test", "-json", "-count=1"}
				if profile != ProfileFast {
					arguments = append(arguments, "-race", "-timeout=20m")
				}
				if len(selection.Tests) > 0 {
					tests := make([]string, len(selection.Tests))
					for index, name := range selection.Tests {
						tests[index] = regexp.QuoteMeta(name)
					}
					arguments = append(arguments, "-run", "^(?:"+strings.Join(tests, "|")+")$")
				} else if profile == ProfileFast {
					arguments = append(arguments, "-run", "a^")
				}
				arguments = append(arguments, selection.Packages[0])
				command := commandSpec{
					Arguments:         arguments,
					Artifact:          "go-" + string(profile) + "-" + sanitizeArtifact(selection.Packages[0]) + ".jsonl",
					Kind:              "go-json",
					RejectSkip:        true,
					ExpectedRunnables: selection.Tests,
					ExpectedPackages:  selection.Packages,
				}
				step, err := runStep(ctx, options.Root, "go", command, dependencies, options.OutputDir)
				outcomes <- outcome{index: index, step: step, err: err}
			}
		}()
	}
	go func() {
		for index := range selections {
			jobs <- index
		}
		close(jobs)
		wait.Wait()
		close(outcomes)
	}()
	results := make([]StepResult, len(selections))
	var runErr error
	for item := range outcomes {
		results[item.index] = item.step
		runErr = errors.Join(runErr, item.err)
	}
	return results, runErr
}

type commandSpec struct {
	Arguments         []string
	WorkingDir        string
	Artifact          string
	Kind              string
	RejectSkip        bool
	ExpectedRunnables []string
	ExpectedFiles     []string
	ExpectedPackages  []string
}

const descendantRegistryEnv = "TETRAL_TEST_DESCENDANT_REGISTRY"

func commandsForSelection(plan Plan, selection Selection, root, outputDir string) ([]commandSpec, error) {
	switch selection.Group {
	case "repository":
		return []commandSpec{{Arguments: []string{"go", "run", "./internal/testinfra/cmd/tetral-format-check"}}}, nil
	case "go-static":
		return []commandSpec{
			{Arguments: []string{"go", "build", "./..."}},
			{Arguments: []string{"go", "vet", "./..."}},
			{Arguments: []string{"golangci-lint", "run", "--config", ".golangci.yml", "./..."}},
		}, nil
	case "go":
		packages := selection.Packages
		if len(packages) == 0 {
			packages = []string{"./..."}
		}
		arguments := []string{"go", "test", "-json", "-count=1"}
		if plan.Profile != ProfileFast {
			arguments = append(arguments, "-race", "-timeout=20m")
		}
		arguments = append(arguments, packages...)
		return []commandSpec{{Arguments: arguments, Artifact: "go.jsonl", Kind: "go-json"}}, nil
	case "runtime":
		dir := "services/agent-runtime"
		var unitTests, integrationTests []string
		for _, file := range selection.Tests {
			switch {
			case strings.Contains(file, "/test/unit/"):
				unitTests = append(unitTests, file)
			case strings.Contains(file, "/test/integration/"):
				integrationTests = append(integrationTests, file)
			default:
				return nil, fmt.Errorf("runtime test file %q has no execution class", file)
			}
		}
		if len(unitTests) == 0 {
			return nil, fmt.Errorf("runtime selection contains no unit test files")
		}
		commands := []commandSpec{
			{Arguments: []string{"bun", "install", "--frozen-lockfile"}, WorkingDir: "services/gateway"},
			{Arguments: []string{"bun", "install", "--frozen-lockfile"}, WorkingDir: dir},
			{Arguments: []string{"bun", "run", "typecheck"}, WorkingDir: dir},
			{Arguments: append([]string{"bun", "test", "--reporter=junit", "--reporter-outfile=" + filepath.Join(outputDir, "runtime-junit.xml")}, unitTests...), WorkingDir: dir, Artifact: "runtime-junit.xml", Kind: "junit", RejectSkip: true, ExpectedFiles: unitTests},
		}
		if fullEvidencePlan(plan) {
			if len(integrationTests) == 0 {
				return nil, fmt.Errorf("full runtime selection contains no integration test files")
			}
			commands = append(commands,
				commandSpec{Arguments: append([]string{"bun", "test", "--reporter=junit", "--reporter-outfile=" + filepath.Join(outputDir, "runtime-integration-junit.xml")}, integrationTests...), WorkingDir: dir, Artifact: "runtime-integration-junit.xml", Kind: "junit", ExpectedFiles: integrationTests},
				commandSpec{Arguments: []string{"bun", "run", "build"}, WorkingDir: dir},
			)
		}
		return commands, nil
	case "gateway":
		dir := "services/gateway"
		testPaths := selection.Tests
		if len(testPaths) == 0 {
			return nil, fmt.Errorf("gateway selection contains no test files")
		}
		expected, err := enumerateBunTestFiles(filepath.Join(root, dir), testPaths)
		if err != nil {
			return nil, err
		}
		commands := []commandSpec{
			{Arguments: []string{"bun", "install", "--frozen-lockfile"}, WorkingDir: "services/agent-runtime"},
			{Arguments: []string{"bun", "install", "--frozen-lockfile"}, WorkingDir: dir},
			{Arguments: []string{"bun", "run", "typecheck"}, WorkingDir: dir},
			{Arguments: append([]string{"bun", "test", "--reporter=junit", "--reporter-outfile=" + filepath.Join(outputDir, "gateway-junit.xml")}, testPaths...), WorkingDir: dir, Artifact: "gateway-junit.xml", Kind: "junit", RejectSkip: true, ExpectedFiles: expected},
		}
		if fullEvidencePlan(plan) {
			commands = append(commands, commandSpec{Arguments: []string{"bun", "run", "build"}, WorkingDir: dir})
		}
		return commands, nil
	case "protocol":
		return []commandSpec{
			{Arguments: []string{"bun", "install", "--frozen-lockfile"}, WorkingDir: "services/agent-runtime"},
			{Arguments: []string{"bun", "install", "--frozen-lockfile"}, WorkingDir: "services/gateway"},
			{Arguments: []string{"buf", "lint"}},
			{Arguments: []string{"go", "test", "./integration/static", "-run", "Test.*BufGenerationFreshness|TestFinalArchitectureTypeScriptProtocolGenerationIsServiceLocal|TestGitHubIntegrationRuntimeEventsTrackForkSDKShapes|TestForkSDKAgentModelUnionTracksDraftPinnedCatalog|TestPublicEventProjectionTracksFrozenForkSDKUnion", "-count=1"}},
			{Arguments: []string{"go", "test", "-race", "-count=1", "./integration/grpcinterop"}},
			{Arguments: []string{"./scripts/check-sdk-compatibility-traceability.sh"}},
		}, nil
	case "deployment":
		return []commandSpec{
			{Arguments: []string{"go", "test", "-count=1", "./deploy/kubernetes"}},
			{Arguments: []string{"helm", "lint", "deploy/helm/tetral"}},
		}, nil
	case "security":
		return []commandSpec{
			{Arguments: []string{"bun", "install", "--frozen-lockfile"}, WorkingDir: "services/agent-runtime"},
			{Arguments: []string{"bun", "audit"}, WorkingDir: "services/agent-runtime"},
			{Arguments: []string{"bun", "install", "--frozen-lockfile"}, WorkingDir: "services/gateway"},
			{Arguments: []string{"bun", "audit"}, WorkingDir: "services/gateway"},
			{Arguments: []string{"go", "test", "./integration/static", "-run", "Test.*Secret|Test.*Redact|Test.*Import|Test.*Boundary|Test.*Log", "-count=1"}},
		}, nil
	case "sandbox-image":
		return []commandSpec{
			{Arguments: []string{"./scripts/run-helper-privilege-container.sh"}},
			{Arguments: []string{"./scripts/run-sandbox-local-image-smoke.sh"}},
		}, nil
	case "coverage":
		return []commandSpec{
			{Arguments: []string{"go", "test", "-count=1", "-covermode=atomic", "-coverprofile=" + filepath.Join(outputDir, "go-coverage.out"), "./..."}},
			{Arguments: []string{"bun", "install", "--frozen-lockfile"}, WorkingDir: "services/agent-runtime"},
			{Arguments: []string{"bun", "test", "--coverage", "--coverage-reporter=lcov", "--coverage-dir=" + filepath.Join(outputDir, "runtime-coverage"), "packages/core/test/unit/", "packages/protocol/test/unit/", "packages/runtime-pod/test/unit/"}, WorkingDir: "services/agent-runtime"},
			{Arguments: []string{"bun", "install", "--frozen-lockfile"}, WorkingDir: "services/gateway"},
			{Arguments: []string{"bun", "test", "--coverage", "--coverage-reporter=lcov", "--coverage-dir=" + filepath.Join(outputDir, "gateway-coverage"), "packages/protocol/test/unit/", "packages/lowering/test/", "packages/provider-gateway/test/unit/", "packages/provider-gateway/test/golden/", "packages/mcp-connector/test/unit/"}, WorkingDir: "services/gateway"},
		}, nil
	default:
		return nil, fmt.Errorf("unknown evidence group %q", selection.Group)
	}
}

func runStep(ctx context.Context, root, group string, spec commandSpec, dependencies *dependencyManager, outputDir string) (StepResult, error) {
	workingDir := root
	if spec.WorkingDir != "" {
		workingDir = filepath.Join(root, spec.WorkingDir)
	}
	step := StepResult{Group: group, Command: spec.Arguments, WorkingDir: spec.WorkingDir, Status: "pass", Artifact: spec.Artifact}
	if len(spec.Arguments) == 0 {
		return step, nil
	}
	environment, processRunID, err := dependencies.environmentForProcess()
	if err != nil {
		step.Status = "apparatus-failed"
		step.FirstFailure = "prepare isolated process environment"
		return step, err
	}
	descendantRegistry := filepath.Join(outputDir, "descendants-"+sanitizeArtifact(group)+"-"+fmt.Sprintf("%x", sha256.Sum256([]byte(strings.Join(spec.Arguments, "\x00"))))[:12]+".txt")
	environment = append(withoutEnvironmentVariable(environment, descendantRegistryEnv), descendantRegistryEnv+"="+descendantRegistry)
	artifactPath := ""
	if spec.Artifact != "" {
		artifactPath = filepath.Join(outputDir, spec.Artifact)
	}
	logName := spec.Artifact + ".log"
	if spec.Kind == "go-json" {
		logName = spec.Artifact
	}
	if logName == "" {
		digest := sha256.Sum256([]byte(strings.Join(spec.Arguments, "\x00")))
		logName = sanitizeArtifact(group) + "-" + fmt.Sprintf("%x", digest[:6]) + ".log"
	}
	logPath := filepath.Join(outputDir, logName)
	// Output paths are derived from the runner-owned result directory and closed inventory.
	//nolint:gosec
	file, err := os.Create(logPath)
	if err != nil {
		return step, err
	}
	var output io.Writer = file
	// Commands are selected from the runner's closed evidence inventory; no shell is used.
	//nolint:gosec
	command := exec.Command(spec.Arguments[0], spec.Arguments[1:]...)
	command.Dir = workingDir
	command.Env = environment
	command.Stdout = output
	command.Stderr = output
	started := time.Now()
	processErr := startManagedCommand(ctx, command)
	step.Elapsed = time.Since(started)
	if closeErr := file.Close(); closeErr != nil {
		processErr = errors.Join(processErr, invalidReport("close diagnostic report: %v", closeErr))
	}
	var firstFailure string
	var reportErr error
	if artifactPath != "" {
		switch spec.Kind {
		case "go-json":
			firstFailure, reportErr = inspectGoJSON(artifactPath, spec.RejectSkip, spec.ExpectedPackages, spec.ExpectedRunnables)
		case "junit":
			firstFailure, reportErr = inspectJUnit(artifactPath, spec.RejectSkip, spec.ExpectedFiles)
		}
	}
	step.FirstFailure = firstFailure
	err = reconcileProcessAndReport(processErr, reportErr, spec.Kind != "")
	if cleanupErr := dependencies.closeProcessRun(processRunID); cleanupErr != nil {
		err = errors.Join(err, invalidReport("test process cleanup failed"))
	}
	if descendantErr := terminateRegisteredDescendants(descendantRegistry); descendantErr != nil {
		err = errors.Join(err, invalidReport("test process left a live detached descendant"))
	}
	if err != nil {
		step.Status = "fail"
		var reportErr *reportError
		if errors.As(err, &reportErr) {
			step.Status = "apparatus-failed"
		}
		if step.FirstFailure == "" {
			step.FirstFailure = err.Error()
		}
		return step, fmt.Errorf("%s evidence failed (diagnostic %s): %w", group, logPath, err)
	}
	return step, nil
}

func terminateRegisteredDescendants(path string) error {
	// The path is the runner-owned descendant registry for this step.
	//nolint:gosec
	body, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	type registeredDescendant struct {
		pid      int
		identity string
	}
	var descendants []registeredDescendant
	for _, line := range strings.Split(strings.TrimSpace(string(body)), "\n") {
		parts := strings.SplitN(line, ":", 2)
		if len(parts) != 2 {
			return fmt.Errorf("malformed descendant registration")
		}
		pid, err := strconv.Atoi(parts[0])
		if err != nil {
			return fmt.Errorf("malformed descendant registration")
		}
		descendants = append(descendants, registeredDescendant{pid: pid, identity: parts[1]})
	}
	// PostgreSQL capability cleanup runs before descendant cleanup. Give a
	// cooperative process a short bounded window to observe that revocation and
	// exit; a process that remains alive is killed and reported as apparatus
	// leakage.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		live := false
		for _, descendant := range descendants {
			live = live || processIdentityAlive(descendant.pid, descendant.identity)
		}
		if !live {
			return nil
		}
		time.Sleep(20 * time.Millisecond)
	}
	var live bool
	for _, descendant := range descendants {
		if !processIdentityAlive(descendant.pid, descendant.identity) {
			continue
		}
		live = true
		if process, err := os.FindProcess(descendant.pid); err == nil {
			_ = process.Kill()
		}
	}
	if live {
		return fmt.Errorf("registered detached descendant remained alive")
	}
	return nil
}

func reconcileProcessAndReport(processErr, reportErr error, structured bool) error {
	if !structured {
		return processErr
	}
	if processErr == nil {
		return reportErr
	}
	if reportErr == nil {
		return invalidReport("command failed despite a passing structured report")
	}
	var malformed *reportError
	if errors.As(reportErr, &malformed) {
		return errors.Join(processErr, reportErr)
	}
	return reportErr
}

type reportError struct{ message string }

func (e *reportError) Error() string { return e.message }

func invalidReport(format string, arguments ...any) error {
	return &reportError{message: fmt.Sprintf(format, arguments...)}
}

func inspectGoJSON(path string, rejectSkip bool, expectedPackages, expectedRunnables []string) (string, error) {
	// The path is a runner-owned structured result artifact.
	//nolint:gosec
	file, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer func() { _ = file.Close() }()
	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 64*1024), 4*1024*1024)
	terminal := map[string]int{}
	runEvents := map[string]int{}
	packagePassed := map[string]bool{}
	packageHasNoTests := map[string]bool{}
	for scanner.Scan() {
		var event struct {
			Action  string `json:"Action"`
			Package string `json:"Package"`
			Test    string `json:"Test"`
			Output  string `json:"Output"`
		}
		if err := json.Unmarshal(scanner.Bytes(), &event); err != nil {
			return "", invalidReport("Go JSON report contains a malformed event: %v", err)
		}
		if event.Action == "fail" {
			return event.Package + " " + event.Test, fmt.Errorf("go test failure")
		}
		if event.Action == "pass" && event.Test == "" {
			packagePassed[event.Package] = true
		}
		if event.Action == "output" && event.Test == "" && strings.Contains(event.Output, "[no test files]") {
			packageHasNoTests[event.Package] = true
		}
		if event.Test != "" && !strings.Contains(event.Test, "/") {
			key := event.Package + "\x00" + event.Test
			if event.Action == "run" {
				runEvents[key]++
			}
			if event.Action == "pass" || event.Action == "fail" || event.Action == "skip" {
				terminal[key]++
			}
		}
		if rejectSkip && event.Action == "skip" && event.Test != "" {
			return event.Package + " " + event.Test, fmt.Errorf("unexpected Go test skip")
		}
	}
	if err := scanner.Err(); err != nil {
		return "", invalidReport("read Go JSON report: %v", err)
	}
	if len(expectedPackages) == 0 && len(packagePassed) == 0 {
		return "", invalidReport("Go JSON report is missing the package pass event")
	}
	for _, packageName := range expectedPackages {
		if !packagePassed[packageName] && (len(expectedRunnables) != 0 || !packageHasNoTests[packageName]) {
			return packageName, invalidReport("Go JSON report omitted selected package %s", packageName)
		}
	}
	expected := make(map[string]bool, len(expectedRunnables))
	for _, name := range expectedRunnables {
		expected[name] = true
	}
	if len(expected) > 0 && len(expectedPackages) == 1 {
		prefix := expectedPackages[0] + "\x00"
		for key, count := range terminal {
			if !strings.HasPrefix(key, prefix) {
				return key, invalidReport("Go JSON report contains an unexpected package runnable")
			}
			name := strings.TrimPrefix(key, prefix)
			if !expected[name] {
				return name, invalidReport("Go JSON report contains unexpected runnable %s", name)
			}
			if count != 1 || runEvents[key] != 1 {
				return name, invalidReport("Go JSON report contains an inconsistent lifecycle for runnable %s", name)
			}
		}
	}
	for _, name := range expectedRunnables {
		key := name
		if len(expectedPackages) == 1 {
			key = expectedPackages[0] + "\x00" + name
		}
		if terminal[key] != 1 || runEvents[key] != 1 {
			return name, invalidReport("Go JSON report omitted selected runnable %s", name)
		}
	}
	return "", nil
}

type junitSuites struct {
	XMLName  xml.Name     `xml:"testsuites"`
	Tests    *int         `xml:"tests,attr"`
	Failures *int         `xml:"failures,attr"`
	Skipped  *int         `xml:"skipped,attr"`
	Suites   []junitSuite `xml:"testsuite"`
}

type junitSuite struct {
	XMLName   xml.Name        `xml:"testsuite"`
	Name      string          `xml:"name,attr"`
	File      string          `xml:"file,attr"`
	Tests     *int            `xml:"tests,attr"`
	Failures  *int            `xml:"failures,attr"`
	Skipped   *int            `xml:"skipped,attr"`
	Suites    []junitSuite    `xml:"testsuite"`
	TestCases []junitTestCase `xml:"testcase"`
}

type junitTestCase struct {
	Name    string       `xml:"name,attr"`
	Failure *junitDetail `xml:"failure"`
	Error   *junitDetail `xml:"error"`
	Skipped *struct{}    `xml:"skipped"`
}

type junitDetail struct {
	Message string `xml:"message,attr"`
	Body    string `xml:",chardata"`
}

func inspectJUnit(path string, rejectSkip bool, expectedFiles []string) (string, error) {
	// The path is a runner-owned structured result artifact.
	//nolint:gosec
	body, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	var suites junitSuites
	if err := xml.Unmarshal(body, &suites); err != nil || suites.XMLName.Local != "testsuites" {
		var suite junitSuite
		if singleErr := xml.Unmarshal(body, &suite); singleErr != nil {
			return "", invalidReport("Bun JUnit report is malformed")
		}
		if suite.XMLName.Local != "testsuite" {
			return "", invalidReport("Bun JUnit report has an unsupported root element")
		}
		suites.Suites = []junitSuite{suite}
	}
	seenFiles := map[string]bool{}
	type totals struct{ tests, failures, skipped int }
	var visit func(junitSuite) (totals, string, error)
	visit = func(suite junitSuite) (totals, string, error) {
		actual := totals{}
		if suite.File != "" {
			seenFiles[filepath.ToSlash(suite.File)] = true
		}
		for _, test := range suite.TestCases {
			actual.tests++
			if test.Failure != nil || test.Error != nil {
				actual.failures++
				return actual, suite.Name + " " + test.Name, fmt.Errorf("bun test failure")
			}
			if test.Skipped != nil {
				actual.skipped++
				if rejectSkip {
					return actual, suite.Name + " " + test.Name, fmt.Errorf("unexpected Bun test skip")
				}
			}
		}
		for _, child := range suite.Suites {
			childTotals, failure, err := visit(child)
			if err != nil {
				return actual, failure, err
			}
			actual.tests += childTotals.tests
			actual.failures += childTotals.failures
			actual.skipped += childTotals.skipped
		}
		if (suite.Tests != nil && *suite.Tests != actual.tests) || (suite.Failures != nil && *suite.Failures != actual.failures) || (suite.Skipped != nil && *suite.Skipped != actual.skipped) {
			return actual, suite.Name, invalidReport("Bun JUnit suite totals are inconsistent")
		}
		return actual, "", nil
	}
	documentTotals := totals{}
	for _, suite := range suites.Suites {
		actual, failure, err := visit(suite)
		if err != nil {
			return failure, err
		}
		documentTotals.tests += actual.tests
		documentTotals.failures += actual.failures
		documentTotals.skipped += actual.skipped
	}
	if (suites.Tests != nil && *suites.Tests != documentTotals.tests) || (suites.Failures != nil && *suites.Failures != documentTotals.failures) || (suites.Skipped != nil && *suites.Skipped != documentTotals.skipped) {
		return "", invalidReport("Bun JUnit document totals are inconsistent")
	}
	if documentTotals.tests == 0 {
		return "", invalidReport("Bun JUnit report contains no test cases")
	}
	for _, expected := range expectedFiles {
		if !seenFiles[filepath.ToSlash(expected)] {
			return expected, invalidReport("Bun JUnit report omitted selected file %s", expected)
		}
	}
	return "", nil
}

func allBunTestFiles(root string) ([]string, error) {
	var files []string
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() && (entry.Name() == "node_modules" || entry.Name() == "dist") {
			return filepath.SkipDir
		}
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".test.ts") {
			return nil
		}
		relative, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		files = append(files, filepath.ToSlash(relative))
		return nil
	})
	sort.Strings(files)
	return files, err
}

func gatewayTestFiles(root string, profile Profile) ([]string, []Exclusion, error) {
	all, err := allBunTestFiles(root)
	if err != nil {
		return nil, nil, err
	}
	var selected []string
	var excluded []Exclusion
	for _, file := range all {
		// Files are enumerated beneath the repository-owned Gateway test root.
		//nolint:gosec
		body, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(file)))
		if err != nil {
			return nil, nil, err
		}
		capability := ""
		reason := ""
		switch {
		case strings.Contains(file, "/test/live/"):
			capability = "live-external-service"
			reason = "requires an operator-authorized live external service"
		case profile == ProfileFast && strings.Contains(string(body), "TETRAL_TEST_DATABASE_URL"):
			capability = "postgresql"
			reason = "requires the PostgreSQL test database contract"
		}
		if capability != "" {
			excluded = append(excluded, Exclusion{Runnable: file, Capability: capability, Disposition: "not-applicable", Reason: reason})
			continue
		}
		selected = append(selected, file)
	}
	return selected, excluded, nil
}

func runtimeTestFiles(root string, full bool) ([]string, []Exclusion, error) {
	all, err := allBunTestFiles(root)
	if err != nil {
		return nil, nil, err
	}
	var selected []string
	var excluded []Exclusion
	for _, file := range all {
		switch {
		case strings.Contains(file, "/test/unit/"):
			selected = append(selected, file)
		case strings.Contains(file, "/test/integration/"):
			if full {
				selected = append(selected, file)
			} else {
				excluded = append(excluded, Exclusion{
					Runnable: file, Capability: "runtime-integration", Disposition: "not-applicable",
					Reason: "executed by the Full evidence profile",
				})
			}
		default:
			return nil, nil, fmt.Errorf("runtime test file %q has no evidence disposition", file)
		}
	}
	return selected, excluded, nil
}

func enumerateBunTestFiles(root string, selections []string) ([]string, error) {
	files := map[string]bool{}
	for _, selection := range selections {
		path := filepath.Join(root, filepath.FromSlash(selection))
		info, err := os.Stat(path)
		if err != nil {
			return nil, err
		}
		if !info.IsDir() {
			relative, err := filepath.Rel(root, path)
			if err != nil {
				return nil, err
			}
			files[filepath.ToSlash(relative)] = true
			continue
		}
		if err := filepath.WalkDir(path, func(candidate string, entry os.DirEntry, walkErr error) error {
			if walkErr != nil {
				return walkErr
			}
			if entry.IsDir() && (entry.Name() == "node_modules" || entry.Name() == "dist") {
				return filepath.SkipDir
			}
			if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".test.ts") {
				return nil
			}
			relative, err := filepath.Rel(root, candidate)
			if err != nil {
				return err
			}
			files[filepath.ToSlash(relative)] = true
			return nil
		}); err != nil {
			return nil, err
		}
	}
	result := make([]string, 0, len(files))
	for file := range files {
		result = append(result, file)
	}
	sort.Strings(result)
	return result, nil
}

func writeJSON(path string, value any) error {
	body, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, append(body, '\n'), 0o600)
}

func sanitizeArtifact(value string) string {
	replacer := strings.NewReplacer("/", "_", ".", "_", "[", "_", "]", "_")
	return replacer.Replace(value)
}
