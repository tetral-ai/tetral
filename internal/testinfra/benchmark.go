package testinfra

import (
	"context"
	"encoding/json"
	"fmt"
	"io/fs"
	"maps"
	"math"
	"os"
	"path"
	"path/filepath"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"
)

// DefaultBenchmarkJobName is the display name of a Go Race job in Pull Request
// and Main Branch Verification; %d is the shard index.
const DefaultBenchmarkJobName = "Go Race (shard %d)"

const (
	benchmarkReportVersion = 1
	// The evidence action runs the repository runner with two Go package
	// workers; replays schedule a shard's steps the same way.
	benchmarkReplayWorkers = 2
	// Relative job speed comes from shared unchanged top-level tests that ran
	// at least this long on both sides; shorter tests mostly measure fixed
	// per-test costs rather than the job's speed.
	benchmarkSpeedTestFloorSeconds   = 1.0
	benchmarkSpeedFactorMinimumTests = 8
)

// BenchmarkSideOptions names one downloaded Go Race evidence set and the
// commit every result in it must have tested.
type BenchmarkSideOptions struct {
	Directory string
	SHA       string
	// JobName formats the side's GitHub job name from the shard index.
	JobName string
}

type BenchmarkCompareOptions struct {
	A, B BenchmarkSideOptions
	// RepositoryRoot is a checkout that holds both commits; their diff
	// selects the changed packages.
	RepositoryRoot string
	// ChangedTests lists the tests a change targets, each written
	// <import path>.<Test>.
	ChangedTests []string
	// GitHubJobs reads the jobs API for the timings results do not record.
	GitHubJobs bool
}

type BenchmarkComparison struct {
	Version         int                    `json:"version"`
	A               BenchmarkSide          `json:"a"`
	B               BenchmarkSide          `json:"b"`
	PlansIdentical  bool                   `json:"plans_identical"`
	DifferentShards []int                  `json:"different_shards,omitempty"`
	ChangedPackages []string               `json:"changed_packages"`
	ChangedTests    []string               `json:"changed_tests,omitempty"`
	SpeedFactors    []BenchmarkSpeedFactor `json:"speed_factors"`
	Tests           []BenchmarkTest        `json:"tests"`
	Replay          []BenchmarkReplayShard `json:"replay"`
	Metrics         []BenchmarkMetric      `json:"metrics"`
	Missing         []string               `json:"missing,omitempty"`
	Warnings        []string               `json:"warnings,omitempty"`
}

type BenchmarkSide struct {
	SHA   string `json:"sha"`
	RunID string `json:"run_id,omitempty"`
	// Complete is true when shards 0..K-1 are all present and each ran every
	// planned step.
	Complete bool             `json:"complete"`
	Shards   []BenchmarkShard `json:"shards"`
}

// BenchmarkShard holds one Go Race job's phases in seconds. A nil phase could
// not be computed from the available evidence; the comparison says why.
// Preparation runs from job start to the shard's first test, so it depends on
// which step the plan puts first; FixedPreparation instead adds the time
// before the runner, setup and the shard's longest step compile. RunnerFixed
// is the job's runner time outside the steps phase: before the runner, setup,
// teardown and upload.
type BenchmarkShard struct {
	Index            int             `json:"index"`
	RunAttempt       string          `json:"run_attempt,omitempty"`
	Status           string          `json:"status"`
	StepsRun         int             `json:"steps_run"`
	StepsPlanned     int             `json:"steps_planned"`
	CPUModel         string          `json:"cpu_model,omitempty"`
	RunnerImage      string          `json:"runner_image,omitempty"`
	BeforeRunner     *float64        `json:"before_runner_s,omitempty"`
	Setup            float64         `json:"setup_s"`
	Preparation      *float64        `json:"preparation_s,omitempty"`
	FixedPreparation *float64        `json:"fixed_preparation_s,omitempty"`
	RunnerFixed      *float64        `json:"runner_fixed_s,omitempty"`
	StepsPhase       *float64        `json:"steps_phase_s,omitempty"`
	Job              *float64        `json:"job_s,omitempty"`
	RunnerMinutes    *float64        `json:"runner_minutes,omitempty"`
	Steps            []BenchmarkStep `json:"steps,omitempty"`
}

type BenchmarkStep struct {
	Package string   `json:"package"`
	Tests   int      `json:"tests"`
	Status  string   `json:"status"`
	Elapsed float64  `json:"elapsed_s"`
	Compile *float64 `json:"compile_s,omitempty"`
}

// BenchmarkSpeedFactor is the speed of B's job relative to A's job: the median
// B/A duration ratio of the unchanged top-level tests that passed in both jobs
// and ran at least one second in each.
type BenchmarkSpeedFactor struct {
	AShard int      `json:"a_shard"`
	BShard int      `json:"b_shard"`
	Factor *float64 `json:"factor,omitempty"`
	Tests  int      `json:"tests"`
}

// BenchmarkTest compares one top-level test both sides ran. Changed marks a
// listed changed test; ChangedPackage marks a test whose package's files
// differ between the commits. Neither supports a speed factor.
type BenchmarkTest struct {
	Package         string   `json:"package"`
	Test            string   `json:"test"`
	Changed         bool     `json:"changed,omitempty"`
	ChangedPackage  bool     `json:"changed_package,omitempty"`
	AShard          int      `json:"a_shard"`
	BShard          int      `json:"b_shard"`
	A               float64  `json:"a_s"`
	B               float64  `json:"b_s"`
	APassed         bool     `json:"a_passed"`
	BPassed         bool     `json:"b_passed"`
	Ratio           *float64 `json:"ratio,omitempty"`
	NormalizedRatio *float64 `json:"normalized_ratio,omitempty"`
}

// BenchmarkReplayShard holds steps phases replayed through one shard's plan:
// the runner's two workers take its steps in plan order, each step costing
// its measured duration. A's durations fill units A never ran from B's.
type BenchmarkReplayShard struct {
	Shard           int      `json:"shard"`
	APlanADurations *float64 `json:"a_plan_a_durations_s,omitempty"`
	BPlanADurations *float64 `json:"b_plan_a_durations_s,omitempty"`
	APlanBDurations *float64 `json:"a_plan_b_durations_s,omitempty"`
}

// BenchmarkMetric is one compared quantity. D = A − B, so a positive D means
// B is faster. Shards holds the per-shard pairs the aggregate is built from.
type BenchmarkMetric struct {
	Name          string               `json:"name"`
	Unit          string               `json:"unit"`
	Aggregate     string               `json:"aggregate"`
	PlanDependent bool                 `json:"plan_dependent,omitempty"`
	A             float64              `json:"a"`
	B             float64              `json:"b"`
	D             float64              `json:"d"`
	Shards        []BenchmarkShardPair `json:"shards,omitempty"`
}

type BenchmarkShardPair struct {
	Shard int     `json:"shard"`
	A     float64 `json:"a"`
	B     float64 `json:"b"`
}

const (
	benchmarkAggregateMean  = "mean"
	benchmarkAggregateMax   = "max"
	benchmarkAggregateSum   = "sum"
	benchmarkAggregateTotal = "total"

	benchmarkChangedTestsMetric = "changed_tests_duration"
)

type benchmarkTestKey struct{ Package, Test string }

type benchmarkTestRun struct {
	shard   int
	seconds float64
	passed  bool
}

type benchmarkStepEvidence struct {
	selection   Selection
	result      StepResult
	report      goTestReport
	testSeconds float64
}

type benchmarkShardEvidence struct {
	result   Result
	plan     []Selection
	steps    []benchmarkStepEvidence
	complete bool
	firstRun time.Time
	job      *workflowJob
}

type benchmarkEvidence struct {
	label  string
	sha    string
	runID  string
	shards map[int]*benchmarkShardEvidence
	tests  map[benchmarkTestKey]benchmarkTestRun
}

type benchmarkNotes struct {
	missing, warnings []string
}

func (notes *benchmarkNotes) miss(format string, arguments ...any) {
	notes.missing = appendUnique(notes.missing, fmt.Sprintf(format, arguments...))
}

func (notes *benchmarkNotes) warn(format string, arguments ...any) {
	notes.warnings = appendUnique(notes.warnings, fmt.Sprintf(format, arguments...))
}

// CompareBenchmarkEvidence compares two Go Race evidence sets: the A and B
// sides of one CI Benchmark run, or two downloaded verification runs.
func CompareBenchmarkEvidence(ctx context.Context, options BenchmarkCompareOptions) (BenchmarkComparison, error) {
	return compareBenchmarkEvidence(ctx, options, commandGitHubClient{})
}

func compareBenchmarkEvidence(ctx context.Context, options BenchmarkCompareOptions, client githubAPIClient) (BenchmarkComparison, error) {
	notes := &benchmarkNotes{}
	a, err := loadBenchmarkEvidence("A", options.A)
	if err != nil {
		return BenchmarkComparison{}, err
	}
	b, err := loadBenchmarkEvidence("B", options.B)
	if err != nil {
		return BenchmarkComparison{}, err
	}
	listed, err := parseBenchmarkChangedTests(options.ChangedTests)
	if err != nil {
		return BenchmarkComparison{}, err
	}
	for name := range listed {
		for _, side := range []*benchmarkEvidence{a, b} {
			if !side.ran(name) {
				return BenchmarkComparison{}, fmt.Errorf("changed test %s did not run on side %s", name, side.label)
			}
		}
	}
	changedPackages, err := benchmarkChangedPackages(options.RepositoryRoot, a, b)
	if err != nil {
		return BenchmarkComparison{}, err
	}
	if options.GitHubJobs {
		attachBenchmarkJobs(ctx, client, a, options.A.JobName, notes)
		attachBenchmarkJobs(ctx, client, b, options.B.JobName, notes)
	} else {
		notes.miss("GitHub jobs data was not read, so before-runner time, preparation, job time and runner-minutes are missing")
	}

	report := BenchmarkComparison{Version: benchmarkReportVersion, ChangedPackages: slices.Sorted(maps.Keys(changedPackages))}
	if report.ChangedPackages == nil {
		report.ChangedPackages = []string{}
	}
	report.ChangedTests = slices.Sorted(maps.Keys(listed))
	report.A = a.report(notes)
	report.B = b.report(notes)
	report.PlansIdentical, report.DifferentShards = benchmarkPlansIdentical(a, b)
	if !report.PlansIdentical {
		notes.warn("shard plans differ (shards %v), so measured per-shard and slowest-job times are reported but not judged", report.DifferentShards)
	}
	compareBenchmarkTests(&report, a, b, listed, changedPackages, notes)
	if report.A.Complete && report.B.Complete {
		sliced := benchmarkSlicedPackages(a, b)
		report.Replay = replayBenchmarkPlans(a, b, a.durations(sliced), b.durations(sliced))
	} else {
		notes.miss("plan replays need complete evidence on both sides")
	}
	report.Metrics = benchmarkMetrics(report, listed, notes)
	sort.Strings(notes.missing)
	sort.Strings(notes.warnings)
	report.Missing, report.Warnings = notes.missing, notes.warnings
	return report, nil
}

func loadBenchmarkEvidence(label string, options BenchmarkSideOptions) (*benchmarkEvidence, error) {
	if options.Directory == "" || options.SHA == "" {
		return nil, fmt.Errorf("side %s needs an evidence directory and the commit it tested", label)
	}
	evidence := &benchmarkEvidence{label: label, sha: options.SHA, shards: map[int]*benchmarkShardEvidence{}, tests: map[benchmarkTestKey]benchmarkTestRun{}}
	attempts := map[int]uint64{}
	err := filepath.WalkDir(options.Directory, func(file string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() || entry.Name() != "result.json" {
			return nil
		}
		// Results are discovered beneath the caller's downloaded evidence root.
		//nolint:gosec
		body, err := os.ReadFile(file)
		if err != nil {
			return err
		}
		var result Result
		if err := json.Unmarshal(body, &result); err != nil {
			return fmt.Errorf("side %s: malformed result %s: %w", label, file, err)
		}
		index, ok := goRaceShardIndex(result.Execution.Producer)
		if !ok {
			return nil
		}
		if result.Plan.Revision.Head != options.SHA {
			return fmt.Errorf("side %s shard %d tested %s; want %s", label, index, result.Plan.Revision.Head, options.SHA)
		}
		if evidence.runID == "" {
			evidence.runID = result.Execution.RunID
		} else if result.Execution.RunID != evidence.runID {
			return fmt.Errorf("side %s holds results of runs %s and %s", label, evidence.runID, result.Execution.RunID)
		}
		// A rerun of failed jobs leaves the earlier attempts' results beside
		// the new ones; the latest attempt of each shard is the evidence.
		attempt, _ := strconv.ParseUint(result.Execution.RunAttempt, 10, 32)
		if previous, exists := attempts[index]; exists {
			if previous == attempt {
				return fmt.Errorf("side %s holds two results for shard %d attempt %d", label, index, attempt)
			}
			if previous > attempt {
				return nil
			}
		}
		shard, err := loadBenchmarkShard(filepath.Dir(file), result)
		if err != nil {
			return fmt.Errorf("side %s shard %d: %w", label, index, err)
		}
		attempts[index] = attempt
		evidence.shards[index] = shard
		return nil
	})
	if err != nil {
		return nil, err
	}
	if len(evidence.shards) == 0 {
		return nil, fmt.Errorf("side %s: no Go Race results under %s", label, options.Directory)
	}
	for _, index := range evidence.shardIndexes() {
		for _, step := range evidence.shards[index].steps {
			for _, test := range step.report.tests {
				evidence.tests[benchmarkTestKey{Package: test.pkg, Test: test.name}] = benchmarkTestRun{shard: index, seconds: test.elapsed, passed: test.passed}
			}
		}
	}
	return evidence, nil
}

func (evidence *benchmarkEvidence) ran(name string) bool {
	separator := strings.LastIndex(name, ".")
	_, ran := evidence.tests[benchmarkTestKey{Package: name[:separator], Test: name[separator+1:]}]
	return ran
}

// Go Race shards are the producers go-0 … go-(K-1); go-static is not one.
func goRaceShardIndex(producer string) (int, bool) {
	digits, ok := strings.CutPrefix(producer, "go-")
	if !ok {
		return 0, false
	}
	index, err := strconv.Atoi(digits)
	if err != nil || index < 0 || strconv.Itoa(index) != digits {
		return 0, false
	}
	return index, true
}

func loadBenchmarkShard(directory string, result Result) (*benchmarkShardEvidence, error) {
	shard := &benchmarkShardEvidence{result: result}
	for _, selection := range result.Plan.Selections {
		if selection.Group == "go" && len(selection.Packages) == 1 {
			shard.plan = append(shard.plan, selection)
		}
	}
	var steps []StepResult
	for _, step := range result.Steps {
		if step.Group == "go" {
			steps = append(steps, step)
		}
	}
	// The runner reports one step per planned Go selection, in plan order,
	// unless dependency setup failed before any step started.
	shard.complete = len(steps) > 0 && len(steps) == len(shard.plan)
	if !shard.complete {
		return shard, nil
	}
	for position, step := range steps {
		selection := shard.plan[position]
		if len(step.Command) == 0 || step.Command[len(step.Command)-1] != selection.Packages[0] {
			return nil, fmt.Errorf("step %d does not run its planned package %s", position, selection.Packages[0])
		}
		evidence := benchmarkStepEvidence{selection: selection, result: step}
		if step.Artifact != "" {
			report, err := readGoTestReport(filepath.Join(directory, step.Artifact))
			if err != nil {
				return nil, err
			}
			evidence.report = report
		}
		for _, test := range evidence.report.tests {
			evidence.testSeconds += test.elapsed
		}
		if first := evidence.report.firstRun; !first.IsZero() && (shard.firstRun.IsZero() || first.Before(shard.firstRun)) {
			shard.firstRun = first
		}
		shard.steps = append(shard.steps, evidence)
	}
	return shard, nil
}

func (evidence *benchmarkEvidence) shardIndexes() []int {
	return slices.Sorted(maps.Keys(evidence.shards))
}

func (evidence *benchmarkEvidence) complete() bool {
	for position, index := range evidence.shardIndexes() {
		if index != position || !evidence.shards[index].complete {
			return false
		}
	}
	return true
}

func attachBenchmarkJobs(ctx context.Context, client githubAPIClient, evidence *benchmarkEvidence, template string, notes *benchmarkNotes) {
	if template == "" {
		template = DefaultBenchmarkJobName
	}
	type listing struct {
		jobs []workflowJob
		err  error
	}
	listings := map[string]listing{}
	for _, index := range evidence.shardIndexes() {
		shard := evidence.shards[index]
		execution := shard.result.Execution
		runID, runErr := strconv.ParseInt(execution.RunID, 10, 64)
		attempt, attemptErr := strconv.Atoi(execution.RunAttempt)
		if execution.Repository == "" || runErr != nil || attemptErr != nil {
			notes.miss("side %s results name no GitHub run, so its job timings are missing", evidence.label)
			continue
		}
		key := execution.RunID + "/" + execution.RunAttempt
		listed, read := listings[key]
		if !read {
			listed.jobs, listed.err = readAttemptJobs(ctx, client, execution.Repository, runID, attempt)
			listings[key] = listed
		}
		if listed.err != nil {
			notes.miss("side %s: jobs of run %s attempt %s are unavailable (%v), so their timings are missing", evidence.label, execution.RunID, execution.RunAttempt, listed.err)
			continue
		}
		name := fmt.Sprintf(template, index)
		var matches []workflowJob
		for _, job := range listed.jobs {
			if job.Name == name {
				matches = append(matches, job)
			}
		}
		if len(matches) != 1 || matches[0].Status != "completed" || matches[0].StartedAt.IsZero() || matches[0].CompletedAt.Before(matches[0].StartedAt) {
			notes.miss("side %s shard %d: run %s attempt %s has no single completed job named %q", evidence.label, index, execution.RunID, execution.RunAttempt, name)
			continue
		}
		shard.job = &matches[0]
	}
}

func (evidence *benchmarkEvidence) report(notes *benchmarkNotes) BenchmarkSide {
	side := BenchmarkSide{SHA: evidence.sha, RunID: evidence.runID, Complete: evidence.complete()}
	if !side.Complete {
		notes.warn("side %s is incomplete: a shard is missing or did not run all its planned steps", evidence.label)
	}
	for _, index := range evidence.shardIndexes() {
		shard := evidence.shards[index]
		result := shard.result
		report := BenchmarkShard{
			Index: index, RunAttempt: result.Execution.RunAttempt, Status: result.Status,
			StepsRun: len(shard.steps), StepsPlanned: len(shard.plan),
			CPUModel: result.Execution.CPUModel, RunnerImage: result.Execution.RunnerImage,
			Setup: benchmarkSeconds(result.Setup), StepsPhase: shard.stepsPhase(),
		}
		if report.CPUModel == "" || report.RunnerImage == "" {
			notes.miss("side %s results do not record the runner CPU model or image", evidence.label)
		}
		if job := shard.job; job != nil {
			if !result.StartedAt.IsZero() {
				report.BeforeRunner = benchmarkPointer(benchmarkSeconds(result.StartedAt.Sub(job.StartedAt)))
			}
			if !shard.firstRun.IsZero() {
				report.Preparation = benchmarkPointer(benchmarkSeconds(shard.firstRun.Sub(job.StartedAt)))
			}
			report.Job = benchmarkPointer(benchmarkSeconds(job.CompletedAt.Sub(job.StartedAt)))
			firstStep := job.StartedAt
			if len(job.Steps) > 0 && !job.Steps[0].StartedAt.IsZero() {
				firstStep = job.Steps[0].StartedAt
			}
			report.RunnerMinutes = benchmarkPointer(roundBenchmark(job.CompletedAt.Sub(firstStep).Minutes(), 1000))
			if report.StepsPhase != nil {
				report.RunnerFixed = benchmarkPointer(roundBenchmark(job.CompletedAt.Sub(firstStep).Seconds()-*report.StepsPhase, 1000))
			}
		}
		var longestCompile *float64
		for _, step := range shard.steps {
			item := BenchmarkStep{Package: step.selection.Packages[0], Tests: len(step.report.tests), Status: step.result.Status, Elapsed: benchmarkSeconds(step.result.Elapsed)}
			if step.result.StartedAt.IsZero() {
				notes.miss("side %s results predate step timestamps, so per-step compile times are missing", evidence.label)
			} else if !step.report.firstRun.IsZero() {
				item.Compile = benchmarkPointer(benchmarkSeconds(step.report.firstRun.Sub(step.result.StartedAt)))
				if longestCompile == nil || *item.Compile > *longestCompile {
					longestCompile = item.Compile
				}
			}
			report.Steps = append(report.Steps, item)
		}
		if report.BeforeRunner != nil && longestCompile != nil {
			report.FixedPreparation = benchmarkPointer(roundBenchmark(*report.BeforeRunner+report.Setup+*longestCompile, 1000))
		}
		side.Shards = append(side.Shards, report)
	}
	return side
}

// stepsPhase spans the shard's Go steps: first step start to last step end.
func (shard *benchmarkShardEvidence) stepsPhase() *float64 {
	if !shard.complete {
		return nil
	}
	var first, last time.Time
	for _, step := range shard.steps {
		if step.result.StartedAt.IsZero() || step.result.FinishedAt.IsZero() {
			// Results without step timestamps run every step between the end
			// of dependency setup and the start of teardown.
			result := shard.result
			return benchmarkPointer(benchmarkSeconds(result.FinishedAt.Sub(result.StartedAt) - result.Setup - result.Teardown))
		}
		if first.IsZero() || step.result.StartedAt.Before(first) {
			first = step.result.StartedAt
		}
		if step.result.FinishedAt.After(last) {
			last = step.result.FinishedAt
		}
	}
	return benchmarkPointer(benchmarkSeconds(last.Sub(first)))
}

func benchmarkPlansIdentical(a, b *benchmarkEvidence) (bool, []int) {
	var different []int
	for _, index := range benchmarkShardUnion(a, b) {
		left, right := a.shards[index], b.shards[index]
		if left == nil || right == nil || !slices.Equal(benchmarkPlanIdentity(left.plan), benchmarkPlanIdentity(right.plan)) {
			different = append(different, index)
		}
	}
	return len(different) == 0, different
}

func benchmarkShardUnion(a, b *benchmarkEvidence) []int {
	indexes := slices.Concat(a.shardIndexes(), b.shardIndexes())
	sort.Ints(indexes)
	return slices.Compact(indexes)
}

func benchmarkPlanIdentity(plan []Selection) []string {
	identity := make([]string, len(plan))
	for index, selection := range plan {
		identity[index] = benchmarkSelectionIdentity(selection)
	}
	return identity
}

func benchmarkSelectionIdentity(selection Selection) string {
	tests := slices.Clone(selection.Tests)
	sort.Strings(tests)
	return selection.Packages[0] + "\x00" + strings.Join(tests, "\x00")
}

// benchmarkDurations prices a Go selection from one side's measurements. A
// whole package measured with the same tests costs that step's elapsed time.
// Any other selection, including every slice of a package a plan splits
// across shards, costs its tests' durations plus the overhead (time outside
// top-level tests, mostly compile) of that package's step in the same shard,
// or the package's mean overhead when that shard ran no step of it.
type benchmarkDurations struct {
	steps    map[string]float64
	tests    map[benchmarkTestKey]float64
	overhead map[benchmarkShardPackage]float64
	mean     map[string]float64
}

type benchmarkShardPackage struct {
	shard int
	name  string
}

func (evidence *benchmarkEvidence) durations(sliced map[string]bool) benchmarkDurations {
	durations := benchmarkDurations{steps: map[string]float64{}, tests: map[benchmarkTestKey]float64{}, overhead: map[benchmarkShardPackage]float64{}, mean: map[string]float64{}}
	overheads := map[string][]float64{}
	for index, shard := range evidence.shards {
		for _, step := range shard.steps {
			name := step.selection.Packages[0]
			elapsed := step.result.Elapsed.Seconds()
			if !sliced[name] {
				durations.steps[benchmarkSelectionIdentity(step.selection)] = elapsed
			}
			for _, test := range step.report.tests {
				durations.tests[benchmarkTestKey{Package: test.pkg, Test: test.name}] = test.elapsed
			}
			overhead := max(0, elapsed-step.testSeconds)
			durations.overhead[benchmarkShardPackage{shard: index, name: name}] = overhead
			overheads[name] = append(overheads[name], overhead)
		}
	}
	for name, values := range overheads {
		durations.mean[name] = benchmarkMean(values)
	}
	return durations
}

// benchmarkSlicedPackages lists the packages either side's plan splits into
// more than one selection.
func benchmarkSlicedPackages(sides ...*benchmarkEvidence) map[string]bool {
	sliced := map[string]bool{}
	for _, side := range sides {
		selections := map[string]int{}
		for _, shard := range side.shards {
			for _, selection := range shard.plan {
				selections[selection.Packages[0]]++
			}
		}
		for name, count := range selections {
			if count > 1 {
				sliced[name] = true
			}
		}
	}
	return sliced
}

// withFallback fills the units these durations never measured from fallback.
// All overheads of one package come from the side that ran it.
func (durations benchmarkDurations) withFallback(fallback benchmarkDurations) benchmarkDurations {
	merged := benchmarkDurations{steps: maps.Clone(fallback.steps), tests: maps.Clone(fallback.tests), overhead: map[benchmarkShardPackage]float64{}, mean: maps.Clone(fallback.mean)}
	maps.Copy(merged.steps, durations.steps)
	maps.Copy(merged.tests, durations.tests)
	maps.Copy(merged.mean, durations.mean)
	for key, value := range fallback.overhead {
		if _, measured := durations.mean[key.name]; !measured {
			merged.overhead[key] = value
		}
	}
	maps.Copy(merged.overhead, durations.overhead)
	return merged
}

func (durations benchmarkDurations) selection(shard int, selection Selection) float64 {
	if elapsed, ok := durations.steps[benchmarkSelectionIdentity(selection)]; ok {
		return elapsed
	}
	name := selection.Packages[0]
	total, ok := durations.overhead[benchmarkShardPackage{shard: shard, name: name}]
	if !ok {
		total = durations.mean[name]
	}
	for _, test := range selection.Tests {
		total += durations.tests[benchmarkTestKey{Package: name, Test: test}]
	}
	return total
}

func replayBenchmarkSelections(shard int, plan []Selection, durations benchmarkDurations) float64 {
	workers := make([]float64, benchmarkReplayWorkers)
	for _, selection := range plan {
		next := 0
		for index := 1; index < len(workers); index++ {
			if workers[index] < workers[next] {
				next = index
			}
		}
		workers[next] += durations.selection(shard, selection)
	}
	return roundBenchmark(slices.Max(workers), 1000)
}

func replayBenchmarkPlans(a, b *benchmarkEvidence, aDurations, bDurations benchmarkDurations) []BenchmarkReplayShard {
	aFirst := aDurations.withFallback(bDurations)
	var replays []BenchmarkReplayShard
	for _, index := range benchmarkShardUnion(a, b) {
		replay := BenchmarkReplayShard{Shard: index}
		if shard := a.shards[index]; shard != nil {
			replay.APlanADurations = benchmarkPointer(replayBenchmarkSelections(index, shard.plan, aFirst))
			replay.APlanBDurations = benchmarkPointer(replayBenchmarkSelections(index, shard.plan, bDurations))
		}
		if shard := b.shards[index]; shard != nil {
			replay.BPlanADurations = benchmarkPointer(replayBenchmarkSelections(index, shard.plan, aFirst))
		}
		replays = append(replays, replay)
	}
	return replays
}

func compareBenchmarkTests(report *BenchmarkComparison, a, b *benchmarkEvidence, listed, changedPackages map[string]bool, notes *benchmarkNotes) {
	ratios := map[[2]int][]float64{}
	pairs := map[[2]int]bool{}
	for key, left := range a.tests {
		right, shared := b.tests[key]
		if !shared {
			continue
		}
		pair := [2]int{left.shard, right.shard}
		pairs[pair] = true
		unchanged := !listed[key.Package+"."+key.Test] && !changedPackages[key.Package]
		if unchanged && left.passed && right.passed && left.seconds >= benchmarkSpeedTestFloorSeconds && right.seconds >= benchmarkSpeedTestFloorSeconds {
			ratios[pair] = append(ratios[pair], right.seconds/left.seconds)
		}
	}
	factors := map[[2]int]float64{}
	for pair := range pairs {
		factor := BenchmarkSpeedFactor{AShard: pair[0], BShard: pair[1], Tests: len(ratios[pair])}
		if factor.Tests >= benchmarkSpeedFactorMinimumTests {
			factors[pair] = benchmarkMedian(ratios[pair])
			factor.Factor = benchmarkPointer(roundBenchmark(factors[pair], 10000))
		}
		report.SpeedFactors = append(report.SpeedFactors, factor)
	}
	sort.Slice(report.SpeedFactors, func(i, j int) bool {
		left, right := report.SpeedFactors[i], report.SpeedFactors[j]
		return left.AShard < right.AShard || left.AShard == right.AShard && left.BShard < right.BShard
	})
	for key, left := range a.tests {
		right, shared := b.tests[key]
		changed := listed[key.Package+"."+key.Test]
		if !shared || !changed && max(left.seconds, right.seconds) < benchmarkSpeedTestFloorSeconds {
			continue
		}
		test := BenchmarkTest{
			Package: key.Package, Test: key.Test, Changed: changed, ChangedPackage: changedPackages[key.Package], AShard: left.shard, BShard: right.shard,
			A: roundBenchmark(left.seconds, 1000), B: roundBenchmark(right.seconds, 1000), APassed: left.passed, BPassed: right.passed,
		}
		if left.seconds > 0 {
			ratio := right.seconds / left.seconds
			test.Ratio = benchmarkPointer(roundBenchmark(ratio, 10000))
			if factor, known := factors[[2]int{left.shard, right.shard}]; known {
				test.NormalizedRatio = benchmarkPointer(roundBenchmark(ratio/factor, 10000))
			} else {
				notes.warn("A shard %d / B shard %d share fewer than %d speed tests, so their test ratios stay unnormalized", left.shard, right.shard, benchmarkSpeedFactorMinimumTests)
			}
		}
		report.Tests = append(report.Tests, test)
	}
	sort.Slice(report.Tests, func(i, j int) bool {
		left, right := report.Tests[i], report.Tests[j]
		return left.Package < right.Package || left.Package == right.Package && left.Test < right.Test
	})
}

func benchmarkMetrics(report BenchmarkComparison, listed map[string]bool, notes *benchmarkNotes) []BenchmarkMetric {
	var metrics []BenchmarkMetric
	perShard := func(name, unit, aggregate string, planDependent bool, value func(BenchmarkShard) *float64) {
		metric, ok := benchmarkShardMetric(name, unit, aggregate, report.A.Shards, report.B.Shards, value)
		if !ok {
			notes.miss("metric %s is missing because a shard lacks its value", name)
			return
		}
		metric.PlanDependent = planDependent
		metrics = append(metrics, metric)
	}
	perShard("setup", "s", benchmarkAggregateMean, false, func(shard BenchmarkShard) *float64 { return &shard.Setup })
	perShard("fixed_preparation", "s", benchmarkAggregateMean, false, func(shard BenchmarkShard) *float64 { return shard.FixedPreparation })
	perShard("slowest_steps_phase", "s", benchmarkAggregateMax, true, func(shard BenchmarkShard) *float64 { return shard.StepsPhase })
	perShard("slowest_job", "s", benchmarkAggregateMax, true, func(shard BenchmarkShard) *float64 { return shard.Job })
	perShard("go_race_runner_minutes", "min", benchmarkAggregateSum, true, func(shard BenchmarkShard) *float64 { return shard.RunnerMinutes })

	if len(report.Replay) > 0 {
		replayed := func(field func(BenchmarkReplayShard) *float64) map[int]float64 {
			values := map[int]float64{}
			for _, replay := range report.Replay {
				if value := field(replay); value != nil {
					values[replay.Shard] = *value
				}
			}
			return values
		}
		aPlanA := replayed(func(replay BenchmarkReplayShard) *float64 { return replay.APlanADurations })
		metrics = append(metrics, benchmarkReplayMetric("replayed_slowest_steps_phase", aPlanA, replayed(func(replay BenchmarkReplayShard) *float64 { return replay.BPlanADurations }), nil, nil))
		aFixed, aOK := benchmarkFixedTime(report.A.Shards)
		bFixed, bOK := benchmarkFixedTime(report.B.Shards)
		aPlanB := replayed(func(replay BenchmarkReplayShard) *float64 { return replay.APlanBDurations })
		if aOK && bOK {
			metrics = append(metrics, benchmarkReplayMetric("plan_fixed_slowest_job", aPlanA, aPlanB, aFixed, bFixed))
		} else {
			notes.miss("metric plan_fixed_slowest_job is missing because a shard lacks its before-runner time")
		}
		if metric, ok := benchmarkPlanFixedRunnerMinutes(report, aPlanA, aPlanB); ok {
			metrics = append(metrics, metric)
		} else {
			notes.miss("metric plan_fixed_runner_minutes is missing because a shard lacks its runner time")
		}
	}

	if len(listed) > 0 {
		metric, err := benchmarkChangedTestsDuration(report, listed)
		if err != nil {
			notes.miss("metric %s is missing: %v", benchmarkChangedTestsMetric, err)
		} else {
			metrics = append(metrics, metric)
		}
	}
	return metrics
}

// benchmarkChangedTestsDuration sums the listed tests' A durations against
// their B durations divided by the speed factor of the two jobs each ran in.
func benchmarkChangedTestsDuration(comparison BenchmarkComparison, listed map[string]bool) (BenchmarkMetric, error) {
	factors := map[[2]int]float64{}
	for _, factor := range comparison.SpeedFactors {
		if factor.Factor != nil {
			factors[[2]int{factor.AShard, factor.BShard}] = *factor.Factor
		}
	}
	found := map[string]bool{}
	var a, b float64
	for _, test := range comparison.Tests {
		name := test.Package + "." + test.Test
		if !listed[name] {
			continue
		}
		if !test.APassed || !test.BPassed {
			return BenchmarkMetric{}, fmt.Errorf("changed test %s did not pass on both sides", name)
		}
		factor, known := factors[[2]int{test.AShard, test.BShard}]
		if !known {
			return BenchmarkMetric{}, fmt.Errorf("changed test %s ran in jobs without a speed factor", name)
		}
		found[name] = true
		a += test.A
		b += test.B / factor
	}
	for name := range listed {
		if !found[name] {
			return BenchmarkMetric{}, fmt.Errorf("changed test %s is not in the comparison, which keeps a test under 1 s only when made with --changed-tests", name)
		}
	}
	return BenchmarkMetric{
		Name: benchmarkChangedTestsMetric, Unit: "s", Aggregate: benchmarkAggregateTotal,
		A: roundBenchmark(a, 1000), B: roundBenchmark(b, 1000), D: roundBenchmark(a-b, 1000),
	}, nil
}

func benchmarkShardMetric(name, unit, aggregate string, aShards, bShards []BenchmarkShard, value func(BenchmarkShard) *float64) (BenchmarkMetric, bool) {
	collect := func(shards []BenchmarkShard) (map[int]float64, bool) {
		values := map[int]float64{}
		for _, shard := range shards {
			item := value(shard)
			if item == nil {
				return nil, false
			}
			values[shard.Index] = *item
		}
		return values, len(values) > 0
	}
	aValues, aOK := collect(aShards)
	bValues, bOK := collect(bShards)
	if !aOK || !bOK {
		return BenchmarkMetric{}, false
	}
	metric := BenchmarkMetric{Name: name, Unit: unit, Aggregate: aggregate, A: benchmarkAggregate(aggregate, aValues), B: benchmarkAggregate(aggregate, bValues)}
	metric.D = roundBenchmark(metric.A-metric.B, 1000)
	metric.Shards = benchmarkShardPairs(aValues, bValues)
	return metric, true
}

// benchmarkReplayMetric compares each side's slowest replayed steps phase. A
// side's fixed map adds each shard's before-runner and setup time to that
// shard's pair, and the mean of that time to the side's aggregate.
func benchmarkReplayMetric(name string, aReplay, bReplay, aFixed, bFixed map[int]float64) BenchmarkMetric {
	side := func(replay, fixed map[int]float64) (float64, map[int]float64) {
		if fixed == nil {
			return benchmarkAggregate(benchmarkAggregateMax, replay), replay
		}
		pairs := map[int]float64{}
		for shard, value := range replay {
			if offset, ok := fixed[shard]; ok {
				pairs[shard] = value + offset
			}
		}
		return roundBenchmark(benchmarkAggregate(benchmarkAggregateMax, replay)+benchmarkAggregate(benchmarkAggregateMean, fixed), 1000), pairs
	}
	metric := BenchmarkMetric{Name: name, Unit: "s", Aggregate: benchmarkAggregateMax}
	var aPairs, bPairs map[int]float64
	metric.A, aPairs = side(aReplay, aFixed)
	metric.B, bPairs = side(bReplay, bFixed)
	metric.D = roundBenchmark(metric.A-metric.B, 1000)
	metric.Shards = benchmarkShardPairs(aPairs, bPairs)
	return metric
}

// benchmarkPlanFixedRunnerMinutes prices each side's Go Race jobs without its
// own shard plan: its durations replayed through A's shard plan plus the
// runner time outside the steps phase of every job the side ran, so a side
// that runs more shards pays for each. Per-shard pairs exist only when both
// sides ran the same shards.
func benchmarkPlanFixedRunnerMinutes(report BenchmarkComparison, aReplay, bReplay map[int]float64) (BenchmarkMetric, bool) {
	side := func(shards []BenchmarkShard, replay map[int]float64) (map[int]float64, float64, bool) {
		minutes, total := map[int]float64{}, 0.0
		for _, shard := range shards {
			if shard.RunnerFixed == nil {
				return nil, 0, false
			}
			minutes[shard.Index] = *shard.RunnerFixed / 60
			total += *shard.RunnerFixed / 60
		}
		for shard, seconds := range replay {
			minutes[shard] += seconds / 60
			total += seconds / 60
		}
		return minutes, total, true
	}
	if len(aReplay) == 0 || len(bReplay) != len(aReplay) {
		return BenchmarkMetric{}, false
	}
	aShards, aTotal, aOK := side(report.A.Shards, aReplay)
	bShards, bTotal, bOK := side(report.B.Shards, bReplay)
	if !aOK || !bOK {
		return BenchmarkMetric{}, false
	}
	metric := BenchmarkMetric{Name: "plan_fixed_runner_minutes", Unit: "min", Aggregate: benchmarkAggregateSum, A: roundBenchmark(aTotal, 1000), B: roundBenchmark(bTotal, 1000)}
	metric.D = roundBenchmark(metric.A-metric.B, 1000)
	if slices.Equal(slices.Sorted(maps.Keys(aShards)), slices.Sorted(maps.Keys(bShards))) {
		metric.Shards = benchmarkShardPairs(aShards, bShards)
	}
	return metric, true
}

// benchmarkFixedTime returns each shard's before-runner plus setup time.
func benchmarkFixedTime(shards []BenchmarkShard) (map[int]float64, bool) {
	fixed := map[int]float64{}
	for _, shard := range shards {
		if shard.BeforeRunner == nil {
			return nil, false
		}
		fixed[shard.Index] = *shard.BeforeRunner + shard.Setup
	}
	return fixed, len(fixed) > 0
}

func benchmarkShardPairs(a, b map[int]float64) []BenchmarkShardPair {
	var pairs []BenchmarkShardPair
	for _, shard := range slices.Sorted(maps.Keys(a)) {
		if right, ok := b[shard]; ok {
			pairs = append(pairs, BenchmarkShardPair{Shard: shard, A: roundBenchmark(a[shard], 1000), B: roundBenchmark(right, 1000)})
		}
	}
	return pairs
}

func benchmarkAggregate(aggregate string, values map[int]float64) float64 {
	list := slices.Collect(maps.Values(values))
	switch aggregate {
	case benchmarkAggregateMax:
		return roundBenchmark(slices.Max(list), 1000)
	case benchmarkAggregateSum:
		total := 0.0
		for _, value := range list {
			total += value
		}
		return roundBenchmark(total, 1000)
	default:
		return roundBenchmark(benchmarkMean(list), 1000)
	}
}

func benchmarkChangedPackages(root string, a, b *benchmarkEvidence) (map[string]bool, error) {
	changed := map[string]bool{}
	if a.sha == b.sha {
		return changed, nil
	}
	modulePath, err := benchmarkModulePath(root)
	if err != nil {
		return nil, err
	}
	directories := map[string]string{}
	for _, evidence := range []*benchmarkEvidence{a, b} {
		for _, shard := range evidence.shards {
			for _, selection := range shard.plan {
				name := selection.Packages[0]
				if name == modulePath {
					directories["."] = name
				} else if relative, ok := strings.CutPrefix(name, modulePath+"/"); ok {
					directories[relative] = name
				}
			}
		}
	}
	output, err := gitOutput(root, "diff", "--name-only", "--no-renames", a.sha, b.sha, "--")
	if err != nil {
		return nil, fmt.Errorf("list files changed between the two commits: %w", err)
	}
	// A file belongs to the nearest enclosing package directory, so testdata
	// and embedded files count for the package that reads them.
	for _, file := range strings.Split(output, "\n") {
		if file == "" {
			continue
		}
		for directory := path.Dir(file); ; directory = path.Dir(directory) {
			if name, ok := directories[directory]; ok {
				changed[name] = true
				break
			}
			if directory == "." {
				break
			}
		}
	}
	return changed, nil
}

func parseBenchmarkChangedTests(entries []string) (map[string]bool, error) {
	tests := map[string]bool{}
	for _, entry := range entries {
		separator := strings.LastIndex(entry, ".")
		if separator <= 0 || separator == len(entry)-1 || !strings.Contains(entry[:separator], "/") {
			return nil, fmt.Errorf("changed test %q is not <import path>.<Test>", entry)
		}
		tests[entry] = true
	}
	return tests, nil
}

func benchmarkModulePath(root string) (string, error) {
	// go.mod is read from the caller's repository checkout.
	//nolint:gosec
	body, err := os.ReadFile(filepath.Join(root, "go.mod"))
	if err != nil {
		return "", fmt.Errorf("read the module path: %w", err)
	}
	for _, line := range strings.Split(string(body), "\n") {
		if name, ok := strings.CutPrefix(strings.TrimSpace(line), "module "); ok {
			return strings.Trim(strings.TrimSpace(name), `"`), nil
		}
	}
	return "", fmt.Errorf("go.mod declares no module path")
}

func benchmarkSeconds(duration time.Duration) float64 {
	return roundBenchmark(duration.Seconds(), 1000)
}

func roundBenchmark(value, scale float64) float64 {
	return math.Round(value*scale) / scale
}

func benchmarkPointer(value float64) *float64 {
	return &value
}

func benchmarkMean(values []float64) float64 {
	if len(values) == 0 {
		return 0
	}
	total := 0.0
	for _, value := range values {
		total += value
	}
	return total / float64(len(values))
}

func benchmarkMedian(values []float64) float64 {
	sorted := slices.Clone(values)
	sort.Float64s(sorted)
	middle := len(sorted) / 2
	if len(sorted)%2 == 1 {
		return sorted[middle]
	}
	return (sorted[middle-1] + sorted[middle]) / 2
}
