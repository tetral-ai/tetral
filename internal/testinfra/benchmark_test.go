package testinfra

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"
)

type benchmarkFixtureTest struct {
	name    string
	seconds float64
	failed  bool
}

type benchmarkFixtureStep struct {
	pkg              string
	tests            []benchmarkFixtureTest
	elapsed, compile float64
}

type benchmarkFixtureSide struct {
	sha, runID, attempt string
	// legacy results come from a runner without step timestamps or host fields.
	legacy bool
	shards [][]benchmarkFixtureStep
}

var benchmarkFixtureJobStart = time.Date(2026, 10, 10, 6, 0, 0, 0, time.UTC)

func benchmarkFixtureSeconds(value float64) time.Duration {
	return time.Duration(value * float64(time.Second))
}

func benchmarkFixtureTests(prefix string, count int, seconds float64) []benchmarkFixtureTest {
	tests := make([]benchmarkFixtureTest, count)
	for index := range tests {
		tests[index] = benchmarkFixtureTest{name: fmt.Sprintf("%s%02d", prefix, index), seconds: seconds}
	}
	return tests
}

// writeBenchmarkSide writes one downloaded evidence set. A shard's job starts
// at benchmarkFixtureJobStart, its runner 60 s later, dependency setup takes
// 100 s and its steps run one after another.
func writeBenchmarkSide(t *testing.T, side benchmarkFixtureSide) string {
	t.Helper()
	root := t.TempDir()
	for index, steps := range side.shards {
		directory := filepath.Join(root, fmt.Sprintf("bench-go-%d-%s-%s", index, side.runID, side.attempt))
		if err := os.MkdirAll(directory, 0o700); err != nil {
			t.Fatal(err)
		}
		started := benchmarkFixtureJobStart.Add(60 * time.Second)
		result := Result{
			Plan:      Plan{Profile: ProfileFull, Revision: Revision{Head: side.sha}},
			Execution: ExecutionEnvelope{Repository: "owner/repo", RunID: side.runID, RunAttempt: side.attempt, Producer: fmt.Sprintf("go-%d", index)},
			StartedAt: started, Status: "pass", Setup: 100 * time.Second, Teardown: time.Second,
		}
		if !side.legacy {
			result.Execution.CPUModel, result.Execution.RunnerImage = "Fixture CPU", "ubuntu24/1"
		}
		clock := started.Add(result.Setup)
		for position, step := range steps {
			selection := Selection{Group: "go", Packages: []string{step.pkg}, Mode: "race"}
			var events []string
			eventClock := clock.Add(benchmarkFixtureSeconds(step.compile))
			for _, test := range step.tests {
				selection.Tests = append(selection.Tests, test.name)
				events = append(events, benchmarkFixtureEvent(t, eventClock, "run", step.pkg, test.name, 0))
				eventClock = eventClock.Add(benchmarkFixtureSeconds(test.seconds))
				action := "pass"
				if test.failed {
					action = "fail"
				}
				events = append(events, benchmarkFixtureEvent(t, eventClock, action, step.pkg, test.name, test.seconds))
			}
			artifact := fmt.Sprintf("step-%d.jsonl", position)
			writeTestFile(t, directory, artifact, strings.Join(events, "\n")+"\n")
			report := StepResult{Group: "go", Command: []string{"go", "test", "-json", step.pkg}, Status: "pass", Elapsed: benchmarkFixtureSeconds(step.elapsed), Artifact: artifact}
			if !side.legacy {
				report.StartedAt, report.FinishedAt = clock, clock.Add(report.Elapsed)
			}
			result.Plan.Selections = append(result.Plan.Selections, selection)
			result.Steps = append(result.Steps, report)
			clock = clock.Add(report.Elapsed)
		}
		result.FinishedAt = clock.Add(result.Teardown)
		body, err := json.Marshal(result)
		if err != nil {
			t.Fatal(err)
		}
		writeTestFile(t, directory, "result.json", string(body))
	}
	return root
}

func benchmarkFixtureEvent(t *testing.T, at time.Time, action, pkg, test string, elapsed float64) string {
	t.Helper()
	body, err := json.Marshal(map[string]any{"Time": at, "Action": action, "Package": pkg, "Test": test, "Elapsed": elapsed})
	if err != nil {
		t.Fatal(err)
	}
	return string(body)
}

// benchmarkFixtureGitHub answers the jobs API from fixed responses.
type benchmarkFixtureGitHub map[string]any

func (fixture benchmarkFixtureGitHub) JSON(_ context.Context, endpoint string, value any) error {
	response, ok := fixture[endpoint]
	if !ok {
		return fmt.Errorf("no response for %s", endpoint)
	}
	body, err := json.Marshal(response)
	if err != nil {
		return err
	}
	return json.Unmarshal(body, value)
}

// benchmarkFixtureJobs lists one completed job per shard that finishes 5 s
// after its runner and whose first step starts 1 s after the job.
func benchmarkFixtureJobs(t *testing.T, side benchmarkFixtureSide, template string, client benchmarkFixtureGitHub) {
	t.Helper()
	var jobs []workflowJob
	for index, steps := range side.shards {
		finished := benchmarkFixtureJobStart.Add(60*time.Second + 100*time.Second + time.Second + 5*time.Second)
		for _, step := range steps {
			finished = finished.Add(benchmarkFixtureSeconds(step.elapsed))
		}
		jobs = append(jobs, workflowJob{
			Name: fmt.Sprintf(template, index), Status: "completed", Conclusion: "success",
			StartedAt: benchmarkFixtureJobStart, CompletedAt: finished,
			Steps: []workflowJobStep{{Name: "Set up job", StartedAt: benchmarkFixtureJobStart.Add(time.Second)}},
		})
	}
	endpoint := fmt.Sprintf("repos/owner/repo/actions/runs/%s/attempts/%s/jobs?per_page=100", side.runID, side.attempt)
	if existing, ok := client[endpoint].(map[string]any); ok {
		jobs = append(existing["jobs"].([]workflowJob), jobs...)
	}
	client[endpoint] = map[string]any{"jobs": jobs}
}

func TestCompareBenchmarkEvidence(t *testing.T) {
	const (
		shaA, shaB = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
		fast       = "example.test/m/p/fast"
		changed    = "example.test/m/p/changed"
	)
	speedSide := func(sha string, testSeconds float64, speedTests int) benchmarkFixtureSide {
		return benchmarkFixtureSide{sha: sha, runID: "7", attempt: "1", shards: [][]benchmarkFixtureStep{{
			{pkg: fast, tests: benchmarkFixtureTests("TestFast", speedTests, testSeconds), elapsed: float64(speedTests)*testSeconds + 3, compile: 2},
			{pkg: changed, tests: []benchmarkFixtureTest{{name: "TestChanged", seconds: 10}}, elapsed: 12, compile: 1.5},
		}}}
	}
	whole := func(name string, seconds float64) benchmarkFixtureStep {
		return benchmarkFixtureStep{pkg: "example.test/m/p/" + name, tests: []benchmarkFixtureTest{{name: "TestWhole", seconds: seconds}}, elapsed: seconds}
	}
	bigSlice := func(test string) benchmarkFixtureStep {
		return benchmarkFixtureStep{pkg: "example.test/m/p/big", tests: []benchmarkFixtureTest{{name: test, seconds: 21}}, elapsed: 72, compile: 51}
	}
	moduleRoot := t.TempDir()
	writeTestFile(t, moduleRoot, "go.mod", "module example.test/m\n\ngo 1.26\n")

	tests := []struct {
		name    string
		a, b    benchmarkFixtureSide
		options func(*BenchmarkCompareOptions)
		jobs    bool
		check   func(*testing.T, BenchmarkComparison, error)
	}{
		{
			name: "results that tested another commit are rejected",
			a:    speedSide(shaA, 2, 8), b: speedSide(shaA, 2, 8),
			options: func(options *BenchmarkCompareOptions) { options.B.SHA = shaB },
			check: func(t *testing.T, _ BenchmarkComparison, err error) {
				if err == nil || !strings.Contains(err.Error(), "side B shard 0 tested "+shaA) {
					t.Fatalf("error = %v; want side identity failure", err)
				}
			},
		},
		{
			name: "older runner results are compared without the fields they lack",
			a:    func() benchmarkFixtureSide { side := speedSide(shaA, 2, 8); side.legacy = true; return side }(),
			b:    func() benchmarkFixtureSide { side := speedSide(shaA, 2, 8); side.legacy = true; return side }(),
			check: func(t *testing.T, report BenchmarkComparison, err error) {
				if err != nil {
					t.Fatal(err)
				}
				shard := report.A.Shards[0]
				if shard.StepsPhase == nil || *shard.StepsPhase != 31 || shard.Steps[0].Compile != nil || shard.CPUModel != "" {
					t.Fatalf("legacy shard = %+v; want steps phase 31 s from result phases and no compile or CPU", shard)
				}
				for _, want := range []string{"predate step timestamps", "CPU model", "GitHub jobs data was not read"} {
					if !slices.ContainsFunc(report.Missing, func(note string) bool { return strings.Contains(note, want) }) {
						t.Fatalf("missing notes %v do not name %q", report.Missing, want)
					}
				}
				if benchmarkComparisonMetric(report, "fixed_preparation") != nil || benchmarkComparisonMetric(report, "setup") == nil {
					t.Fatalf("metrics = %+v; want setup without fixed preparation", report.Metrics)
				}
			},
		},
		{
			name: "job timings join results to their GitHub jobs",
			a:    speedSide(shaA, 2, 8), b: speedSide(shaA, 2, 8), jobs: true,
			check: func(t *testing.T, report BenchmarkComparison, err error) {
				if err != nil {
					t.Fatal(err)
				}
				shard := report.B.Shards[0]
				// Job start → runner 60 s, setup 100 s, first and longest step
				// compile 2 s; the runner works 165 s outside the steps phase.
				want := BenchmarkShard{
					BeforeRunner: benchmarkPointer(60), Preparation: benchmarkPointer(162), FixedPreparation: benchmarkPointer(162), StepsPhase: benchmarkPointer(31),
					Job: benchmarkPointer(197), RunnerMinutes: benchmarkPointer(3.267), RunnerFixed: benchmarkPointer(165),
				}
				for name, pair := range map[string][2]*float64{
					"before runner": {shard.BeforeRunner, want.BeforeRunner}, "preparation": {shard.Preparation, want.Preparation}, "fixed preparation": {shard.FixedPreparation, want.FixedPreparation},
					"steps phase": {shard.StepsPhase, want.StepsPhase}, "job": {shard.Job, want.Job}, "runner-minutes": {shard.RunnerMinutes, want.RunnerMinutes}, "runner fixed": {shard.RunnerFixed, want.RunnerFixed},
				} {
					if pair[0] == nil || *pair[0] != *pair[1] {
						t.Fatalf("%s = %v; want %v", name, pair[0], *pair[1])
					}
				}
				if shard.Steps[0].Compile == nil || *shard.Steps[0].Compile != 2 || shard.Steps[1].Compile == nil || *shard.Steps[1].Compile != 1.5 {
					t.Fatalf("per-step compile = %+v; want 2 s and 1.5 s", shard.Steps)
				}
				if metric := benchmarkComparisonMetric(report, "plan_fixed_slowest_job"); metric == nil || metric.A != 60+100+19 {
					t.Fatalf("plan-fixed slowest job = %+v; want 179 s", metric)
				}
				if metric := benchmarkComparisonMetric(report, "plan_fixed_runner_minutes"); metric == nil || metric.A != 3.067 {
					t.Fatalf("plan-fixed runner-minutes = %+v; want (165 + 19) s", metric)
				}
			},
		},
		{
			name: "speed factors normalize changed tests",
			a:    speedSide(shaA, 2, 8), b: speedSide(shaA, 4, 8),
			options: func(options *BenchmarkCompareOptions) { options.ChangedTests = []string{changed + ".TestChanged"} },
			check: func(t *testing.T, report BenchmarkComparison, err error) {
				if err != nil {
					t.Fatal(err)
				}
				if len(report.SpeedFactors) != 1 || report.SpeedFactors[0].Factor == nil || *report.SpeedFactors[0].Factor != 2 || report.SpeedFactors[0].Tests != 8 {
					t.Fatalf("speed factors = %+v; want 2.0 from 8 tests", report.SpeedFactors)
				}
				changedTest := report.Tests[0]
				if changedTest.Package != changed || !changedTest.Changed || changedTest.NormalizedRatio == nil || *changedTest.NormalizedRatio != 0.5 {
					t.Fatalf("changed test = %+v; want speed-normalized ratio 0.5", changedTest)
				}
				if metric := benchmarkComparisonMetric(report, "changed_tests_duration"); metric == nil || metric.A != 10 || metric.B != 5 || metric.D != 5 {
					t.Fatalf("changed tests metric = %+v; want A 10 s, B 5 s", metric)
				}
			},
		},
		{
			name: "too few shared tests leave ratios unnormalized",
			a:    speedSide(shaA, 2, 7), b: speedSide(shaA, 4, 7),
			options: func(options *BenchmarkCompareOptions) { options.ChangedTests = []string{changed + ".TestChanged"} },
			check: func(t *testing.T, report BenchmarkComparison, err error) {
				if err != nil {
					t.Fatal(err)
				}
				if report.SpeedFactors[0].Factor != nil || report.SpeedFactors[0].Tests != 7 || report.Tests[0].NormalizedRatio != nil {
					t.Fatalf("factors = %+v, changed test = %+v; want no factor from 7 tests", report.SpeedFactors, report.Tests[0])
				}
				if !slices.ContainsFunc(report.Warnings, func(note string) bool { return strings.Contains(note, "unnormalized") }) {
					t.Fatalf("warnings %v do not say the ratios stay unnormalized", report.Warnings)
				}
				if benchmarkComparisonMetric(report, benchmarkChangedTestsMetric) != nil {
					t.Fatal("changed tests were summed without a speed factor")
				}
			},
		},
		{
			name: "a listed changed test that failed leaves the metric out",
			a:    speedSide(shaA, 2, 8),
			b: func() benchmarkFixtureSide {
				side := speedSide(shaA, 2, 8)
				side.shards[0][1].tests[0] = benchmarkFixtureTest{name: "TestChanged", seconds: 1, failed: true}
				return side
			}(),
			options: func(options *BenchmarkCompareOptions) { options.ChangedTests = []string{changed + ".TestChanged"} },
			check: func(t *testing.T, report BenchmarkComparison, err error) {
				if err != nil {
					t.Fatal(err)
				}
				if metric := benchmarkComparisonMetric(report, benchmarkChangedTestsMetric); metric != nil {
					t.Fatalf("changed tests metric = %+v; a failed test must not count as faster", metric)
				}
			},
		},
		{
			name: "a listed changed test that did not run is rejected",
			a:    speedSide(shaA, 2, 8), b: speedSide(shaA, 2, 8),
			options: func(options *BenchmarkCompareOptions) { options.ChangedTests = []string{changed + ".TestRenamed"} },
			check: func(t *testing.T, _ BenchmarkComparison, err error) {
				if err == nil || !strings.Contains(err.Error(), "did not run on side A") {
					t.Fatalf("error = %v; want the unknown changed test rejected", err)
				}
			},
		},
		{
			name: "replays take each plan's steps in order on two workers",
			a: benchmarkFixtureSide{sha: shaA, runID: "7", attempt: "1", shards: [][]benchmarkFixtureStep{
				{whole("one", 4), whole("two", 4), whole("three", 4), whole("four", 10)},
			}},
			b: benchmarkFixtureSide{sha: shaA, runID: "8", attempt: "1", shards: [][]benchmarkFixtureStep{
				{whole("four", 20), whole("one", 8), whole("two", 8), whole("three", 8)},
			}},
			check: func(t *testing.T, report BenchmarkComparison, err error) {
				if err != nil {
					t.Fatal(err)
				}
				// A's order puts the 10 s step last: 4+4 | 4+10. B's order
				// starts with it: 10 | 4+4+4 over A's durations.
				replay := report.Replay[0]
				if *replay.APlanADurations != 14 || *replay.BPlanADurations != 12 || *replay.APlanBDurations != 28 {
					t.Fatalf("replay = A plan %v / B plan %v over A's durations, A plan %v over B's; want 14, 12, 28",
						*replay.APlanADurations, *replay.BPlanADurations, *replay.APlanBDurations)
				}
				if report.PlansIdentical || !slices.Equal(report.DifferentShards, []int{0}) {
					t.Fatalf("plans identical = %v (%v); want shard 0 to differ", report.PlansIdentical, report.DifferentShards)
				}
				if metric := benchmarkComparisonMetric(report, "replayed_slowest_steps_phase"); metric == nil || metric.D != 2 {
					t.Fatalf("replayed slowest steps phase = %+v; want d = 2 s", metric)
				}
				if metric := benchmarkComparisonMetric(report, "slowest_steps_phase"); metric == nil || !metric.PlanDependent {
					t.Fatalf("slowest steps phase = %+v; want it marked plan-dependent", metric)
				}
			},
		},
		{
			// A plans one's 30 s compile first in shard 0 and runs one whole; B
			// starts both shards with big's 51 s compile and splits one, paying
			// its compile twice. Every unit lasts the same on both sides.
			name: "a different shard plan moves measured preparation and runner-minutes but not their plan-fixed forms",
			a: benchmarkFixtureSide{sha: shaA, runID: "7", attempt: "1", shards: [][]benchmarkFixtureStep{
				{{pkg: "example.test/m/p/one", tests: []benchmarkFixtureTest{{name: "TestA", seconds: 3}, {name: "TestB", seconds: 3}}, elapsed: 36, compile: 30}, bigSlice("TestOne")},
				{bigSlice("TestTwo"), whole("two", 3)},
			}},
			b: benchmarkFixtureSide{sha: shaA, runID: "8", attempt: "1", shards: [][]benchmarkFixtureStep{
				{bigSlice("TestOne"), {pkg: "example.test/m/p/one", tests: []benchmarkFixtureTest{{name: "TestA", seconds: 3}}, elapsed: 33, compile: 30}},
				{bigSlice("TestTwo"), whole("two", 3), {pkg: "example.test/m/p/one", tests: []benchmarkFixtureTest{{name: "TestB", seconds: 3}}, elapsed: 33, compile: 30}},
			}},
			jobs: true,
			check: func(t *testing.T, report BenchmarkComparison, err error) {
				if err != nil {
					t.Fatal(err)
				}
				preparation := func(side BenchmarkSide) float64 {
					return (*side.Shards[0].Preparation + *side.Shards[1].Preparation) / 2
				}
				if report.PlansIdentical || preparation(report.A) != 200.5 || preparation(report.B) != 211 {
					t.Fatalf("plans identical %v, measured preparation A %v and B %v; want different plans moving it from 200.5 s to 211 s", report.PlansIdentical, preparation(report.A), preparation(report.B))
				}
				if benchmarkComparisonMetric(report, "preparation") != nil {
					t.Fatal("measured preparation is offered for judgement")
				}
				for name, want := range map[string]BenchmarkMetric{
					"fixed_preparation":         {A: 211, B: 211, D: 0},
					"plan_fixed_runner_minutes": {A: 7.9, B: 7.9, D: 0},
					"go_race_runner_minutes":    {A: 8.55, B: 9.05, D: -0.5, PlanDependent: true},
				} {
					metric := benchmarkComparisonMetric(report, name)
					if metric == nil || metric.A != want.A || metric.B != want.B || metric.D != want.D || metric.PlanDependent != want.PlanDependent {
						t.Fatalf("%s = %+v; want A %v, B %v, d %v, plan-dependent %v", name, metric, want.A, want.B, want.D, want.PlanDependent)
					}
				}
			},
		},
		{
			// Every fixture job spends 165 s outside its steps phase.
			name: "a side that runs one more shard pays its runner time in plan-fixed runner-minutes",
			a: benchmarkFixtureSide{sha: shaA, runID: "7", attempt: "1", shards: [][]benchmarkFixtureStep{
				{whole("one", 10), whole("two", 10)},
				{whole("three", 10)},
			}},
			b: benchmarkFixtureSide{sha: shaA, runID: "8", attempt: "1", shards: [][]benchmarkFixtureStep{
				{whole("one", 10)},
				{whole("two", 10)},
				{whole("three", 10)},
			}},
			jobs: true,
			check: func(t *testing.T, report BenchmarkComparison, err error) {
				if err != nil {
					t.Fatal(err)
				}
				// A's plan replays to 10 s per shard on both sides; A runs two
				// jobs, B three.
				metric := benchmarkComparisonMetric(report, "plan_fixed_runner_minutes")
				if metric == nil || metric.A != 5.833 || metric.B != 8.583 || metric.D != -2.75 || metric.Shards != nil {
					t.Fatalf("plan-fixed runner-minutes = %+v; want A (2 × 165 + 20) s, B (3 × 165 + 20) s and no shard pairs", metric)
				}
			},
		},
		{
			// Slices' overheads are mostly compile, which depends on what else
			// the shard built; a package mean would misprice both slices.
			name: "a slice is priced with its own shard's overhead",
			a: benchmarkFixtureSide{sha: shaA, runID: "7", attempt: "1", shards: [][]benchmarkFixtureStep{
				{{pkg: "example.test/m/p/sliced", tests: []benchmarkFixtureTest{{name: "TestOne", seconds: 5}}, elapsed: 7}},
				{{pkg: "example.test/m/p/sliced", tests: []benchmarkFixtureTest{{name: "TestTwo", seconds: 7}}, elapsed: 107}},
			}},
			b: benchmarkFixtureSide{sha: shaA, runID: "8", attempt: "1", shards: [][]benchmarkFixtureStep{
				{{pkg: "example.test/m/p/sliced", tests: []benchmarkFixtureTest{{name: "TestOne", seconds: 5}}, elapsed: 35}},
				{{pkg: "example.test/m/p/sliced", tests: []benchmarkFixtureTest{{name: "TestTwo", seconds: 7}}, elapsed: 17}},
			}},
			check: func(t *testing.T, report BenchmarkComparison, err error) {
				if err != nil {
					t.Fatal(err)
				}
				for shard, replay := range report.Replay {
					if *replay.APlanADurations != *report.A.Shards[shard].StepsPhase || *replay.APlanBDurations != *report.B.Shards[shard].StepsPhase {
						t.Fatalf("shard %d replays A's plan at %v over A's and %v over B's durations; want the measured %v and %v",
							shard, *replay.APlanADurations, *replay.APlanBDurations, *report.A.Shards[shard].StepsPhase, *report.B.Shards[shard].StepsPhase)
					}
				}
			},
		},
		{
			name: "a shard that ran no step of a package uses that side's package mean",
			a: benchmarkFixtureSide{sha: shaA, runID: "7", attempt: "1", shards: [][]benchmarkFixtureStep{
				{{pkg: "example.test/m/p/sliced", tests: []benchmarkFixtureTest{{name: "TestOne", seconds: 5}}, elapsed: 7}},
				{{pkg: "example.test/m/p/sliced", tests: []benchmarkFixtureTest{{name: "TestTwo", seconds: 7}}, elapsed: 107}},
				{whole("other", 1)},
			}},
			b: benchmarkFixtureSide{sha: shaA, runID: "8", attempt: "1", shards: [][]benchmarkFixtureStep{
				{{pkg: "example.test/m/p/sliced", tests: []benchmarkFixtureTest{{name: "TestOne", seconds: 5}}, elapsed: 35}},
				{whole("other", 1)},
				{{pkg: "example.test/m/p/sliced", tests: []benchmarkFixtureTest{{name: "TestTwo", seconds: 7}}, elapsed: 17}},
			}},
			check: func(t *testing.T, report BenchmarkComparison, err error) {
				if err != nil {
					t.Fatal(err)
				}
				// Overheads: A 2 and 100 s (mean 51) in shards 0 and 1; B 30
				// and 10 s (mean 20) in shards 0 and 2. A's durations never
				// take B's overheads for a package A ran.
				got := []float64{*report.Replay[1].APlanBDurations, *report.Replay[0].BPlanADurations, *report.Replay[2].BPlanADurations}
				if !slices.Equal(got, []float64{20 + 7, 2 + 5, 51 + 7}) {
					t.Fatalf("A plan shard 1 over B, B plan shards 0 and 2 over A = %v; want 27, 7, 58", got)
				}
			},
		},
		{
			name: "the latest attempt of a shard is its evidence",
			a:    speedSide(shaA, 2, 8),
			b: func() benchmarkFixtureSide {
				side := speedSide(shaA, 4, 8)
				side.attempt = "2"
				return side
			}(),
			options: func(options *BenchmarkCompareOptions) {
				// Named to be walked after attempt 2.
				stale := speedSide(shaA, 9, 8)
				if err := os.Rename(writeBenchmarkSide(t, stale), filepath.Join(options.B.Directory, "zz-attempt-1")); err != nil {
					t.Fatal(err)
				}
			},
			check: func(t *testing.T, report BenchmarkComparison, err error) {
				if err != nil {
					t.Fatal(err)
				}
				if report.B.Shards[0].RunAttempt != "2" || *report.SpeedFactors[0].Factor != 2 {
					t.Fatalf("side B = attempt %s with factor %v; want attempt 2 at factor 2", report.B.Shards[0].RunAttempt, *report.SpeedFactors[0].Factor)
				}
			},
		},
		{
			name: "other producers in the download are ignored",
			a:    speedSide(shaA, 2, 8), b: speedSide(shaA, 2, 8),
			options: func(options *BenchmarkCompareOptions) {
				static := Result{Plan: Plan{Revision: Revision{Head: shaB}}, Execution: ExecutionEnvelope{Producer: "go-static", RunID: "7", RunAttempt: "1"}, Status: "pass"}
				body, err := json.Marshal(static)
				if err != nil {
					t.Fatal(err)
				}
				if err := os.MkdirAll(filepath.Join(options.A.Directory, "go-static"), 0o700); err != nil {
					t.Fatal(err)
				}
				writeTestFile(t, options.A.Directory, "go-static/result.json", string(body))
			},
			check: func(t *testing.T, report BenchmarkComparison, err error) {
				if err != nil || len(report.A.Shards) != 1 {
					t.Fatalf("A shards = %d, error = %v; want go-static ignored", len(report.A.Shards), err)
				}
			},
		},
		{
			name: "two results for one shard attempt are rejected",
			a:    speedSide(shaA, 2, 8), b: speedSide(shaA, 2, 8),
			options: func(options *BenchmarkCompareOptions) {
				if err := os.Rename(writeBenchmarkSide(t, speedSide(shaA, 2, 8)), filepath.Join(options.B.Directory, "copy")); err != nil {
					t.Fatal(err)
				}
			},
			check: func(t *testing.T, _ BenchmarkComparison, err error) {
				if err == nil || !strings.Contains(err.Error(), "two results for shard 0 attempt 1") {
					t.Fatalf("error = %v; want duplicate shard rejection", err)
				}
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			options := BenchmarkCompareOptions{
				A:              BenchmarkSideOptions{Directory: writeBenchmarkSide(t, tc.a), SHA: tc.a.sha, JobName: "A Go Race (shard %d)"},
				B:              BenchmarkSideOptions{Directory: writeBenchmarkSide(t, tc.b), SHA: tc.b.sha, JobName: "B Go Race (shard %d)"},
				RepositoryRoot: moduleRoot,
				GitHubJobs:     tc.jobs,
			}
			client := benchmarkFixtureGitHub{}
			if tc.jobs {
				benchmarkFixtureJobs(t, tc.a, options.A.JobName, client)
				benchmarkFixtureJobs(t, tc.b, options.B.JobName, client)
			}
			if tc.options != nil {
				tc.options(&options)
			}
			report, err := compareBenchmarkEvidence(context.Background(), options, client)
			tc.check(t, report, err)
		})
	}
}

func TestCompareBenchmarkEvidenceMarksTestsOfChangedPackages(t *testing.T) {
	root := t.TempDir()
	runGit(t, root, "init", "-q")
	runGit(t, root, "config", "user.name", "Test")
	runGit(t, root, "config", "user.email", "test@example.invalid")
	writeTestFile(t, root, "go.mod", "module example.test/m\n")
	for _, directory := range []string{"p/fast", "p/changed/testdata", "docs"} {
		if err := os.MkdirAll(filepath.Join(root, directory), 0o700); err != nil {
			t.Fatal(err)
		}
	}
	writeTestFile(t, root, "p/fast/fast.go", "package fast\n")
	writeTestFile(t, root, "p/changed/testdata/input.txt", "before\n")
	writeTestFile(t, root, "docs/notes.md", "before\n")
	runGit(t, root, "add", ".")
	runGit(t, root, "commit", "-qm", "base")
	base := runGit(t, root, "rev-parse", "HEAD")
	writeTestFile(t, root, "p/changed/testdata/input.txt", "after\n")
	writeTestFile(t, root, "docs/notes.md", "after\n")
	runGit(t, root, "commit", "-qam", "head")
	head := runGit(t, root, "rev-parse", "HEAD")

	side := func(sha string) benchmarkFixtureSide {
		return benchmarkFixtureSide{sha: sha, runID: "7", attempt: "1", shards: [][]benchmarkFixtureStep{{
			{pkg: "example.test/m/p/fast", tests: benchmarkFixtureTests("TestFast", 8, 2), elapsed: 16},
			{pkg: "example.test/m/p/changed", tests: benchmarkFixtureTests("TestChanged", 8, 2), elapsed: 16},
		}}}
	}
	report, err := compareBenchmarkEvidence(context.Background(), BenchmarkCompareOptions{
		A:              BenchmarkSideOptions{Directory: writeBenchmarkSide(t, side(base)), SHA: base},
		B:              BenchmarkSideOptions{Directory: writeBenchmarkSide(t, side(head)), SHA: head},
		RepositoryRoot: root,
		ChangedTests:   []string{"example.test/m/p/changed.TestChanged00"},
	}, benchmarkFixtureGitHub{})
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(report.ChangedPackages, []string{"example.test/m/p/changed"}) {
		t.Fatalf("changed packages = %v; want the package owning the changed testdata only", report.ChangedPackages)
	}
	if report.SpeedFactors[0].Tests != 8 {
		t.Fatalf("speed factor rests on %d tests; want the 8 unchanged ones", report.SpeedFactors[0].Tests)
	}
	for _, test := range report.Tests {
		if test.ChangedPackage != (test.Package == "example.test/m/p/changed") || test.Changed != (test.Test == "TestChanged00") {
			t.Fatalf("test %s.%s changed package = %v, listed = %v", test.Package, test.Test, test.ChangedPackage, test.Changed)
		}
	}
	if metric := benchmarkComparisonMetric(report, benchmarkChangedTestsMetric); metric == nil || metric.A != 2 {
		t.Fatalf("changed tests metric = %+v; want the one listed test's 2 s", metric)
	}
}

func TestBenchmarkNoiseFromAAComparisons(t *testing.T) {
	const listed = "example.test/m/p.TestSlow"
	aa := func(differences [4]float64, changedB float64) BenchmarkComparison {
		comparison := BenchmarkComparison{
			A: BenchmarkSide{SHA: "a", Complete: true}, B: BenchmarkSide{SHA: "a", Complete: true}, PlansIdentical: true,
			SpeedFactors: []BenchmarkSpeedFactor{{Factor: benchmarkPointer(1), Tests: 8}},
			Tests:        []BenchmarkTest{{Package: "example.test/m/p", Test: "TestSlow", A: 10, B: changedB, APassed: true, BPassed: true}},
		}
		for _, metric := range []struct {
			name, aggregate string
			scale           float64
		}{
			{"setup", benchmarkAggregateMean, 1}, {"go_race_runner_minutes", benchmarkAggregateSum, 1}, {"slowest_job", benchmarkAggregateMax, 1},
			{"fixed_preparation", benchmarkAggregateMean, 0}, {"replayed_slowest_steps_phase", benchmarkAggregateMax, 0},
		} {
			item := BenchmarkMetric{Name: metric.name, Unit: "s", Aggregate: metric.aggregate}
			for shard, difference := range differences {
				item.Shards = append(item.Shards, BenchmarkShardPair{Shard: shard, A: 100 + metric.scale*difference, B: 100})
			}
			comparison.Metrics = append(comparison.Metrics, item)
		}
		return comparison
	}
	// Per-shard differences 1, -1, 2, -2, 3, -3, 0, 0 have a standard
	// deviation of 2; the listed test's run differences 1 and 3 one of √2.
	comparisons := []BenchmarkComparison{aa([4]float64{1, -1, 2, -2}, 9), aa([4]float64{3, -3, 0, 0}, 7)}
	noise, err := BenchmarkNoiseFromComparisons(comparisons, []string{listed})
	if err != nil {
		t.Fatal(err)
	}
	for name, want := range map[string]struct {
		sd      float64
		samples int
	}{
		"setup": {1, 8}, "go_race_runner_minutes": {4, 8}, "slowest_job": {2, 8}, benchmarkChangedTestsMetric: {1.414, 2},
	} {
		metric := benchmarkNoiseMetric(noise, name)
		if metric == nil || metric.SD != want.sd || metric.Samples != want.samples {
			t.Fatalf("%s noise = %+v; want s_D %v from %d samples", name, metric, want.sd, want.samples)
		}
	}
	for name, note := range map[string]string{"fixed_preparation": "is the same", "replayed_slowest_steps_phase": "over 5 runs"} {
		if benchmarkNoiseMetric(noise, name) != nil || !slices.ContainsFunc(noise.Notes, func(text string) bool { return strings.HasPrefix(text, name) && strings.Contains(text, note) }) {
			t.Fatalf("%s got s_D %+v (notes %v); want none, with a note saying %q", name, benchmarkNoiseMetric(noise, name), noise.Notes, note)
		}
	}
	if _, err := BenchmarkNoiseFromComparisons(comparisons, []string{"example.test/m/p.TestShort"}); err == nil {
		t.Fatal("noise summed a changed test the comparisons do not hold")
	}
	notAA := aa([4]float64{}, 10)
	notAA.B.SHA = "b"
	if _, err := BenchmarkNoiseFromComparisons([]BenchmarkComparison{notAA}, nil); err == nil {
		t.Fatal("noise accepted a comparison of two commits")
	}
}

func TestBenchmarkVerdictRule(t *testing.T) {
	run := func(d float64) BenchmarkComparison {
		return BenchmarkComparison{
			A: BenchmarkSide{SHA: "a", RunID: "1", Complete: true}, B: BenchmarkSide{SHA: "b", RunID: "1", Complete: true}, PlansIdentical: true,
			Metrics: []BenchmarkMetric{
				{Name: "fixed_preparation", Unit: "s", Aggregate: benchmarkAggregateMean, A: 500, B: 500 - d, D: d},
				{Name: "slowest_job", Unit: "s", Aggregate: benchmarkAggregateMax, PlanDependent: true, A: 1200, B: 1200 - d, D: d},
				{Name: "plan_fixed_runner_minutes", Unit: "min", Aggregate: benchmarkAggregateSum, A: 70, B: 70},
				{Name: "plan_fixed_slowest_job", Unit: "s", Aggregate: benchmarkAggregateMax, A: 1300, B: 1300},
				{Name: "replayed_slowest_steps_phase", Unit: "s", Aggregate: benchmarkAggregateMax, A: 900, B: 900 - d, D: d},
				{Name: "go_race_runner_minutes", Unit: "min", Aggregate: benchmarkAggregateSum, PlanDependent: true, A: 70, B: 70},
			},
		}
	}
	runs := func(differences ...float64) []BenchmarkComparison {
		var comparisons []BenchmarkComparison
		for _, difference := range differences {
			comparisons = append(comparisons, run(difference))
		}
		return comparisons
	}
	noise := BenchmarkNoise{Metrics: []BenchmarkNoiseMetric{
		{Name: "fixed_preparation", SD: 20}, {Name: "slowest_job", SD: 20}, {Name: "plan_fixed_runner_minutes", SD: 1}, {Name: "go_race_runner_minutes", SD: 1}, {Name: "plan_fixed_slowest_job", SD: 10},
	}}
	tests := []struct {
		name         string
		metric       string
		effect       float64
		sd           *float64
		comparisons  []BenchmarkComparison
		mutate       func([]BenchmarkComparison)
		wantStatus   string
		wantRequired int
		wantErr      string
	}{
		{name: "E of 10 s_D passes one run at E/2", metric: "fixed_preparation", effect: 200, comparisons: runs(100), wantStatus: BenchmarkVerdictPass, wantRequired: 1},
		{name: "E of 10 s_D fails one run below E/2", metric: "fixed_preparation", effect: 200, comparisons: runs(99), wantStatus: BenchmarkVerdictFail, wantRequired: 1},
		{name: "E of 3 to 10 s_D needs three runs", metric: "fixed_preparation", effect: 100, comparisons: runs(100), wantStatus: BenchmarkVerdictInsufficientRuns, wantRequired: 3},
		{name: "three positive runs with mean at E/2 pass", metric: "fixed_preparation", effect: 100, comparisons: runs(60, 40, 50), wantStatus: BenchmarkVerdictPass, wantRequired: 3},
		{name: "a run without improvement fails", metric: "fixed_preparation", effect: 100, comparisons: runs(120, 0, 60), wantStatus: BenchmarkVerdictFail, wantRequired: 3},
		{name: "E of 2 to 3 s_D needs five runs", metric: "fixed_preparation", effect: 50, comparisons: runs(40, 40, 40), wantStatus: BenchmarkVerdictInsufficientRuns, wantRequired: 5},
		{name: "E below 2 s_D is not measurable", metric: "fixed_preparation", effect: 39, comparisons: runs(40), wantStatus: BenchmarkVerdictNotMeasurable, wantRequired: 0},
		{name: "a stated s_D replaces the noise file's", metric: "fixed_preparation", effect: 100, sd: benchmarkPointer(5), comparisons: runs(50), wantStatus: BenchmarkVerdictPass, wantRequired: 1},
		{
			name: "plan-fixed runner-minutes guard", metric: "fixed_preparation", effect: 200, comparisons: runs(100), wantStatus: BenchmarkVerdictFail, wantRequired: 1,
			mutate: func(comparisons []BenchmarkComparison) { comparisons[0].Metrics[2].B = 72.1 },
		},
		{
			name: "measured runner-minutes, which move with the plan, guard nothing", metric: "fixed_preparation", effect: 200, comparisons: runs(100), wantStatus: BenchmarkVerdictPass, wantRequired: 1,
			mutate: func(comparisons []BenchmarkComparison) { comparisons[0].Metrics[5].B = 72.1 },
		},
		{
			name: "plan-fixed slowest job guard", metric: "fixed_preparation", effect: 200, comparisons: runs(100), wantStatus: BenchmarkVerdictFail, wantRequired: 1,
			mutate: func(comparisons []BenchmarkComparison) { comparisons[0].Metrics[3].B = 1320.1 },
		},
		{
			name: "plan-dependent metric over different plans", metric: "slowest_job", effect: 200, comparisons: runs(100), wantStatus: BenchmarkVerdictFail, wantRequired: 1,
			mutate: func(comparisons []BenchmarkComparison) { comparisons[0].PlansIdentical = false },
		},
		{
			name: "incomplete run", metric: "fixed_preparation", effect: 200, comparisons: runs(100), wantStatus: BenchmarkVerdictFail, wantRequired: 1,
			mutate: func(comparisons []BenchmarkComparison) { comparisons[0].B.Complete = false },
		},
		{name: "the replayed metric needs five runs without an s_D", metric: "replayed_slowest_steps_phase", effect: 60, comparisons: runs(60), wantStatus: BenchmarkVerdictInsufficientRuns, wantRequired: 5},
		{name: "five improving replays at E/2 pass", metric: "replayed_slowest_steps_phase", effect: 60, comparisons: runs(40, 20, 30, 30, 30), wantStatus: BenchmarkVerdictPass, wantRequired: 5},
		{name: "a comparison without the changed-test metric is an error", metric: benchmarkChangedTestsMetric, effect: 60, sd: benchmarkPointer(5), comparisons: runs(60), wantErr: "--changed-tests"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if tc.mutate != nil {
				tc.mutate(tc.comparisons)
			}
			verdict, err := BenchmarkVerdictFromComparisons(BenchmarkVerdictOptions{Metric: tc.metric, Effect: tc.effect, Noise: noise, SD: tc.sd}, tc.comparisons)
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("error = %v; want %q", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if verdict.Status != tc.wantStatus || verdict.RunsRequired != tc.wantRequired {
				t.Fatalf("verdict = %s with %d runs required (%v); want %s with %d", verdict.Status, verdict.RunsRequired, verdict.Reasons, tc.wantStatus, tc.wantRequired)
			}
		})
	}
}
