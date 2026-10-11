package testinfra

import (
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestCollectGoTestDurationsCountsCompletedTopLevelTests(t *testing.T) {
	path := filepath.Join(t.TempDir(), "go-test.jsonl")
	body := []byte("{\"Action\":\"pass\",\"Package\":\"example/pkg\",\"Test\":\"TestOne\",\"Elapsed\":1.25}\n" +
		"{\"Action\":\"pass\",\"Package\":\"example/pkg\",\"Test\":\"TestOne/sub\",\"Elapsed\":0.25}\n" +
		"{\"Action\":\"fail\",\"Package\":\"example/pkg\",\"Test\":\"TestTwo\",\"Elapsed\":2}\n")
	if err := os.WriteFile(path, body, 0o600); err != nil {
		t.Fatal(err)
	}
	durations := map[string]float64{}
	if err := collectGoTestDurations(path, durations); err != nil {
		t.Fatal(err)
	}
	if len(durations) != 1 || durations["example/pkg/TestOne"] != 1.25 {
		t.Fatalf("durations = %#v", durations)
	}
}

func TestCollectRunHealthCountsCompletedJobsAndGoShardResults(t *testing.T) {
	started := time.Date(2026, 8, 28, 12, 0, 0, 0, time.UTC)
	report := CIHealthReport{JobConclusions: map[string]int{}, EvidenceStatuses: map[string]int{}, StepDurations: map[string]int64{}, GoShardMinutes: map[string]float64{}}
	collectCompletedJobHealth(&report, []workflowJob{
		{Name: "Go Race (shard 0)", Status: "completed", Conclusion: "success", StartedAt: started, CompletedAt: started.Add(2 * time.Minute)},
		{Name: "CI Health and Calibration", Status: "in_progress", StartedAt: started},
		{Name: "broken", Status: "completed", Conclusion: "failure", StartedAt: started},
	})
	// go-static shares the go- prefix but records no Go shard identity.
	seen := map[string]bool{}
	for _, result := range []Result{
		{Plan: Plan{GoSelection: &GoShardIdentity{ShardCount: 1}}, Execution: ExecutionEnvelope{Producer: "go-0"}, Steps: []StepResult{{Group: "go", Elapsed: time.Minute}}},
		{Execution: ExecutionEnvelope{Producer: "go-static"}, Steps: []StepResult{{Group: "go-static", Elapsed: time.Minute}}},
	} {
		collectResultHealth(&report, result, seen)
	}
	if report.RunnerMinutes != 2 || report.JobConclusions["success"] != 1 || len(report.JobConclusions) != 1 || report.GoShardMinutes["Go Race (shard 0)"] != 2 ||
		report.GoShardMinutes["go-0"] != 1 || len(report.GoShardMinutes) != 2 {
		t.Fatalf("health summary = %+v", report)
	}
}

func TestGoRaceHealthWarnsAtEachThreshold(t *testing.T) {
	// p/s is sliceable with TestA at 20 s and TestLong at 40 s; p/w weighs
	// 100 s, p/k1-p/k6 2 s, p/h1-p/h3 0.5 s, and p/a1-p/a6 and p/b1-p/b6
	// 100 s each. p/u is unweighted.
	weightsJSON := `{"version": 2, "generated_at": "2026-10-11", "base": "b", "source_runs": ["1"], "cpu_models": {"Fixture CPU": {"factor": 1, "jobs": 1, "samples": 1, "shared_units": 0}, "Slow CPU": {"factor": 2, "jobs": 1, "samples": 1, "shared_units": 0}}, "median_test_ms": 1000, "sliceable": ["p/s"], "packages": {
		"p/s": {"elapsed_ms": 60000, "overhead_ms": 0, "tests_ms": {"TestA": 20000, "TestLong": 40000}}, "p/w": {"elapsed_ms": 100000, "overhead_ms": 0}`
	for index := 1; index <= 6; index++ {
		weightsJSON += fmt.Sprintf(`, "p/k%d": {"elapsed_ms": 2000, "overhead_ms": 0}, "p/a%d": {"elapsed_ms": 100000, "overhead_ms": 0}, "p/b%d": {"elapsed_ms": 100000, "overhead_ms": 0}`, index, index, index)
	}
	for index := 1; index <= 3; index++ {
		weightsJSON += fmt.Sprintf(`, "p/h%d": {"elapsed_ms": 500, "overhead_ms": 0}`, index)
	}
	weights, err := parseGoShardWeights([]byte(weightsJSON + "}}"))
	if err != nil {
		t.Fatal(err)
	}
	// A unit is a whole package or, with a test name, a test of p/s; it is
	// one step of its shard. A shard's steps phase is given, not summed.
	type unit struct {
		pkg, test string
		seconds   float64
	}
	type shard struct {
		phase  float64
		units  []unit
		failed bool
		cpu    string
	}
	build := func(id string, shards ...shard) *goRaceRun {
		run := &goRaceRun{runID: id, shardCount: len(shards), shards: map[int]*benchmarkShardEvidence{}}
		start := time.Date(2026, 10, 11, 12, 0, 0, 0, time.UTC)
		for index, item := range shards {
			evidence := &benchmarkShardEvidence{
				result:   Result{Status: "pass", Execution: ExecutionEnvelope{Workflow: verificationWorkflowPR, GoCache: &GoCacheRestore{Match: "prefix"}, CPUModel: "Fixture CPU"}},
				complete: true,
			}
			if item.cpu != "" {
				evidence.result.Execution.CPUModel = item.cpu
			}
			if item.failed {
				evidence.result.Status = "fail"
			}
			for position, member := range item.units {
				test := member.test
				if test == "" {
					test = "TestW"
				}
				finished := start.Add(time.Second)
				if position == 0 {
					finished = start.Add(benchmarkFixtureSeconds(item.phase))
				}
				step := benchmarkStepEvidence{
					selection: Selection{Group: "go", Packages: []string{member.pkg}, Tests: []string{test}},
					result:    StepResult{Status: "pass", StartedAt: start, FinishedAt: finished},
					report:    goTestReport{packageAction: "pass", packageElapsed: member.seconds, tests: []goTestOutcome{{pkg: member.pkg, name: test, active: member.seconds, passed: true}}},
				}
				evidence.plan = append(evidence.plan, step.selection)
				evidence.steps = append(evidence.steps, step)
			}
			run.shards[index] = evidence
		}
		return run
	}
	runs := func(count int, shards ...shard) map[string]*goRaceRun {
		result := map[string]*goRaceRun{}
		for index := 0; index < count; index++ {
			id := strconv.Itoa(index + 1)
			result[id] = build(id, shards...)
		}
		return result
	}
	// The single-shard plan puts p/w (100 s) on one worker and the rest on
	// the other: predicted makespan 100 s. Its fillers run at their weight,
	// so its job speed stays 1.
	single := func(phase, whole, testA float64, extra ...unit) shard {
		units := []unit{{pkg: "p/w", seconds: whole}, {pkg: "p/s", test: "TestA", seconds: testA}}
		for index := 1; index <= 5; index++ {
			units = append(units, unit{pkg: fmt.Sprintf("p/k%d", index), seconds: 2})
		}
		return shard{phase: phase, units: append(units, extra...)}
	}
	// Six 100 s packages on two workers: predicted makespan 300 s.
	six := func(prefix string, phase float64, seconds ...float64) shard {
		item := shard{phase: phase}
		for index, value := range seconds {
			item.units = append(item.units, unit{pkg: fmt.Sprintf("p/%s%d", prefix, index+1), seconds: value})
		}
		return item
	}
	// twiceAsLongOn runs a shard on a model where every unit and the steps
	// phase take twice as long. The weights give Slow CPU factor 2 and know
	// no Unknown CPU.
	twiceAsLongOn := func(model string, item shard) shard {
		item.units = slices.Clone(item.units)
		for index := range item.units {
			item.units[index].seconds *= 2
		}
		item.phase, item.cpu = item.phase*2, model
		return item
	}
	fourUnits := shard{phase: 200, units: []unit{{pkg: "p/w", seconds: 100}, {pkg: "p/k1", seconds: 2}, {pkg: "p/k2", seconds: 2}, {pkg: "p/k3", seconds: 2},
		{pkg: "p/h1", seconds: 0.5}, {pkg: "p/h2", seconds: 0.5}, {pkg: "p/h3", seconds: 0.5}}}
	quiet := single(100, 100, 20)
	failed := single(200, 100, 20)
	failed.failed = true
	tests := []struct {
		name         string
		runs         map[string]*goRaceRun
		want         []string
		insufficient bool
		// unscaled names the one CPU model whose samples must be left out.
		unscaled string
	}{
		{name: "just below every threshold", runs: runs(3, single(159, 129, 49, unit{pkg: "p/s", test: "TestNew", seconds: 9.9}, unit{pkg: "p/u", test: "TestU", seconds: 9.9}))},
		{name: "a slowest steps phase 61 s over the prediction", runs: runs(3, single(161, 100, 20)), want: []string{"longer than the shard weights predict"}},
		{
			// Counting the two failed runs would move the median excess from
			// 0 s to 100 s.
			name: "failed runs left out of the excess",
			runs: map[string]*goRaceRun{"1": build("1", quiet), "2": build("2", quiet), "3": build("3", single(200, 100, 20)), "4": build("4", failed), "5": build("5", failed)},
		},
		{
			// Raw, the slow machine's 375 s would exceed the prediction by
			// 75 s; divided by its job speed 1.25 it meets it.
			name: "a balanced plan with every unit of one shard 1.25 times its weight",
			runs: runs(3, six("a", 300, 100, 100, 100, 100, 100, 100), six("b", 375, 125, 125, 125, 125, 125, 125)),
		},
		{
			// Two units drift 20 s, so the job speed stays 1 and the shard's
			// normalized steps phase exceeds the prediction by 61 s.
			name: "a shard whose units drift past the prediction",
			runs: runs(3, six("a", 300, 100, 100, 100, 100, 100, 100), six("b", 361, 100, 100, 100, 100, 120, 120)),
			want: []string{"ran a median 61 s longer than the shard weights predict"},
		},
		{
			// p/w and three fillers weigh a second or more, three units less:
			// no job speed, so the 100 s excess is not judged.
			name: "a job with four units of a second or more has no speed",
			runs: runs(3, fourUnits),
		},
		{
			name: "a job with five units of a second or more has a speed",
			runs: runs(3, shard{phase: 161, units: []unit{{pkg: "p/w", seconds: 100}, {pkg: "p/k1", seconds: 2}, {pkg: "p/k2", seconds: 2}, {pkg: "p/k3", seconds: 2}, {pkg: "p/k4", seconds: 2}}}),
			want: []string{"ran a median 61 s longer than the shard weights predict"},
		},
		{
			// Every unit takes twice its weight on a model with factor 2, so
			// neither the units nor the speed-normalized phase drift.
			name: "a CPU model's units at its factor",
			runs: runs(3, twiceAsLongOn("Slow CPU", single(100, 100, 20, unit{pkg: "p/s", test: "TestLong", seconds: 40}))),
		},
		{
			// Divided by the job's speed 2, p/w is 31 s over its weight.
			name: "units of a CPU model without a factor at their job's speed",
			runs: runs(3, twiceAsLongOn("Unknown CPU", single(100, 131, 20))),
			want: []string{"p/w (131.0 s, weight 100.0 s)"},
		},
		{
			name:     "units of a CPU model without a factor from a job without a speed",
			runs:     runs(3, twiceAsLongOn("Unknown CPU", fourUnits)),
			unscaled: "Unknown CPU",
		},
		{name: "a whole package 31 s over its weight", runs: runs(3, single(100, 131, 20)), want: []string{"p/w (131.0 s, weight 100.0 s)"}},
		{name: "a test 31 s over its weight", runs: runs(3, single(100, 100, 51)), want: []string{"p/s.TestA (51.0 s, weight 20.0 s)"}},
		{name: "a new test of 10 s in a sliceable package", runs: runs(3, single(100, 100, 20, unit{pkg: "p/s", test: "TestNew", seconds: 10})), want: []string{"do not know take 10 s or more: p/s.TestNew"}},
		{name: "a test of 10 s in an unweighted package", runs: runs(3, single(100, 100, 20, unit{pkg: "p/u", test: "TestU", seconds: 10})), want: []string{"do not know take 10 s or more: p/u.TestU"}},
		{name: "fewer than three runs", runs: runs(2, single(161, 100, 20)), insufficient: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			health := analyzeGoRaceHealth(test.runs, weights)
			if health.InsufficientData != test.insufficient || len(health.Warnings) != len(test.want) {
				t.Fatalf("insufficient = %v, warnings = %q; want %v and %q", health.InsufficientData, health.Warnings, test.insufficient, test.want)
			}
			if unscaled := slices.Sorted(maps.Keys(health.Unscaled)); test.unscaled == "" && len(unscaled) > 0 ||
				test.unscaled != "" && (!slices.Equal(unscaled, []string{test.unscaled}) || health.Unscaled[test.unscaled] == 0) {
				t.Fatalf("unscaled samples by model = %v; want only %q", health.Unscaled, test.unscaled)
			}
			for index, want := range test.want {
				if !strings.Contains(health.Warnings[index], want) {
					t.Fatalf("warning %q does not name %q", health.Warnings[index], want)
				}
			}
		})
	}
}
