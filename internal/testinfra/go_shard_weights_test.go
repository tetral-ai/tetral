package testinfra

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestGoShardWeightsRejectMalformedFilesAndPriceEveryUnit(t *testing.T) {
	const valid = `{"version": 2, "generated_at": "2026-10-11", "base": "b", "source_runs": ["1"], "cpu_models": {"Fixture CPU": {"factor": 1, "jobs": 1, "samples": 1, "shared_units": 0}}, "median_test_ms": 50,
		"sliceable": ["p/s", "p/s-new"],
		"packages": {
			"p/s": {"elapsed_ms": 900, "overhead_ms": 100, "tests_ms": {"TestA": 300, "TestB": 500, "TestRenamed": 700}},
			"p/w": {"elapsed_ms": 2000, "overhead_ms": 200},
			"p/removed": {"elapsed_ms": 9999, "overhead_ms": 1}}}`
	for _, test := range []struct{ name, old, new string }{
		{name: "the previous version", old: `"version": 2`, new: `"version": 1`},
		{name: "an unknown field", old: `"base": "b"`, new: `"base": "b", "calibrated_at": "2026-10-11"`},
		{name: "a malformed date", old: `"2026-10-11"`, new: `"11.10.2026"`},
		{name: "no source runs", old: `["1"]`, new: `[]`},
		{name: "a negative weight", old: `"TestA": 300`, new: `"TestA": -1`},
		{name: "test weights on a package that is not sliceable", old: `"overhead_ms": 200}`, new: `"overhead_ms": 200, "tests_ms": {"TestW": 1}}`},
		{name: "a duplicate sliceable package", old: `["p/s", "p/s-new"]`, new: `["p/s", "p/s"]`},
		{name: "a CPU model without a positive factor", old: `"factor": 1,`, new: `"factor": 0,`},
	} {
		t.Run("rejects "+test.name, func(t *testing.T) {
			if !strings.Contains(valid, test.old) {
				t.Fatalf("fixture lacks %s", test.old)
			}
			if _, err := parseGoShardWeights([]byte(strings.Replace(valid, test.old, test.new, 1))); err == nil {
				t.Fatal("malformed weights accepted")
			}
		})
	}
	weights, err := parseGoShardWeights([]byte(valid))
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name      string
		selection Selection
		want      int64
	}{
		{name: "a known test with its package overhead", selection: Selection{Packages: []string{"p/s"}, Tests: []string{"TestA"}}, want: 400},
		{name: "a new test at its package's median test weight", selection: Selection{Packages: []string{"p/s"}, Tests: []string{"TestNew"}}, want: 600},
		{name: "a whole package", selection: Selection{Packages: []string{"p/w"}, Tests: []string{"TestW"}}, want: 2200},
		{name: "a new package at the repository median test weight", selection: Selection{Packages: []string{"p/new"}, Tests: []string{"TestA", "TestB", "TestC"}}, want: 150},
		{name: "a new sliceable package at the repository median test weight", selection: Selection{Packages: []string{"p/s-new"}, Tests: []string{"TestA", "TestB"}}, want: 100},
	} {
		t.Run("prices "+test.name, func(t *testing.T) {
			if got := weights.selection(test.selection); got != test.want {
				t.Fatalf("weight = %d; want %d", got, test.want)
			}
		})
	}
	t.Run("plans without the names the plan no longer has", func(t *testing.T) {
		selections := []Selection{
			{Group: "go", Packages: []string{"p/s"}, Tests: []string{"TestA", "TestB", "TestNew"}},
			{Group: "go", Packages: []string{"p/w"}, Tests: []string{"TestW"}},
			{Group: "go", Packages: []string{"p/new"}, Tests: []string{"TestA"}},
		}
		var planned []string
		for _, shard := range balanceGoSelections(selections, 2, weights) {
			for _, selection := range shard {
				for _, test := range selection.Tests {
					planned = append(planned, selection.Packages[0]+"."+test)
				}
			}
		}
		slices.Sort(planned)
		want := []string{"p/new.TestA", "p/s.TestA", "p/s.TestB", "p/s.TestNew", "p/w.TestW"}
		if !slices.Equal(planned, want) {
			t.Fatalf("planned %v; want %v", planned, want)
		}
	})
}

type shardWeightFixtureTest struct {
	name string
	// Seconds after the step starts; pause and cont are zero for a test that
	// never waits.
	run, pause, cont, final, elapsed float64
	failed                           bool
}

type shardWeightFixtureStep struct {
	pkg                         string
	tests                       []shardWeightFixtureTest
	packageElapsed, stepElapsed float64
}

type shardWeightFixtureShard struct {
	cpu   string
	steps []shardWeightFixtureStep
}

// shardWeightFixtureRun's cpu and steps are its shard 0; more holds any
// further shards. An empty cpu is the fixture's single CPU model.
type shardWeightFixtureRun struct {
	id, workflow, head, cache string
	cpu                       string
	steps                     []shardWeightFixtureStep
	more                      []shardWeightFixtureShard
}

func TestGenerateGoShardWeights(t *testing.T) {
	repository := t.TempDir()
	runGit(t, repository, "init", "-q")
	commit := func(message string) string {
		runGit(t, repository, "-c", "user.name=Test", "-c", "user.email=test@example.invalid", "commit", "-q", "--allow-empty", "-m", message)
		return runGit(t, repository, "rev-parse", "HEAD")
	}
	before, base, head := commit("before"), commit("base"), commit("head")
	const sliced, whole = "example.test/m/s", "example.test/m/w"
	// TestPaused waits 40 s paused by t.Parallel and is active 12 s;
	// TestParallelParent's subtests run in parallel, so it reports Elapsed
	// 0.02 s while it is active 54 s.
	paused := shardWeightFixtureTest{name: "TestPaused", run: 0, pause: 0.5, cont: 40.5, final: 52, elapsed: 12}
	parent := shardWeightFixtureTest{name: "TestParallelParent", run: 52, final: 106, elapsed: 0.02}
	run := func(id string, extra ...shardWeightFixtureTest) shardWeightFixtureRun {
		tests := append([]shardWeightFixtureTest{paused, parent}, extra...)
		return shardWeightFixtureRun{id: id, workflow: verificationWorkflowPR, head: head, cache: "prefix", steps: []shardWeightFixtureStep{
			{pkg: sliced, tests: tests, packageElapsed: 107, stepElapsed: 110},
			// The package's Elapsed exceeds its one test's active time.
			{pkg: whole, tests: []shardWeightFixtureTest{{name: "TestWhole", final: 3, elapsed: 3}}, packageElapsed: 4, stepElapsed: 6},
		}}
	}
	slow := shardWeightFixtureTest{name: "TestPaused", final: 100, elapsed: 100}
	// tested is a shard on a CPU model running tests of 10 s times scale.
	tested := func(cpu string, scale float64, names ...string) shardWeightFixtureShard {
		var tests []shardWeightFixtureTest
		for _, name := range names {
			tests = append(tests, shardWeightFixtureTest{name: name, final: 10 * scale, elapsed: 10 * scale})
		}
		total := 10 * scale * float64(len(names))
		return shardWeightFixtureShard{cpu: cpu, steps: []shardWeightFixtureStep{{pkg: sliced, tests: tests, packageElapsed: total, stepElapsed: total + scale}}}
	}
	cpuRun := func(id string, shards ...shardWeightFixtureShard) shardWeightFixtureRun {
		return shardWeightFixtureRun{id: id, workflow: verificationWorkflowPR, head: head, cache: "prefix", cpu: shards[0].cpu, steps: shards[0].steps, more: shards[1:]}
	}
	shared := []string{"TestShared1", "TestShared2", "TestShared3", "TestShared4", "TestShared5"}
	// The current weights know TestKept and the whole package.
	current := `{"version": 2, "generated_at": "2026-10-10", "base": "b", "source_runs": ["1"], "cpu_models": {"Fixture CPU": {"factor": 1, "jobs": 1, "samples": 1, "shared_units": 0}}, "median_test_ms": 1, "sliceable": ["` + sliced + `"],
		"packages": {"` + sliced + `": {"elapsed_ms": 1, "overhead_ms": 1, "tests_ms": {"TestKept": 7777}}, "` + whole + `": {"elapsed_ms": 9999, "overhead_ms": 1111}}}`
	tests := []struct {
		name    string
		runs    []shardWeightFixtureRun
		check   func(*testing.T, GoShardWeightsGeneration, goShardWeights)
		wantErr string
	}{
		{
			name: "a test weighs its active time, without a paused wait and with parallel subtests",
			runs: []shardWeightFixtureRun{run("1"), run("2"), run("3")},
			check: func(t *testing.T, generation GoShardWeightsGeneration, weights goShardWeights) {
				got := weights.Packages[sliced]
				if got.TestsMS["TestPaused"] != 12000 || got.TestsMS["TestParallelParent"] != 54000 || got.OverheadMS != 44000 || got.ElapsedMS != 107000 {
					t.Fatalf("sliced package = %+v; want tests 12 s and 54 s, overhead 110 - 66 s and Elapsed 107 s", got)
				}
				if got := weights.Packages[whole]; got.ElapsedMS != 4000 || got.OverheadMS != 2000 || got.TestsMS != nil {
					t.Fatalf("whole package = %+v; want Elapsed 4 s and overhead 6 - 4 s", got)
				}
				if weights.MedianTestMS != 12000 || !slices.Equal(weights.SourceRuns, []string{"1", "2", "3"}) || weights.Base != base {
					t.Fatalf("weights = %+v", weights)
				}
			},
		},
		{
			name: "a failed test counts only its passing runs",
			runs: func() []shardWeightFixtureRun {
				// Passing runs take 50, 54 and 58 s; counting the failed 100 s
				// run would move the median to 56 s.
				runs := []shardWeightFixtureRun{run("1"), run("2"), run("3"), run("4")}
				for index, final := range []float64{102, 106, 110} {
					runs[index].steps[0].tests[1].final = final
				}
				runs[3].steps[0].tests[1] = shardWeightFixtureTest{name: "TestParallelParent", run: 52, final: 152, failed: true}
				return runs
			}(),
			check: func(t *testing.T, _ GoShardWeightsGeneration, weights goShardWeights) {
				if got := weights.Packages[sliced]; got.TestsMS["TestParallelParent"] != 54000 {
					t.Fatalf("sliced package tests = %v; want the failed run left out", got.TestsMS)
				}
			},
		},
		{
			name: "a unit with fewer than three passing runs keeps its current weight or gets none",
			runs: func() []shardWeightFixtureRun {
				// TestNew and TestKept pass twice; the whole package fails once.
				added := []shardWeightFixtureTest{{name: "TestNew", run: 110, final: 111}, {name: "TestKept", run: 111, final: 112}}
				runs := []shardWeightFixtureRun{run("1"), run("2", added...), run("3", added...)}
				runs[2].steps[1].tests[0].failed = true
				return runs
			}(),
			check: func(t *testing.T, generation GoShardWeightsGeneration, weights goShardWeights) {
				tests, kept, omitted := weights.Packages[sliced].TestsMS, strings.Join(generation.Kept, "\n"), strings.Join(generation.Omitted, "\n")
				if _, ok := tests["TestNew"]; ok || tests["TestKept"] != 7777 || !strings.Contains(omitted, "TestNew: 2 samples") || !strings.Contains(kept, "TestKept: 2 samples") {
					t.Fatalf("tests = %v, kept = %q, omitted = %q; want TestNew omitted and TestKept at its current 7777 ms", tests, kept, omitted)
				}
				if got := weights.Packages[whole]; got.ElapsedMS != 9999 || got.OverheadMS != 1111 || !strings.Contains(kept, "package "+whole+": 2 complete-run and 2 step samples") {
					t.Fatalf("whole package = %+v, kept = %q; want its current weights", got, kept)
				}
			},
		},
		{
			// Shard 0 always ran on the slow model; shard 1 ran there once and
			// twice on a model twice as fast. Every test takes 10 s on the
			// fast model, so all weigh the same on the common scale, which
			// the median job's model, the slow one, sets.
			name: "a test measured only on a slow CPU model weighs as much as its equals",
			runs: []shardWeightFixtureRun{
				cpuRun("1", tested("slow CPU", 2, "TestOnlySlow"), tested("slow CPU", 2, shared...)),
				cpuRun("2", tested("slow CPU", 2, "TestOnlySlow"), tested("fast CPU", 1, shared...)),
				cpuRun("3", tested("slow CPU", 2, "TestOnlySlow"), tested("fast CPU", 1, shared...)),
			},
			check: func(t *testing.T, _ GoShardWeightsGeneration, weights goShardWeights) {
				tests := weights.Packages[sliced].TestsMS
				if tests["TestOnlySlow"] != 20000 || tests["TestShared1"] != 20000 || tests["TestShared5"] != 20000 {
					t.Fatalf("tests = %v; want every test at 20 s", tests)
				}
				if slow, fast := weights.CPUModels["slow CPU"], weights.CPUModels["fast CPU"]; slow.Factor != 1 || fast.Factor != 0.5 || slow.Jobs != 4 || fast.SharedUnits != 5 {
					t.Fatalf("CPU models = %+v; want the slow model at 1 over 4 jobs and the fast one at 0.5 with 5 shared units", weights.CPUModels)
				}
			},
		},
		{
			// Three runs, three jobs on each model, whose 10 s tests jitter by
			// up to 8 % on a fast model at half the slow one's time. With
			// three samples a median unit weight would be one sample and
			// hold the fit at its start; the middle of the six jobs averaged
			// would put the reference between the two models.
			name: "a factor fitted from three jittered runs",
			runs: func() []shardWeightFixtureRun {
				jitter := []float64{0.92, 0.92, 1.04, 1.04, 0.92, 1.0, 1.08, 0.96, 0.96, 1.04, 1.04, 0.92, 1.0, 1.0, 1.08, 1.04, 1.08, 1.0,
					1.04, 0.92, 1.0, 1.04, 1.0, 1.0, 0.92, 1.0, 1.04, 1.04, 0.92, 1.0, 1.08, 0.96, 0.96, 1.04, 0.96, 0.92}
				var runs []shardWeightFixtureRun
				for index, models := range [][2]string{{"slow CPU", "fast CPU"}, {"fast CPU", "slow CPU"}, {"slow CPU", "fast CPU"}} {
					var shards []shardWeightFixtureShard
					for shard, model := range models {
						scale := 1.0
						if model == "fast CPU" {
							scale = 0.5
						}
						var tests []shardWeightFixtureTest
						var total float64
						for unit := 0; unit < 6; unit++ {
							seconds := 10 * scale * jitter[(index*2+shard)*6+unit]
							tests = append(tests, shardWeightFixtureTest{name: fmt.Sprintf("TestUnit%d_%d", shard, unit), final: seconds, elapsed: seconds})
							total += seconds
						}
						shards = append(shards, shardWeightFixtureShard{cpu: model, steps: []shardWeightFixtureStep{{pkg: sliced, tests: tests, packageElapsed: total, stepElapsed: total + 1}}})
					}
					runs = append(runs, cpuRun(strconv.Itoa(index+1), shards...))
				}
				return runs
			}(),
			check: func(t *testing.T, _ GoShardWeightsGeneration, weights goShardWeights) {
				if slow, fast := weights.CPUModels["slow CPU"].Factor, weights.CPUModels["fast CPU"].Factor; slow != 1 || fast < 0.475 || fast > 0.525 {
					t.Fatalf("factors = slow %v, fast %v; want the slow model at 1 and the fast one at 0.5 within 5 %%", slow, fast)
				}
			},
		},
		{
			name: "a CPU model sharing fewer than five units with the others",
			runs: []shardWeightFixtureRun{
				cpuRun("1", tested("odd CPU", 1, shared[:4]...)), cpuRun("2", tested("", 1, shared...)), cpuRun("3", tested("", 1, shared...)),
			},
			wantErr: "shares 4 units with other models; its factor needs 5",
		},
		{
			name:    "a result without a CPU model",
			runs:    []shardWeightFixtureRun{cpuRun("1", tested("none", 1, shared...)), cpuRun("2", tested("", 1, shared...)), cpuRun("3", tested("", 1, shared...))},
			wantErr: "run 1 shard 0 records no runner CPU model",
		},
		{
			name:    "fewer than three qualifying runs",
			runs:    []shardWeightFixtureRun{run("1"), run("2")},
			wantErr: "2 runs qualify; every unit needs 3 samples",
		},
		{
			name: "runs without a cache hit, outside verification or before the base are left out",
			runs: func() []shardWeightFixtureRun {
				miss, benchmark, old := run("4"), run("5"), run("6")
				miss.cache, benchmark.workflow, old.head = "miss", "CI Benchmark", before
				for _, item := range []*shardWeightFixtureRun{&miss, &benchmark, &old} {
					item.steps[0].tests[0] = slow
				}
				return []shardWeightFixtureRun{run("1"), run("2"), run("3"), miss, benchmark, old}
			}(),
			check: func(t *testing.T, generation GoShardWeightsGeneration, weights goShardWeights) {
				reasons := strings.Join(generation.Excluded, "\n")
				if !slices.Equal(generation.Runs, []string{"1", "2", "3"}) || len(generation.Excluded) != 3 ||
					!strings.Contains(reasons, "run 4: a shard did not restore") || !strings.Contains(reasons, `run 5: workflow "CI Benchmark"`) || !strings.Contains(reasons, "run 6: tested commit "+before) {
					t.Fatalf("runs = %v, excluded = %v", generation.Runs, generation.Excluded)
				}
				if weights.Packages[sliced].TestsMS["TestPaused"] != 12000 {
					t.Fatalf("TestPaused = %d; want only the used runs", weights.Packages[sliced].TestsMS["TestPaused"])
				}
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			// Each run has its own evidence root.
			var roots []string
			for _, item := range test.runs {
				root := t.TempDir()
				writeShardWeightRun(t, root, item)
				roots = append(roots, root)
			}
			generate := func(roots []string) (GoShardWeightsGeneration, error) {
				return GenerateGoShardWeights(GoShardWeightsOptions{
					Evidence: roots, Base: base, RepositoryRoot: repository,
					Current: []byte(current), ShardCount: 4, Date: time.Date(2026, 10, 11, 0, 0, 0, 0, time.UTC),
				})
			}
			generation, err := generate(roots)
			if test.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), test.wantErr) {
					t.Fatalf("error = %v; want %q", err, test.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			// The evidence's order never changes the file.
			backward := slices.Clone(roots)
			slices.Reverse(backward)
			reversed, err := generate(backward)
			if err != nil || !bytes.Equal(reversed.Weights, generation.Weights) {
				t.Fatalf("generating from the evidence in reverse order gave a different file (error %v)", err)
			}
			weights, err := parseGoShardWeights(generation.Weights)
			if err != nil {
				t.Fatal(err)
			}
			test.check(t, generation, weights)
		})
	}
}

// writeShardWeightRun writes a run's Go Race shards.
func writeShardWeightRun(t *testing.T, root string, run shardWeightFixtureRun) {
	t.Helper()
	shards := append([]shardWeightFixtureShard{{cpu: run.cpu, steps: run.steps}}, run.more...)
	for index, shard := range shards {
		directory := filepath.Join(root, run.id, fmt.Sprintf("go-%d", index))
		if err := os.MkdirAll(directory, 0o700); err != nil {
			t.Fatal(err)
		}
		switch shard.cpu {
		case "":
			shard.cpu = "Fixture CPU"
		case "none":
			shard.cpu = ""
		}
		writeShardWeightShard(t, directory, run, index, len(shards), shard)
	}
}

// writeShardWeightShard writes one Go Race shard: a result and one go test
// -json report per step.
func writeShardWeightShard(t *testing.T, directory string, run shardWeightFixtureRun, index, count int, shard shardWeightFixtureShard) {
	t.Helper()
	start := time.Date(2026, 10, 11, 12, 0, 0, 0, time.UTC)
	result := Result{
		Plan: Plan{Profile: ProfileFull, Revision: Revision{Head: run.head}, GoSelection: &GoShardIdentity{Digest: "fixture", ShardIndex: index, ShardCount: count}},
		Execution: ExecutionEnvelope{Workflow: run.workflow, RunID: run.id, RunAttempt: "1", Producer: fmt.Sprintf("go-%d", index),
			GoCache: &GoCacheRestore{Match: run.cache}, CPUModel: shard.cpu},
		Status: "pass",
	}
	for position, step := range shard.steps {
		selection := Selection{Group: "go", Packages: []string{step.pkg}, Mode: "race"}
		var events []string
		event := func(offset float64, action, test string, elapsed float64) {
			body, err := json.Marshal(map[string]any{"Time": start.Add(benchmarkFixtureSeconds(offset)), "Action": action, "Package": step.pkg, "Test": test, "Elapsed": elapsed})
			if err != nil {
				t.Fatal(err)
			}
			events = append(events, string(body))
		}
		packageAction := "pass"
		for _, test := range step.tests {
			selection.Tests = append(selection.Tests, test.name)
			event(test.run, "run", test.name, 0)
			if test.pause > 0 {
				event(test.pause, "pause", test.name, 0)
				event(test.cont, "cont", test.name, 0)
			}
			action := "pass"
			if test.failed {
				action, packageAction = "fail", "fail"
			}
			event(test.final, action, test.name, test.elapsed)
		}
		event(step.packageElapsed, packageAction, "", step.packageElapsed)
		artifact := fmt.Sprintf("step-%d.jsonl", position)
		writeTestFile(t, directory, artifact, strings.Join(events, "\n")+"\n")
		status := packageAction
		if status == "fail" {
			result.Status = "fail"
		}
		result.Plan.Selections = append(result.Plan.Selections, selection)
		result.Steps = append(result.Steps, StepResult{Group: "go", Command: []string{"go", "test", "-json", step.pkg}, Status: status, Elapsed: benchmarkFixtureSeconds(step.stepElapsed), Artifact: artifact})
	}
	body, err := json.Marshal(result)
	if err != nil {
		t.Fatal(err)
	}
	writeTestFile(t, directory, "result.json", string(body))
}
