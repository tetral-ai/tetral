package testinfra

import (
	"context"
	"fmt"
	"maps"
	"math"
	"net/url"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"
)

const (
	// Verification evidence artifacts expire after 14 days; the trend shows
	// its two weeks.
	goRaceHealthWeek  = 7 * 24 * time.Hour
	goRaceHealthWeeks = 2
	// Per week, the newest runs whose Go Race evidence is downloaded and the
	// pull request runs, spread across the week, whose job timings are read.
	goRaceHealthEvidenceRuns = 10
	goRaceHealthTimedRuns    = 40
	goRaceHealthListPages    = 20
	goRaceHealthConcurrency  = 8
	goRaceHealthMinimumRuns  = 3
	// Warning thresholds, in seconds.
	goRaceMakespanTolerance = 60.0
	goRaceUnitTolerance     = 30.0
	goRaceNewTestFloor      = 10.0
	// A job that waited longer than this for a runner makes its attempt queued.
	goRaceQueueTolerance = 60 * time.Second
	// The download is chosen by artifact name; shards are then identified by
	// their recorded identity.
	goRaceEvidencePattern = "*-evidence-go-[0-9]*"
)

var goRaceHealthWorkflows = map[string]string{
	"main-branch-verification.yml":  verificationWorkflowMain,
	"pull-request-verification.yml": verificationWorkflowPR,
}

// GoRaceHealth compares recent Go Race runs with the committed shard weights.
type GoRaceHealth struct {
	Runs             []GoRaceHealthRun `json:"runs"`
	InsufficientData bool              `json:"insufficient_data,omitempty"`
	// MakespanExcess is the median over runs whose shards all passed of the
	// speed-normalized slowest steps phase minus the weights' predicted
	// makespan of that run's plan, in seconds.
	MakespanExcess            *float64          `json:"median_makespan_excess_s,omitempty"`
	DriftedUnits              []GoRaceUnitDrift `json:"drifted_units,omitempty"`
	NewSlowTests              []GoRaceUnitDrift `json:"new_slow_tests,omitempty"`
	TestWorkMinutes           *float64          `json:"median_test_work_min,omitempty"`
	CalibratedTestWorkMinutes float64           `json:"calibrated_test_work_min"`
	WeightsGeneratedAt        string            `json:"weights_generated_at"`
	// Workers is the number of Go package processes a run has across its
	// shards: that many minutes of summed Go test work add about one minute
	// of wall time.
	Workers int `json:"workers"`
	// Unscaled counts, by CPU model, the unit samples left out of the drift
	// checks: their model has no factor and their job no speed.
	Unscaled map[string]int       `json:"unscaled_samples,omitempty"`
	Trend    []GoRaceHealthWindow `json:"trend,omitempty"`
	Warnings []string             `json:"warnings,omitempty"`
	Notes    []string             `json:"notes,omitempty"`
}

// GoRaceHealthRun summarizes one run. NormalizedStepsPhase is the slowest of
// its shards' steps phases each divided by its job's speed; shards whose job
// has no speed are listed in Unnormalized and left out of it.
type GoRaceHealthRun struct {
	RunID                string   `json:"run_id"`
	Workflow             string   `json:"workflow"`
	Passed               bool     `json:"passed"`
	SlowestStepsPhase    float64  `json:"slowest_steps_phase_s"`
	NormalizedStepsPhase *float64 `json:"normalized_slowest_steps_phase_s,omitempty"`
	PredictedMakespan    float64  `json:"predicted_makespan_s"`
	SlowestJobSpeed      *float64 `json:"slowest_job_speed,omitempty"`
	FastestJobSpeed      *float64 `json:"fastest_job_speed,omitempty"`
	Unnormalized         []int    `json:"unnormalized_shards,omitempty"`
	TestWorkMinutes      float64  `json:"test_work_min"`
}

// GoRaceUnitDrift is a unit's median over recent runs against the weight the
// planner gives it; Test is empty for a package that runs whole.
type GoRaceUnitDrift struct {
	Package string  `json:"package"`
	Test    string  `json:"test,omitempty"`
	Median  float64 `json:"median_s"`
	Weight  float64 `json:"weight_s"`
	Samples int     `json:"samples"`
}

// GoRaceHealthWindow summarizes first attempts of Pull Request Verification
// runs that ended in success or failure and were created in [From, To).
// Timings come from TimedAttempts of them, spread across the window. A solo
// attempt had every job, Merge Gate included, start within a minute of
// becoming ready. The normalized wall time removes the window's change in
// summed Go test work since the weights were generated.
type GoRaceHealthWindow struct {
	From                    time.Time `json:"from"`
	To                      time.Time `json:"to"`
	Attempts                int       `json:"attempts"`
	TimedAttempts           int       `json:"timed_attempts"`
	QueuedShare             *float64  `json:"queued_share,omitempty"`
	SoloWallMinutes         *float64  `json:"median_solo_wall_min,omitempty"`
	NormalizedSoloWall      *float64  `json:"median_normalized_solo_wall_min,omitempty"`
	RunnerMinutesPerAttempt *float64  `json:"median_runner_minutes,omitempty"`
	TestWorkMinutes         *float64  `json:"median_test_work_min,omitempty"`
	TestWorkRuns            int       `json:"test_work_runs"`
}

type githubEvidenceClient interface {
	githubAPIClient
	DownloadRunArtifacts(ctx context.Context, repository string, runID int64, pattern, directory string) error
}

type goRaceListedRun struct {
	githubWorkflowRun
	workflow string
}

// collectGoRaceHealth lists the last two weeks of Pull Request and Main
// Branch Verification runs, downloads the Go Race evidence of each week's
// newest runs into directory, compares the runs whose base contains the
// weights' base with the weights, and reads job timings for the trend. Read
// failures become notes.
func collectGoRaceHealth(ctx context.Context, client githubEvidenceClient, repository, directory string, now time.Time, weights goShardWeights, workers int) GoRaceHealth {
	var notes []string
	windows := make([]GoRaceHealthWindow, goRaceHealthWeeks)
	for index := range windows {
		windows[index].To = now.Add(-time.Duration(index) * goRaceHealthWeek).UTC()
		windows[index].From = windows[index].To.Add(-goRaceHealthWeek)
	}
	listed, listNotes := listGoRaceRuns(ctx, client, repository, windows[len(windows)-1].From)
	notes = append(notes, listNotes...)

	var evidence, timed []goRaceListedRun
	for index := range windows {
		var inside, pullRequests []goRaceListedRun
		for _, run := range listed {
			if !run.CreatedAt.Before(windows[index].From) && run.CreatedAt.Before(windows[index].To) {
				inside = append(inside, run)
				if run.workflow == verificationWorkflowPR {
					pullRequests = append(pullRequests, run)
				}
			}
		}
		windows[index].Attempts = len(pullRequests)
		evidence = append(evidence, inside[:min(len(inside), goRaceHealthEvidenceRuns)]...)
		timed = append(timed, spreadGoRaceRuns(pullRequests, goRaceHealthTimedRuns)...)
	}

	downloaded := make([]map[string]*goRaceRun, len(evidence))
	errs := make([]error, len(evidence))
	forEachConcurrently(len(evidence), goRaceHealthConcurrency, func(index int) {
		target := filepath.Join(directory, fmt.Sprint(evidence[index].ID))
		if errs[index] = client.DownloadRunArtifacts(ctx, repository, evidence[index].ID, goRaceEvidencePattern, target); errs[index] == nil {
			// Each run loads on its own, so one bad download costs only its run.
			downloaded[index], errs[index] = loadGoRaceRuns(target)
		}
	})
	all, current := map[string]*goRaceRun{}, map[string]*goRaceRun{}
	contains, older := map[string]bool{}, 0
	for index, runs := range downloaded {
		if errs[index] != nil {
			notes = append(notes, errs[index].Error())
			continue
		}
		for id, run := range runs {
			all[id] = run
			base := goRaceRunBase(run)
			included, known := contains[base]
			if !known {
				var err error
				if included, err = githubCommitContains(ctx, client, repository, base, weights.Base); err != nil {
					notes = append(notes, err.Error())
				}
				contains[base] = included
			}
			if included {
				current[id] = run
			} else {
				older++
			}
		}
	}
	health := analyzeGoRaceHealth(current, weights)
	health.Workers = workers
	if older > 0 {
		notes = append(notes, fmt.Sprintf("%d downloaded runs tested a base older than the weights' base %s and were left out of the comparison", older, weights.Base))
	}

	attempts := make([]*goRaceAttempt, len(timed))
	jobErrors := make([]error, len(timed))
	forEachConcurrently(len(timed), goRaceHealthConcurrency, func(index int) {
		jobs, err := readAttemptJobs(ctx, client, repository, timed[index].ID, 1)
		if err != nil {
			jobErrors[index] = err
			return
		}
		if attempt, ok := goRaceAttemptTiming(timed[index].CreatedAt, jobs); ok {
			attempts[index] = &attempt
		}
	})
	var timings []goRaceAttempt
	for index, attempt := range attempts {
		if jobErrors[index] != nil {
			notes = append(notes, jobErrors[index].Error())
		} else if attempt != nil {
			timings = append(timings, *attempt)
		}
	}
	var work []goRaceRunWork
	for _, run := range evidence {
		if loaded := all[fmt.Sprint(run.ID)]; loaded != nil && loaded.complete() {
			work = append(work, goRaceRunWork{created: run.CreatedAt, minutes: goRaceTestWorkMinutes(loaded)})
		}
	}
	health.Trend = goRaceHealthTrend(windows, timings, work, health.CalibratedTestWorkMinutes, workers)
	health.Notes = append(health.Notes, notes...)
	return health
}

// listGoRaceRuns lists completed verification runs created since the given
// time that ended in success or failure: a run cancelled by a newer push
// measures nothing.
func listGoRaceRuns(ctx context.Context, client githubAPIClient, repository string, since time.Time) ([]goRaceListedRun, []string) {
	var listed []goRaceListedRun
	var notes []string
	for _, file := range slices.Sorted(maps.Keys(goRaceHealthWorkflows)) {
		for page := 1; ; page++ {
			if page > goRaceHealthListPages {
				notes = append(notes, fmt.Sprintf("%s has more than %d pages of runs since %s; older runs are left out", file, goRaceHealthListPages, since.Format(time.DateOnly)))
				break
			}
			query := url.Values{"status": {"completed"}, "per_page": {"100"}, "page": {fmt.Sprint(page)}, "created": {">=" + since.UTC().Format(time.RFC3339)}}
			var response struct {
				Runs []githubWorkflowRun `json:"workflow_runs"`
			}
			if err := client.JSON(ctx, fmt.Sprintf("repos/%s/actions/workflows/%s/runs?%s", repository, file, query.Encode()), &response); err != nil {
				notes = append(notes, err.Error())
				break
			}
			for _, run := range response.Runs {
				if run.Conclusion == "success" || run.Conclusion == "failure" {
					listed = append(listed, goRaceListedRun{githubWorkflowRun: run, workflow: goRaceHealthWorkflows[file]})
				}
			}
			if len(response.Runs) < 100 {
				break
			}
		}
	}
	sort.Slice(listed, func(i, j int) bool { return listed[i].CreatedAt.After(listed[j].CreatedAt) })
	return listed, notes
}

// spreadGoRaceRuns picks at most limit runs evenly across a newest-first list.
func spreadGoRaceRuns(runs []goRaceListedRun, limit int) []goRaceListedRun {
	if len(runs) <= limit {
		return runs
	}
	picked := make([]goRaceListedRun, limit)
	for index := range picked {
		picked[index] = runs[index*len(runs)/limit]
	}
	return picked
}

// goRaceRunBase is the commit a run's change was tested on: the pull
// request's base, or the pushed commit itself on main.
func goRaceRunBase(run *goRaceRun) string {
	indexes := run.shardIndexes()
	if len(indexes) == 0 {
		return ""
	}
	result := run.shards[indexes[0]].result
	if result.Execution.EventBaseSHA != "" {
		return result.Execution.EventBaseSHA
	}
	return result.Plan.Revision.Head
}

func githubCommitContains(ctx context.Context, client githubAPIClient, repository, commit, ancestor string) (bool, error) {
	if commit == "" {
		return false, nil
	}
	var comparison struct {
		Status string `json:"status"`
	}
	if err := client.JSON(ctx, fmt.Sprintf("repos/%s/compare/%s...%s?per_page=1", repository, ancestor, commit), &comparison); err != nil {
		return false, err
	}
	return comparison.Status == "ahead" || comparison.Status == "identical", nil
}

// forEachConcurrently calls work for every index below count, at most limit
// at a time, and returns when all have finished.
func forEachConcurrently(count, limit int, work func(int)) {
	var wait sync.WaitGroup
	slots := make(chan struct{}, limit)
	for index := 0; index < count; index++ {
		wait.Add(1)
		slots <- struct{}{}
		go func() {
			defer wait.Done()
			defer func() { <-slots }()
			work(index)
		}()
	}
	wait.Wait()
}

// goRaceShardFit is one shard's measured steps phase, its job's speed when it
// has one, and the weights' predicted makespan of its plan, in seconds.
type goRaceShardFit struct {
	measured, predicted float64
	speed               *float64
}

func goRaceShardFitOf(shard *benchmarkShardEvidence, split map[string]bool, weights goShardWeights) goRaceShardFit {
	fit := goRaceShardFit{measured: *shard.stepsPhase(), predicted: float64(predictGoShardMakespan(shard.plan, weights)) / 1000}
	if speed, _, ok := goRaceJobSpeed(shard, split, weights); ok {
		fit.speed = &speed
	}
	return fit
}

// summarizeGoRaceRuns uses runs whose every shard ran all its steps after a
// Go build cache hit. Besides each run's summary it returns the runs' unit
// samples and every package's planned tests.
func summarizeGoRaceRuns(runs map[string]*goRaceRun, weights goShardWeights) ([]GoRaceHealthRun, *goShardWeightSamples, map[string]Selection, map[string]float64) {
	var summaries []GoRaceHealthRun
	samples := newGoShardWeightSamples()
	planned := map[string]Selection{}
	speeds := map[string]float64{}
	for _, id := range slices.Sorted(maps.Keys(runs)) {
		run := runs[id]
		if !run.complete() || !goRaceRunRestoredCache(run) {
			continue
		}
		summary := GoRaceHealthRun{RunID: id, Passed: true}
		tests := map[string][]string{}
		split := run.splitPackages()
		for _, index := range run.shardIndexes() {
			shard := run.shards[index]
			fit := goRaceShardFitOf(shard, split, weights)
			summary.Workflow = shard.result.Execution.Workflow
			summary.Passed = summary.Passed && shard.result.Status == "pass"
			summary.SlowestStepsPhase = max(summary.SlowestStepsPhase, fit.measured)
			summary.PredictedMakespan = max(summary.PredictedMakespan, fit.predicted)
			if fit.speed == nil {
				summary.Unnormalized = append(summary.Unnormalized, index)
			} else {
				speeds[fmt.Sprintf("%s/%d", id, index)] = *fit.speed
				normalized := roundBenchmark(fit.measured / *fit.speed, 1000)
				if summary.NormalizedStepsPhase == nil || normalized > *summary.NormalizedStepsPhase {
					summary.NormalizedStepsPhase = &normalized
				}
				speed := roundBenchmark(*fit.speed, 1000)
				if summary.SlowestJobSpeed == nil || speed > *summary.SlowestJobSpeed {
					summary.SlowestJobSpeed = &speed
				}
				if summary.FastestJobSpeed == nil || speed < *summary.FastestJobSpeed {
					summary.FastestJobSpeed = &speed
				}
			}
			for _, step := range shard.steps {
				tests[step.selection.Packages[0]] = append(tests[step.selection.Packages[0]], step.selection.Tests...)
			}
		}
		summary.TestWorkMinutes = goRaceTestWorkMinutes(run)
		for name, names := range tests {
			planned[name] = Selection{Group: "go", Packages: []string{name}, Tests: names}
		}
		samples.add(run, weights.sliceable)
		summary.SlowestStepsPhase = roundBenchmark(summary.SlowestStepsPhase, 1000)
		summary.PredictedMakespan = roundBenchmark(summary.PredictedMakespan, 1000)
		summaries = append(summaries, summary)
	}
	return summaries, samples, planned, speeds
}

// goRaceTestWorkMinutes sums a complete run's package Elapsed times.
func goRaceTestWorkMinutes(run *goRaceRun) float64 {
	var seconds float64
	for _, shard := range run.shards {
		for _, step := range shard.steps {
			seconds += step.report.packageElapsed
		}
	}
	return roundBenchmark(seconds/60, 100)
}

// analyzeGoRaceHealth checks the weights against the runs. A unit's median
// needs three passing samples, and the makespan excess three passed runs.
func analyzeGoRaceHealth(runs map[string]*goRaceRun, weights goShardWeights) GoRaceHealth {
	health := GoRaceHealth{CalibratedTestWorkMinutes: roundBenchmark(weights.totalTestWork().Minutes(), 100), WeightsGeneratedAt: weights.GeneratedAt}
	summaries, samples, planned, speeds := summarizeGoRaceRuns(runs, weights)
	health.Runs = summaries
	if len(summaries) < goRaceHealthMinimumRuns {
		health.InsufficientData = true
		return health
	}
	var excess, work []float64
	for _, summary := range summaries {
		work = append(work, summary.TestWorkMinutes)
		if summary.Passed && summary.NormalizedStepsPhase != nil {
			excess = append(excess, *summary.NormalizedStepsPhase-summary.PredictedMakespan)
		}
	}
	health.TestWorkMinutes = benchmarkPointer(roundBenchmark(benchmarkMedian(work), 100))
	if len(excess) >= goRaceHealthMinimumRuns {
		health.MakespanExcess = benchmarkPointer(roundBenchmark(benchmarkMedian(excess), 1000))
	}
	drift := func(name, test string, values []float64, weight int64) GoRaceUnitDrift {
		return GoRaceUnitDrift{Package: name, Test: test, Median: roundBenchmark(benchmarkMedian(values), 1000), Weight: float64(weight) / 1000, Samples: len(values)}
	}
	// Samples go on the weights' machine scale through their CPU model's
	// factor or, for a model without one, their job's speed; samples with
	// neither are left out and counted.
	factors := weights.cpuFactors()
	scale := func(sample goShardSample) (float64, bool) {
		if factor, ok := factors[sample.cpu]; ok {
			return factor, true
		}
		if speed, ok := speeds[sample.job]; ok {
			return speed, true
		}
		if health.Unscaled == nil {
			health.Unscaled = map[string]int{}
		}
		health.Unscaled[sample.cpu]++
		return 0, false
	}
	for _, name := range slices.Sorted(maps.Keys(samples.elapsed)) {
		values := normalizeGoShardSums(samples.elapsed[name], scale)
		if weights.sliceable[name] || len(values) < goRaceHealthMinimumRuns {
			continue
		}
		unit := drift(name, "", values, weights.selection(planned[name])-weights.overhead(name))
		if math.Abs(unit.Median-unit.Weight) > goRaceUnitTolerance {
			health.DriftedUnits = append(health.DriftedUnits, unit)
		}
	}
	for _, key := range slices.SortedFunc(maps.Keys(samples.tests), compareTestKeys) {
		values := normalizeGoShardSamples(samples.tests[key], scale)
		if len(values) < goRaceHealthMinimumRuns {
			continue
		}
		name, test := key[0], key[1]
		_, weighed := weights.Packages[name].TestsMS[test]
		_, known := weights.Packages[name]
		unit := drift(name, test, values, weights.test(name, test))
		if weights.sliceable[name] && math.Abs(unit.Median-unit.Weight) > goRaceUnitTolerance {
			health.DriftedUnits = append(health.DriftedUnits, unit)
		}
		if unit.Median >= goRaceNewTestFloor && (weights.sliceable[name] && !weighed || !known) {
			health.NewSlowTests = append(health.NewSlowTests, unit)
		}
	}
	if health.MakespanExcess != nil && *health.MakespanExcess > goRaceMakespanTolerance {
		health.Warnings = append(health.Warnings, fmt.Sprintf("Go Race's speed-normalized slowest steps phase ran a median %.0f s longer than the shard weights predict (more than %.0f s); regenerate go_shard_weights.json", *health.MakespanExcess, goRaceMakespanTolerance))
	}
	if len(health.DriftedUnits) > 0 {
		health.Warnings = append(health.Warnings, fmt.Sprintf("%d Go units differ from their shard weights by more than %.0f s: %s", len(health.DriftedUnits), goRaceUnitTolerance, goRaceUnitNames(health.DriftedUnits)))
	}
	if len(health.NewSlowTests) > 0 {
		health.Warnings = append(health.Warnings, fmt.Sprintf("%d top-level Go tests the shard weights do not know take %.0f s or more: %s", len(health.NewSlowTests), goRaceNewTestFloor, goRaceUnitNames(health.NewSlowTests)))
	}
	return health
}

func goRaceRunRestoredCache(run *goRaceRun) bool {
	for _, shard := range run.shards {
		if cache := shard.result.Execution.GoCache; cache == nil || cache.Match != "exact" && cache.Match != "prefix" {
			return false
		}
	}
	return true
}

func goRaceUnitNames(units []GoRaceUnitDrift) string {
	var names []string
	for _, unit := range units {
		name := unit.Package
		if unit.Test != "" {
			name += "." + unit.Test
		}
		names = append(names, fmt.Sprintf("%s (%.1f s, weight %.1f s)", name, unit.Median, unit.Weight))
	}
	return strings.Join(names, ", ")
}

// goRaceAttempt is a Pull Request Verification first attempt's timing from
// the jobs API: wall time from the run's creation to Merge Gate's
// completion, and the runner time of every job it executed.
type goRaceAttempt struct {
	created       time.Time
	wall          time.Duration
	queued        bool
	runnerMinutes float64
}

func goRaceAttemptTiming(created time.Time, jobs []workflowJob) (goRaceAttempt, bool) {
	attempt := goRaceAttempt{created: created}
	var gate *workflowJob
	var lastNeed time.Time
	for index := range jobs {
		job := &jobs[index]
		// A cancelled job lets Merge Gate finish early.
		if job.Conclusion == "cancelled" {
			return attempt, false
		}
		if job.Status != "completed" || job.Conclusion == "skipped" || job.StartedAt.IsZero() || job.StartedAt.Before(created) || job.CompletedAt.Before(job.StartedAt) {
			continue
		}
		firstStep := job.StartedAt
		if len(job.Steps) > 0 && !job.Steps[0].StartedAt.IsZero() {
			firstStep = job.Steps[0].StartedAt
		}
		attempt.runnerMinutes += job.CompletedAt.Sub(firstStep).Minutes()
		if !job.CreatedAt.IsZero() && job.StartedAt.Sub(job.CreatedAt) > goRaceQueueTolerance {
			attempt.queued = true
		}
		if job.Name == "Merge Gate" {
			gate = job
		} else if job.CompletedAt.After(lastNeed) {
			lastNeed = job.CompletedAt
		}
	}
	if gate == nil {
		return attempt, false
	}
	if gate.StartedAt.Sub(lastNeed) > goRaceQueueTolerance {
		attempt.queued = true
	}
	attempt.wall = gate.CompletedAt.Sub(created)
	return attempt, true
}

// goRaceRunWork is a complete run's summed Go test work by its creation time.
type goRaceRunWork struct {
	created time.Time
	minutes float64
}

// goRaceHealthTrend fills each window from its timed attempts and the test
// work of the complete runs whose evidence was downloaded.
func goRaceHealthTrend(windows []GoRaceHealthWindow, attempts []goRaceAttempt, work []goRaceRunWork, calibrated float64, workers int) []GoRaceHealthWindow {
	median := func(values []float64) *float64 {
		if len(values) == 0 {
			return nil
		}
		return benchmarkPointer(roundBenchmark(benchmarkMedian(values), 100))
	}
	for index := range windows {
		window := &windows[index]
		inside := func(at time.Time) bool { return !at.Before(window.From) && at.Before(window.To) }
		var solo, runner, testWork []float64
		queued := 0
		for _, attempt := range attempts {
			if !inside(attempt.created) {
				continue
			}
			window.TimedAttempts++
			runner = append(runner, attempt.runnerMinutes)
			if attempt.queued {
				queued++
			} else {
				solo = append(solo, attempt.wall.Minutes())
			}
		}
		for _, run := range work {
			if inside(run.created) {
				testWork = append(testWork, run.minutes)
			}
		}
		if window.TimedAttempts > 0 {
			window.QueuedShare = benchmarkPointer(roundBenchmark(float64(queued)/float64(window.TimedAttempts), 100))
		}
		window.SoloWallMinutes, window.RunnerMinutesPerAttempt, window.TestWorkMinutes = median(solo), median(runner), median(testWork)
		window.TestWorkRuns = len(testWork)
		if window.SoloWallMinutes != nil && window.TestWorkMinutes != nil && workers > 0 {
			window.NormalizedSoloWall = benchmarkPointer(roundBenchmark(*window.SoloWallMinutes-(*window.TestWorkMinutes-calibrated)/float64(workers), 100))
		}
	}
	return windows
}

// RenderGoRaceHealth writes the health job's step summary section.
func RenderGoRaceHealth(health GoRaceHealth) string {
	var out strings.Builder
	value := func(pointer *float64, format string) string {
		if pointer == nil {
			return "n/a"
		}
		return fmt.Sprintf(format, *pointer)
	}
	out.WriteString("## Go Race shard weights\n\n")
	fmt.Fprintf(&out, "Weights generated %s; %d recent runs compared.\n\n", health.WeightsGeneratedAt, len(health.Runs))
	if health.InsufficientData {
		fmt.Fprintf(&out, "Insufficient data: fewer than %d complete Go Race runs with a Go build cache hit test the weights' base.\n\n", goRaceHealthMinimumRuns)
	} else {
		fmt.Fprintf(&out, "- Median speed-normalized slowest steps phase minus predicted makespan: %s (warning above %.0f s)\n", value(health.MakespanExcess, "%.0f s"), goRaceMakespanTolerance)
		fmt.Fprintf(&out, "- Summed Go test work: median %s against %.1f min in the weights; every %d min more adds about 1 min of solo wall time\n", value(health.TestWorkMinutes, "%.1f min"), health.CalibratedTestWorkMinutes, health.Workers)
		fmt.Fprintf(&out, "- Units more than %.0f s off their weight: %d; unweighted top-level tests of %.0f s or more: %d\n\n", goRaceUnitTolerance, len(health.DriftedUnits), goRaceNewTestFloor, len(health.NewSlowTests))
	}
	for _, warning := range health.Warnings {
		fmt.Fprintf(&out, "> **Warning:** %s\n\n", warning)
	}
	if len(health.Runs) > 0 {
		out.WriteString("A job's speed is its median measured ÷ weight; a normalized steps phase divides each shard's by its job's speed.\n\n")
		out.WriteString("| Run | Workflow | Passed | Slowest steps phase | Normalized | Predicted | Job speeds | Unnormalized shards |\n|---|---|---|---:|---:|---:|---|---|\n")
		for _, run := range health.Runs {
			speeds := "n/a"
			if run.FastestJobSpeed != nil {
				speeds = fmt.Sprintf("%.2f–%.2f", *run.FastestJobSpeed, *run.SlowestJobSpeed)
			}
			fmt.Fprintf(&out, "| %s | %s | %t | %.0f s | %s | %.0f s | %s | %v |\n", run.RunID, run.Workflow, run.Passed, run.SlowestStepsPhase,
				value(run.NormalizedStepsPhase, "%.0f s"), run.PredictedMakespan, speeds, run.Unnormalized)
		}
		out.WriteString("\n")
	}
	if len(health.Trend) > 0 {
		out.WriteString("| Pull Request Verification first attempts | Attempts (timed) | Queued | Median solo wall | Normalized | Runner-minutes | Go test work |\n|---|---|---|---|---|---|---|\n")
		for _, window := range health.Trend {
			fmt.Fprintf(&out, "| %s to %s | %d (%d) | %s | %s | %s | %s | %s (%d runs) |\n", window.From.Format(time.DateOnly), window.To.Format(time.DateOnly), window.Attempts, window.TimedAttempts,
				value(window.QueuedShare, "%.2f"), value(window.SoloWallMinutes, "%.1f min"), value(window.NormalizedSoloWall, "%.1f min"),
				value(window.RunnerMinutesPerAttempt, "%.1f"), value(window.TestWorkMinutes, "%.1f min"), window.TestWorkRuns)
		}
		out.WriteString("\n")
	}
	for _, model := range slices.Sorted(maps.Keys(health.Unscaled)) {
		fmt.Fprintf(&out, "- Note: %d unit samples from jobs on %s, which has no factor in the weights, had no job speed and were left out of the drift checks\n", health.Unscaled[model], model)
	}
	for _, note := range health.Notes {
		fmt.Fprintf(&out, "- Note: %s\n", note)
	}
	return out.String()
}
