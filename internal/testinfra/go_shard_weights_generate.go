package testinfra

import (
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"math"
	"os/exec"
	"slices"
	"strings"
	"time"
)

const (
	// A unit needs this many passing runs before it gets its own weight.
	goShardWeightMinimumSamples = 3
	// A CPU model's factor needs this many fit units it shares with other
	// models.
	goShardWeightMinimumSharedUnits = 5
	goShardWeightFitIterations      = 100
	verificationWorkflowPR          = "Pull Request Verification"
	verificationWorkflowMain        = "Main Branch Verification"
)

// GoShardWeightsOptions names the evidence tetral-shard-weights reads.
type GoShardWeightsOptions struct {
	// Evidence lists roots of downloaded Go Race evidence of Pull Request or
	// Main Branch Verification runs.
	Evidence []string
	// Base is the commit every used run's tested commit must contain.
	Base string
	// RepositoryRoot holds Base and every tested commit.
	RepositoryRoot string
	// Current is the committed weights file. Its sliceable set is kept, and
	// so is its weight for a unit that ran with too few passing samples.
	Current    []byte
	ShardCount int
	Date       time.Time
}

// GoShardWeightsGeneration is a generated weights file and what shaped it.
type GoShardWeightsGeneration struct {
	Weights  []byte
	Runs     []string
	Excluded []string
	// Kept lists units that ran with too few passing samples and keep their
	// current weight; Omitted lists those without one, which fall back to
	// median weights.
	Kept     []string
	Omitted  []string
	Warnings []string
}

// GenerateGoShardWeights computes Go shard weights from the evidence of runs
// whose tested commit contains the base and whose every shard restored the Go
// build cache. Only passed units count, and a unit needs three samples.
func GenerateGoShardWeights(options GoShardWeightsOptions) (GoShardWeightsGeneration, error) {
	var generation GoShardWeightsGeneration
	if options.Base == "" || options.ShardCount < 1 || len(options.Evidence) == 0 {
		return generation, fmt.Errorf("generation needs a base commit, a shard count and evidence")
	}
	current, err := parseGoShardWeights(options.Current)
	if err != nil {
		return generation, fmt.Errorf("read the current weights: %w", err)
	}
	runs := map[string]*goRaceRun{}
	for _, root := range options.Evidence {
		loaded, err := loadGoRaceRuns(root)
		if err != nil {
			return generation, err
		}
		for id, run := range loaded {
			if _, duplicate := runs[id]; duplicate {
				return generation, fmt.Errorf("run %s appears under more than one evidence root", id)
			}
			runs[id] = run
		}
	}
	contains := map[string]bool{}
	samples := newGoShardWeightSamples()
	for _, id := range slices.Sorted(maps.Keys(runs)) {
		run := runs[id]
		reason, err := goShardWeightRunExclusion(options, run, contains)
		if err != nil {
			return generation, err
		}
		if reason != "" {
			generation.Excluded = append(generation.Excluded, fmt.Sprintf("run %s: %s", id, reason))
			continue
		}
		for _, index := range run.shardIndexes() {
			if run.shards[index].result.Execution.CPUModel == "" {
				return generation, fmt.Errorf("run %s shard %d records no runner CPU model, so its samples cannot be put on the common scale", id, index)
			}
		}
		generation.Runs = append(generation.Runs, id)
		samples.add(run, current.sliceable)
	}
	if len(generation.Runs) < goShardWeightMinimumSamples {
		return generation, fmt.Errorf("%d runs qualify; every unit needs %d samples", len(generation.Runs), goShardWeightMinimumSamples)
	}
	cpuModels, settled, err := fitGoCPUFactors(samples, current.sliceable)
	if err != nil {
		return generation, err
	}
	if !settled {
		generation.Warnings = append(generation.Warnings, fmt.Sprintf("CPU model factors still moved after %d rounds of the fit", goShardWeightFitIterations))
	}
	weights := goShardWeights{
		Version: goShardWeightsVersion, GeneratedAt: options.Date.UTC().Format(time.DateOnly), Base: options.Base,
		SourceRuns: generation.Runs, Sliceable: slices.Sorted(maps.Keys(current.sliceable)), Packages: map[string]goPackageWeights{}, CPUModels: cpuModels,
	}
	factors := weights.cpuFactors()
	var testMedians []int64
	tests := map[string][]string{}
	for _, key := range slices.SortedFunc(maps.Keys(samples.ran), compareTestKeys) {
		tests[key[0]] = append(tests[key[0]], key[1])
		if values := normalizeGoShardSamples(samples.tests[key], byModel(factors)); len(values) >= goShardWeightMinimumSamples {
			testMedians = append(testMedians, medianSecondsMS(values))
		}
	}
	if len(testMedians) > 0 {
		weights.MedianTestMS = medianMilliseconds(testMedians)
	}
	for _, name := range slices.Sorted(maps.Keys(samples.packages)) {
		elapsed, overhead := normalizeGoShardSums(samples.elapsed[name], byModel(factors)), normalizeGoShardSamples(samples.overhead[name], byModel(factors))
		previous, known := current.Packages[name]
		var item goPackageWeights
		switch shortfall := fmt.Sprintf("package %s: %d complete-run and %d step samples", name, len(elapsed), len(overhead)); {
		case len(elapsed) >= goShardWeightMinimumSamples && len(overhead) >= goShardWeightMinimumSamples:
			item = goPackageWeights{ElapsedMS: medianSecondsMS(elapsed), OverheadMS: max(0, medianSecondsMS(overhead))}
		case known:
			item = goPackageWeights{ElapsedMS: previous.ElapsedMS, OverheadMS: previous.OverheadMS}
			generation.Kept = append(generation.Kept, shortfall)
		default:
			generation.Omitted = append(generation.Omitted, shortfall)
			continue
		}
		if current.sliceable[name] {
			item.TestsMS = map[string]int64{}
			for _, test := range tests[name] {
				values := normalizeGoShardSamples(samples.tests[[2]string{name, test}], byModel(factors))
				weight, weighed := previous.TestsMS[test]
				shortfall := fmt.Sprintf("test %s %s: %d samples", name, test, len(values))
				switch {
				case len(values) >= goShardWeightMinimumSamples:
					item.TestsMS[test] = medianSecondsMS(values)
				case weighed:
					item.TestsMS[test] = weight
					generation.Kept = append(generation.Kept, shortfall)
				default:
					generation.Omitted = append(generation.Omitted, shortfall)
				}
			}
		}
		weights.Packages[name] = item
	}
	var total int64
	for _, item := range weights.Packages {
		total += item.ElapsedMS + item.OverheadMS
	}
	limit := total / int64(options.ShardCount*goRaceWorkers)
	for _, name := range slices.Sorted(maps.Keys(weights.Packages)) {
		item := weights.Packages[name]
		if !current.sliceable[name] && item.ElapsedMS+item.OverheadMS > limit {
			generation.Warnings = append(generation.Warnings, fmt.Sprintf("package %s is not sliceable and takes %.1f s, more than a worker's share %.1f s of %d shards", name, float64(item.ElapsedMS+item.OverheadMS)/1000, float64(limit)/1000, options.ShardCount))
		}
	}
	body, err := json.MarshalIndent(weights, "", "  ")
	if err != nil {
		return generation, err
	}
	body = append(body, '\n')
	if _, err := parseGoShardWeights(body); err != nil {
		return generation, fmt.Errorf("generated weights are invalid: %w", err)
	}
	generation.Weights = body
	return generation, nil
}

func goShardWeightRunExclusion(options GoShardWeightsOptions, run *goRaceRun, contains map[string]bool) (string, error) {
	if !goRaceRunRestoredCache(run) {
		return "a shard did not restore the Go build cache", nil
	}
	for _, index := range run.shardIndexes() {
		if workflow := run.shards[index].result.Execution.Workflow; workflow != verificationWorkflowPR && workflow != verificationWorkflowMain {
			return fmt.Sprintf("workflow %q is not Pull Request or Main Branch Verification", workflow), nil
		}
		head := run.shards[index].result.Plan.Revision.Head
		included, known := contains[head]
		if !known {
			var err error
			included, err = commitContains(options.RepositoryRoot, head, options.Base)
			if err != nil {
				return "", err
			}
			contains[head] = included
		}
		if !included {
			return fmt.Sprintf("tested commit %s does not contain the base", head), nil
		}
	}
	return "", nil
}

func commitContains(root, commit, ancestor string) (bool, error) {
	if commit == "" {
		return false, nil
	}
	_, err := gitOutput(root, "merge-base", "--is-ancestor", ancestor, commit)
	var exit *exec.ExitError
	if errors.As(err, &exit) && exit.ExitCode() == 1 {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("check whether %s contains %s (fetch both commits first): %w", commit, ancestor, err)
	}
	return true, nil
}

// goShardSample is one measured duration, the CPU model of the job that
// measured it and that job, written run/shard.
type goShardSample struct {
	seconds  float64
	cpu, job string
}

// goShardScale returns the divisor that puts a sample on the weights'
// machine scale, or false when there is none.
type goShardScale func(goShardSample) (float64, bool)

func byModel(factors map[string]float64) goShardScale {
	return func(sample goShardSample) (float64, bool) {
		factor, ok := factors[sample.cpu]
		return factor, ok
	}
}

// goShardWeightSamples collects unit durations from the runs it is given.
type goShardWeightSamples struct {
	// tests holds every passed top-level test's active times; ran names every
	// top-level test that reported an outcome, and packages every package
	// with a step that ran.
	tests    map[[2]string][]goShardSample
	ran      map[[2]string]bool
	packages map[string]bool
	// overhead holds each passed step's time outside its units.
	overhead map[string][]goShardSample
	// elapsed holds, for each complete run where every step of a package
	// passed, the Elapsed of each of its steps.
	elapsed map[string][][]goShardSample
	// jobs counts the jobs that gave samples, by CPU model.
	jobs map[string]int
}

func newGoShardWeightSamples() *goShardWeightSamples {
	return &goShardWeightSamples{tests: map[[2]string][]goShardSample{}, ran: map[[2]string]bool{}, packages: map[string]bool{}, overhead: map[string][]goShardSample{}, elapsed: map[string][][]goShardSample{}, jobs: map[string]int{}}
}

func (samples *goShardWeightSamples) add(run *goRaceRun, sliceable map[string]bool) {
	sums := map[string][]goShardSample{}
	broken := map[string]bool{}
	for _, index := range run.shardIndexes() {
		shard := run.shards[index]
		if !shard.complete {
			for _, selection := range shard.plan {
				broken[selection.Packages[0]] = true
			}
			continue
		}
		cpu, job := shard.result.Execution.CPUModel, fmt.Sprintf("%s/%d", run.runID, index)
		samples.jobs[cpu]++
		for _, step := range shard.steps {
			name := step.selection.Packages[0]
			samples.packages[name] = true
			var active float64
			for _, test := range step.report.tests {
				key := [2]string{name, test.name}
				samples.ran[key] = true
				active += test.active
				if test.passed {
					samples.tests[key] = append(samples.tests[key], goShardSample{seconds: test.active, cpu: cpu, job: job})
				}
			}
			action := step.report.packageAction
			if step.result.Status != "pass" || action != "pass" && action != "skip" {
				broken[name] = true
				continue
			}
			// A whole package's unit is the package, so its overhead is the
			// step time outside the package's Elapsed.
			units := step.report.packageElapsed
			if sliceable[name] {
				units = active
			}
			samples.overhead[name] = append(samples.overhead[name], goShardSample{seconds: step.result.Elapsed.Seconds() - units, cpu: cpu, job: job})
			sums[name] = append(sums[name], goShardSample{seconds: step.report.packageElapsed, cpu: cpu, job: job})
		}
	}
	if !run.complete() {
		return
	}
	for name, parts := range sums {
		if !broken[name] {
			samples.elapsed[name] = append(samples.elapsed[name], parts)
		}
	}
}

// normalizeGoShardSamples divides each sample by its scale, leaving out
// samples without one.
func normalizeGoShardSamples(samples []goShardSample, scale goShardScale) []float64 {
	var values []float64
	for _, sample := range samples {
		if divisor, ok := scale(sample); ok {
			values = append(values, sample.seconds/divisor)
		}
	}
	return values
}

// normalizeGoShardSums sums each run's normalized parts, leaving out a run
// with a part without a scale.
func normalizeGoShardSums(runs [][]goShardSample, scale goShardScale) []float64 {
	var sums []float64
	for _, parts := range runs {
		if values := normalizeGoShardSamples(parts, scale); len(values) == len(parts) {
			var sum float64
			for _, value := range values {
				sum += value
			}
			sums = append(sums, sum)
		}
	}
	return sums
}

// fitGoCPUFactors fits one speed factor per CPU model jointly with unit
// weights, alternating the two to a fixed point: a unit's weight is the
// geometric mean of its samples each divided by its model's factor, and a
// model's factor is the median of its samples divided by their unit's
// weight. A median unit weight would equal one sample when a unit has
// three, pinning that sample's model and leaving the fit unstable at the
// three-run minimum; the weights written to the file stay medians. The fit
// units are the sliceable packages' tests and the other packages' whole
// Elapsed, each weighing at least a second. A unit measured on one model
// only is still normalized by that model's factor, which the units it shares
// with other models fix. The reference is the median source job, the slower
// of the two middle jobs when their count is even: its model's factor is 1,
// so the weights describe a typical job of the source runs without naming a
// model that GitHub may stop assigning.
func fitGoCPUFactors(samples *goShardWeightSamples, sliceable map[string]bool) (map[string]goCPUModelWeights, bool, error) {
	units := map[string][]goShardSample{}
	for key, values := range samples.tests {
		if sliceable[key[0]] {
			units[key[0]+"\x00"+key[1]] = values
		}
	}
	for name, runs := range samples.elapsed {
		if sliceable[name] {
			continue
		}
		for _, parts := range runs {
			if len(parts) == 1 {
				units[name] = append(units[name], parts[0])
			}
		}
	}
	models := slices.Sorted(maps.Keys(samples.jobs))
	factors := map[string]float64{}
	for _, model := range models {
		factors[model] = 1
	}
	weigh := func() map[string]float64 {
		weights := map[string]float64{}
		for unit, values := range units {
			var logs float64
			normalized := normalizeGoShardSamples(values, byModel(factors))
			for _, value := range normalized {
				logs += math.Log(max(value, 0.001))
			}
			weights[unit] = math.Exp(logs / float64(len(normalized)))
		}
		return weights
	}
	result := map[string]goCPUModelWeights{}
	for _, model := range models {
		result[model] = goCPUModelWeights{Jobs: samples.jobs[model]}
	}
	shared := map[string]map[string]bool{}
	for _, unit := range slices.Sorted(maps.Keys(units)) {
		seen := map[string]bool{}
		for _, sample := range units[unit] {
			seen[sample.cpu] = true
		}
		if len(seen) < 2 {
			continue
		}
		for model := range seen {
			item := result[model]
			item.SharedUnits++
			result[model] = item
			if shared[model] == nil {
				shared[model] = map[string]bool{}
			}
			for other := range seen {
				shared[model][other] = true
			}
		}
	}
	if len(models) > 1 {
		for _, model := range models {
			if result[model].SharedUnits < goShardWeightMinimumSharedUnits {
				return nil, false, fmt.Errorf("CPU model %q shares %d units with other models; its factor needs %d", model, result[model].SharedUnits, goShardWeightMinimumSharedUnits)
			}
		}
		reached := map[string]bool{models[0]: true}
		for queue := []string{models[0]}; len(queue) > 0; queue = queue[1:] {
			for other := range shared[queue[0]] {
				if !reached[other] {
					reached[other] = true
					queue = append(queue, other)
				}
			}
		}
		if len(reached) != len(models) {
			return nil, false, fmt.Errorf("CPU models %v share no unit with %v, so their factors cannot be compared", slices.Sorted(maps.Keys(reached)), models)
		}
	}
	for iteration := 0; iteration < goShardWeightFitIterations; iteration++ {
		weights := weigh()
		next := map[string]float64{}
		counts := map[string]int{}
		for _, model := range models {
			var ratios []float64
			for _, unit := range slices.Sorted(maps.Keys(units)) {
				if weights[unit] < goRaceJobSpeedFloorMS/1000.0 {
					continue
				}
				for _, sample := range units[unit] {
					if sample.cpu == model {
						ratios = append(ratios, sample.seconds/weights[unit])
					}
				}
			}
			if len(ratios) == 0 {
				return nil, false, fmt.Errorf("CPU model %q measured no unit of a second or more", model)
			}
			next[model], counts[model] = benchmarkMedian(ratios), len(ratios)
		}
		var byJob []float64
		for _, model := range models {
			for job := 0; job < samples.jobs[model]; job++ {
				byJob = append(byJob, next[model])
			}
		}
		slices.Sort(byJob)
		reference := byJob[len(byJob)/2]
		converged := true
		for _, model := range models {
			value := math.Round(next[model]/reference*10000) / 10000
			converged = converged && value == factors[model]
			factors[model] = value
			item := result[model]
			item.Factor, item.Samples = value, counts[model]
			result[model] = item
		}
		if converged {
			return result, true, nil
		}
	}
	return result, false, nil
}

func medianSecondsMS(values []float64) int64 {
	return int64(math.Round(benchmarkMedian(values) * 1000))
}

func compareTestKeys(left, right [2]string) int {
	return strings.Compare(left[0]+"\x00"+left[1], right[0]+"\x00"+right[1])
}
