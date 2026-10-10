package testinfra

import (
	"fmt"
	"math"
	"slices"
)

// BenchmarkNoise holds the paired-difference standard deviation s_D of each
// metric over A/A comparisons, where both sides test the same commit.
type BenchmarkNoise struct {
	Version int                    `json:"version"`
	Runs    int                    `json:"runs"`
	Metrics []BenchmarkNoiseMetric `json:"metrics"`
	Notes   []string               `json:"notes,omitempty"`
}

type BenchmarkNoiseMetric struct {
	Name      string  `json:"name"`
	Unit      string  `json:"unit"`
	Aggregate string  `json:"aggregate"`
	SD        float64 `json:"s_d"`
	// ShardSD is the standard deviation of the per-shard differences that SD
	// is scaled from.
	ShardSD        *float64  `json:"shard_s_d,omitempty"`
	Samples        int       `json:"samples"`
	RunDifferences []float64 `json:"run_differences"`
	Method         string    `json:"method"`
}

// benchmarkGuardMetrics bound every benchmarked change: B may not exceed A by
// more than two s_D on either. Both price A's shard plan, because measured
// runner-minutes and the slowest job move whenever B's plan differs.
var benchmarkGuardMetrics = []string{"plan_fixed_runner_minutes", "plan_fixed_slowest_job"}

// benchmarkFixedRuns holds metrics that are deterministic within a run: both
// plans are replayed over the same durations, so A/A runs give them no s_D
// and a fixed number of runs judges them.
var benchmarkFixedRuns = map[string]int{"replayed_slowest_steps_phase": 5}

const (
	BenchmarkVerdictPass             = "pass"
	BenchmarkVerdictFail             = "fail"
	BenchmarkVerdictInsufficientRuns = "insufficient-runs"
	BenchmarkVerdictNotMeasurable    = "not-measurable"
)

type BenchmarkVerdictOptions struct {
	Metric string
	// Effect is the expected reduction E of the metric, in its unit.
	Effect float64
	Noise  BenchmarkNoise
	// SD, when set, replaces the noise file's s_D for Metric.
	SD *float64
}

type BenchmarkVerdict struct {
	Version      int                   `json:"version"`
	Metric       string                `json:"metric"`
	Unit         string                `json:"unit"`
	Effect       float64               `json:"expected_effect"`
	SD           *float64              `json:"s_d,omitempty"`
	RunsRequired int                   `json:"runs_required"`
	Runs         []BenchmarkVerdictRun `json:"runs"`
	MeanD        *float64              `json:"mean_d,omitempty"`
	Guards       []BenchmarkGuard      `json:"guards"`
	Status       string                `json:"status"`
	Reasons      []string              `json:"reasons,omitempty"`
}

type BenchmarkVerdictRun struct {
	ARun string  `json:"a_run"`
	BRun string  `json:"b_run"`
	A    float64 `json:"a"`
	B    float64 `json:"b"`
	D    float64 `json:"d"`
}

type BenchmarkGuard struct {
	Metric string  `json:"metric"`
	Unit   string  `json:"unit,omitempty"`
	MeanA  float64 `json:"mean_a"`
	MeanB  float64 `json:"mean_b"`
	SD     float64 `json:"s_d"`
	Limit  float64 `json:"limit"`
	Pass   bool    `json:"pass"`
	Reason string  `json:"reason,omitempty"`
}

// BenchmarkNoiseFromComparisons estimates s_D from the per-shard pairs of A/A
// comparisons. A metric aggregated over K shards scales the per-shard
// standard deviation: by 1/√K for a mean and by √K for a sum; a slowest-shard
// metric takes the per-shard value as its approximation. A metric without
// shard pairs uses the run-level differences. With changed tests, the changed
// test metric is computed from each comparison's per-test durations.
func BenchmarkNoiseFromComparisons(comparisons []BenchmarkComparison, changedTests []string) (BenchmarkNoise, error) {
	noise := BenchmarkNoise{Version: benchmarkReportVersion, Runs: len(comparisons)}
	listed, err := parseBenchmarkChangedTests(changedTests)
	if err != nil {
		return noise, err
	}
	type collected struct {
		unit, aggregate string
		shards          []float64
		runs            []float64
		shardCount      int
		consistent      bool
	}
	var order []string
	metrics := map[string]*collected{}
	for position, comparison := range comparisons {
		if comparison.A.SHA != comparison.B.SHA {
			return noise, fmt.Errorf("comparison %d is not an A/A run: A tested %s and B %s", position+1, comparison.A.SHA, comparison.B.SHA)
		}
		if !comparison.A.Complete || !comparison.B.Complete || !comparison.PlansIdentical {
			return noise, fmt.Errorf("comparison %d is incomplete or its shard plans differ", position+1)
		}
		compared := slices.DeleteFunc(slices.Clone(comparison.Metrics), func(metric BenchmarkMetric) bool { return metric.Name == benchmarkChangedTestsMetric })
		if len(listed) > 0 {
			metric, err := benchmarkChangedTestsDuration(comparison, listed)
			if err != nil {
				return noise, fmt.Errorf("comparison %d: %w", position+1, err)
			}
			compared = append(compared, metric)
		}
		for _, metric := range compared {
			item := metrics[metric.Name]
			if item == nil {
				item = &collected{unit: metric.Unit, aggregate: metric.Aggregate, shardCount: len(metric.Shards), consistent: true}
				metrics[metric.Name] = item
				order = append(order, metric.Name)
			}
			if item.unit != metric.Unit || item.aggregate != metric.Aggregate || item.shardCount != len(metric.Shards) {
				item.consistent = false
			}
			item.runs = append(item.runs, metric.D)
			for _, pair := range metric.Shards {
				item.shards = append(item.shards, pair.A-pair.B)
			}
		}
	}
	for _, name := range order {
		item := metrics[name]
		if runs, fixed := benchmarkFixedRuns[name]; fixed {
			noise.Notes = append(noise.Notes, fmt.Sprintf("%s replays both plans over the same durations, so A/A runs give it no s_D; verdict judges it over %d runs", name, runs))
			continue
		}
		if !item.consistent {
			noise.Notes = append(noise.Notes, fmt.Sprintf("%s changes unit, aggregate or shard count between comparisons; no s_D", name))
			continue
		}
		metric := BenchmarkNoiseMetric{Name: name, Unit: item.unit, Aggregate: item.aggregate, RunDifferences: item.runs}
		samples := item.shards
		if item.aggregate == benchmarkAggregateTotal || item.shardCount == 0 {
			samples = item.runs
		}
		metric.Samples = len(samples)
		if metric.Samples < 2 {
			noise.Notes = append(noise.Notes, fmt.Sprintf("%s has %d sample(s); s_D needs at least 2", name, metric.Samples))
			continue
		}
		deviation := benchmarkStandardDeviation(samples)
		if deviation == 0 {
			noise.Notes = append(noise.Notes, fmt.Sprintf("%s: every A/A difference is the same, so it has no s_D", name))
			continue
		}
		shards := float64(item.shardCount)
		switch {
		case item.aggregate == benchmarkAggregateTotal || item.shardCount == 0:
			metric.SD, metric.Method = deviation, fmt.Sprintf("s.d. of %d run differences", metric.Samples)
		case item.aggregate == benchmarkAggregateMean:
			metric.SD, metric.Method = deviation/math.Sqrt(shards), fmt.Sprintf("per-shard s.d. of %d shard pairs / √%d", metric.Samples, item.shardCount)
		case item.aggregate == benchmarkAggregateSum:
			metric.SD, metric.Method = deviation*math.Sqrt(shards), fmt.Sprintf("per-shard s.d. of %d shard pairs × √%d", metric.Samples, item.shardCount)
		default:
			metric.SD, metric.Method = deviation, fmt.Sprintf("per-shard s.d. of %d shard pairs (slowest-shard approximation)", metric.Samples)
		}
		if item.aggregate != benchmarkAggregateTotal && item.shardCount > 0 {
			metric.ShardSD = benchmarkPointer(roundBenchmark(deviation, 1000))
		}
		metric.SD = roundBenchmark(metric.SD, 1000)
		noise.Metrics = append(noise.Metrics, metric)
	}
	return noise, nil
}

// benchmarkRunsRequired applies the run-count rule: E ≥ 10 s_D needs one
// run, 3 s_D ≤ E < 10 s_D three, 2 s_D ≤ E < 3 s_D five, and a smaller
// effect is not measurable (0).
func benchmarkRunsRequired(effect, sd float64) int {
	ratio := math.Inf(1)
	if sd > 0 {
		ratio = effect / sd
	}
	switch {
	case ratio >= 10:
		return 1
	case ratio >= 3:
		return 3
	case ratio >= 2:
		return 5
	default:
		return 0
	}
}

// BenchmarkVerdictFromComparisons judges a change from the comparisons of its
// benchmark runs. It passes when d = A − B of the metric is positive in every
// run and its mean reaches E/2, and B stays within A + 2 s_D on every guard.
func BenchmarkVerdictFromComparisons(options BenchmarkVerdictOptions, comparisons []BenchmarkComparison) (BenchmarkVerdict, error) {
	verdict := BenchmarkVerdict{Version: benchmarkReportVersion, Metric: options.Metric, Effect: options.Effect}
	if !(options.Effect > 0) {
		return verdict, fmt.Errorf("expected effect must be a positive reduction")
	}
	if len(comparisons) == 0 {
		return verdict, fmt.Errorf("verdict needs at least one comparison")
	}
	if runs, fixed := benchmarkFixedRuns[options.Metric]; fixed {
		if options.SD != nil {
			return verdict, fmt.Errorf("%s is judged over %d runs and takes no s_D", options.Metric, runs)
		}
		verdict.RunsRequired = runs
	} else {
		switch noiseMetric := benchmarkNoiseMetric(options.Noise, options.Metric); {
		case options.SD != nil:
			verdict.SD = options.SD
		case noiseMetric != nil:
			verdict.SD = &noiseMetric.SD
		default:
			return verdict, fmt.Errorf("no s_D for metric %s: give one or a noise file that has it", options.Metric)
		}
		if *verdict.SD < 0 {
			return verdict, fmt.Errorf("s_D must not be negative")
		}
		verdict.RunsRequired = benchmarkRunsRequired(options.Effect, *verdict.SD)
	}
	valid := true
	var differences []float64
	for position, comparison := range comparisons {
		// An incomplete run lacks most metrics; its incompleteness is the cause.
		if !comparison.A.Complete || !comparison.B.Complete {
			verdict.Reasons = append(verdict.Reasons, fmt.Sprintf("run %d is incomplete: a Go Race shard did not run all its steps", position+1))
			valid = false
			continue
		}
		metric := benchmarkComparisonMetric(comparison, options.Metric)
		if metric == nil {
			if options.Metric == benchmarkChangedTestsMetric {
				return verdict, fmt.Errorf("comparison %d has no %s: compare with --changed-tests", position+1, options.Metric)
			}
			return verdict, fmt.Errorf("comparison %d has no %s value", position+1, options.Metric)
		}
		verdict.Unit = metric.Unit
		if metric.PlanDependent && !comparison.PlansIdentical {
			verdict.Reasons = append(verdict.Reasons, fmt.Sprintf("run %d: %s depends on the shard plan and A and B ran different plans", position+1, options.Metric))
			valid = false
			continue
		}
		verdict.Runs = append(verdict.Runs, BenchmarkVerdictRun{ARun: comparison.A.RunID, BRun: comparison.B.RunID, A: metric.A, B: metric.B, D: metric.D})
		differences = append(differences, metric.D)
	}
	if len(differences) > 0 {
		verdict.MeanD = benchmarkPointer(roundBenchmark(benchmarkMean(differences), 1000))
	}
	guardsPass := true
	for _, name := range benchmarkGuardMetrics {
		guard := benchmarkGuard(name, options.Noise, comparisons)
		guardsPass = guardsPass && guard.Pass
		verdict.Guards = append(verdict.Guards, guard)
	}

	switch {
	case !valid:
		verdict.Status = BenchmarkVerdictFail
	case verdict.RunsRequired == 0:
		verdict.Status = BenchmarkVerdictNotMeasurable
		verdict.Reasons = append(verdict.Reasons, fmt.Sprintf("E = %.3g %s is below 2 s_D = %.3g: no measurable wall-time change; report the per-test effect", options.Effect, verdict.Unit, 2**verdict.SD))
	case len(differences) < verdict.RunsRequired:
		verdict.Status = BenchmarkVerdictInsufficientRuns
		verdict.Reasons = append(verdict.Reasons, fmt.Sprintf("%s needs %d runs; %d given", options.Metric, verdict.RunsRequired, len(differences)))
	default:
		verdict.Status = BenchmarkVerdictPass
		if slices.ContainsFunc(differences, func(d float64) bool { return d <= 0 }) {
			verdict.Status = BenchmarkVerdictFail
			verdict.Reasons = append(verdict.Reasons, "d = A − B is not positive in every run")
		}
		if *verdict.MeanD < options.Effect/2 {
			verdict.Status = BenchmarkVerdictFail
			verdict.Reasons = append(verdict.Reasons, fmt.Sprintf("mean d = %.3f is below E/2 = %.3f", *verdict.MeanD, options.Effect/2))
		}
	}
	if !guardsPass && verdict.Status != BenchmarkVerdictInsufficientRuns {
		verdict.Status = BenchmarkVerdictFail
		verdict.Reasons = append(verdict.Reasons, "a guard failed")
	}
	return verdict, nil
}

func benchmarkGuard(name string, noise BenchmarkNoise, comparisons []BenchmarkComparison) BenchmarkGuard {
	guard := BenchmarkGuard{Metric: name}
	noiseMetric := benchmarkNoiseMetric(noise, name)
	if noiseMetric == nil {
		guard.Reason = "the noise file has no s_D for this guard"
		return guard
	}
	guard.SD = noiseMetric.SD
	var a, b []float64
	for position, comparison := range comparisons {
		metric := benchmarkComparisonMetric(comparison, name)
		if metric == nil {
			guard.Reason = fmt.Sprintf("run %d has no value", position+1)
			return guard
		}
		guard.Unit = metric.Unit
		a = append(a, metric.A)
		b = append(b, metric.B)
	}
	guard.MeanA = roundBenchmark(benchmarkMean(a), 1000)
	guard.MeanB = roundBenchmark(benchmarkMean(b), 1000)
	guard.Limit = roundBenchmark(benchmarkMean(a)+2*guard.SD, 1000)
	guard.Pass = benchmarkMean(b) <= benchmarkMean(a)+2*guard.SD
	if !guard.Pass {
		guard.Reason = "mean B exceeds mean A + 2 s_D"
	}
	return guard
}

func benchmarkNoiseMetric(noise BenchmarkNoise, name string) *BenchmarkNoiseMetric {
	for index := range noise.Metrics {
		if noise.Metrics[index].Name == name {
			return &noise.Metrics[index]
		}
	}
	return nil
}

func benchmarkComparisonMetric(comparison BenchmarkComparison, name string) *BenchmarkMetric {
	for index := range comparison.Metrics {
		if comparison.Metrics[index].Name == name {
			return &comparison.Metrics[index]
		}
	}
	return nil
}

func benchmarkStandardDeviation(values []float64) float64 {
	mean := benchmarkMean(values)
	total := 0.0
	for _, value := range values {
		total += (value - mean) * (value - mean)
	}
	return math.Sqrt(total / float64(len(values)-1))
}
