package testinfra

import (
	"fmt"
	"strings"
)

var benchmarkMetricTitles = map[string]string{
	"setup":                        "Dependency setup (mean of shards)",
	"fixed_preparation":            "Fixed preparation: before runner + setup + longest step compile (mean of shards)",
	"slowest_steps_phase":          "Slowest steps phase",
	"slowest_job":                  "Slowest job",
	"go_race_runner_minutes":       "Go Race runner-minutes, measured",
	"plan_fixed_runner_minutes":    "Plan-fixed Go Race runner-minutes: A's plan over each side's durations",
	"replayed_slowest_steps_phase": "Replayed slowest steps phase: each plan over A's durations",
	"plan_fixed_slowest_job":       "Plan-fixed slowest job: A's plan over each side's durations",
	benchmarkChangedTestsMetric:    "Changed tests, summed (B speed-normalized)",
}

// RenderBenchmarkComparison writes the comparison as a Markdown step summary.
func RenderBenchmarkComparison(report BenchmarkComparison) string {
	var out strings.Builder
	sides := []struct {
		label string
		value BenchmarkSide
	}{{"A", report.A}, {"B", report.B}}
	out.WriteString("## CI Benchmark comparison\n\n")
	out.WriteString("| Side | Commit | Run | Shards | Complete |\n|---|---|---|---|---|\n")
	for _, side := range sides {
		fmt.Fprintf(&out, "| %s | `%s` | %s | %d | %s |\n", side.label, side.value.SHA, side.value.RunID, len(side.value.Shards), benchmarkYesNo(side.value.Complete))
	}
	plans := "identical"
	if !report.PlansIdentical {
		plans = fmt.Sprintf("differ in shards %v; plan-dependent metrics are not judged", report.DifferentShards)
	}
	fmt.Fprintf(&out, "\nShard plans: %s.\n\n", plans)

	out.WriteString("### Metrics\n\nd = A − B; a positive d means B is faster.\n\n| Metric | A | B | d | Note |\n|---|---:|---:|---:|---|\n")
	for _, metric := range report.Metrics {
		note := ""
		if metric.PlanDependent && !report.PlansIdentical {
			note = "not judged: shard plans differ"
		}
		title := benchmarkMetricTitles[metric.Name]
		if title == "" {
			title = metric.Name
		}
		fmt.Fprintf(&out, "| %s (`%s`) | %.1f %s | %.1f %s | %+.1f %s | %s |\n", title, metric.Name, metric.A, metric.Unit, metric.B, metric.Unit, metric.D, metric.Unit, note)
	}

	out.WriteString("\n### Shards\n\nSeconds unless stated. Compile is the longest step from its start to its first test. Preparation (job start → first test) depends on the step the plan puts first and is not judged.\n\n")
	out.WriteString("| Side | Shard | Status | Steps | CPU | Image | Before runner | Setup | Preparation | Longest compile | Fixed preparation | Steps phase | Job | Runner-min |\n")
	out.WriteString("|---|---:|---|---:|---|---|---:|---:|---:|---|---:|---:|---:|---:|\n")
	for _, side := range sides {
		for _, shard := range side.value.Shards {
			fmt.Fprintf(&out, "| %s | %d | %s | %d/%d | %s | %s | %s | %.1f | %s | %s | %s | %s | %s | %s |\n",
				side.label, shard.Index, shard.Status, shard.StepsRun, shard.StepsPlanned,
				benchmarkText(shard.CPUModel), benchmarkText(shard.RunnerImage),
				benchmarkValue(shard.BeforeRunner), shard.Setup, benchmarkValue(shard.Preparation),
				benchmarkLongestCompile(shard.Steps), benchmarkValue(shard.FixedPreparation), benchmarkValue(shard.StepsPhase),
				benchmarkValue(shard.Job), benchmarkValue(shard.RunnerMinutes))
		}
	}

	if len(report.Replay) > 0 {
		out.WriteString("\n### Replayed steps phase\n\nTwo workers take each shard plan's steps in plan order. A slice of a split package costs its tests plus its own shard's overhead; A's durations take units A never ran from B.\n\n")
		out.WriteString("| Shard | A plan, A durations | B plan, A durations | A plan, B durations |\n|---:|---:|---:|---:|\n")
		for _, replay := range report.Replay {
			fmt.Fprintf(&out, "| %d | %s | %s | %s |\n", replay.Shard, benchmarkValue(replay.APlanADurations), benchmarkValue(replay.BPlanADurations), benchmarkValue(replay.APlanBDurations))
		}
	}

	out.WriteString("\n### Speed factors\n\nRelative speed of B's job over A's job: the median B/A ratio of shared unchanged tests that passed and ran at least 1 s on both sides.\n\n")
	out.WriteString("| A shard | B shard | Factor | Supporting tests |\n|---:|---:|---:|---:|\n")
	for _, factor := range report.SpeedFactors {
		value := "too few tests: unnormalized"
		if factor.Factor != nil {
			value = fmt.Sprintf("%.3f", *factor.Factor)
		}
		fmt.Fprintf(&out, "| %d | %d | %s | %d |\n", factor.AShard, factor.BShard, value, factor.Tests)
	}

	out.WriteString("\n### Changed tests\n\n")
	if len(report.ChangedPackages) == 0 {
		out.WriteString("Changed packages: none.\n")
	} else {
		fmt.Fprintf(&out, "Changed packages, whose tests support no speed factor: `%s`.\n", strings.Join(report.ChangedPackages, "`, `"))
	}
	if len(report.ChangedTests) == 0 {
		out.WriteString("\nNo changed-test list was given.\n")
	} else {
		out.WriteString("\n| Test | A shard | B shard | A s | B s | B/A | Speed-normalized B/A |\n|---|---:|---:|---:|---:|---:|---:|\n")
		for _, test := range report.Tests {
			if test.Changed {
				fmt.Fprintf(&out, "| `%s.%s` | %d | %d | %.2f | %.2f | %s | %s |\n", test.Package, test.Test, test.AShard, test.BShard, test.A, test.B, benchmarkRatio(test.Ratio), benchmarkRatio(test.NormalizedRatio))
			}
		}
	}

	for _, notes := range []struct {
		title string
		items []string
	}{{"Missing", report.Missing}, {"Warnings", report.Warnings}} {
		if len(notes.items) == 0 {
			continue
		}
		fmt.Fprintf(&out, "\n### %s\n\n", notes.title)
		for _, item := range notes.items {
			fmt.Fprintf(&out, "- %s\n", item)
		}
	}
	return out.String()
}

// RenderBenchmarkNoise writes s_D per metric as Markdown.
func RenderBenchmarkNoise(noise BenchmarkNoise) string {
	var out strings.Builder
	fmt.Fprintf(&out, "## CI Benchmark noise from %d A/A runs\n\n| Metric | s_D | Samples | Method |\n|---|---:|---:|---|\n", noise.Runs)
	for _, metric := range noise.Metrics {
		fmt.Fprintf(&out, "| `%s` | %.3f %s | %d | %s |\n", metric.Name, metric.SD, metric.Unit, metric.Samples, metric.Method)
	}
	for _, note := range noise.Notes {
		fmt.Fprintf(&out, "\n- %s", note)
	}
	out.WriteString("\n")
	return out.String()
}

// RenderBenchmarkVerdict writes the verdict and the numbers behind it.
func RenderBenchmarkVerdict(verdict BenchmarkVerdict) string {
	var out strings.Builder
	fmt.Fprintf(&out, "## CI Benchmark verdict: %s\n\n", strings.ToUpper(verdict.Status))
	noise := "no s_D (fixed run count)"
	if verdict.SD != nil {
		noise = fmt.Sprintf("s_D = %.3f", *verdict.SD)
	}
	fmt.Fprintf(&out, "Metric `%s`: E = %.3f %s, %s, runs required = %d.\n\n", verdict.Metric, verdict.Effect, verdict.Unit, noise, verdict.RunsRequired)
	if len(verdict.Runs) > 0 {
		out.WriteString("| Run | A | B | d = A − B |\n|---|---:|---:|---:|\n")
		for index, run := range verdict.Runs {
			fmt.Fprintf(&out, "| %d (%s / %s) | %.3f | %.3f | %+.3f |\n", index+1, run.ARun, run.BRun, run.A, run.B, run.D)
		}
		fmt.Fprintf(&out, "\nmean d = %s; pass needs every d > 0 and mean d ≥ E/2 = %.3f.\n", benchmarkValue(verdict.MeanD), verdict.Effect/2)
	}
	out.WriteString("\n| Guard | mean A | mean B | s_D | limit A + 2 s_D | Result |\n|---|---:|---:|---:|---:|---|\n")
	for _, guard := range verdict.Guards {
		result := "pass"
		if !guard.Pass {
			result = "fail: " + guard.Reason
		}
		fmt.Fprintf(&out, "| `%s` | %.3f | %.3f | %.3f | %.3f | %s |\n", guard.Metric, guard.MeanA, guard.MeanB, guard.SD, guard.Limit, result)
	}
	for _, reason := range verdict.Reasons {
		fmt.Fprintf(&out, "\n- %s", reason)
	}
	out.WriteString("\n")
	return out.String()
}

func benchmarkLongestCompile(steps []BenchmarkStep) string {
	longest := -1
	for index, step := range steps {
		if step.Compile != nil && (longest < 0 || *step.Compile > *steps[longest].Compile) {
			longest = index
		}
	}
	if longest < 0 {
		return "—"
	}
	name := steps[longest].Package
	if slash := strings.LastIndex(name, "/"); slash >= 0 {
		name = name[slash+1:]
	}
	return fmt.Sprintf("%.1f (%s)", *steps[longest].Compile, name)
}

func benchmarkValue(value *float64) string {
	if value == nil {
		return "—"
	}
	return fmt.Sprintf("%.1f", *value)
}

func benchmarkRatio(value *float64) string {
	if value == nil {
		return "—"
	}
	return fmt.Sprintf("%.3f", *value)
}

func benchmarkText(value string) string {
	if value == "" {
		return "—"
	}
	return value
}

func benchmarkYesNo(value bool) string {
	if value {
		return "yes"
	}
	return "no"
}
