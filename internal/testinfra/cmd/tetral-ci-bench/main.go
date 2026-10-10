package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/tetral-ai/tetral/internal/testinfra"
)

const usage = `Usage:
  tetral-ci-bench compare --a DIR --a-sha SHA --b DIR --b-sha SHA [flags]
  tetral-ci-bench noise [--changed-tests LIST] [--output FILE] COMPARISON.json...
  tetral-ci-bench verdict --metric NAME --effect E --noise FILE [--s-d S] [--output FILE] COMPARISON.json...`

func main() {
	command := ""
	if len(os.Args) > 1 {
		command = os.Args[1]
	}
	var err error
	switch command {
	case "compare":
		err = compare(os.Args[2:])
	case "noise":
		err = noise(os.Args[2:])
	case "verdict":
		err = verdict(os.Args[2:])
	default:
		fmt.Fprintln(os.Stderr, usage)
		os.Exit(2)
	}
	if err != nil {
		fatal(err)
	}
}

func compare(arguments []string) error {
	flags := flag.NewFlagSet("compare", flag.ExitOnError)
	aDirectory := flags.String("a", "", "downloaded Go Race evidence of side A")
	aSHA := flags.String("a-sha", "", "full commit every side A result must have tested")
	aJob := flags.String("a-job", testinfra.DefaultBenchmarkJobName, "side A GitHub job name; %d is the shard index")
	bDirectory := flags.String("b", "", "downloaded Go Race evidence of side B")
	bSHA := flags.String("b-sha", "", "full commit every side B result must have tested")
	bJob := flags.String("b-job", testinfra.DefaultBenchmarkJobName, "side B GitHub job name; %d is the shard index")
	repository := flags.String("repo", ".", "repository checkout holding both commits")
	changedTests := flags.String("changed-tests", "", "comma-separated <import path>.<Test> tests the change targets")
	offline := flags.Bool("offline", false, "do not read GitHub job timings")
	output := flags.String("output", ".test-results/ci-benchmark/compare.json", "comparison JSON")
	markdown := flags.String("markdown", "", "Markdown summary file; standard output when empty")
	if err := flags.Parse(arguments); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	report, err := testinfra.CompareBenchmarkEvidence(ctx, testinfra.BenchmarkCompareOptions{
		A:              testinfra.BenchmarkSideOptions{Directory: *aDirectory, SHA: *aSHA, JobName: *aJob},
		B:              testinfra.BenchmarkSideOptions{Directory: *bDirectory, SHA: *bSHA, JobName: *bJob},
		RepositoryRoot: *repository,
		ChangedTests:   splitList(*changedTests),
		GitHubJobs:     !*offline,
	})
	if err != nil {
		return err
	}
	if err := writeJSON(*output, report); err != nil {
		return err
	}
	return writeText(*markdown, testinfra.RenderBenchmarkComparison(report))
}

func noise(arguments []string) error {
	flags := flag.NewFlagSet("noise", flag.ExitOnError)
	changedTests := flags.String("changed-tests", "", "comma-separated <import path>.<Test> tests whose summed duration needs an s_D")
	output := flags.String("output", "", "noise JSON")
	if err := flags.Parse(arguments); err != nil {
		return err
	}
	comparisons, err := readComparisons(flags.Args())
	if err != nil {
		return err
	}
	report, err := testinfra.BenchmarkNoiseFromComparisons(comparisons, splitList(*changedTests))
	if err != nil {
		return err
	}
	if *output != "" {
		if err := writeJSON(*output, report); err != nil {
			return err
		}
	}
	return writeText("", testinfra.RenderBenchmarkNoise(report))
}

func verdict(arguments []string) error {
	flags := flag.NewFlagSet("verdict", flag.ExitOnError)
	metric := flags.String("metric", "", "metric name from the comparison")
	effect := flags.Float64("effect", 0, "expected reduction E, in the metric's unit")
	noiseFile := flags.String("noise", "", "noise JSON with s_D per metric")
	sd := flags.String("s-d", "", "s_D of the metric, replacing the noise file's value")
	output := flags.String("output", "", "verdict JSON")
	if err := flags.Parse(arguments); err != nil {
		return err
	}
	options := testinfra.BenchmarkVerdictOptions{Metric: *metric, Effect: *effect}
	if *noiseFile == "" {
		return errors.New("verdict needs --noise for the guards' s_D")
	}
	if err := readJSON(*noiseFile, &options.Noise); err != nil {
		return err
	}
	if *sd != "" {
		value, err := strconv.ParseFloat(*sd, 64)
		if err != nil {
			return fmt.Errorf("--s-d: %w", err)
		}
		options.SD = &value
	}
	comparisons, err := readComparisons(flags.Args())
	if err != nil {
		return err
	}
	result, err := testinfra.BenchmarkVerdictFromComparisons(options, comparisons)
	if err != nil {
		return err
	}
	if *output != "" {
		if err := writeJSON(*output, result); err != nil {
			return err
		}
	}
	if err := writeText("", testinfra.RenderBenchmarkVerdict(result)); err != nil {
		return err
	}
	if result.Status != testinfra.BenchmarkVerdictPass && result.Status != testinfra.BenchmarkVerdictNotMeasurable {
		os.Exit(1)
	}
	return nil
}

func readComparisons(files []string) ([]testinfra.BenchmarkComparison, error) {
	if len(files) == 0 {
		return nil, errors.New("no comparison files given")
	}
	comparisons := make([]testinfra.BenchmarkComparison, len(files))
	for index, file := range files {
		if err := readJSON(file, &comparisons[index]); err != nil {
			return nil, err
		}
	}
	return comparisons, nil
}

func readJSON(file string, value any) error {
	// The file is an explicit operator-supplied report path.
	//nolint:gosec
	body, err := os.ReadFile(file)
	if err != nil {
		return err
	}
	if err := json.Unmarshal(body, value); err != nil {
		return fmt.Errorf("decode %s: %w", file, err)
	}
	return nil
}

func writeJSON(file string, value any) error {
	body, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(file), 0o700); err != nil {
		return err
	}
	// The file is an explicit operator- or workflow-supplied report path.
	//nolint:gosec
	return os.WriteFile(file, append(body, '\n'), 0o600)
}

func writeText(file, text string) error {
	if file == "" {
		_, err := os.Stdout.WriteString(text)
		return err
	}
	// The file is an explicit operator- or workflow-supplied summary path.
	//nolint:gosec
	return os.WriteFile(file, []byte(text), 0o600)
}

func splitList(value string) []string {
	var result []string
	for _, item := range strings.Split(value, ",") {
		if item = strings.TrimSpace(item); item != "" {
			result = append(result, item)
		}
	}
	return result
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}
