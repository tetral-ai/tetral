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

	"github.com/tetral-ai/tetral/internal/testinfra"
)

func main() {
	artifacts := flag.String("artifacts", ".test-results/downloaded", "downloaded evidence artifact root")
	goRace := flag.String("go-race-evidence", ".test-results/go-race", "directory for recent Go Race evidence; empty skips the shard weights check")
	output := flag.String("output", ".test-results/ci-health.json", "CI health report")
	flag.Parse()
	runID, err := strconv.ParseInt(os.Getenv("GITHUB_RUN_ID"), 10, 64)
	if err != nil {
		fatal(fmt.Errorf("GitHub run ID is malformed"))
	}
	runAttempt, err := strconv.Atoi(os.Getenv("GITHUB_RUN_ATTEMPT"))
	if err != nil {
		fatal(fmt.Errorf("GitHub run attempt is malformed"))
	}
	report, err := testinfra.BuildCIHealthReport(context.Background(), *artifacts, *goRace, os.Getenv("GITHUB_REPOSITORY"), runID, runAttempt)
	if err != nil {
		fatal(err)
	}
	if report.GoRace != nil {
		for _, warning := range report.GoRace.Warnings {
			fmt.Printf("::warning title=Go shard weights::%s\n", workflowCommandEscape(warning))
		}
		if summary := os.Getenv("GITHUB_STEP_SUMMARY"); summary != "" {
			if err := appendFile(summary, testinfra.RenderGoRaceHealth(*report.GoRace)); err != nil {
				fatal(err)
			}
		}
	}
	body, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		fatal(err)
	}
	if err := os.MkdirAll(filepath.Dir(*output), 0o700); err != nil {
		fatal(err)
	}
	// output is the explicit workflow-owned report path supplied by the caller.
	//nolint:gosec
	if err := os.WriteFile(*output, append(body, '\n'), 0o600); err != nil {
		fatal(err)
	}
}

// workflowCommandEscape keeps a message on one workflow command line.
func workflowCommandEscape(value string) string {
	return strings.NewReplacer("%", "%25", "\r", "%0D", "\n", "%0A").Replace(value)
}

func appendFile(path, text string) error {
	// GitHub names the step summary file through the runner environment.
	//nolint:gosec
	file, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	_, writeErr := file.WriteString(text)
	return errors.Join(writeErr, file.Close())
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}
