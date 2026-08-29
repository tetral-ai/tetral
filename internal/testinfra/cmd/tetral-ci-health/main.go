package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strconv"

	"github.com/tetral-ai/tetral/internal/testinfra"
)

func main() {
	artifacts := flag.String("artifacts", ".test-results/downloaded", "downloaded evidence artifact root")
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
	report, err := testinfra.BuildCIHealthReport(context.Background(), *artifacts, os.Getenv("GITHUB_REPOSITORY"), runID, runAttempt)
	if err != nil {
		fatal(err)
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

func fatal(err error) {
	fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}
