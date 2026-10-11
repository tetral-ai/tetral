package main

import (
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/tetral-ai/tetral/internal/testinfra"
)

const usage = `Usage:
  tetral-shard-weights --base SHA [--weights FILE] [--repo DIR] EVIDENCE-DIR...

Each EVIDENCE-DIR holds downloaded Go Race evidence (result.json and its go
test -json reports) of Pull Request or Main Branch Verification runs.`

func main() {
	base := flag.String("base", "", "commit every used run's tested commit must contain")
	weights := flag.String("weights", "internal/testinfra/go_shard_weights.json", "weights file to read the sliceable packages from and rewrite")
	repository := flag.String("repo", ".", "repository checkout holding the base and every tested commit")
	flag.Usage = func() { fmt.Fprintln(os.Stderr, usage) }
	flag.Parse()
	if *base == "" || flag.NArg() == 0 {
		flag.Usage()
		os.Exit(2)
	}
	// The weights path is the caller's explicit repository file.
	//nolint:gosec
	current, err := os.ReadFile(*weights)
	if err != nil {
		fatal(err)
	}
	inventory, err := testinfra.LoadPRProducers()
	if err != nil {
		fatal(err)
	}
	generation, err := testinfra.GenerateGoShardWeights(testinfra.GoShardWeightsOptions{
		Evidence: flag.Args(), Base: *base, RepositoryRoot: *repository, Current: current, ShardCount: len(inventory.GoShards), Date: time.Now(),
	})
	for _, line := range generation.Excluded {
		fmt.Println("excluded", line)
	}
	if err != nil {
		fatal(err)
	}
	for _, line := range generation.Kept {
		fmt.Println("too few samples, current weight kept:", line)
	}
	for _, line := range generation.Omitted {
		fmt.Println("too few samples, no weight:", line)
	}
	for _, line := range generation.Warnings {
		fmt.Println("warning:", line)
	}
	// The file was read above, so it exists and keeps its mode.
	if err := os.WriteFile(*weights, generation.Weights, 0o600); err != nil {
		fatal(err)
	}
	fmt.Printf("wrote %s from runs %v\n", *weights, generation.Runs)
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}
