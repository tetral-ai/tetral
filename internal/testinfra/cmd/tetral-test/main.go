package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"sort"
	"strings"
	"syscall"
	"time"

	"github.com/tetral-ai/tetral/internal/testinfra"
)

func main() {
	profileFlag := flag.String("profile", "fast", "evidence profile: fast, affected, or full")
	base := flag.String("base", "", "local base ref or commit for affected selection")
	planOnly := flag.Bool("plan-only", false, "print the selection plan without executing it")
	output := flag.String("output", "", "result artifact directory")
	workers := flag.Int("workers", 0, "maximum local package workers")
	dependencyAudit := flag.String("dependency-audit", "changed", "online dependency audit policy: changed, always, or never")
	groups := flag.String("groups", "", "comma-separated evidence groups to execute from the selected profile")
	shardIndex := flag.Int("shard-index", 0, "zero-based Go package shard index")
	shardCount := flag.Int("shard-count", 1, "number of deterministic Go package shards")
	runLimit := flag.Duration("run-limit", 0, "wall-clock limit of the whole run; 0 is the profile's limit")
	flag.Parse()

	root, err := repositoryRoot()
	if err != nil {
		fatal(err)
	}
	profile := testinfra.Profile(*profileFlag)
	dependencyAuditMode, err := testinfra.ParseDependencyAuditMode(*dependencyAudit)
	if err != nil {
		fatal(err)
	}
	options := testinfra.RunOptions{
		Root:                root,
		OutputDir:           *output,
		PlanOnly:            *planOnly,
		MaxWorkers:          *workers,
		DependencyAuditMode: dependencyAuditMode,
	}

	limit := *runLimit
	if limit <= 0 {
		limit = testinfra.LocalRunLimit(profile)
	}
	signalled, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	// The limit ends the run as a stop does, planning included: commands
	// killed, result written, dependencies removed.
	ctx, stop := context.WithTimeoutCause(signalled, limit, fmt.Errorf("stopped by the run's limit of %s", limit))
	defer stop()
	started := time.Now()
	plan, err := testinfra.BuildPlan(ctx, root, profile, *base)
	if err != nil {
		result, planErr := testinfra.PlanFailed(ctx, profile, options, started, err)
		fmt.Fprintf(os.Stderr, "verification %s: %v\n", result.Status, planErr)
		os.Exit(1)
	}
	if *groups != "" || *shardCount != 1 || *shardIndex != 0 {
		plan, err = testinfra.SelectPlan(plan, splitComma(*groups), *shardIndex, *shardCount)
		if err != nil {
			fatal(err)
		}
	}
	printPlan(plan, *base)
	result, err := testinfra.Execute(ctx, plan, options)
	if err != nil {
		fmt.Fprintf(os.Stderr, "verification %s: %v\n", result.Status, err)
		os.Exit(1)
	}
	fmt.Printf("verification %s in %s\n", result.Status, result.FinishedAt.Sub(result.StartedAt).Round(1e6))
}

func splitComma(value string) []string {
	if value == "" {
		return nil
	}
	var result []string
	for _, item := range strings.Split(value, ",") {
		if item = strings.TrimSpace(item); item != "" {
			result = append(result, item)
		}
	}
	return result
}

func repositoryRoot() (string, error) {
	current, err := os.Getwd()
	if err != nil {
		return "", err
	}
	for {
		if _, err := os.Stat(filepath.Join(current, ".git")); err == nil {
			return current, nil
		}
		parent := filepath.Dir(current)
		if parent == current {
			return "", fmt.Errorf("not inside the Tetral repository")
		}
		current = parent
	}
}

func printPlan(plan testinfra.Plan, base string) {
	fmt.Println("Selection Plan")
	fmt.Printf("  profile: %s\n", plan.Profile)
	fmt.Printf("  head: %s\n", plan.Revision.Head)
	if plan.Revision.ComparisonCommit != "" {
		fmt.Printf("  comparison: %s (base tip %s)\n", plan.Revision.ComparisonCommit, plan.Revision.ResolvedBaseTip)
	}
	if plan.Revision.FullFallbackCause != "" {
		fmt.Printf("  full fallback: %s\n", plan.Revision.FullFallbackCause)
	}
	goPackages, goRunnables := 0, 0
	goMode := "ordinary"
	for _, selection := range plan.Selections {
		if selection.Group == "go" && len(selection.Packages) == 1 {
			goPackages++
			goRunnables += len(selection.Tests)
			goMode = selection.Mode
			continue
		}
		detail := ""
		if len(selection.Packages) > 0 {
			detail = fmt.Sprintf(" (%d package, %d tests)", len(selection.Packages), len(selection.Tests))
		}
		fmt.Printf("  - %s [%s]%s: %s\n", selection.Group, selection.Mode, detail, selection.Reason)
	}
	if goPackages > 0 {
		fmt.Printf("  - go [%s] (%d packages, %d runnable units): %s profile package universe\n", goMode, goPackages, goRunnables, plan.Profile)
	}
	if len(plan.Excluded) > 0 {
		counts := map[string]int{}
		for _, exclusion := range plan.Excluded {
			counts[exclusion.Disposition+"/"+exclusion.Capability]++
		}
		fmt.Println("  alternate evidence dispositions:")
		capabilities := make([]string, 0, len(counts))
		for capability := range counts {
			capabilities = append(capabilities, capability)
		}
		sort.Strings(capabilities)
		for _, capability := range capabilities {
			fmt.Printf("    - %s: %d runnable unit(s)\n", capability, counts[capability])
			if strings.HasPrefix(capability, "delegated/") {
				for _, exclusion := range plan.Excluded {
					if exclusion.Disposition+"/"+exclusion.Capability == capability {
						fmt.Printf("      - %s %s\n", exclusion.Package, exclusion.Runnable)
					}
				}
			}
		}
	}
	if len(plan.Dependencies) == 0 {
		fmt.Println("  dependencies: none")
	} else {
		fmt.Printf("  dependencies: %v\n", plan.Dependencies)
	}
	reproduction := "go run ./internal/testinfra/cmd/tetral-test --profile " + string(plan.Profile)
	if base != "" {
		reproduction += " --base " + base
	}
	fmt.Println("Reproduce:", reproduction)
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, err)
	os.Exit(2)
}
