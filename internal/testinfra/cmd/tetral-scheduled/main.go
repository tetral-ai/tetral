package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"github.com/tetral-ai/tetral/internal/testinfra"
)

func main() {
	output := flag.String("output", ".test-results/scheduled", "scheduled evidence directory")
	flag.Parse()
	root, err := repositoryRoot()
	if err != nil {
		fatal(err)
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	report, err := testinfra.ExecuteScheduled(ctx, root, *output)
	if err != nil {
		fmt.Fprintf(os.Stderr, "scheduled verification %s: %v\n", report.Status, err)
		os.Exit(1)
	}
	fmt.Printf("scheduled verification %s\n", report.Status)
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

func fatal(err error) {
	fmt.Fprintln(os.Stderr, err)
	os.Exit(2)
}
