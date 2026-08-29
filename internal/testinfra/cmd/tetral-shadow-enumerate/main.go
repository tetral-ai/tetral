package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/tetral-ai/tetral/internal/testinfra"
)

func main() {
	repository := flag.String("repository", "", "owner/repository for read-only live enumeration")
	eligibleAfter := flag.String("eligible-after", "", "RFC3339 start of the observation window")
	output := flag.String("output", "shadow-universe.json", "enumerated observation-window output")
	flag.Parse()
	if *repository == "" {
		fatal(fmt.Errorf("repository is required"))
	}
	start, err := time.Parse(time.RFC3339, *eligibleAfter)
	if err != nil {
		fatal(fmt.Errorf("eligible-after must be RFC3339: %w", err))
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	universe, err := testinfra.EnumerateLiveShadowUniverse(ctx, *repository, start)
	if err != nil {
		fatal(err)
	}
	body, err := json.MarshalIndent(universe, "", "  ")
	if err != nil {
		fatal(err)
	}
	if err := os.WriteFile(*output, append(body, '\n'), 0o600); err != nil { //nolint:gosec // explicit operator-owned output.
		fatal(err)
	}
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}
