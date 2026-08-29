package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"

	"github.com/tetral-ai/tetral/internal/testinfra"
)

func main() {
	input := flag.String("input", "shadow-ledger.json", "normalized shadow ledger")
	excludedPullRequest := flag.Int("exclude-pull-request", 0, "pull request that introduced shadow collection")
	output := flag.String("output", "shadow-acceptance.json", "acceptance report")
	flag.Parse()
	body, err := os.ReadFile(*input) //nolint:gosec // explicit operator-owned ledger path.
	if err != nil {
		fatal(err)
	}
	var ledger struct {
		Version int                         `json:"version"`
		Rows    []testinfra.ShadowLedgerRow `json:"rows"`
	}
	if err := json.Unmarshal(body, &ledger); err != nil || ledger.Version != 1 {
		fatal(fmt.Errorf("decode supported shadow ledger: %w", err))
	}
	report := testinfra.EvaluateShadowAcceptance(ledger.Rows, *excludedPullRequest)
	encoded, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		fatal(err)
	}
	if err := os.WriteFile(*output, append(encoded, '\n'), 0o600); err != nil { //nolint:gosec // explicit operator-owned report path.
		fatal(err)
	}
	if !report.Ready {
		os.Exit(1)
	}
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}
