package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/tetral-ai/tetral/internal/testinfra"
)

func main() {
	input := flag.String("input", "shadow-ledger.json", "normalized shadow ledger")
	introductionPullRequest := flag.Int("introduction-pull-request", 0, "pull request that introduced shadow collection")
	introducedByCommit := flag.String("introduced-by-commit", "", "exact merged commit that introduced shadow collection")
	workflowSourceSHA := flag.String("workflow-source-sha", "", "exact workflow source revision eligible for comparison")
	eligibleAfter := flag.String("eligible-after", "", "RFC3339 time of the first eligible event after introduction merged")
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
	eligibleAfterTime, err := time.Parse(time.RFC3339, *eligibleAfter)
	if err != nil {
		fatal(fmt.Errorf("eligible-after must be RFC3339: %w", err))
	}
	report := testinfra.EvaluateShadowAcceptance(ledger.Rows, testinfra.ShadowAcceptanceAuthority{
		IntroductionPullRequest: *introductionPullRequest,
		IntroducedByCommit:      *introducedByCommit,
		WorkflowSourceSHA:       *workflowSourceSHA,
		EligibleAfter:           eligibleAfterTime,
	})
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
