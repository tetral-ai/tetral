package testinfra

import (
	"context"
	"time"
)

// CoveragePlan is a report-only owner for ordinary Go, Runtime, and Gateway
// coverage. It deliberately stays outside Pull Request Verification.
func CoveragePlan(ctx context.Context, root string) (Plan, error) {
	// Coverage executes the same Go package universe. Derive prerequisites
	// from its owners so new cross-language or image fixtures cannot silently
	// run without the setup and provenance required by ordinary verification.
	goSelections, _, err := fullGoSelections(ctx, root, "report-only coverage")
	if err != nil {
		return Plan{}, err
	}
	return Plan{
		Profile:      ProfileFull,
		Revision:     inspectRevision(ctx, root, ""),
		Selections:   []Selection{{Group: "coverage", Reason: "scheduled report-only coverage", Mode: "coverage"}},
		Dependencies: selectedDependencies(goSelections),
		CreatedAt:    time.Now().UTC(),
	}, nil
}
