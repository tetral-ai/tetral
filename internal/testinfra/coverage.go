package testinfra

import "time"

// CoveragePlan is a report-only owner for ordinary Go, Runtime, and Gateway
// coverage. It deliberately stays outside Pull Request Verification.
func CoveragePlan(root string) Plan {
	return Plan{
		Profile:      ProfileFull,
		Revision:     inspectRevision(root, ""),
		Selections:   []Selection{{Group: "coverage", Reason: "main-branch report-only coverage", Mode: "coverage"}},
		Dependencies: []string{"postgresql", "minio", "sdk"},
		CreatedAt:    time.Now().UTC(),
	}
}
