// Package static anchors Engine's permanent-rule guardrails so the package
// contains a non-test Go file: `go build` with explicit package arguments
// (as CI uses) rejects test-only packages, while the guards themselves live
// in the package's test files and run in the CI main test gate.
package static
