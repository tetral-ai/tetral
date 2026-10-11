package static

import (
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

func TestBunAuditRetriesOnlyTransportFailures(t *testing.T) {
	repositoryRoot := finalArchitectureEngineRoot(t)
	script := filepath.Join(repositoryRoot, "scripts", "run-bun-audit.sh")

	tests := []struct {
		name            string
		scenario        string
		packages        []string
		wantSuccess     bool
		wantRuns        string
		wantDirectories []string
	}{
		{name: "transient then clean", scenario: "transient_then_clean", wantSuccess: true, wantRuns: "2"},
		{name: "vulnerability remains red", scenario: "vulnerability", wantSuccess: false, wantRuns: "1"},
		{name: "persistent outage remains red", scenario: "persistent_transient", wantSuccess: false, wantRuns: "3"},
		{name: "hung transport remains bounded", scenario: "hung_transport", wantSuccess: false, wantRuns: "3"},
		{name: "every package audited and earlier findings stay red", scenario: "vulnerability_then_clean", packages: []string{"services/agent-runtime", "services/gateway"}, wantSuccess: false, wantRuns: "2", wantDirectories: []string{"services/agent-runtime", "services/gateway"}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			binDir := t.TempDir()
			counter := filepath.Join(t.TempDir(), "runs")
			directories := filepath.Join(t.TempDir(), "directories")
			fakeBun := filepath.Join(binDir, "bun")
			body := `#!/usr/bin/env bash
set -euo pipefail
runs=0
if [[ -f "$TETRAL_FAKE_BUN_COUNTER" ]]; then
  runs="$(cat "$TETRAL_FAKE_BUN_COUNTER")"
fi
runs=$((runs + 1))
printf '%s' "$runs" > "$TETRAL_FAKE_BUN_COUNTER"
printf '%s\n' "$(basename "$(dirname "$PWD")")/$(basename "$PWD")" >> "$TETRAL_FAKE_BUN_DIRECTORIES"
case "$TETRAL_FAKE_BUN_SCENARIO" in
  transient_then_clean)
    if (( runs == 1 )); then
      echo 'error: audit request failed (status 503)' >&2
      exit 1
    fi
    echo 'No vulnerabilities found'
    ;;
  vulnerability)
    echo 'high severity vulnerability found' >&2
    exit 1
    ;;
  vulnerability_then_clean)
    if (( runs == 1 )); then
      echo 'high severity vulnerability found' >&2
      exit 1
    fi
    echo 'No vulnerabilities found'
    ;;
  persistent_transient)
    echo 'ConnectionClosed: audit request failed' >&2
    exit 1
    ;;
  hung_transport)
    sleep 30
    ;;
  *)
    exit 2
    ;;
esac
`
			if err := os.WriteFile(fakeBun, []byte(body), 0o700); err != nil {
				t.Fatalf("write fake bun: %v", err)
			}

			packages := test.packages
			if len(packages) == 0 {
				packages = []string{"services/gateway"}
			}
			command := exec.Command(script, packages...) //nolint:gosec // Repository script with fixed package paths.
			command.Env = append(os.Environ(),
				"PATH="+binDir+":"+os.Getenv("PATH"),
				"TETRAL_BUN_AUDIT_RETRY_DELAY_SECONDS=0",
				"TETRAL_BUN_AUDIT_TIMEOUT_SECONDS=1",
				"TETRAL_FAKE_BUN_COUNTER="+counter,
				"TETRAL_FAKE_BUN_DIRECTORIES="+directories,
				"TETRAL_FAKE_BUN_SCENARIO="+test.scenario,
			)
			output, err := command.CombinedOutput()
			if test.wantSuccess && err != nil {
				t.Fatalf("audit failed: %v\n%s", err, output)
			}
			if !test.wantSuccess && err == nil {
				t.Fatalf("audit unexpectedly passed:\n%s", output)
			}
			runs, readErr := os.ReadFile(counter) //nolint:gosec // Test-owned temporary counter.
			if readErr != nil {
				t.Fatalf("read fake bun counter: %v", readErr)
			}
			if got := strings.TrimSpace(string(runs)); got != test.wantRuns {
				t.Fatalf("bun audit runs = %s; want %s\n%s", got, test.wantRuns, output)
			}
			if test.wantDirectories != nil {
				audited, readErr := os.ReadFile(directories) //nolint:gosec // Test-owned temporary record.
				if readErr != nil {
					t.Fatalf("read fake bun directories: %v", readErr)
				}
				if got := strings.Fields(string(audited)); !slices.Equal(got, test.wantDirectories) {
					t.Fatalf("bun audit directories = %v; want %v\n%s", got, test.wantDirectories, output)
				}
			}
		})
	}
}
