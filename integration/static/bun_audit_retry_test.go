package static

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestBunAuditRetriesOnlyTransportFailures(t *testing.T) {
	repositoryRoot := finalArchitectureEngineRoot(t)
	script := filepath.Join(repositoryRoot, "scripts", "run-bun-audit.sh")

	tests := []struct {
		name        string
		scenario    string
		wantSuccess bool
		wantRuns    string
	}{
		{name: "transient then clean", scenario: "transient_then_clean", wantSuccess: true, wantRuns: "2"},
		{name: "vulnerability remains red", scenario: "vulnerability", wantSuccess: false, wantRuns: "1"},
		{name: "persistent outage remains red", scenario: "persistent_transient", wantSuccess: false, wantRuns: "3"},
		{name: "hung transport remains bounded", scenario: "hung_transport", wantSuccess: false, wantRuns: "3"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			binDir := t.TempDir()
			counter := filepath.Join(t.TempDir(), "runs")
			fakeBun := filepath.Join(binDir, "bun")
			body := `#!/usr/bin/env bash
set -euo pipefail
runs=0
if [[ -f "$TETRAL_FAKE_BUN_COUNTER" ]]; then
  runs="$(cat "$TETRAL_FAKE_BUN_COUNTER")"
fi
runs=$((runs + 1))
printf '%s' "$runs" > "$TETRAL_FAKE_BUN_COUNTER"
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

			command := exec.Command(script, "services/gateway") //nolint:gosec // Repository script with a fixed package path.
			command.Env = append(os.Environ(),
				"PATH="+binDir+":"+os.Getenv("PATH"),
				"TETRAL_BUN_AUDIT_RETRY_DELAY_SECONDS=0",
				"TETRAL_BUN_AUDIT_TIMEOUT_SECONDS=1",
				"TETRAL_FAKE_BUN_COUNTER="+counter,
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
		})
	}
}
