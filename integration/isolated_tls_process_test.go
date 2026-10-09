package integration

import (
	"context"
	"net/url"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/integration/transporttest"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
)

// Environment shared by an isolated TLS root and its child process. The child
// reads the private PostgreSQL CA directory from envIsolatedTLSPGCerts when it
// configures production database openers.
const (
	envIsolatedTLSRoot    = "TETRAL_ISOLATED_TLS_ROOT"
	envIsolatedTLSPGCerts = "TETRAL_ISOLATED_TLS_PG_CERTS"
	envIsolatedTLSPGURL   = "TETRAL_ISOLATED_TLS_PG_URL"
)

// Budgets for one isolated child process. A translated Envoy Gateway root runs
// both installation profiles under one ten-minute watchdog; other roots keep
// five minutes.
const (
	envoyGatewayCompositionBudget = 10 * time.Minute
	isolatedTLSRootBudget         = 5 * time.Minute
	// isolatedTLSReportMargin leaves the child's own -test.timeout this much
	// earlier than the parent's kill, so a stuck child prints Go's timeout
	// report with its goroutine stacks.
	isolatedTLSReportMargin = 30 * time.Second
)

// isolatedTLSPostgreSQLRoot runs body in a child test process that selects a
// private TLS PostgreSQL before the process-global storage registry
// initializes. Real services in the child verify that server's explicit CA and
// hostname through their production openers. The parent requires the child's
// PASS line for this root and the owning assertion marker.
func isolatedTLSPostgreSQLRoot(t *testing.T, marker string, budget time.Duration, body func(*testing.T)) {
	t.Helper()
	if os.Getenv(envIsolatedTLSRoot) == t.Name() {
		runIsolatedTLSChild(t, body)
		return
	}
	postgres := transporttest.NewPostgreSQL(t)
	output, err := runIsolatedTLSProcess(context.Background(), t, t.Name(), "^"+t.Name()+"$", postgres, budget)
	if err != nil {
		t.Fatalf("isolated TLS root %s failed: %v", t.Name(), err)
	}
	if !strings.Contains(output, "--- PASS: "+t.Name()) || !strings.Contains(output, marker) {
		t.Fatalf("isolated TLS root %s lacks marker %q", t.Name(), marker)
	}
}

// runIsolatedTLSChild is the child half: it points storagetest at the private
// TLS PostgreSQL and runs the owning body.
func runIsolatedTLSChild(t *testing.T, body func(*testing.T)) {
	t.Helper()
	dsn, err := url.Parse(os.Getenv(envIsolatedTLSPGURL))
	if err != nil || dsn.Host == "" {
		t.Fatal("isolated TLS PostgreSQL is absent")
	}
	query := dsn.Query()
	query.Set("sslmode", "require")
	dsn.RawQuery = query.Encode()
	t.Setenv(storagetest.EnvTestDatabaseURL, dsn.String())
	t.Setenv(storagetest.EnvTestRunID, "")
	body(t)
}

// runIsolatedTLSProcess re-executes the current test binary for one selector
// under the root's budget and returns its combined output.
func runIsolatedTLSProcess(parent context.Context, t *testing.T, root, selector string, postgres *transporttest.PostgreSQL, budget time.Duration) (string, error) {
	t.Helper()
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(parent, budget)
	defer cancel()
	//nolint:gosec // Current test executable and a fixed owning selector.
	command := exec.CommandContext(ctx, executable, "-test.run="+selector, "-test.v", "-test.timeout="+(budget-isolatedTLSReportMargin).String())
	command.Env = append(os.Environ(), envIsolatedTLSRoot+"="+root, envIsolatedTLSPGCerts+"="+postgres.Directory, envIsolatedTLSPGURL+"="+postgres.URL)
	output, err := command.CombinedOutput()
	t.Logf("isolated TLS root %s (%s) output:\n%s", root, selector, output)
	return string(output), err
}
