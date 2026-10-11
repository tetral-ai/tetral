package integration

import (
	"bytes"
	"context"
	"fmt"
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
	output := newElapsedLineWriter()
	command.Stdout, command.Stderr = output, output
	err = command.Run()
	t.Logf("isolated TLS root %s (%s) output:\n%s", root, selector, output)
	return output.String(), err
}

// elapsedLineWriter collects an isolated child's combined output and starts
// every line with the time since the child started, so the parent's evidence
// shows where the child spent its time. Assign one value to both Stdout and
// Stderr, as CombinedOutput does: os/exec then copies one pipe from one
// goroutine, so a prefix never lands inside a line, and Run returns only after
// the last write.
type elapsedLineWriter struct {
	started time.Time
	output  bytes.Buffer
	midLine bool
}

func newElapsedLineWriter() *elapsedLineWriter {
	return &elapsedLineWriter{started: time.Now()}
}

func (w *elapsedLineWriter) Write(data []byte) (int, error) {
	written := len(data)
	for len(data) > 0 {
		if !w.midLine {
			fmt.Fprintf(&w.output, "[%8.3fs] ", time.Since(w.started).Seconds())
		}
		line := data
		if end := bytes.IndexByte(data, '\n'); end >= 0 {
			line = data[:end+1]
		}
		w.output.Write(line)
		w.midLine = line[len(line)-1] != '\n'
		data = data[len(line):]
	}
	return written, nil
}

func (w *elapsedLineWriter) String() string {
	return w.output.String()
}
