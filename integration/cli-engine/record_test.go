package joint

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// acceptanceRecord collects what one test run did and writes it, when the
// test ends, to TETRAL_ACCEPTANCE_DIR as
// <name>_<os>-<arch>_<Engine commit>_<CLI commit>.md; a later run of the same
// test on the same platform at the same two commits replaces the file.
type acceptanceRecord struct {
	t            *testing.T
	name         string
	platform     string
	engineCommit string
	cliCommit    string
	cliVersion   string
	started      time.Time

	mu           sync.Mutex
	commands     []recordLine
	observations []recordLine
	notCovered   []recordLine
}

// recordLine is one line of a record section; a line that repeats the one
// before it, such as a polled read, is counted instead of written again.
type recordLine struct {
	text  string
	count int
}

func appendLine(lines []recordLine, text string) []recordLine {
	if n := len(lines); n > 0 && lines[n-1].text == text {
		lines[n-1].count++
		return lines
	}
	return append(lines, recordLine{text: text, count: 1})
}

// newAcceptanceRecord starts the record of t, whose machines run on
// platform, and writes it when t ends.
func newAcceptanceRecord(t *testing.T, in inputs, platform string) *acceptanceRecord {
	t.Helper()
	record := &acceptanceRecord{
		t:            t,
		name:         strings.TrimPrefix(t.Name(), "Test"),
		platform:     platform,
		engineCommit: engineCommit(t),
		cliCommit:    in.cliCommit,
		started:      time.Now().UTC(),
	}
	t.Cleanup(record.write)
	return record
}

// command notes a command the run executed; it never holds a key.
func (r *acceptanceRecord) command(format string, args ...any) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.commands = appendLine(r.commands, fmt.Sprintf(format, args...))
}

// setCLIVersion records the tetral --version of the run's machines.
func (r *acceptanceRecord) setCLIVersion(version string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.cliVersion == "" {
		r.cliVersion = version
	} else if r.cliVersion != version {
		r.cliVersion += ", " + version
	}
}

// observe notes one observation of the run.
func (r *acceptanceRecord) observe(format string, args ...any) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.observations = appendLine(r.observations, fmt.Sprintf(format, args...))
}

// boundary notes something the run did not cover.
func (r *acceptanceRecord) boundary(format string, args ...any) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.notCovered = appendLine(r.notCovered, fmt.Sprintf(format, args...))
}

func (r *acceptanceRecord) write() {
	r.mu.Lock()
	defer r.mu.Unlock()
	result := "pass"
	if r.t.Failed() {
		result = "fail"
	}
	var b strings.Builder
	fmt.Fprintf(&b, "# %s on %s\n\n", r.name, r.platform)
	fmt.Fprintf(&b, "Result: **%s**. Test `%s` of the CLI and Engine harness, started %s, ended %s.\n\n", result, r.t.Name(), r.started.Format(time.RFC3339), time.Now().UTC().Format(time.RFC3339))
	fmt.Fprintf(&b, "## Builds\n\n- Engine commit `%s`, its services built from that worktree.\n- CLI commit `%s`, `tetral --version`: `%s`.\n- Machines: `%s`.\n\n", r.engineCommit, r.cliCommit, r.cliVersion, r.platform)
	section := func(title string, lines []recordLine) {
		fmt.Fprintf(&b, "## %s\n\n", title)
		if len(lines) == 0 {
			b.WriteString("None.\n\n")
			return
		}
		for _, line := range lines {
			if line.count > 1 {
				fmt.Fprintf(&b, "- %s (%d times in a row)\n", line.text, line.count)
			} else {
				fmt.Fprintf(&b, "- %s\n", line.text)
			}
		}
		b.WriteString("\n")
	}
	section("Commands", r.commands)
	section("Observations", r.observations)
	section("Not covered", r.notCovered)
	name := fmt.Sprintf("%s_%s_%s_%s.md", r.name, r.platform, r.engineCommit, r.cliCommit)
	if err := os.WriteFile(filepath.Join(acceptanceDir(), name), []byte(b.String()), 0o600); err != nil {
		r.t.Errorf("write acceptance record %s: %v", name, err)
	}
}

// engineCommit is the full commit of the Engine worktree the harness runs in.
func engineCommit(t *testing.T) string {
	t.Helper()
	out, err := exec.Command("git", "-C", repositoryRoot(t), "rev-parse", "HEAD").Output()
	if err != nil {
		t.Fatalf("git rev-parse HEAD: %v", err)
	}
	return strings.TrimSpace(string(out))
}
