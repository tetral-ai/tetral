package joint

import (
	"crypto/sha256"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// acceptanceRecord collects what one test, or one row of a table test, did
// and writes it, when that ends, to TETRAL_ACCEPTANCE_DIR as
// <name>_<os>-<arch>_<Engine commit>_<CLI commit>.md, <name> being the test's
// name without Test, followed for a row by "." and the row's name; a later
// run of the same test or row on the same platform at the same two commits
// replaces the file. A table test's record writes no file of its own: each
// row's file carries the table's builds and what the table's shared Engine
// did outside the rows.
type acceptanceRecord struct {
	t            *testing.T
	name         string
	platform     string
	engineCommit string
	cliCommit    string
	cliVersion   string
	cliSHA256    string
	sdkCommit    string
	engineState  string
	sdkState     string
	started      time.Time

	mu           sync.Mutex
	commands     []recordLine
	observations []recordLine
	notCovered   []recordLine
	// table marks the record of a table test; parent is set on the record of
	// one of its rows.
	table  bool
	parent *acceptanceRecord
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
	binary := in.cliBin
	if in.machineOS == "macos" {
		binary = in.cliBinMacOS
	}
	f, err := os.Open(binary)
	if err != nil {
		t.Fatal(err)
	}
	hash := sha256.New()
	_, err = io.Copy(hash, f)
	_ = f.Close()
	if err != nil {
		t.Fatal(err)
	}
	record.cliSHA256 = fmt.Sprintf("%x", hash.Sum(nil))
	record.engineState = worktreeState(t, repositoryRoot(t))
	record.sdkCommit = "not supplied (SDK not exercised)"
	if root := os.Getenv("TETRAL_ENGINE_SDK_ROOT"); root != "" {
		output, err := exec.Command("git", "-C", root, "rev-parse", "HEAD").Output()
		if err != nil {
			t.Fatal(err)
		}
		record.sdkCommit = strings.TrimSpace(string(output))
		record.sdkState = worktreeState(t, root)
	}
	t.Cleanup(record.write)
	return record
}

// row starts the record of the row name of r's table, which t runs, and
// writes it when t ends.
func (r *acceptanceRecord) row(t *testing.T, name string) *acceptanceRecord {
	row := &acceptanceRecord{t: t, name: r.name + "." + name, started: time.Now().UTC(), parent: r}
	t.Cleanup(row.write)
	return row
}

// command notes a command the run executed; it never holds a key.
func (r *acceptanceRecord) command(format string, args ...any) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.commands = appendLine(r.commands, fmt.Sprintf(format, args...))
}

// setCLIVersion records the tetral --version of the run's machines.
func (r *acceptanceRecord) setCLIVersion(version string) {
	if r.parent != nil {
		r.parent.setCLIVersion(version)
		return
	}
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
	if r.table {
		return
	}
	head := r
	if r.parent != nil {
		head = r.parent
		head.mu.Lock()
		defer head.mu.Unlock()
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	result := "pass"
	if r.t.Failed() {
		result = "fail"
	}
	var b strings.Builder
	fmt.Fprintf(&b, "# %s on %s\n\n", r.name, head.platform)
	fmt.Fprintf(&b, "Result: **%s**. Test `%s` of the CLI and Engine harness, started %s, ended %s.\n\n", result, r.t.Name(), r.started.Format(time.RFC3339), time.Now().UTC().Format(time.RFC3339))
	fmt.Fprintf(&b, "## Builds\n\n- Engine commit `%s`, its services built from that worktree.\n- CLI commit `%s`, `tetral --version`: `%s`.\n- Machines: `%s`.\n\n", head.engineCommit, head.cliCommit, head.cliVersion, head.platform)
	fmt.Fprintf(&b, "- Engine source state at start: %s.\n- CLI artifact SHA-256: `%s`.\n- SDK source commit: `%s`; source state: %s. SDK use is stated in Observations.\n\n", head.engineState, head.cliSHA256, head.sdkCommit, head.sdkState)
	section := func(level, title string, lines []recordLine) {
		fmt.Fprintf(&b, "%s %s\n\n", level, title)
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
	if r.parent != nil {
		fmt.Fprintf(&b, "## Table setup\n\nWhat test `%s` did on its shared Engine and Runtime outside its rows, up to this row's end.\n\n", head.t.Name())
		section("###", "Commands", head.commands)
		section("###", "Observations", head.observations)
		section("###", "Not covered", head.notCovered)
	}
	section("##", "Commands", r.commands)
	section("##", "Observations", r.observations)
	section("##", "Not covered", r.notCovered)
	name := fmt.Sprintf("%s_%s_%s_%s.md", r.name, head.platform, head.engineCommit, head.cliCommit)
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

func worktreeState(t *testing.T, root string) string {
	t.Helper()
	output, err := exec.Command("git", "-C", root, "status", "--porcelain").Output()
	if err != nil {
		t.Fatal(err)
	}
	if len(output) > 0 {
		return "modified (development run; not clean-commit evidence)"
	}
	return "clean"
}
