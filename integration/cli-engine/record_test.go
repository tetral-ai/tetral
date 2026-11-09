package joint

import (
	"crypto/sha256"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"
)

// acceptanceRecord collects what one test, or one row of a table test, did
// and writes it, when that ends, to TETRAL_ACCEPTANCE_DIR as
// <name>_<os>-<arch>_<Engine commit>_<CLI commit>_<start>.md, <name> being
// the test's name without Test, followed for a row by "." and the row's
// name, and <start> the UTC time the test or row started, such as
// 20261108T141503Z. Each run writes a new file and never replaces an
// earlier run's record. A table test's record writes no file of its own: each
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
	// phases are the timed sub-steps of the run, such as one machine's
	// setup.
	phases []phase
	// table marks the record of a table test; parent is set on the record of
	// one of its rows.
	table  bool
	parent *acceptanceRecord
	// budget is the run's time budget on its machines' OS, 0 for none: a
	// run that writes its record later than that after its start fails.
	budget time.Duration
}

// recordLine is one line of a record section; a line that repeats the one
// before it, such as a polled read, is counted instead of written again. at
// and last are the times since the record started at which the line was
// first and last noted; site is the harness code that noted it.
type recordLine struct {
	text     string
	count    int
	at, last time.Duration
	site     string
}

func appendLine(lines []recordLine, text string, at time.Duration, site string) []recordLine {
	if n := len(lines); n > 0 && lines[n-1].text == text {
		lines[n-1].count++
		lines[n-1].last = at
		return lines
	}
	return append(lines, recordLine{text: text, count: 1, at: at, last: at, site: site})
}

// phase is one timed sub-step of a run: it started at, since the record
// started, and took took.
type phase struct {
	name     string
	at, took time.Duration
}

// timed starts the phase name and returns the function that ends it.
func (r *acceptanceRecord) timed(format string, args ...any) func() {
	start := time.Now()
	name := fmt.Sprintf(format, args...)
	return func() {
		r.mu.Lock()
		defer r.mu.Unlock()
		r.phases = append(r.phases, phase{name: name, at: start.Sub(r.started), took: time.Since(start)})
	}
}

// callSite is the harness code that noted a line: its innermost test-file
// frame outside this file.
func callSite() string {
	pcs := make([]uintptr, 48)
	frames := runtime.CallersFrames(pcs[:runtime.Callers(3, pcs)])
	for {
		frame, more := frames.Next()
		if file := filepath.Base(frame.File); strings.HasSuffix(file, "_test.go") && file != "record_test.go" {
			return fmt.Sprintf("%s:%d", file, frame.Line)
		}
		if !more {
			return ""
		}
	}
}

func seconds(d time.Duration) string { return fmt.Sprintf("%.1fs", d.Seconds()) }

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
	if budgets, ok := ownBudgets.Load(t); ok {
		if record.budget = time.Duration(budgets.(map[string]int)[in.machineOS]) * time.Second; record.budget <= 0 {
			t.Fatalf("%s has no budget on %s", t.Name(), in.machineOS)
		}
	}
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
	r.commands = appendLine(r.commands, fmt.Sprintf(format, args...), time.Since(r.started), callSite())
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
	r.observations = appendLine(r.observations, fmt.Sprintf(format, args...), time.Since(r.started), callSite())
}

// boundary notes something the run did not cover.
func (r *acceptanceRecord) boundary(format string, args ...any) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.notCovered = appendLine(r.notCovered, fmt.Sprintf(format, args...), time.Since(r.started), callSite())
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
	// Written after the cleanups registered after it, which stop the run's
	// machines and, for a test with an Engine of its own, the Engine.
	if took := time.Since(r.started); r.budget > 0 && took > r.budget {
		over := fmt.Sprintf("over budget: %s took %s, its budget on %s is %s", r.name, took.Round(100*time.Millisecond), head.platform, r.budget)
		r.t.Error(over)
		r.observations = append(r.observations, recordLine{text: over, count: 1, at: took, last: took, site: "record_test.go"})
	}
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
				fmt.Fprintf(&b, "- [+%s..+%s] %s (%d times in a row) <!-- at %s -->\n", seconds(line.at), seconds(line.last), line.text, line.count, line.site)
			} else {
				fmt.Fprintf(&b, "- [+%s] %s <!-- at %s -->\n", seconds(line.at), line.text, line.site)
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
	b.WriteString(r.timing(head))
	name := fmt.Sprintf("%s_%s_%s_%s_%s.md", r.name, head.platform, head.engineCommit, head.cliCommit, r.started.Format("20060102T150405Z"))
	if err := os.WriteFile(filepath.Join(acceptanceDir(), name), []byte(b.String()), 0o600); err != nil {
		r.t.Errorf("write acceptance record %s: %v", name, err)
	}
}

// timing is the record's Timing section: the run's timed phases, and every
// wait of more than 5 seconds before a noted line, which is the time the
// harness code between that line's site and the site of the line before it
// took. The caller holds r.mu.
func (r *acceptanceRecord) timing(head *acceptanceRecord) string {
	var b strings.Builder
	end := time.Since(r.started)
	b.WriteString("## Timing\n\n")
	if r.parent != nil {
		fmt.Fprintf(&b, "- Row started %s after its table; the table's phases, from the table's start:\n", seconds(r.started.Sub(head.started)))
		for _, p := range head.phases {
			fmt.Fprintf(&b, "  - +%s took %s: %s\n", seconds(p.at), seconds(p.took), p.name)
		}
	}
	lines := slices.Concat(r.commands, r.observations, r.notCovered)
	slices.SortStableFunc(lines, func(a, b recordLine) int { return int(a.at - b.at) })
	var lastStep time.Duration
	if n := len(lines); n > 0 {
		lastStep = slices.MaxFunc(lines, func(a, b recordLine) int { return int(a.last - b.last) }).last
	}
	fmt.Fprintf(&b, "- Last step at +%s; record written at +%s.\n", seconds(lastStep), seconds(end))
	if r.budget > 0 {
		fmt.Fprintf(&b, "- Budget on %s: %s.\n", head.platform, r.budget)
	}
	b.WriteString("- Phases:\n")
	for _, p := range r.phases {
		fmt.Fprintf(&b, "  - +%s took %s: %s\n", seconds(p.at), seconds(p.took), p.name)
	}
	b.WriteString("- Waits over 5s before a step:\n")
	waits := b.Len()
	var previous time.Duration
	previousSite := "row start"
	for _, line := range lines {
		if wait := line.at - previous; wait > 5*time.Second {
			text := line.text
			if len(text) > 160 {
				text = text[:160] + "..."
			}
			fmt.Fprintf(&b, "  - %s before +%s: %s <!-- at %s; previous step at %s -->\n", seconds(wait), seconds(line.at), text, line.site, previousSite)
		}
		if line.last > line.at+5*time.Second {
			fmt.Fprintf(&b, "  - %s repeating from +%s: %s (%d times) <!-- at %s -->\n", seconds(line.last-line.at), seconds(line.at), line.text, line.count, line.site)
		}
		if line.last >= previous {
			previous, previousSite = line.last, line.site
		}
	}
	if wait := end - previous; wait > 5*time.Second {
		fmt.Fprintf(&b, "  - %s after the last step, in cleanups <!-- previous step at %s -->\n", seconds(wait), previousSite)
	}
	if b.Len() == waits {
		b.WriteString("  - None.\n")
	}
	b.WriteString("\n")
	return b.String()
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
