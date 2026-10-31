package drive_test

import (
	"bufio"
	"bytes"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"sync"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/drive"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	"github.com/tetral-ai/tetral/internal/workload"
	"github.com/tetral-ai/tetral/internal/workspace"
)

// budgetFixture holds one budget per service pool of a private database,
// both on a clock the test moves.
type budgetFixture struct {
	t       *testing.T
	admin   *sql.DB
	hubDB   *sql.DB
	api     *dbconnect.Client
	now     time.Time
	mu      sync.Mutex
	records *bytes.Buffer
}

func newBudgetFixture(t *testing.T) *budgetFixture {
	t.Helper()
	_, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	workloadDB := storagetest.OpenWorkloadDB(t, admin, "api")
	f := &budgetFixture{
		t:       t,
		admin:   admin,
		hubDB:   workloadDB.OpenWorkload(t, "computer_hub", nil),
		api:     dbconnect.NewClientForTesting(workloadDB.DB),
		now:     time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		records: &bytes.Buffer{},
	}
	installBudgetTelemetry(t, f.records)
	return f
}

// installBudgetTelemetry installs the computer-hub's telemetry, whose table
// declares the budget's family, with its records written to records.
func installBudgetTelemetry(t *testing.T, records io.Writer) {
	t.Helper()
	table, err := os.ReadFile("../../services/computer-hub/observability.json")
	if err != nil {
		t.Fatal(err)
	}
	workload.RegisterDeclarations("computer-hub", table)
	telemetry := workload.NewTelemetry(workload.TelemetryOptions{Service: "computer-hub", Logger: workload.NewLogger(records, "computer-hub", "test", "unit")})
	t.Cleanup(workload.InstallTelemetry(telemetry))
}

func (f *budgetFixture) clock() time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.now
}

func (f *budgetFixture) advance(d time.Duration) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.now = f.now.Add(d)
}

func (f *budgetFixture) budget(perMinute int64) *drive.UploadBudget {
	f.t.Helper()
	return newBudget(f.t, f.api, perMinute, f.clock)
}

func newBudget(t *testing.T, db *dbconnect.Client, perMinute int64, clock func() time.Time) *drive.UploadBudget {
	t.Helper()
	budget, err := drive.NewUploadBudget(db, perMinute, workload.CurrentTelemetry().Registry())
	if err != nil {
		t.Fatal(err)
	}
	return budget.WithClock(clock)
}

// workspace adds a Workspace with no budget row.
func (f *budgetFixture) workspace(id string) workspace.ID {
	f.t.Helper()
	if _, err := f.admin.Exec(`INSERT INTO workspaces (id,type,name,created_at) VALUES ($1,'workspace',$1,now())`, id); err != nil {
		f.t.Fatal(err)
	}
	return workspace.ID(id)
}

// take returns 0 when the budget took the request, or its Retry-After.
func take(t *testing.T, budget *drive.UploadBudget, ws workspace.ID, contentLength int64) int {
	t.Helper()
	err := budget.Take(t.Context(), ws, contentLength)
	var refused *drive.UploadRefusedError
	switch {
	case err == nil:
		return 0
	case errors.As(err, &refused):
		var rejection *workload.Rejection
		if !errors.As(err, &rejection) || rejection.Point != "drive_upload_budget" || rejection.Cause != "budget" {
			t.Fatalf("refusal %v does not unwrap to the pressure point's rejection", err)
		}
		return refused.RetryAfterSeconds
	default:
		t.Fatal(err)
		return 0
	}
}

func TestUploadBudgetTakes(t *testing.T) {
	f := newBudgetFixture(t)
	type step struct {
		advance       time.Duration
		contentLength int64
		retryAfter    int
	}
	for i, tc := range []struct {
		name      string
		perMinute int64
		steps     []step
	}{
		{"starts full and refills at the per-minute amount", 500_000_000, []step{
			{0, 500_000_000, 0},
			{0, 65_536, 1},
			{30 * time.Second, 250_000_000, 0},
			{0, 250_000_001, 31},
		}},
		{"a request counts at least 65,536 bytes", 500_000_000, []step{
			{0, 499_934_465, 0},
			{0, 1, 1},
		}},
		{"a request counts at most the per-minute amount", 500_000_000, []step{
			{0, 600_000_000, 0},
			{0, 65_536, 1},
		}},
		{"an unknown length counts the per-minute amount", 500_000_000, []step{
			{0, 1, 0},
			{0, -1, 1},
			{60 * time.Second, -1, 0},
		}},
		{"never holds more than the per-minute amount", 500_000_000, []step{
			{0, 1, 0},
			{time.Hour, 500_000_000, 0},
			{0, 65_536, 1},
		}},
		{"a clock behind refilled_at refills nothing", 500_000_000, []step{
			{0, 500_000_000, 0},
			{-5 * time.Second, 65_536, 1},
		}},
		{"the smallest budget refills over a minute", 65_536, []step{
			{0, 1, 0},
			{0, 1, 60},
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			budget := f.budget(tc.perMinute)
			ws := f.workspace(fmt.Sprintf("wksp_take_%d", i))
			for n, step := range tc.steps {
				f.advance(step.advance)
				if got := take(t, budget, ws, step.contentLength); got != step.retryAfter {
					t.Fatalf("step %d: Retry-After = %d, want %d (0: taken)", n, got, step.retryAfter)
				}
			}
		})
	}
}

const (
	budgetChildDatabase  = "TETRAL_DRIVE_BUDGET_CHILD_DATABASE"
	budgetChildWorkspace = "TETRAL_DRIVE_BUDGET_CHILD_WORKSPACE"
	budgetChildClock     = "TETRAL_DRIVE_BUDGET_CHILD_CLOCK"
)

// Fifty requests against one Workspace's row on a clock that does not move:
// 25 from this process through the api pool and 25 from a child process
// through the computer-hub pool. Exactly floor(1,000,000 / 65,536) are taken.
func TestUploadBudgetIsOneBudgetAcrossProcesses(t *testing.T) {
	if dsn := os.Getenv(budgetChildDatabase); dsn != "" {
		runBudgetChild(t, dsn)
		return
	}
	f := newBudgetFixture(t)
	ws := f.workspace("wksp_concurrent")
	child := exec.CommandContext(t.Context(), os.Args[0], "-test.run=^TestUploadBudgetIsOneBudgetAcrossProcesses$")
	child.Env = append(os.Environ(), budgetChildDatabase+"="+storagetest.RuntimeDatabaseURL(t, f.hubDB),
		budgetChildWorkspace+"="+string(ws), budgetChildClock+"="+f.now.Format(time.RFC3339Nano))
	child.Stderr = os.Stderr
	stdin, err := child.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	stdout, err := child.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := child.Start(); err != nil {
		t.Fatal(err)
	}
	lines := bufio.NewScanner(stdout)
	if !lines.Scan() || lines.Text() != "ready" {
		t.Fatalf("child said %q, want ready", lines.Text())
	}
	if _, err := io.WriteString(stdin, "go\n"); err != nil {
		t.Fatal(err)
	}
	taken, refused := takeConcurrently(t, f.budget(1_000_000), ws, 25)
	var childTaken, childRefused int
	if !lines.Scan() {
		t.Fatalf("child gave no result: %v", lines.Err())
	}
	if _, err := fmt.Sscanf(lines.Text(), "taken %d refused %d", &childTaken, &childRefused); err != nil {
		t.Fatalf("child result %q: %v", lines.Text(), err)
	}
	_, _ = io.Copy(io.Discard, stdout)
	if err := child.Wait(); err != nil {
		t.Fatalf("child: %v", err)
	}
	if taken+childTaken != 15 || refused+childRefused != 35 {
		t.Fatalf("taken %d+%d, refused %d+%d; want 15 and 35 in all", taken, childTaken, refused, childRefused)
	}
}

func runBudgetChild(t *testing.T, dsn string) {
	opened, err := dbconnect.OpenPlainDSN(t.Context(), budgetChildDatabase, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = opened.Client.Close() }()
	installBudgetTelemetry(t, io.Discard)
	at, err := time.Parse(time.RFC3339Nano, os.Getenv(budgetChildClock))
	if err != nil {
		t.Fatal(err)
	}
	budget := newBudget(t, opened.Client, 1_000_000, func() time.Time { return at })
	fmt.Println("ready")
	if _, err := bufio.NewReader(os.Stdin).ReadString('\n'); err != nil {
		t.Fatal(err)
	}
	taken, refused := takeConcurrently(t, budget, workspace.ID(os.Getenv(budgetChildWorkspace)), 25)
	fmt.Printf("taken %d refused %d\n", taken, refused)
}

func takeConcurrently(t *testing.T, budget *drive.UploadBudget, ws workspace.ID, n int) (taken, refused int) {
	t.Helper()
	var mu sync.Mutex
	var wg sync.WaitGroup
	for range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := budget.Take(t.Context(), ws, 65_536)
			mu.Lock()
			defer mu.Unlock()
			var refusal *drive.UploadRefusedError
			switch {
			case err == nil:
				taken++
			case errors.As(err, &refusal):
				refused++
			default:
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	return taken, refused
}

func TestEmptyBudgetDoesNotSlowAnotherWorkspace(t *testing.T) {
	f := newBudgetFixture(t)
	budget := f.budget(65_536)
	empty, other := f.workspace("wksp_empty"), f.workspace("wksp_other")
	take(t, budget, empty, 1)
	if got := take(t, budget, empty, 1); got == 0 {
		t.Fatal("the empty Workspace's take was served")
	}
	if got := take(t, budget, other, 1); got != 0 {
		t.Fatalf("the other Workspace's take was refused with Retry-After %d", got)
	}
}

func TestUploadBudgetRefusalEpisodes(t *testing.T) {
	t.Run("ends at the first admission a minute after the last refusal", func(t *testing.T) {
		f := newBudgetFixture(t)
		budget := f.budget(65_536)
		ws := f.workspace("wksp_episode")
		take(t, budget, ws, 1)
		for range 3 {
			f.advance(time.Second)
			if take(t, budget, ws, 1) == 0 {
				t.Fatal("take served from an empty budget")
			}
		}
		f.advance(60 * time.Second)
		if take(t, budget, ws, 1) != 0 {
			t.Fatal("take refused after a minute of refill")
		}
		records := f.pressureRecords()
		if len(records) != 2 || records[0]["event"] != "pressure.rejecting_started" || records[1]["event"] != "pressure.rejecting_ended" || records[1]["pressure.rejected"] != float64(3) {
			t.Fatalf("records = %v; want one started and one ended with 3 rejected", records)
		}
		if got := f.rejectedTotal(); got != 3 {
			t.Fatalf("drive_upload_budget_rejected_total{cause=budget} = %v, want 3", got)
		}
	})

	t.Run("another Workspace's admissions do not end an ongoing episode", func(t *testing.T) {
		f := newBudgetFixture(t)
		budget := f.budget(65_536)
		refused, admitted := f.workspace("wksp_refused"), f.workspace("wksp_admitted")
		take(t, budget, refused, 1)
		for i := range 60 {
			if i > 0 {
				f.advance(time.Second)
			}
			if take(t, budget, refused, 1) == 0 {
				t.Fatal("take served from an empty budget")
			}
			take(t, budget, admitted, -1)
		}
		records := f.pressureRecords()
		if len(records) != 1 || records[0]["event"] != "pressure.rejecting_started" {
			t.Fatalf("records = %v; want only rejecting_started", records)
		}
	})
}

func (f *budgetFixture) pressureRecords() []map[string]any {
	f.t.Helper()
	var records []map[string]any
	scanner := bufio.NewScanner(bytes.NewReader(f.records.Bytes()))
	for scanner.Scan() {
		var record map[string]any
		if err := json.Unmarshal(scanner.Bytes(), &record); err != nil {
			f.t.Fatalf("record %q: %v", scanner.Text(), err)
		}
		if record["pressure.point"] == "drive_upload_budget" {
			records = append(records, record)
		}
	}
	return records
}

func (f *budgetFixture) rejectedTotal() float64 {
	f.t.Helper()
	samples, err := workload.CurrentTelemetry().Registry().Collector()(f.t.Context())
	if err != nil {
		f.t.Fatal(err)
	}
	for _, sample := range samples {
		if sample.Name == "drive_upload_budget_rejected_total" && len(sample.Labels) == 1 && sample.Labels[0].Value == "budget" {
			return sample.Value
		}
	}
	return 0
}
