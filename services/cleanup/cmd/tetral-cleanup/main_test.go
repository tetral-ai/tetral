package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/storage"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	"github.com/tetral-ai/tetral/internal/workload"
	tetralcleanup "github.com/tetral-ai/tetral/services/cleanup"
)

func TestCleanupSchemaBehindStopsBeforeScheduling(t *testing.T) {
	runtimeDB := storagetest.NewPostgreSQLDB(t)
	client := dbconnect.NewClientForTesting(runtimeDB)
	previousOpen, previousVerify := openDatabase, verifySchema
	openDatabase = func(context.Context) (dbconnect.OpenResult, error) { return dbconnect.OpenResult{Client: client}, nil }
	verifySchema = func(context.Context, *dbconnect.Client) error {
		return &storage.SchemaMigrationError{Kind: storage.SchemaErrorBehind, Version: 1}
	}
	t.Cleanup(func() { openDatabase, verifySchema = previousOpen, previousVerify })

	err := run(context.Background(), cleanupEnvMap{})
	var schemaErr *storage.SchemaMigrationError
	if !errors.As(err, &schemaErr) || schemaErr.Kind != storage.SchemaErrorBehind {
		t.Fatalf("run error = %v, want schema-behind", err)
	}
}

func TestCleanupCommandStartupFailureLogUsesSharedFields(t *testing.T) {
	stderr, finish := captureStderr(t)
	err := run(context.Background(), cleanupEnvMap{tetralcleanup.EnvClaimLimit: "0"})
	if err == nil {
		t.Fatal("run returned nil for config failure")
	}
	finish()
	output := stderr.String()
	for _, want := range []string{
		`"msg":"startup.failed"`,
		`"service.name":"cleanup"`,
		`"component":"cleanup"`,
		`"error.class":"config_error"`,
		tetralcleanup.EnvClaimLimit,
	} {
		if !strings.Contains(output, want) {
			t.Fatalf("startup log missing %s: %s", want, output)
		}
	}
}

func TestCleanupCommandSuccessLogUsesSharedOperationFields(t *testing.T) {
	var buffer bytes.Buffer
	logger := workload.NewLogger(&buffer, tetralcleanup.ServiceName, "test", "unit")

	logCleanupClaimDue(logger, tetralcleanup.SchedulingResult{Elected: true, Attempted: 3, Claimed: 2}, nil, 10, 25*time.Millisecond)

	var fields map[string]any
	if err := json.Unmarshal(buffer.Bytes(), &fields); err != nil {
		t.Fatalf("decode success log: %v; line=%s", err, buffer.String())
	}
	want := map[string]any{
		"msg":                    "cleanup.claim_due.completed",
		"service.name":           tetralcleanup.ServiceName,
		"deployment.environment": "test",
		"service.version":        "unit",
		"operation":              "cleanup.claim_due",
		"event.kind":             "cleanup.claim_due.completed",
		"component":              tetralcleanup.ServiceName,
		"outcome":                "completed",
		"duration.ms":            float64(25),
		"candidate.count":        float64(3),
		"cleanup.jobs.claimed":   float64(2),
		"failed.count":           float64(0),
		"cleanup.claim.limit":    float64(10),
	}
	for key, value := range want {
		if fields[key] != value {
			t.Fatalf("field %s = %#v; want %#v in %#v", key, fields[key], value, fields)
		}
	}
	for _, forbidden := range []string{"workspace.id", "session.id", "thread.id", "job.id", "cleanup.id", "error.message", "secret"} {
		if _, ok := fields[forbidden]; ok {
			t.Fatalf("success log included forbidden field %s: %#v", forbidden, fields)
		}
	}
}

func TestExportCleanupMetricsExportsSchedulerSeries(t *testing.T) {
	metrics := tetralcleanup.NewSchedulerMetrics()
	metrics.ObserveClaimDue(3, 40*time.Millisecond, nil)
	exporter := &recordingMetricsExporter{}

	exportCleanupMetrics(context.Background(), nil, exporter, metrics, time.Second)

	legacy, budget := 0, map[string]bool{}
	for _, sample := range exporter.samples {
		switch {
		case sample.Family == "tetral_operation_duration_seconds":
		case sample.Name == "tetral_cleanup_retention_budget_exhausted_total":
			if len(sample.Labels) != 1 || sample.Labels[0].Name != "phase" {
				t.Fatalf("budget sample labels = %#v; want only the fixed phase label", sample.Labels)
			}
			budget[sample.Labels[0].Value] = true
		default:
			legacy++
			if len(sample.Labels) != 0 {
				t.Fatalf("sample %s labels = %#v; want no workspace/session labels", sample.Name, sample.Labels)
			}
		}
	}
	if legacy != 3 || !budget[tetralcleanup.RetentionPhaseIdempotency] || !budget[tetralcleanup.RetentionPhaseStreamChanges] || len(budget) != 2 {
		t.Fatalf("exported samples = %#v; want three scheduler series and one budget series per retention phase", exporter.samples)
	}
}

// One invocation runs scheduling, then receipt retention, then change
// retention. A failing receipt batch (a fixture trigger on a marker receipt)
// stops only its own phase: change retention still runs, metrics are exported
// first, and the command returns the failure. Cancelling the process context
// during receipt retention skips change retention.
func TestCleanupRunsEveryPhaseInOrderAndReturnsJoinedErrorsAfterExport(t *testing.T) {
	ctx := context.Background()
	admin := storagetest.NewPostgreSQLAdminDB(t)
	w := storagetest.OpenWorkloadDB(t, admin, "cleanup")
	for _, statement := range []string{
		`INSERT INTO workspaces (id, type, name, created_at) VALUES ('ws_phases', 'workspace', 'ws_phases', now())`,
		`INSERT INTO agents (workspace_id, id, name, version, created_at, updated_at) VALUES ('ws_phases', 'agent_phases', 'agent', 1, now(), now())`,
		`INSERT INTO agent_versions (workspace_id, id, agent_id, version, config_json, config_hash, created_at) VALUES ('ws_phases', 'agv_phases', 'agent_phases', 1, '{}', 'h', now())`,
		`INSERT INTO environments (workspace_id, id, name, config_json, created_at, updated_at) VALUES ('ws_phases', 'env_phases', 'env', '{}', now(), now())`,
		`INSERT INTO sessions (workspace_id, id, main_thread_id, type, status, lifecycle_state, agent_id, agent_version, environment_id, created_at, updated_at) VALUES ('ws_phases', 'sesn_phases', 'thr_phases', 'session', 'idle', 'active', 'agent_phases', 1, 'env_phases', now(), now())`,
		`INSERT INTO session_threads (workspace_id, id, session_id, role, visibility, status, created_at, last_active_at, updated_at) VALUES ('ws_phases', 'thr_phases', 'sesn_phases', 'main', 'public', 'idle', now(), now(), now())`,
		`INSERT INTO session_event_idempotency_keys (workspace_id, session_id, idempotency_key_digest, canonical_request_hash, response_events_json, created_at, updated_at) VALUES ('ws_phases', 'sesn_phases', '\xdead', '\x00', '[]', now() - interval '2 days', now() - interval '2 days')`,
		`INSERT INTO session_events (workspace_id, session_id, session_thread_id, event_id, sequence, type, payload_json, created_at, updated_at) VALUES ('ws_phases', 'sesn_phases', 'thr_phases', 'evt_phases', 1, 'agent.message', '{}', now(), now())`,
		`INSERT INTO session_event_stream_changes (workspace_id, session_id, event_id, session_thread_id, revision, visibility, session_visible, changed_at) VALUES ('ws_phases', 'sesn_phases', 'evt_phases', 'thr_phases', 1, 'public', true, now() - interval '2 days')`,
		`CREATE FUNCTION public.fail_marker_receipt() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
			IF OLD.idempotency_key_digest = '\xdead'::bytea THEN RAISE EXCEPTION 'fixture receipt failure' USING ERRCODE = 'P0001'; END IF; RETURN OLD; END $$`,
		`CREATE TRIGGER fail_marker_receipt BEFORE DELETE ON session_event_idempotency_keys FOR EACH ROW EXECUTE FUNCTION public.fail_marker_receipt()`,
	} {
		if _, err := admin.Exec(statement); err != nil {
			t.Fatalf("%s: %v", statement, err)
		}
	}
	tracer := &phaseTracer{}
	previousOpen, previousVerify, previousExporter := openDatabase, verifySchema, newMetricsExporter
	exporter := &recordingMetricsExporter{}
	openDatabase = func(context.Context) (dbconnect.OpenResult, error) {
		return dbconnect.OpenResult{Client: dbconnect.NewClientForTesting(w.OpenWorkload(t, "cleanup", tracer))}, nil
	}
	verifySchema = func(context.Context, *dbconnect.Client) error { return nil }
	newMetricsExporter = func(string) tetralcleanup.MetricsExporter { return exporter }
	t.Cleanup(func() {
		openDatabase, verifySchema, newMetricsExporter = previousOpen, previousVerify, previousExporter
	})

	err := run(ctx, cleanupEnvMap{tetralcleanup.EnvMetricsExportURL: "http://metrics.invalid/push"})
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) || pgErr.Code != "P0001" {
		t.Fatalf("run error = %v; want the receipt phase's fixture failure", err)
	}
	if got := strings.Join(tracer.phases(), " "); got != "schedule idempotency changes" {
		t.Fatalf("phase order = %q; want schedule idempotency changes", got)
	}
	var changes int
	if err := admin.QueryRow(`SELECT count(*) FROM session_event_stream_changes`).Scan(&changes); err != nil || changes != 0 {
		t.Fatalf("change retention after the receipt failure left %d rows (%v)", changes, err)
	}
	if len(exporter.samples) == 0 {
		t.Fatal("metrics were not exported before the joined error returned")
	}

	cancelled, cancel := context.WithCancel(ctx)
	tracer.reset()
	tracer.cancelAt, tracer.cancel = "tetral_prune_event_idempotency", cancel
	stderr, finish := captureStderr(t)
	err = run(cancelled, cleanupEnvMap{})
	finish()
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled run error = %v; want cancellation", err)
	}
	if got := strings.Join(tracer.phases(), " "); got != "schedule idempotency" {
		t.Fatalf("phases after cancellation = %q; want change retention skipped", got)
	}
	if strings.Contains(stderr.String(), `"phase":"stream_changes"`) {
		t.Fatalf("change retention ran after cancellation: %s", stderr.String())
	}
}

// phaseTracer records which Cleanup phase each statement belongs to and can
// cancel the process context when a statement starts.
type phaseTracer struct {
	mu       sync.Mutex
	seen     []string
	cancelAt string
	cancel   context.CancelFunc
}

func (p *phaseTracer) TraceQueryStart(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	phase := ""
	switch {
	case strings.Contains(data.SQL, "tetral_cleanup_due_sessions"):
		phase = "schedule"
	case strings.Contains(data.SQL, "tetral_prune_event_idempotency"):
		phase = "idempotency"
	case strings.Contains(data.SQL, "tetral_prune_event_changes"):
		phase = "changes"
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if phase != "" && (len(p.seen) == 0 || p.seen[len(p.seen)-1] != phase) {
		p.seen = append(p.seen, phase)
	}
	if p.cancelAt != "" && strings.Contains(data.SQL, p.cancelAt) && p.cancel != nil {
		p.cancel()
	}
	return ctx
}

func (*phaseTracer) TraceQueryEnd(context.Context, *pgx.Conn, pgx.TraceQueryEndData) {}

func (p *phaseTracer) phases() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]string(nil), p.seen...)
}

func (p *phaseTracer) reset() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.seen = nil
}

func TestExportCleanupMetricsBoundsAndLogsExporterFailure(t *testing.T) {
	metrics := tetralcleanup.NewSchedulerMetrics()
	metrics.ObserveClaimDue(1, time.Millisecond, nil)
	exporter := &recordingMetricsExporter{waitForCancellation: true, err: errors.New("hostile exporter detail")}
	var buffer bytes.Buffer
	logger := workload.NewLogger(&buffer, tetralcleanup.ServiceName, "test", "unit")
	started := time.Now()

	exportCleanupMetrics(context.Background(), logger, exporter, metrics, 10*time.Millisecond)

	if elapsed := time.Since(started); elapsed > 250*time.Millisecond {
		t.Fatalf("export failure took %s; want bounded return", elapsed)
	}
	output := buffer.String()
	for _, want := range []string{
		`"msg":"cleanup.metrics_export.failed"`,
		`"operation":"cleanup.metrics_export"`,
		`"terminal":true`,
		`"retryable":false`,
		`"error.class":"metrics_export_error"`,
		`"error.code":"cleanup_metrics_export_failed"`,
		`"error.message_safe":"cleanup metrics export failed"`,
	} {
		if !strings.Contains(output, want) {
			t.Fatalf("export failure log missing %s: %s", want, output)
		}
	}
	if strings.Contains(output, "hostile exporter detail") {
		t.Fatalf("export failure log leaked exporter error: %s", output)
	}
}

type cleanupEnvMap map[string]string

func (m cleanupEnvMap) Getenv(key string) string { return m[key] }

type recordingMetricsExporter struct {
	samples             []workload.Metric
	waitForCancellation bool
	err                 error
}

func (e *recordingMetricsExporter) Export(ctx context.Context, samples []workload.Metric) error {
	e.samples = append([]workload.Metric(nil), samples...)
	if e.waitForCancellation {
		<-ctx.Done()
	}
	return e.err
}

func captureStderr(t *testing.T) (*bytes.Buffer, func()) {
	t.Helper()
	previous := os.Stderr
	readEnd, writeEnd, err := os.Pipe()
	if err != nil {
		t.Fatalf("pipe: %v", err)
	}
	os.Stderr = writeEnd
	var buffer bytes.Buffer
	done := make(chan struct{})
	go func() {
		_, _ = buffer.ReadFrom(readEnd)
		close(done)
	}()
	finish := func() {
		_ = writeEnd.Close()
		os.Stderr = previous
		<-done
		_ = readEnd.Close()
	}
	t.Cleanup(func() {
		if os.Stderr == writeEnd {
			finish()
		}
	})
	return &buffer, finish
}
