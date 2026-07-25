package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/storage"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	"github.com/tetral-ai/tetral/internal/workload"
	"github.com/tetral-ai/tetral/internal/workspace"
	tetralcleanup "github.com/tetral-ai/tetral/services/cleanup"
)

func TestCleanupSchemaBehindStopsBeforeWorkspaceScan(t *testing.T) {
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

	logCleanupClaimDue(logger, workspace.ID("default"), 2, 10, 25*time.Millisecond)

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
		"workspace.id":           "default",
		"duration.ms":            float64(25),
		"cleanup.jobs.claimed":   float64(2),
		"cleanup.claim.limit":    float64(10),
	}
	for key, value := range want {
		if fields[key] != value {
			t.Fatalf("field %s = %#v; want %#v in %#v", key, fields[key], value, fields)
		}
	}
	for _, forbidden := range []string{"session.id", "thread.id", "job.id", "cleanup.id", "error.message", "secret"} {
		if _, ok := fields[forbidden]; ok {
			t.Fatalf("success log included forbidden field %s: %#v", forbidden, fields)
		}
	}
}

func TestExportCleanupMetricsExportsSchedulerSeries(t *testing.T) {
	metrics := tetralcleanup.NewSchedulerMetrics()
	metrics.ObserveClaimDue(3, 40*time.Millisecond)
	exporter := &recordingMetricsExporter{}

	exportCleanupMetrics(context.Background(), nil, exporter, metrics, time.Second)

	if len(exporter.samples) != 3 {
		t.Fatalf("exported samples = %#v; want three scheduler series", exporter.samples)
	}
	for _, sample := range exporter.samples {
		if len(sample.Labels) != 0 {
			t.Fatalf("sample %s labels = %#v; want no workspace/session labels", sample.Name, sample.Labels)
		}
	}
}

func TestExportCleanupMetricsBoundsAndLogsExporterFailure(t *testing.T) {
	metrics := tetralcleanup.NewSchedulerMetrics()
	metrics.ObserveClaimDue(1, time.Millisecond)
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
