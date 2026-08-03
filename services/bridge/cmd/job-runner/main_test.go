package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"net"
	"os"
	"strconv"
	"strings"
	"testing"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/queue"
	"github.com/tetral-ai/tetral/internal/storage"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	agentruntimebridge "github.com/tetral-ai/tetral/services/bridge"
)

func TestJobRunnerOversizedRuntimeInboxStopsBeforeListener(t *testing.T) {
	runtimeDB, adminDB := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		workspaceID    = "ws_job_runner_inbox_preflight"
		runtimeInputID = "rin_job_runner_inbox_preflight"
	)
	seedJobRunnerRuntimeInbox(t, adminDB, workspaceID, runtimeInputID, queue.MaxRuntimeInputEventRefsPerJob+1)
	client := dbconnect.NewClientForTesting(runtimeDB)
	previousOpen, previousVerify, previousListen := openDatabase, verifySchema, listenTCP
	openDatabase = func(context.Context, string, string) (dbconnect.OpenResult, error) {
		return dbconnect.OpenResult{Client: client, RawDatabaseForExcludedStores: runtimeDB}, nil
	}
	verifySchema = func(context.Context, *dbconnect.Client) error { return nil }
	listenTCP = func(string, string) (net.Listener, error) {
		t.Fatal("job-runner listener started after oversized runtime inbox detection")
		return nil, nil
	}
	t.Cleanup(func() { openDatabase, verifySchema, listenTCP = previousOpen, previousVerify, previousListen })

	err := run(context.Background(), validJobRunnerSchemaEnv())
	var capacityErr *agentruntimebridge.RuntimeInboxEventRefsExceededError
	if !errors.As(err, &capacityErr) {
		t.Fatalf("run error = %T %v; want RuntimeInboxEventRefsExceededError", err, err)
	}
	if capacityErr.WorkspaceID != workspaceID || capacityErr.RuntimeInputID != runtimeInputID {
		t.Fatalf("startup error identity = workspace %q row %q; want %q/%q", capacityErr.WorkspaceID, capacityErr.RuntimeInputID, workspaceID, runtimeInputID)
	}
}

func TestJobRunnerSchemaBehindStopsBeforeListenerAndClients(t *testing.T) {
	runtimeDB := storagetest.NewPostgreSQLDB(t)
	client := dbconnect.NewClientForTesting(runtimeDB)
	previousOpen, previousVerify, previousListen := openDatabase, verifySchema, listenTCP
	openDatabase = func(context.Context, string, string) (dbconnect.OpenResult, error) {
		return dbconnect.OpenResult{Client: client}, nil
	}
	verifySchema = func(context.Context, *dbconnect.Client) error {
		return &storage.SchemaMigrationError{Kind: storage.SchemaErrorBehind, Version: 1}
	}
	listenTCP = func(string, string) (net.Listener, error) {
		t.Fatal("job-runner listener started after schema-behind failure")
		return nil, nil
	}
	t.Cleanup(func() { openDatabase, verifySchema, listenTCP = previousOpen, previousVerify, previousListen })

	err := run(context.Background(), validJobRunnerSchemaEnv())
	var schemaErr *storage.SchemaMigrationError
	if !errors.As(err, &schemaErr) || schemaErr.Kind != storage.SchemaErrorBehind {
		t.Fatalf("run error = %v, want schema-behind", err)
	}
}

func validJobRunnerSchemaEnv() jobRunnerEnvMap {
	return jobRunnerEnvMap{
		agentruntimebridge.EnvQueueGRPCAddress:                 "queue:9090",
		agentruntimebridge.EnvDatabaseURL:                      "postgres://runtime@postgres/tetral",
		agentruntimebridge.EnvKubernetesNamespace:              "tetral-system",
		agentruntimebridge.EnvAgentRuntimeLabelSelector:        "app=runtime",
		agentruntimebridge.EnvRuntimePodServiceTokenPath:       "/runtime-token",
		agentruntimebridge.EnvJobRunnerMCPConnectorGRPCAddress: "gateway:9091",
		agentruntimebridge.EnvJobRunnerGatewayTokenPath:        "/gateway-token",
	}
}

func seedJobRunnerRuntimeInbox(t *testing.T, db *sql.DB, workspaceID string, runtimeInputID string, eventCount int) {
	t.Helper()
	const (
		sessionID = "sesn_job_runner_inbox_preflight"
		threadID  = "thr_job_runner_inbox_preflight"
		now       = "2026-01-01T00:00:00Z"
	)
	agentID := "agent_" + sessionID
	agentVersionID := "agv_" + sessionID
	environmentID := "env_" + sessionID
	statements := []struct {
		query string
		args  []any
	}{
		{`INSERT INTO workspaces (id, type, name, created_at) VALUES ($1, 'workspace', $1, $2)`, []any{workspaceID, now}},
		{`INSERT INTO agents (workspace_id, id, name, version, created_at, updated_at) VALUES ($1, $2, $2, 1, $3, $3)`, []any{workspaceID, agentID, now}},
		{`INSERT INTO agent_versions (workspace_id, id, agent_id, version, config_json, config_hash, created_at) VALUES ($1, $2, $3, 1, '{"tools":[{"type":"tetral_agent_toolset","family":"claude"}]}', $4, $5)`, []any{workspaceID, agentVersionID, agentID, "hash_" + sessionID, now}},
		{`INSERT INTO environments (workspace_id, id, name, config_json, created_at, updated_at) VALUES ($1, $2, $2, '{}', $3, $3)`, []any{workspaceID, environmentID, now}},
		{`INSERT INTO sessions (workspace_id, id, main_thread_id, type, status, lifecycle_state, agent_id, agent_version, environment_id, installed_tools_json, created_at, updated_at) VALUES ($1, $2, $3, 'session', 'idle', 'active', $4, 1, $5, '{"tools":[{"type":"tetral_agent_toolset","family":"claude"}]}', $6, $6)`, []any{workspaceID, sessionID, threadID, agentID, environmentID, now}},
		{`INSERT INTO session_threads (workspace_id, id, session_id, role, visibility, status, created_at, last_active_at, updated_at) VALUES ($1, $2, $3, 'main', 'public', 'idle', $4, $4, $4)`, []any{workspaceID, threadID, sessionID, now}},
	}
	for _, statement := range statements {
		if _, err := db.ExecContext(context.Background(), statement.query, statement.args...); err != nil {
			t.Fatalf("seed job-runner runtime inbox dependency: %v", err)
		}
	}
	eventIDs := make([]string, eventCount)
	for index := range eventIDs {
		eventIDs[index] = "evt_job_runner_inbox_preflight_" + strconv.Itoa(index+1)
	}
	eventIDsJSON, err := json.Marshal(eventIDs)
	if err != nil {
		t.Fatalf("marshal oversized event ids: %v", err)
	}
	if _, err := db.ExecContext(context.Background(),
		`INSERT INTO session_runtime_inbox (
			workspace_id, session_id, session_thread_id, runtime_input_id, input_kind,
			event_ids_json, sequence_from, sequence_to, status, binding_id, binding_generation,
			target_pod_uid, created_at, updated_at
		) VALUES ($1, $2, $3, $4, 'messages', $5, 1, $6, 'accepted', 'bind_job_runner_inbox_preflight', 1, 'pod_job_runner_inbox_preflight', $7, $7)`,
		workspaceID, sessionID, threadID, runtimeInputID, string(eventIDsJSON), eventCount, now,
	); err != nil {
		t.Fatalf("seed oversized runtime inbox: %v", err)
	}
}

func TestJobRunnerCommandStartupFailureLogUsesSharedFields(t *testing.T) {
	stderr, finish := captureStderr(t)
	err := run(context.Background(), jobRunnerEnvMap{})
	if err == nil {
		t.Fatal("run returned nil for config failure")
	}
	finish()
	output := stderr.String()
	for _, want := range []string{
		`"msg":"startup.failed"`,
		`"service.name":"bridge-job-runner"`,
		`"component":"bridge-job-runner"`,
		`"error.class":"config_error"`,
		agentruntimebridge.EnvQueueGRPCAddress,
	} {
		if !strings.Contains(output, want) {
			t.Fatalf("startup log missing %s: %s", want, output)
		}
	}
}

type jobRunnerEnvMap map[string]string

func (m jobRunnerEnvMap) Getenv(key string) string { return m[key] }

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
