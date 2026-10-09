package main

import (
	"bytes"
	"context"
	"errors"
	"net"
	"os"
	"strings"
	"testing"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/storage"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	jobrunner "github.com/tetral-ai/tetral/services/job-runner"
)

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
		jobrunner.EnvQueueGRPCAddress:                 "queue:9090",
		jobrunner.EnvDatabaseURL:                      "postgres://runtime@postgres/tetral",
		jobrunner.EnvKubernetesNamespace:              "tetral-system",
		jobrunner.EnvAgentRuntimeLabelSelector:        "app=runtime",
		jobrunner.EnvRuntimePodServiceTokenPath:       "/runtime-token",
		jobrunner.EnvJobRunnerMCPConnectorGRPCAddress: "gateway:9091",
		jobrunner.EnvJobRunnerGatewayTokenPath:        "/gateway-token",
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
		`"service.name":"job-runner"`,
		`"component":"job-runner"`,
		`"startup.cause":"configuration"`,
		`"error.class":"config_error"`,
		jobrunner.EnvQueueGRPCAddress,
	} {
		if !strings.Contains(output, want) {
			t.Fatalf("startup log missing %s: %s", want, output)
		}
	}
}

func TestJobRunnerSingleDatabaseConnectionStopsBeforeDatabaseOpen(t *testing.T) {
	previousOpen := openDatabase
	openDatabase = func(context.Context, string, string) (dbconnect.OpenResult, error) {
		t.Fatal("database opened after single-connection configuration was rejected")
		return dbconnect.OpenResult{}, nil
	}
	t.Cleanup(func() { openDatabase = previousOpen })
	env := validJobRunnerSchemaEnv()
	env[dbconnect.EnvDBMaxOpenConns] = "1"

	err := run(context.Background(), env)
	if err == nil || !strings.Contains(err.Error(), dbconnect.EnvDBMaxOpenConns+" must be at least 2") {
		t.Fatalf("run error = %v; want single-connection startup rejection", err)
	}
}

func TestJobRunnerDatabaseOpenFailureUsesDependencyReadinessCause(t *testing.T) {
	previousOpen := openDatabase
	openDatabase = func(context.Context, string, string) (dbconnect.OpenResult, error) {
		return dbconnect.OpenResult{}, errors.New("database unavailable")
	}
	t.Cleanup(func() { openDatabase = previousOpen })

	stderr, finish := captureStderr(t)
	err := run(context.Background(), validJobRunnerSchemaEnv())
	if err == nil {
		t.Fatal("run returned nil for database open failure")
	}
	finish()
	output := stderr.String()
	for _, want := range []string{
		`"msg":"startup.failed"`,
		`"startup.cause":"dependency_readiness"`,
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
