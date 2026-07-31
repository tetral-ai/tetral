package main

import (
	"bytes"
	"context"
	"errors"
	"os"
	"strings"
	"testing"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/storage"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	"github.com/tetral-ai/tetral/internal/workload"
	tetralsandbox "github.com/tetral-ai/tetral/services/sandbox"
)

func TestTetralSandboxSchemaBehindStopsBeforeQueueAndListeners(t *testing.T) {
	runtimeDB := storagetest.NewPostgreSQLDB(t)
	client := dbconnect.NewClientForTesting(runtimeDB)
	previousOpen, previousVerify, previousRun := openDatabase, verifySchema, runWorkload
	openDatabase = func(context.Context, string, string) (dbconnect.OpenResult, error) {
		return dbconnect.OpenResult{Client: client}, nil
	}
	verifySchema = func(context.Context, *dbconnect.Client) error {
		return &storage.SchemaMigrationError{Kind: storage.SchemaErrorBehind, Version: 1}
	}
	runWorkload = func(context.Context, workload.Config) error {
		t.Fatal("sandbox listener started after schema-behind failure")
		return nil
	}
	t.Cleanup(func() { openDatabase, verifySchema, runWorkload = previousOpen, previousVerify, previousRun })

	err := run(context.Background(), validSandboxSchemaEnv())
	var schemaErr *storage.SchemaMigrationError
	if !errors.As(err, &schemaErr) || schemaErr.Kind != storage.SchemaErrorBehind {
		t.Fatalf("run error = %v, want schema-behind", err)
	}
}

func validSandboxSchemaEnv() sandboxEnvMap {
	return sandboxEnvMap{
		tetralsandbox.EnvPostgresDSN:         "postgres://runtime@postgres/tetral",
		tetralsandbox.EnvSandboxDriver:       "daytona",
		tetralsandbox.EnvSandboxBaseImage:    "ghcr.io/tetral-ai/sandbox:0.1.0-alpha.test",
		tetralsandbox.EnvDaytonaAPIURL:       "https://daytona.example",
		tetralsandbox.EnvDaytonaAPIKey:       "test-key",
		tetralsandbox.EnvQueueGRPCAddress:    "queue:9090",
		tetralsandbox.EnvBlobEndpoint:        "https://blob.example",
		tetralsandbox.EnvBlobRegion:          "auto",
		tetralsandbox.EnvBlobBucket:          "bucket",
		tetralsandbox.EnvBlobAccessKey:       "access",
		tetralsandbox.EnvBlobSecretKey:       "secret",
		tetralsandbox.EnvR2AccountID:         "account",
		tetralsandbox.EnvR2ParentAPIToken:    "token",
		tetralsandbox.EnvR2ParentAccessKeyID: "parent",
		tetralsandbox.EnvGitProxyHost:        "git-proxy",
	}
}

func TestTetralSandboxCommandStartsAllSandboxOwnedQueueRunners(t *testing.T) {
	source, err := os.ReadFile("main.go")
	if err != nil {
		t.Fatalf("read main.go: %v", err)
	}
	required := []string{
		"RunWorkspaceConsumerLoop",
		"workspace.NewStore",
		"EnvironmentBuildJobRunner",
		"EnvironmentReadyFanoutJobRunner",
		"SandboxToolExecutionJobRunner",
		"SandboxActivationJobRunner",
		"SandboxMaterializationJobRunner",
		"SandboxQueueOverLimitReconciler",
		"RunSandboxQueueOverLimitLoop",
		"SessionPrepareJobRunner",
		"ResourcePrefixGCRunner",
		"StaleCreatingReconciler",
		"StartupCleanupReconciler",
		"MethodAuthorizer:  tetralsandbox.SandboxServiceMethodAuthorizer",
	}
	for _, symbol := range required {
		if !strings.Contains(string(source), symbol) {
			t.Fatalf("main.go does not start %s", symbol)
		}
	}
}

func TestTetralSandboxCommandStartupFailureLogUsesSharedFields(t *testing.T) {
	stderr, finish := captureStderr(t)
	err := run(context.Background(), sandboxEnvMap{})
	if err == nil {
		t.Fatal("run returned nil for config failure")
	}
	finish()
	output := stderr.String()
	for _, want := range []string{
		`"msg":"startup.failed"`,
		`"service.name":"sandbox"`,
		`"component":"sandbox"`,
		`"error.class":"config_error"`,
		tetralsandbox.EnvPostgresDSN,
	} {
		if !strings.Contains(output, want) {
			t.Fatalf("startup log missing %s: %s", want, output)
		}
	}
}

type sandboxEnvMap map[string]string

func (m sandboxEnvMap) Getenv(key string) string { return m[key] }

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
