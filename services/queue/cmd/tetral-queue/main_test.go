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
	tetralqueue "github.com/tetral-ai/tetral/services/queue"
)

func TestTetralQueueSchemaBehindStopsBeforeStoreAndListener(t *testing.T) {
	runtimeDB := storagetest.NewPostgreSQLDB(t)
	client := dbconnect.NewClientForTesting(runtimeDB)
	previousOpen, previousVerify, previousRun := openDatabase, verifySchema, runQueueService
	openDatabase = func(context.Context) (dbconnect.OpenResult, error) { return dbconnect.OpenResult{Client: client}, nil }
	verifySchema = func(context.Context, *dbconnect.Client) error {
		return &storage.SchemaMigrationError{Kind: storage.SchemaErrorBehind, Version: 1}
	}
	runQueueService = func(context.Context, tetralqueue.Config, tetralqueue.Store, tetralqueue.RuntimeConfig) error {
		t.Fatal("queue service started after schema-behind failure")
		return nil
	}
	t.Cleanup(func() { openDatabase, verifySchema, runQueueService = previousOpen, previousVerify, previousRun })

	err := run(context.Background(), queueEnvMap{})
	var schemaErr *storage.SchemaMigrationError
	if !errors.As(err, &schemaErr) || schemaErr.Kind != storage.SchemaErrorBehind {
		t.Fatalf("run error = %v, want schema-behind", err)
	}
}

func TestTetralQueueCommandStartupFailureLogUsesSharedFields(t *testing.T) {
	stderr, finish := captureStderr(t)
	err := run(context.Background(), queueEnvMap{
		tetralqueue.EnvLeaseReclaimLimit: "0",
	})
	if err == nil {
		t.Fatal("run returned nil for config failure")
	}
	finish()
	output := stderr.String()
	for _, want := range []string{
		`"msg":"startup.failed"`,
		`"service.name":"queue"`,
		`"component":"queue"`,
		`"error.class":"config_error"`,
		tetralqueue.EnvLeaseReclaimLimit,
	} {
		if !strings.Contains(output, want) {
			t.Fatalf("startup log missing %s: %s", want, output)
		}
	}
}

type queueEnvMap map[string]string

func (m queueEnvMap) Getenv(key string) string { return m[key] }

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
