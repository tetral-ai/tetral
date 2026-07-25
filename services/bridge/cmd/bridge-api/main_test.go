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
	agentruntimebridge "github.com/tetral-ai/tetral/services/bridge"
)

func TestBridgeAPISchemaBehindStopsBeforeStoreAndListeners(t *testing.T) {
	runtimeDB := storagetest.NewPostgreSQLDB(t)
	client := dbconnect.NewClientForTesting(runtimeDB)
	previousOpen, previousVerify := openDatabase, verifySchema
	openDatabase = func(context.Context, string, string) (dbconnect.OpenResult, error) {
		return dbconnect.OpenResult{Client: client}, nil
	}
	verifySchema = func(context.Context, *dbconnect.Client) error {
		return &storage.SchemaMigrationError{Kind: storage.SchemaErrorBehind, Version: 1}
	}
	t.Cleanup(func() { openDatabase, verifySchema = previousOpen, previousVerify })

	err := run(context.Background(), bridgeEnvMap{agentruntimebridge.EnvDatabaseURL: "postgres://runtime@postgres/tetral"})
	var schemaErr *storage.SchemaMigrationError
	if !errors.As(err, &schemaErr) || schemaErr.Kind != storage.SchemaErrorBehind {
		t.Fatalf("run error = %v, want schema-behind", err)
	}
}

func TestBridgeAPICommandStartupFailureLogRedactsDependencyError(t *testing.T) {
	previousOpen := openDatabase
	openDatabase = func(context.Context, string, string) (dbconnect.OpenResult, error) {
		return dbconnect.OpenResult{}, errors.New("postgres://user:secret@db.internal/tetral provider payload")
	}
	t.Cleanup(func() { openDatabase = previousOpen })

	stderr, finish := captureStderr(t)
	err := run(context.Background(), bridgeEnvMap{
		agentruntimebridge.EnvDatabaseURL: "postgres://runtime@postgres/tetral",
	})
	if err == nil {
		t.Fatal("run returned nil for dependency failure")
	}
	finish()
	output := stderr.String()
	for _, forbidden := range []string{"postgres://", "secret@db.internal", "provider payload"} {
		if strings.Contains(output, forbidden) {
			t.Fatalf("startup log leaked %q: %s", forbidden, output)
		}
	}
	for _, want := range []string{
		`"msg":"startup.failed"`,
		`"service.name":"bridge"`,
		`"component":"bridge"`,
		`"error.class":"startup_error"`,
		`"error.message_safe":"startup failed"`,
	} {
		if !strings.Contains(output, want) {
			t.Fatalf("startup log missing %s: %s", want, output)
		}
	}
}

type bridgeEnvMap map[string]string

func (m bridgeEnvMap) Getenv(key string) string { return m[key] }

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
