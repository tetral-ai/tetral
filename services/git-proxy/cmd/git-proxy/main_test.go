package main

import (
	gitproxy "github.com/tetral-ai/tetral/services/git-proxy"

	"bytes"
	"context"
	"errors"
	"os"
	"strings"
	"testing"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/storage"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	"github.com/tetral-ai/tetral/internal/vault"
)

func TestGitProxySchemaBehindStopsBeforeEncryptorAndListeners(t *testing.T) {
	runtimeDB := storagetest.NewPostgreSQLDB(t)
	client := dbconnect.NewClientForTesting(runtimeDB)
	previousOpen, previousVerify, previousRun := openDatabase, verifySchema, runGitProxy
	openDatabase = func(context.Context, string, string) (dbconnect.OpenResult, error) {
		return dbconnect.OpenResult{Client: client}, nil
	}
	verifySchema = func(context.Context, *dbconnect.Client) error {
		return &storage.SchemaMigrationError{Kind: storage.SchemaErrorBehind, Version: 1}
	}
	runGitProxy = func(context.Context, gitproxy.Config, *dbconnect.Client, vault.CredentialEncryptor, gitproxy.RuntimeConfig) error {
		t.Fatal("git proxy started after schema-behind failure")
		return nil
	}
	t.Cleanup(func() { openDatabase, verifySchema, runGitProxy = previousOpen, previousVerify, previousRun })

	err := run(context.Background(), commandEnvMap{
		gitproxy.EnvDatabaseURL: "postgres://runtime@postgres/tetral",
		gitproxy.EnvVaultKey:    strings.Repeat("01", 32),
	})
	var schemaErr *storage.SchemaMigrationError
	if !errors.As(err, &schemaErr) || schemaErr.Kind != storage.SchemaErrorBehind {
		t.Fatalf("run error = %v, want schema-behind", err)
	}
}

func TestGitProxyCommandRejectsInvalidConfigBeforeDatabaseOpen(t *testing.T) {
	previousOpen := openDatabase
	openDatabase = func(context.Context, string, string) (dbconnect.OpenResult, error) {
		t.Fatal("openDatabase must not run after config rejection")
		return dbconnect.OpenResult{}, nil
	}
	t.Cleanup(func() { openDatabase = previousOpen })

	err := run(context.Background(), commandEnvMap{
		gitproxy.EnvDatabaseURL: "postgres://runtime@postgres/tetral",
		gitproxy.EnvVaultKey:    "short-secret-sentinel",
	})
	if err == nil || !strings.Contains(err.Error(), gitproxy.EnvVaultKey) {
		t.Fatalf("run err = %v; want safe vault-key config error", err)
	}
	if strings.Contains(err.Error(), "short-secret-sentinel") {
		t.Fatalf("run error leaked vault key material: %q", err.Error())
	}
}

func TestGitProxyCommandStartupFailureLogRedactsDependencyError(t *testing.T) {
	previousOpen := openDatabase
	openDatabase = func(context.Context, string, string) (dbconnect.OpenResult, error) {
		return dbconnect.OpenResult{}, errors.New("postgres://user:secret@db.internal/tetral raw bearer token")
	}
	t.Cleanup(func() { openDatabase = previousOpen })

	stderr, finish := captureStderr(t)
	err := run(context.Background(), commandEnvMap{
		gitproxy.EnvDatabaseURL: "postgres://runtime@postgres/tetral",
		gitproxy.EnvVaultKey:    "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
	})
	if err == nil {
		t.Fatal("run returned nil for dependency failure")
	}
	finish()
	output := stderr.String()
	for _, forbidden := range []string{"postgres://", "secret@db.internal", "raw bearer token"} {
		if strings.Contains(output, forbidden) {
			t.Fatalf("startup log leaked %q: %s", forbidden, output)
		}
	}
	for _, want := range []string{
		`"msg":"startup.failed"`,
		`"service.name":"git-proxy"`,
		`"component":"git-proxy"`,
		`"error.class":"startup_error"`,
		`"error.message_safe":"startup failed"`,
	} {
		if !strings.Contains(output, want) {
			t.Fatalf("startup log missing %s: %s", want, output)
		}
	}
	if strings.Contains(output, `"error.message"`) {
		t.Fatalf("dependency startup log must not carry legacy error.message: %s", output)
	}
}

type commandEnvMap map[string]string

func (e commandEnvMap) Getenv(key string) string { return e[key] }

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
