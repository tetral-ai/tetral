package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"slices"
	"strings"
	"testing"

	"github.com/tetral-ai/tetral/internal/blob"
	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/kubernetes/kubernetest"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	jobrunner "github.com/tetral-ai/tetral/services/job-runner"
)

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

// The dependencies phase lasts until every dependency Job Runner opens at
// start is connected, so a Kubernetes client that cannot be built, and a blob
// store that cannot open once the cluster is watched, each end the cold start
// inside that phase.
func TestJobRunnerDependencyFailuresEndTheColdStartInItsDependenciesPhase(t *testing.T) {
	for _, row := range []struct {
		name string
		// failure is in the error run returns, so the start failed at the
		// dependency the row names.
		failure string
		prepare func(t *testing.T)
	}{
		{"a Kubernetes client that cannot be built", "KUBERNETES_SERVICE_HOST", func(t *testing.T) {
			t.Setenv("KUBERNETES_SERVICE_HOST", "")
		}},
		{"a blob store that cannot open", "blob store unavailable", func(t *testing.T) {
			for name, value := range map[string]string{blob.EnvRegion: "us-east-1", blob.EnvBucket: "attachments", blob.EnvAccessKey: "access", blob.EnvSecretKey: "secret"} {
				t.Setenv(name, value)
			}
			previousClient, previousBlob := newKubernetesClient, openBlobStore
			newKubernetesClient = func() (kubernetesClient, error) { return kubernetest.EmptyCluster{}, nil }
			openBlobStore = func(context.Context, *blob.Config) (*blob.S3BlobStore, error) {
				return nil, errors.New("blob store unavailable")
			}
			t.Cleanup(func() { newKubernetesClient, openBlobStore = previousClient, previousBlob })
		}},
	} {
		t.Run(row.name, func(t *testing.T) {
			// run closes the client it opened, so each row has its own.
			client := dbconnect.NewClientForTesting(storagetest.NewPostgreSQLDB(t))
			previousOpen, previousVerify, previousListen := openDatabase, verifySchema, listenTCP
			openDatabase = func(context.Context, string, string) (dbconnect.OpenResult, error) {
				return dbconnect.OpenResult{Client: client}, nil
			}
			verifySchema = func(context.Context, *dbconnect.Client) error { return nil }
			listenTCP = func(string, string) (net.Listener, error) { return net.Listen("tcp", "127.0.0.1:0") }
			t.Cleanup(func() { openDatabase, verifySchema, listenTCP = previousOpen, previousVerify, previousListen })
			row.prepare(t)

			stderr, finish := captureStderr(t)
			err := run(context.Background(), validJobRunnerSchemaEnv())
			finish()
			if err == nil || !strings.Contains(err.Error(), row.failure) {
				t.Fatalf("run error = %v; want one naming %q", err, row.failure)
			}
			var started []string
			var outcome any
			for _, line := range strings.Split(strings.TrimSpace(stderr.String()), "\n") {
				var record map[string]any
				if json.Unmarshal([]byte(line), &record) != nil || record["cold_start.kind"] != "service_pod" {
					continue
				}
				switch record["event"] {
				case "cold_start.phase_started":
					started = append(started, fmt.Sprint(record["cold_start.phase"]))
				case "cold_start.completed":
					outcome = record["outcome"]
				}
			}
			if !slices.Equal(started, []string{"config", "dependencies"}) || outcome == nil || outcome == "ok" {
				t.Fatalf("cold start phases started %v, outcome %v; want a failure in the dependencies phase:\n%s", started, outcome, stderr.String())
			}
		})
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
