package integration

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/auth"
	"github.com/tetral-ai/tetral/internal/blob"
	"github.com/tetral-ai/tetral/internal/dbconnect"
	internaleventstream "github.com/tetral-ai/tetral/internal/eventstream"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	"github.com/tetral-ai/tetral/internal/workspace"
	tetralapi "github.com/tetral-ai/tetral/services/api"
	tetralauth "github.com/tetral-ai/tetral/services/auth"
	eventstream "github.com/tetral-ai/tetral/services/event-stream"
)

// Each real service factory authenticates as its installer-configured role.
// The edge stands in for the public edge: Auth signs the principal, then
// API/Event Stream validate it. Object storage is the actual isolated MinIO.
func startContentPublicEdge(t *testing.T, pools *storagetest.WorkloadDB, objects blob.BlobStore) (string, string) {
	return startContentPublicEdgeWithEvents(t, pools, objects, func(reader *internaleventstream.PostgreSQLReader, verifier *auth.InternalPrincipalVerifier, _ string) http.Handler {
		idle := eventstream.NewIdleCoalescer(reader, time.Millisecond, nil)
		t.Cleanup(idle.Close)
		return eventstream.NewRouter(reader, verifier, eventstream.WithIdleCoalescer(idle), eventstream.WithStreamCompletedCheckLimit(1))
	})
}

func startContentPublicEdgeWithEvents(t *testing.T, pools *storagetest.WorkloadDB, objects blob.BlobStore, eventsFactory func(*internaleventstream.PostgreSQLReader, *auth.InternalPrincipalVerifier, string) http.Handler) (string, string) {
	t.Helper()
	privateKey, err := auth.GenerateEd25519PrivateKeyBase64()
	if err != nil {
		t.Fatal(err)
	}
	signer, err := auth.NewInternalPrincipalSignerFromBase64(privateKey)
	if err != nil {
		t.Fatal(err)
	}
	verifier, err := auth.NewInternalPrincipalVerifierFromBase64(signer.PublicKeyBase64())
	if err != nil {
		t.Fatal(err)
	}
	bootstrapKey := strings.Repeat("b", auth.MinBootstrapKeyBytes)
	ctx := context.Background()
	authPool := pools.OpenWorkload(t, "auth", nil)
	authRouter, err := tetralauth.BuildRouter(ctx, tetralauth.RouterBuildConfig{RawDatabase: authPool, Config: tetralauth.Config{
		BootstrapAPIKey: bootstrapKey, BootstrapWorkspaceID: workspace.DefaultID, InternalPrincipalPrivateKeyB64: privateKey, InternalPrincipalTTL: time.Minute,
	}})
	if err != nil {
		t.Fatal(err)
	}
	authServer := httptest.NewServer(authRouter)
	t.Cleanup(authServer.Close)
	apiPool := pools.OpenWorkload(t, "api", nil)
	dataDirectory := t.TempDir()
	if err := os.Chmod(dataDirectory, 0700); err != nil {
		t.Fatal(err)
	}
	apiRouter, err := tetralapi.BuildRouter(ctx, tetralapi.RouterConfig{RuntimeClient: dbconnect.NewClientForTesting(apiPool), RawDatabase: apiPool, VaultKey: publicEdgeVaultKey, DataDir: dataDirectory, Env: publicEdgeEnv{"TETRAL_DEFAULT_ENVIRONMENT_ARTIFACT_REF": "artifact_content"}, BlobStore: objects, PrincipalVerifier: verifier})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := apiRouter.(io.Closer).Close(); err != nil {
			t.Error("content API owner failed to close")
		}
	})
	apiServer := httptest.NewServer(apiRouter)
	t.Cleanup(apiServer.Close)
	reader := internaleventstream.NewPostgreSQLReader(dbconnect.NewClientForTesting(pools.OpenWorkload(t, "event_stream", nil)), internaleventstream.WithPageTokenSecret([]byte(publicEdgeVaultKey)))
	events := httptest.NewServer(eventsFactory(reader, verifier, signer.PublicKeyBase64()))
	t.Cleanup(events.Close)
	edge, err := newPublicEdge(authServer.URL, apiServer.URL, events.URL, startEdgeAuthorization(t, authPool, signer, time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(edge)
	t.Cleanup(server.Close)
	return server.URL, mintPublicEdgeAPIKey(t, server.URL, bootstrapKey)
}
