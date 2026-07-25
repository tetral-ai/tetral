package agentruntimebridge

import (
	"context"
	"database/sql"
	"reflect"
	"testing"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/queue"
	"github.com/tetral-ai/tetral/internal/sandbox"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	"github.com/tetral-ai/tetral/internal/workspace"
	queuev1 "github.com/tetral-ai/tetral/services/queue/gen/tetral/queue/v1"
	tetralsandbox "github.com/tetral-ai/tetral/services/sandbox"
)

func TestJobRunnerHandlerTerminalDeleteReleaseDeadLettersAndMarksLeakOnce(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	ctx := context.Background()
	const (
		sessionID       = "sesn_handler_terminal_delete"
		preparationID   = "prep_handler_terminal_delete"
		deleteCleanupID = "delcln_handler_terminal_delete"
		jobID           = "qjob_handler_terminal_delete"
	)
	seedBridgeAPISession(t, admin, "default", sessionID, "thr_handler_terminal_delete")
	seedBridgeAPIPreparationReady(t, admin, "default", sessionID, preparationID)
	seedBridgeAPIActiveSandbox(t, admin, "default", sessionID, "2026-01-01T00:00:00Z")
	if _, err := admin.ExecContext(ctx, `UPDATE sessions SET lifecycle_state='deleted', delete_cleanup_id=$2 WHERE id=$1`, sessionID, deleteCleanupID); err != nil {
		t.Fatalf("stamp tombstone: %v", err)
	}
	prefix := "workspaces/default/sessions/" + sessionID + "/"
	if _, err := admin.ExecContext(ctx,
		`INSERT INTO session_resource_prefix_gc (workspace_id, session_id, prefix, status, created_at, updated_at)
		 VALUES ('default',$1,$2,'pending','2026-01-01T00:00:00Z','2026-01-01T00:00:00Z')`, sessionID, prefix); err != nil {
		t.Fatalf("seed GC marker: %v", err)
	}

	client := dbconnect.NewClientForTesting(runtime)
	sandboxStore := sandbox.NewPostgreSQLStore(client)
	provider := &terminalDeleteReleaseProvider{err: &sandbox.ProviderError{
		Provider: "tetral", Stage: sandbox.StageReleaseSandbox, Kind: sandbox.ProviderErrorAuthFailed,
		Retryable: false, SafeMessage: "provider authentication failed",
	}}
	handler := tetralsandbox.NewReleaseHandler(client, sandbox.NewService(sandboxStore, provider, sandbox.WithProviderName("tetral")), sandboxStore)
	bridgeStore := NewPostgreSQLRuntimeDeliveryStore(client, 9090)
	bridgeStore.SandboxReleaser = releaseHandlerBridgeClient{handler: handler}
	queueClient := &recordingQueueClient{leased: []*queuev1.QueueJob{{
		Id: jobID, WorkspaceId: "default", Kind: queue.KindSessionDeleteCleanup, LeaseToken: "lease_handler_terminal_delete",
		PayloadJson: `{"workspace_id":"default","session_id":"` + sessionID + `","delete_cleanup_id":"` + deleteCleanupID + `"}`,
	}}}
	runner := &JobRunner{
		Queue: queueClient, Workspaces: staticWorkspaceLister{workspace.DefaultID},
		Deliverer: RuntimePodDirectDeliverer{Store: bridgeStore},
	}

	if err := runner.RunOnce(ctx); err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if transitions := queueClient.transitionSnapshot(); !reflect.DeepEqual(transitions, []string{"dead:" + jobID + ":sandbox_release_failed"}) {
		t.Fatalf("queue transitions = %v; want one terminal dead-letter", transitions)
	}
	if provider.releaseCalls != 1 {
		t.Fatalf("provider release calls = %d; want 1", provider.releaseCalls)
	}

	var sandboxStatus, cleanupStatus string
	if err := admin.QueryRowContext(ctx, `SELECT status,cleanup_status FROM sandboxes WHERE session_id=$1`, sessionID).Scan(&sandboxStatus, &cleanupStatus); err != nil {
		t.Fatalf("read sandbox state: %v", err)
	}
	if sandboxStatus != "releasing" || cleanupStatus != "none" {
		t.Fatalf("sandbox state = %q/%q; want releasing/none without cleanup success", sandboxStatus, cleanupStatus)
	}
	var markerCount int
	var markerStatus string
	var markerError sql.NullString
	if err := admin.QueryRowContext(ctx,
		`SELECT count(*), min(status), min(last_error_kind)
		   FROM session_resource_prefix_gc WHERE workspace_id='default' AND session_id=$1`, sessionID,
	).Scan(&markerCount, &markerStatus, &markerError); err != nil {
		t.Fatalf("read GC leak marker: %v", err)
	}
	if markerCount != 1 || markerStatus != "pending" || !markerError.Valid || markerError.String != "sandbox_release_failed" {
		t.Fatalf("GC marker = count %d status %q error %v; want 1/pending/sandbox_release_failed", markerCount, markerStatus, markerError)
	}
	var idempotencyCount int
	var idempotencyStatus, responseStatus string
	if err := admin.QueryRowContext(ctx,
		`SELECT count(*), min(status), min(response_status)
		   FROM sandbox_release_idempotency_keys WHERE workspace_id='default' AND session_id=$1`, sessionID,
	).Scan(&idempotencyCount, &idempotencyStatus, &responseStatus); err != nil {
		t.Fatalf("read release idempotency: %v", err)
	}
	if idempotencyCount != 1 || idempotencyStatus != "completed" || responseStatus != "failed" {
		t.Fatalf("release idempotency = count %d status %q response %q; want 1/completed/failed", idempotencyCount, idempotencyStatus, responseStatus)
	}

	replay, err := handler.ReleaseSandbox(ctx, tetralsandbox.ReleaseSandboxRequest{
		WorkspaceID: "default", SessionID: sessionID, SandboxID: "sandbox_" + sessionID,
		PreparationAttemptID: preparationID, DeleteCleanupID: deleteCleanupID, Reason: "delete",
		IdempotencyKey: queue.FormatSessionDeleteCleanupDedupeKey(workspace.DefaultID, sessionID, deleteCleanupID),
	})
	if err != nil || replay.Status != tetralsandbox.ReleaseSandboxStatusFailed || provider.releaseCalls != 1 {
		t.Fatalf("handler replay = %#v, %v provider calls=%d; want persisted failed and one call", replay, err, provider.releaseCalls)
	}
}

type releaseHandlerBridgeClient struct {
	handler *tetralsandbox.ReleaseHandler
}

func (c releaseHandlerBridgeClient) ReleaseSandbox(ctx context.Context, request SandboxReleaseRequest) (SandboxReleaseResult, error) {
	result, err := c.handler.ReleaseSandbox(ctx, tetralsandbox.ReleaseSandboxRequest{
		WorkspaceID: request.WorkspaceID, SessionID: request.SessionID, SandboxID: request.SandboxID,
		BindingID: request.BindingID, BindingGeneration: request.BindingGeneration,
		PreparationAttemptID: request.PreparationAttemptID, DeleteCleanupID: request.DeleteCleanupID,
		Reason: request.Reason, IdempotencyKey: request.IdempotencyKey,
	})
	return SandboxReleaseResult{Status: SandboxReleaseStatus(result.Status), SandboxStatus: result.SandboxStatus}, err
}

type terminalDeleteReleaseProvider struct {
	err          error
	releaseCalls int
}

func (p *terminalDeleteReleaseProvider) CreateSandbox(context.Context, sandbox.CreateSandboxRequest) (sandbox.ProviderHandle, error) {
	return sandbox.ProviderHandle{Provider: "tetral", SandboxID: "provider_terminal_delete"}, nil
}

func (p *terminalDeleteReleaseProvider) StartSandbox(context.Context, sandbox.ProviderHandle) error {
	return nil
}

func (p *terminalDeleteReleaseProvider) CheckBaseTemplateHealth(context.Context, sandbox.ProviderHandle) error {
	return nil
}

func (p *terminalDeleteReleaseProvider) ApplyNetworkPolicy(context.Context, sandbox.ProviderHandle, sandbox.NetworkSetup) error {
	return nil
}

func (p *terminalDeleteReleaseProvider) PrepareBaseDirectories(context.Context, sandbox.ProviderHandle) error {
	return nil
}

func (p *terminalDeleteReleaseProvider) GetStatus(context.Context, sandbox.ProviderHandle) (sandbox.ProviderStatus, error) {
	return sandbox.ProviderStatus{Availability: sandbox.ProviderAvailable, SandboxStatus: sandbox.StatusReleasing}, nil
}

func (p *terminalDeleteReleaseProvider) ReleaseSandbox(context.Context, sandbox.ProviderHandle, sandbox.ReleaseReason) error {
	p.releaseCalls++
	return p.err
}

var _ sandbox.LifecycleProvider = (*terminalDeleteReleaseProvider)(nil)
var _ SandboxReleaseClient = releaseHandlerBridgeClient{}
