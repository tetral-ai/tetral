package jobrunner

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/queue"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
)

// Capture cleanup runs in Job Runner after the independent Sandbox release.
// A general fixture role cannot establish that this serving boundary can lock,
// schedule, and retire capture custody without gaining capture creation rights.
func TestPostgreSQLInstalledJobRunnerDeletesOutputCaptureCustody(t *testing.T) {
	for _, captureState := range []string{"adopted", "staged"} {
		t.Run(captureState, func(t *testing.T) {
			_, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
			serving := storagetest.OpenWorkloadDB(t, admin, "job_runner")
			client := dbconnect.NewClientForTesting(serving.DB)
			store := NewPostgreSQLRuntimeDeliveryStore(client, 9090)
			now := time.Date(2026, 10, 5, 12, 0, 0, 0, time.UTC)
			store.Clock = func() time.Time { return now }
			const sessionID = "sesn_capture_role"
			seedRunnerCaptureCleanup(t, admin, "default", sessionID, captureState, now)
			seedRunnerCaptureCleanup(t, admin, "foreign", "sesn_capture_foreign", captureState, now)
			seedRunnerCaptureCleanup(t, admin, "default", "sesn_capture_other", captureState, now)
			job := RuntimeJob{Kind: queue.KindSessionDeleteCleanup, WorkspaceID: "default", SessionID: sessionID,
				DeleteCleanupID: "delcln_capture_role", AttemptCount: 5, MaxAttempts: 5}
			snapshot := func(workspaceID, id string) string {
				t.Helper()
				var value string
				if err := admin.QueryRowContext(context.Background(), `SELECT jsonb_build_object(
     'captures',(SELECT jsonb_agg(to_jsonb(c) ORDER BY finish_idle_write_id) FROM sandbox_output_capture_operations c WHERE workspace_id=$1 AND session_id=$2),
     'blobs',(SELECT jsonb_agg(to_jsonb(b) ORDER BY source_path) FROM sandbox_output_capture_blobs b WHERE workspace_id=$1 AND session_id=$2),
     'bindings',(SELECT jsonb_agg(to_jsonb(b)) FROM session_sandbox_bindings b WHERE workspace_id=$1 AND session_id=$2),
     'releases',(SELECT jsonb_agg(to_jsonb(r) ORDER BY operation_id) FROM sandbox_lifecycle_operations r WHERE workspace_id=$1 AND session_id=$2),
     'queue',(SELECT jsonb_agg(to_jsonb(q) ORDER BY id) FROM queue_jobs q WHERE workspace_id=$1 AND payload_json::jsonb->>'session_id'=$2)
     )::text`, workspaceID, id).Scan(&value); err != nil {
					t.Fatal(err)
				}
				return value
			}
			foreignBefore, otherBefore := snapshot("foreign", "sesn_capture_foreign"), snapshot("default", "sesn_capture_other")
			initial := snapshot("default", sessionID)
			result, err := store.FinalizeRuntimeCleanup(context.Background(), job)
			// The unchanged-role baseline must fail here with the actual 42501; no
			// test-side grant or expected-error branch can turn that regression green.
			if err != nil {
				if snapshot("default", sessionID) != initial {
					t.Fatal("failed finalization changed capture custody")
				}
				t.Fatalf("installed job_runner FinalizeRuntimeCleanup: %v", err)
			}
			if captureState == "staged" {
				if result.Status != RuntimeDeliveryRejected || !result.Retryable || result.ErrorKind != "sandbox_output_capture_cleanup_pending" {
					t.Fatalf("staged capture finalization=%+v; want durable cleanup pending", result)
				}
				var state, status, payload string
				var generation int64
				if err := admin.QueryRow(`SELECT c.state,c.cleanup_generation,q.status,q.payload_json FROM sandbox_output_capture_operations c
     JOIN queue_jobs q ON q.workspace_id=c.workspace_id AND q.kind=$3 AND q.payload_json::jsonb->>'session_id'=c.session_id
     WHERE c.workspace_id='default' AND c.session_id=$1 AND c.finish_idle_write_id=$2`, sessionID, "rwrite_"+sessionID, queue.KindSandboxOutputCaptureCleanup).Scan(&state, &generation, &status, &payload); err != nil {
					t.Fatal(err)
				}
				if state != "cleanup_pending" || generation != 1 || status != queue.StatusPending || !strings.Contains(payload, `"finish_idle_write_id":"rwrite_`+sessionID+`"`) {
					t.Fatalf("capture cleanup custody=%s/%d/%s/%s", state, generation, status, payload)
				}
				before := snapshot("default", sessionID)
				again, replayErr := store.FinalizeRuntimeCleanup(context.Background(), job)
				if replayErr != nil || again.ErrorKind != "sandbox_output_capture_cleanup_pending" || snapshot("default", sessionID) != before {
					t.Fatalf("pending cleanup replay changed original custody: %+v/%v", again, replayErr)
				}
				// Independent fixture completion supplies the Sandbox-owned terminal
				// fact. Runner never calls the provider or treats open Queue work as done.
				if _, err := admin.Exec(`UPDATE sandbox_output_capture_operations SET state='cleaned',cleaned_at=$2,updated_at=$2 WHERE workspace_id='default' AND session_id=$1`, sessionID, now); err != nil {
					t.Fatal(err)
				}
				held, heldErr := store.FinalizeRuntimeCleanup(context.Background(), job)
				if heldErr != nil || held.ErrorKind != "sandbox_output_capture_cleanup_pending" {
					t.Fatalf("open cleanup Queue custody bypassed: %+v/%v", held, heldErr)
				}
				if _, err := admin.Exec(`UPDATE queue_jobs SET status='acknowledged',acknowledged_at=$2,updated_at=$2 WHERE workspace_id='default' AND kind=$3 AND payload_json::jsonb->>'session_id'=$1`, sessionID, now, queue.KindSandboxOutputCaptureCleanup); err != nil {
					t.Fatal(err)
				}
				result, err = store.FinalizeRuntimeCleanup(context.Background(), job)
			}
			if err != nil || result.Status != RuntimeDeliveryAccepted {
				t.Fatalf("terminal capture finalization=%+v/%v", result, err)
			}
			var captures, blobs, bindings, releases int
			if err := admin.QueryRow(`SELECT
    (SELECT count(*) FROM sandbox_output_capture_operations WHERE workspace_id='default' AND session_id=$1),
    (SELECT count(*) FROM sandbox_output_capture_blobs WHERE workspace_id='default' AND session_id=$1),
    (SELECT count(*) FROM session_sandbox_bindings WHERE workspace_id='default' AND session_id=$1),
    (SELECT count(*) FROM sandbox_lifecycle_operations WHERE workspace_id='default' AND session_id=$1)`, sessionID).Scan(&captures, &blobs, &bindings, &releases); err != nil {
				t.Fatal(err)
			}
			if captures != 0 || blobs != 0 || bindings != 0 || releases != 0 {
				t.Fatalf("private custody remained capture=%d blob=%d binding=%d release=%d", captures, blobs, bindings, releases)
			}
			if snapshot("foreign", "sesn_capture_foreign") != foreignBefore || snapshot("default", "sesn_capture_other") != otherBefore {
				t.Fatal("cleanup changed another workspace or Session")
			}

			// A false predicate still checks INSERT authority without adding rows.
			for _, table := range []string{"sandbox_output_capture_operations", "sandbox_output_capture_blobs"} {
				err := client.WithWorkspaceTx(context.Background(), "default", "test.capture_insert_denied", func(tx *dbconnect.Tx) error {
					_, err := tx.Exec(context.Background(), "INSERT INTO "+table+" SELECT * FROM "+table+" WHERE false")
					return err
				})
				var pgErr *pgconn.PgError
				if !errors.As(err, &pgErr) || pgErr.Code != "42501" {
					t.Fatalf("job_runner capture INSERT authority on %s=%v", table, err)
				}
			}
			if err := client.WithWorkspaceTx(context.Background(), "default", "test.capture_foreign_denied", func(tx *dbconnect.Tx) error {
				var visible int
				if err := tx.QueryRow(context.Background(), `SELECT count(*) FROM sandbox_output_capture_operations WHERE workspace_id='foreign'`).Scan(&visible); err != nil {
					return err
				}
				if visible != 0 {
					t.Fatal("serving role disclosed foreign capture")
				}
				for _, table := range []string{"sandbox_output_capture_blobs", "sandbox_output_capture_operations"} {
					res, err := tx.Exec(context.Background(), "DELETE FROM "+table+" WHERE workspace_id='foreign'")
					if err != nil {
						return err
					}
					n, err := res.RowsAffected()
					if err != nil {
						return err
					}
					if n != 0 {
						t.Fatal("serving role deleted foreign capture")
					}
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			if snapshot("foreign", "sesn_capture_foreign") != foreignBefore {
				t.Fatal("foreign capability probe changed custody")
			}

			// Separate required SQL primitives: row lock/scheduling, terminal receipt
			// deletion, and scoped blob deletion. Failure must roll back all statements.
			before := snapshot("default", "sesn_capture_other")
			privileges := []string{"SELECT", "UPDATE"}
			if captureState == "adopted" {
				privileges = append(privileges, "DELETE")
			}
			for _, privilege := range privileges {
				serving.RequirePrivilege(t, "sandbox_output_capture_operations", privilege, func() error {
					_, err := store.FinalizeRuntimeCleanup(context.Background(), RuntimeJob{Kind: queue.KindSessionDeleteCleanup, WorkspaceID: "default", SessionID: "sesn_capture_other", DeleteCleanupID: "delcln_capture_role", AttemptCount: 5, MaxAttempts: 5})
					return err
				})
				if snapshot("default", "sesn_capture_other") != before {
					t.Fatalf("missing capture %s changed custody", privilege)
				}
			}
			if captureState == "adopted" {
				for _, privilege := range []string{"SELECT", "DELETE"} {
					serving.RequirePrivilege(t, "sandbox_output_capture_blobs", privilege, func() error {
						return client.WithWorkspaceTx(context.Background(), "default", "test.capture_delete_grant", func(tx *dbconnect.Tx) error {
							return deleteSessionSandboxRowsTx(context.Background(), tx, "default", "sesn_capture_other")
						})
					})
					if snapshot("default", "sesn_capture_other") != before {
						t.Fatalf("missing blob %s changed custody", privilege)
					}
				}
			}
		})
	}
}

func seedRunnerCaptureCleanup(t *testing.T, admin *sql.DB, workspaceID, sessionID, state string, now time.Time) {
	t.Helper()
	threadID := "thr_" + sessionID
	seedBridgeAPISession(t, admin, workspaceID, sessionID, threadID)
	statements := []struct {
		query string
		args  []any
	}{
		{`UPDATE sessions SET lifecycle_state='deleted',delete_cleanup_id='delcln_capture_role' WHERE workspace_id=$1 AND id=$2`, []any{workspaceID, sessionID}},
		{`INSERT INTO session_sandbox_bindings (workspace_id,session_id,logical_sandbox_id,environment_id,environment_generation,provider,provider_resource_id,binding_revision,materialized_resource_revision,resource_roots_json,provider_metadata_json,created_at,updated_at)
    VALUES ($1,$2,$3,$4,1,'daytona',$5,1,1,'[]','{}',$6,$6)`, []any{workspaceID, sessionID, "sbox_" + sessionID, "env_" + sessionID, "provider_" + sessionID, now}},
		{`INSERT INTO sandbox_lifecycle_operations (workspace_id,operation_id,session_id,logical_sandbox_id,kind,state,target_provider_resource_id,release_reason,created_at,updated_at,completed_at)
    VALUES ($1,$2,$3,$4,'release','completed',$5,'session_delete',$6,$6,$6)`, []any{workspaceID, "sop_" + sessionID, sessionID, "sbox_" + sessionID, "provider_" + sessionID, now}},
		{`INSERT INTO sandbox_output_capture_operations (workspace_id,session_id,session_thread_id,finish_idle_write_id,capture_generation,state,binding_id,binding_generation,outcome_state,outcome_digest,retain_until,created_at,updated_at,staged_at)
    VALUES ($1,$2,$3,$4,1,$5,'bind_capture_role',1,'staged',$6,$7,$7,$7,$7)`, []any{workspaceID, sessionID, threadID, "rwrite_" + sessionID, state, strings.Repeat("a", 64), now}},
		{`INSERT INTO sandbox_output_capture_blobs (workspace_id,session_id,finish_idle_write_id,capture_generation,source_path,blob_pointer,size_bytes,sha256,state,created_at,updated_at,uploaded_at)
    VALUES ($1,$2,$3,1,'/mnt/session/outputs/result.txt',$4,1,$5,'uploaded',$6,$6,$6)`, []any{workspaceID, sessionID, "rwrite_" + sessionID, "output-captures/" + workspaceID + "/" + sessionID, strings.Repeat("a", 64), now}},
	}
	for _, statement := range statements {
		if _, err := admin.ExecContext(context.Background(), statement.query, statement.args...); err != nil {
			t.Fatal(err)
		}
	}
}
