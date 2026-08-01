package tetralsandbox

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"strings"
	"time"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	queuev1 "github.com/tetral-ai/tetral/services/queue/gen/tetral/queue/v1"
)

type PostgreSQLSandboxOutputCaptureStore struct {
	client *dbconnect.Client
}

func NewPostgreSQLSandboxOutputCaptureStore(client *dbconnect.Client) *PostgreSQLSandboxOutputCaptureStore {
	return &PostgreSQLSandboxOutputCaptureStore{client: client}
}

func (s *PostgreSQLSandboxOutputCaptureStore) LoadCapture(ctx context.Context, job SandboxOutputCaptureJob, now time.Time) (SandboxOutputCaptureWork, bool, error) {
	if s == nil || s.client == nil {
		return SandboxOutputCaptureWork{}, false, errors.New("sandbox output capture database is required")
	}
	work := SandboxOutputCaptureWork{SandboxOutputCaptureJob: job, Existing: map[string]SandboxOutputCaptureIndexEntry{}}
	current := false
	err := s.client.WithWorkspaceTx(ctx, job.WorkspaceID, "sandbox.output_capture.load", func(tx *dbconnect.Tx) error {
		var state string
		var capturedLogicalID, capturedProvider, capturedProviderResourceID sql.NullString
		var capturedBindingRevision sql.NullInt64
		if err := tx.QueryRow(ctx,
			`SELECT session_thread_id, binding_id, binding_generation, logical_sandbox_id, provider, provider_resource_id, sandbox_binding_revision, state
			   FROM sandbox_output_capture_operations
			  WHERE workspace_id=$1 AND session_id=$2 AND finish_idle_write_id=$3 AND capture_generation=$4
			  FOR UPDATE`,
			job.WorkspaceID, job.SessionID, job.FinishIdleWriteID, job.CaptureGeneration,
		).Scan(&work.SessionThreadID, &work.BindingID, &work.BindingGeneration, &capturedLogicalID, &capturedProvider, &capturedProviderResourceID, &capturedBindingRevision, &state); dbconnect.IsNoRows(err) {
			return nil
		} else if err != nil {
			return err
		}
		if state != "pending" && state != "running" {
			return nil
		}
		if state == "pending" {
			if _, err := tx.Exec(ctx,
				`UPDATE sandbox_output_capture_operations SET state='running', updated_at=$5
				  WHERE workspace_id=$1 AND session_id=$2 AND finish_idle_write_id=$3 AND capture_generation=$4 AND state='pending'`,
				job.WorkspaceID, job.SessionID, job.FinishIdleWriteID, job.CaptureGeneration, now.UTC(),
			); err != nil {
				return err
			}
		}
		var logicalID, provider, providerResourceID string
		var bindingRevision int64
		err := tx.QueryRow(ctx,
			`SELECT logical_sandbox_id, provider, COALESCE(provider_resource_id,''), binding_revision
			   FROM session_sandbox_bindings
			  WHERE workspace_id=$1 AND session_id=$2 AND release_requested_at IS NULL
			  FOR UPDATE`,
			job.WorkspaceID, job.SessionID,
		).Scan(&logicalID, &provider, &providerResourceID, &bindingRevision)
		if dbconnect.IsNoRows(err) || strings.TrimSpace(providerResourceID) == "" {
			if capturedBindingRevision.Valid {
				return failStaleOutputCaptureTx(ctx, tx, work, "sandbox binding is no longer current", now)
			}
			work.ProviderAvailable = false
		} else if err != nil {
			return err
		} else if capturedBindingRevision.Valid {
			if !capturedLogicalID.Valid || !capturedProvider.Valid || !capturedProviderResourceID.Valid ||
				capturedLogicalID.String != logicalID || capturedProvider.String != provider ||
				capturedProviderResourceID.String != providerResourceID || capturedBindingRevision.Int64 != bindingRevision {
				return failStaleOutputCaptureTx(ctx, tx, work, "sandbox binding changed before capture", now)
			}
			work.LogicalSandboxID = logicalID
			work.Provider = provider
			work.ProviderResourceID = providerResourceID
			work.BindingRevision = bindingRevision
			work.ProviderAvailable = true
		} else {
			if _, err := tx.Exec(ctx,
				`UPDATE sandbox_output_capture_operations
				    SET logical_sandbox_id=$5, provider=$6, provider_resource_id=$7, sandbox_binding_revision=$8, updated_at=$9
				  WHERE workspace_id=$1 AND session_id=$2 AND finish_idle_write_id=$3 AND capture_generation=$4 AND state='running'`,
				work.WorkspaceID, work.SessionID, work.FinishIdleWriteID, work.CaptureGeneration,
				logicalID, provider, providerResourceID, bindingRevision, now.UTC(),
			); err != nil {
				return err
			}
			work.LogicalSandboxID = logicalID
			work.Provider = provider
			work.ProviderResourceID = providerResourceID
			work.BindingRevision = bindingRevision
			work.ProviderAvailable = true
		}
		rows, err := tx.Query(ctx,
			`SELECT source_path, COALESCE(last_file_id,''), COALESCE(last_size_bytes,0), COALESCE(last_sha256,'')
			   FROM session_output_captures WHERE workspace_id=$1 AND session_id=$2`,
			job.WorkspaceID, job.SessionID,
		)
		if err != nil {
			return err
		}
		defer func() { _ = rows.Close() }()
		for rows.Next() {
			var sourcePath string
			var existing SandboxOutputCaptureIndexEntry
			if err := rows.Scan(&sourcePath, &existing.FileID, &existing.SizeBytes, &existing.SHA256); err != nil {
				return err
			}
			work.Existing[sourcePath] = existing
		}
		if err := rows.Err(); err != nil {
			return err
		}
		current = true
		return nil
	})
	return work, current, err
}

func (s *PostgreSQLSandboxOutputCaptureStore) EnsureBlobStage(ctx context.Context, work SandboxOutputCaptureWork, entry SandboxOutputCaptureManifestEntry, now time.Time) (string, error) {
	if s == nil || s.client == nil {
		return "", errors.New("sandbox output capture database is required")
	}
	var state string
	err := s.client.WithWorkspaceTx(ctx, work.WorkspaceID, "sandbox.output_capture.ensure_blob", func(tx *dbconnect.Tx) error {
		if err := lockRunningOutputCaptureTx(ctx, tx, work); err != nil {
			return err
		}
		_, err := tx.Exec(ctx,
			`INSERT INTO sandbox_output_capture_blobs (
				workspace_id, session_id, finish_idle_write_id, capture_generation, source_path,
				blob_pointer, size_bytes, sha256, state, created_at, updated_at
			) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,'pending',$9,$9)
			ON CONFLICT (workspace_id, session_id, finish_idle_write_id, capture_generation, source_path) DO NOTHING`,
			work.WorkspaceID, work.SessionID, work.FinishIdleWriteID, work.CaptureGeneration,
			entry.SourcePath, entry.BlobPointer, entry.SizeBytes, entry.SHA256, now.UTC(),
		)
		if err != nil {
			return err
		}
		var pointer, digest string
		var size int64
		if err := tx.QueryRow(ctx,
			`SELECT blob_pointer, size_bytes, sha256, state FROM sandbox_output_capture_blobs
			  WHERE workspace_id=$1 AND session_id=$2 AND finish_idle_write_id=$3 AND capture_generation=$4 AND source_path=$5
			  FOR UPDATE`,
			work.WorkspaceID, work.SessionID, work.FinishIdleWriteID, work.CaptureGeneration, entry.SourcePath,
		).Scan(&pointer, &size, &digest, &state); err != nil {
			return err
		}
		if pointer != entry.BlobPointer || size != entry.SizeBytes || digest != entry.SHA256 {
			return errors.New("sandbox output capture blob stage conflicts with durable identity")
		}
		return nil
	})
	return state, err
}

func (s *PostgreSQLSandboxOutputCaptureStore) MarkBlobUploaded(ctx context.Context, work SandboxOutputCaptureWork, sourcePath string, now time.Time) error {
	if s == nil || s.client == nil {
		return errors.New("sandbox output capture database is required")
	}
	return s.client.WithWorkspaceTx(ctx, work.WorkspaceID, "sandbox.output_capture.mark_blob_uploaded", func(tx *dbconnect.Tx) error {
		if err := lockRunningOutputCaptureTx(ctx, tx, work); err != nil {
			return err
		}
		result, err := tx.Exec(ctx,
			`UPDATE sandbox_output_capture_blobs SET state='uploaded', uploaded_at=COALESCE(uploaded_at,$6), updated_at=$6
			  WHERE workspace_id=$1 AND session_id=$2 AND finish_idle_write_id=$3 AND capture_generation=$4 AND source_path=$5 AND state IN ('pending','uploaded')`,
			work.WorkspaceID, work.SessionID, work.FinishIdleWriteID, work.CaptureGeneration, sourcePath, now.UTC(),
		)
		if err != nil {
			return err
		}
		if !transitionRowsAffected(result) {
			return errors.New("sandbox output capture blob stage is not uploadable")
		}
		return nil
	})
}

func (s *PostgreSQLSandboxOutputCaptureStore) StageCapture(ctx context.Context, work SandboxOutputCaptureWork, manifest []SandboxOutputCaptureManifestEntry, skipped []SandboxOutputCaptureSkippedFile, records []SandboxOutputCaptureScanRecord, unavailable bool, failureKind string, failureDetail string, now time.Time) error {
	if s == nil || s.client == nil {
		return errors.New("sandbox output capture database is required")
	}
	manifestJSON, err := marshalSandboxJSON(manifest)
	if err != nil {
		return err
	}
	skippedJSON, err := marshalSandboxJSON(skipped)
	if err != nil {
		return err
	}
	recordsJSON, err := marshalSandboxJSON(records)
	if err != nil {
		return err
	}
	state := "staged"
	if unavailable {
		state = "skipped_unavailable"
	}
	outcomeDigest := outputCaptureOutcomeDigest(state, manifestJSON, skippedJSON, recordsJSON, failureKind, failureDetail)
	return s.client.WithWorkspaceTx(ctx, work.WorkspaceID, "sandbox.output_capture.stage", func(tx *dbconnect.Tx) error {
		if err := lockRunningOutputCaptureTx(ctx, tx, work); err != nil {
			return err
		}
		expected := make(map[string]string)
		for _, entry := range manifest {
			if (entry.ExistingFileID == "") == (entry.BlobPointer == "") {
				return errors.New("sandbox output capture manifest must carry exactly one content owner")
			}
			if entry.BlobPointer != "" {
				expected[entry.SourcePath] = entry.BlobPointer
			}
		}
		rows, err := tx.Query(ctx,
			`SELECT source_path, blob_pointer, state FROM sandbox_output_capture_blobs
			  WHERE workspace_id=$1 AND session_id=$2 AND finish_idle_write_id=$3 AND capture_generation=$4
			  ORDER BY source_path FOR UPDATE`,
			work.WorkspaceID, work.SessionID, work.FinishIdleWriteID, work.CaptureGeneration,
		)
		if err != nil {
			return err
		}
		seen := 0
		for rows.Next() {
			var sourcePath, pointer, childState string
			if err := rows.Scan(&sourcePath, &pointer, &childState); err != nil {
				_ = rows.Close()
				return err
			}
			if expected[sourcePath] != pointer || childState != "uploaded" {
				_ = rows.Close()
				return errors.New("sandbox output capture Blob custody does not match its manifest")
			}
			seen++
		}
		if err := rows.Close(); err != nil {
			return err
		}
		if err := rows.Err(); err != nil {
			return err
		}
		if seen != len(expected) {
			return errors.New("sandbox output capture manifest is missing uploaded Blob custody")
		}
		result, err := tx.Exec(ctx,
			`UPDATE sandbox_output_capture_operations
			    SET state=$5, manifest_json=$6, skipped_json=$7, scan_records_json=$8,
			        failure_kind=NULLIF($9,''), failure_detail=NULLIF($10,''),
			        outcome_state=$5, outcome_digest=$11, staged_at=$12, updated_at=$12
			  WHERE workspace_id=$1 AND session_id=$2 AND finish_idle_write_id=$3 AND capture_generation=$4 AND state='running'`,
			work.WorkspaceID, work.SessionID, work.FinishIdleWriteID, work.CaptureGeneration,
			state, manifestJSON, skippedJSON, recordsJSON, failureKind, failureDetail, outcomeDigest, now.UTC(),
		)
		if err != nil {
			return err
		}
		if !transitionRowsAffected(result) {
			return errors.New("sandbox output capture operation is not stageable")
		}
		return nil
	})
}

func (s *PostgreSQLSandboxOutputCaptureStore) FailCapture(ctx context.Context, work SandboxOutputCaptureWork, kind string, detail string, now time.Time) error {
	if s == nil || s.client == nil {
		return errors.New("sandbox output capture database is required")
	}
	return s.client.WithWorkspaceTx(ctx, work.WorkspaceID, "sandbox.output_capture.fail", func(tx *dbconnect.Tx) error {
		result, err := tx.Exec(ctx,
			`UPDATE sandbox_output_capture_operations
			    SET state='failed', failure_kind=$5, failure_detail=$6,
			        outcome_state='failed', outcome_digest=$7, updated_at=$8
			  WHERE workspace_id=$1 AND session_id=$2 AND finish_idle_write_id=$3 AND capture_generation=$4 AND state IN ('pending','running')`,
			work.WorkspaceID, work.SessionID, work.FinishIdleWriteID, work.CaptureGeneration, kind, detail,
			outputCaptureOutcomeDigest("failed", "[]", "[]", "[]", kind, detail), now.UTC(),
		)
		if err != nil {
			return err
		}
		if !transitionRowsAffected(result) {
			return errors.New("sandbox output capture operation is not fail-able")
		}
		return nil
	})
}

func (s *PostgreSQLSandboxOutputCaptureStore) FinalizeCaptureExhaustion(ctx context.Context, job *queuev1.QueueJob, now time.Time) error {
	decoded, err := decodeSandboxOutputCaptureTransportIdentity(job)
	if err != nil {
		return err
	}
	return s.client.WithWorkspaceTx(ctx, decoded.WorkspaceID, "sandbox.output_capture.finalize_exhaustion", func(tx *dbconnect.Tx) error {
		return finalizeOutputCaptureExhaustionTx(ctx, tx, decoded, now)
	})
}

func lockRunningOutputCaptureTx(ctx context.Context, tx *dbconnect.Tx, work SandboxOutputCaptureWork) error {
	var state string
	var logicalID, provider, providerResourceID sql.NullString
	var bindingRevision sql.NullInt64
	if err := tx.QueryRow(ctx,
		`SELECT state, logical_sandbox_id, provider, provider_resource_id, sandbox_binding_revision
		   FROM sandbox_output_capture_operations
		  WHERE workspace_id=$1 AND session_id=$2 AND finish_idle_write_id=$3 AND capture_generation=$4
		  FOR UPDATE`,
		work.WorkspaceID, work.SessionID, work.FinishIdleWriteID, work.CaptureGeneration,
	).Scan(&state, &logicalID, &provider, &providerResourceID, &bindingRevision); err != nil {
		return err
	}
	if state != "running" {
		return errors.New("sandbox output capture operation is no longer running")
	}
	if !bindingRevision.Valid {
		return nil
	}
	var currentRevision int64
	var currentLogicalID, currentProvider, currentProviderResourceID string
	if err := tx.QueryRow(ctx,
		`SELECT logical_sandbox_id, provider, COALESCE(provider_resource_id,''), binding_revision
		   FROM session_sandbox_bindings
		  WHERE workspace_id=$1 AND session_id=$2 AND release_requested_at IS NULL
		  FOR UPDATE`, work.WorkspaceID, work.SessionID,
	).Scan(&currentLogicalID, &currentProvider, &currentProviderResourceID, &currentRevision); err != nil {
		return errors.New("sandbox output capture binding is no longer current")
	}
	if !logicalID.Valid || !provider.Valid || !providerResourceID.Valid || logicalID.String != currentLogicalID ||
		provider.String != currentProvider || providerResourceID.String != currentProviderResourceID || bindingRevision.Int64 != currentRevision {
		return errors.New("sandbox output capture binding changed during capture")
	}
	return nil
}

func failStaleOutputCaptureTx(ctx context.Context, tx *dbconnect.Tx, work SandboxOutputCaptureWork, detail string, now time.Time) error {
	digest := outputCaptureOutcomeDigest("failed", "[]", "[]", "[]", "sandbox_binding_changed", detail)
	_, err := tx.Exec(ctx,
		`UPDATE sandbox_output_capture_operations
		    SET state='failed', failure_kind='sandbox_binding_changed', failure_detail=$5,
		        outcome_state='failed', outcome_digest=$6, updated_at=$7
		  WHERE workspace_id=$1 AND session_id=$2 AND finish_idle_write_id=$3 AND capture_generation=$4 AND state IN ('pending','running')`,
		work.WorkspaceID, work.SessionID, work.FinishIdleWriteID, work.CaptureGeneration, detail, digest, now.UTC(),
	)
	return err
}

func outputCaptureOutcomeDigest(state string, manifestJSON string, skippedJSON string, recordsJSON string, failureKind string, failureDetail string) string {
	digest := sha256.Sum256([]byte(state + "\x00" + manifestJSON + "\x00" + skippedJSON + "\x00" + recordsJSON + "\x00" + failureKind + "\x00" + failureDetail))
	return hex.EncodeToString(digest[:])
}
