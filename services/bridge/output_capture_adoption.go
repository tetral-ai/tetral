package agentruntimebridge

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"strings"
	"time"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/files"
	"github.com/tetral-ai/tetral/internal/id"
	"github.com/tetral-ai/tetral/internal/queue"
	"github.com/tetral-ai/tetral/internal/storage"
	"github.com/tetral-ai/tetral/internal/workspace"
	bridgev1 "github.com/tetral-ai/tetral/services/bridge/gen/tetral/bridge/v1"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	outputCaptureRetainDuration = 15 * time.Minute
	outputCaptureWaitInterval   = 100 * time.Millisecond
)

type finishIdleCapture struct {
	Generation int64
	State      string
}

type stagedOutputCaptureEntry struct {
	SourcePath     string `json:"source_path"`
	Filename       string `json:"filename"`
	MIMEType       string `json:"mime_type"`
	SizeBytes      int64  `json:"size_bytes"`
	SHA256         string `json:"sha256"`
	ExistingFileID string `json:"existing_file_id,omitempty"`
	BlobPointer    string `json:"blob_pointer,omitempty"`
}

type stagedOutputCaptureSkipped struct {
	SourcePath string `json:"source_path"`
	Reason     string `json:"reason"`
	SizeBytes  int64  `json:"size_bytes"`
}

type stagedOutputCaptureScanRecord struct {
	ParentPath string `json:"parent_path"`
	Reason     string `json:"reason"`
	Count      int    `json:"count"`
}

type adoptedOutputCapture struct {
	Skipped     []stagedOutputCaptureSkipped
	ScanRecords []stagedOutputCaptureScanRecord
}

func (s *PostgreSQLBridgeAPIStore) ensureFinishIdleOutputCapture(ctx context.Context, request *bridgev1.FinishIdleRequest, sourceKind string, key string, declarationDigest string, now time.Time) (finishIdleCapture, error) {
	var capture finishIdleCapture
	err := s.withScopeTx(ctx, request.GetScope(), "agentruntimebridge.ensure_output_capture", func(tx *dbconnect.Tx) error {
		if err := lockRuntimeMutationSessionTx(ctx, tx, request.GetScope().GetWorkspaceId(), request.GetScope().GetSessionId()); err != nil {
			return err
		}
		if err := verifyRuntimeDeclarationCaller(ctx, request.GetScope()); err != nil {
			return err
		}
		if existing, ok, err := readBridgeDeclarationOperationTx(ctx, tx, request.GetScope(), bridgeOpFinishIdle, sourceKind, key); err != nil {
			return err
		} else if ok {
			if existing.DeclarationDigest != declarationDigest {
				return status.Error(codes.AlreadyExists, "finish idle idempotency conflict")
			}
			return nil
		}
		if err := verifyRuntimeScopeTx(ctx, tx, request.GetScope()); err != nil {
			return err
		}
		threadScope, err := lockThreadMutationTx(ctx, tx, request.GetScope())
		if err != nil {
			return err
		}
		switch threadScope.status {
		case "terminated", "failed", "archived", "closed_for_runtime":
			return scopeSupersededError(status.Error(codes.FailedPrecondition, "runtime thread is already terminal"))
		}
		openTurn, err := loadOpenDurableTurnIDTx(ctx, tx, request.GetScope())
		if err != nil {
			return err
		}
		if openTurn == nil || *openTurn != key {
			return scopeSupersededError(status.Error(codes.FailedPrecondition, "durable turn is not open"))
		}

		var latestGeneration int64
		var latestState string
		err = tx.QueryRow(ctx,
			`SELECT capture_generation, state FROM sandbox_output_capture_operations
			  WHERE workspace_id=$1 AND session_id=$2 AND finish_idle_write_id=$3
			  ORDER BY capture_generation DESC LIMIT 1 FOR UPDATE`,
			request.GetScope().GetWorkspaceId(), request.GetScope().GetSessionId(), key,
		).Scan(&latestGeneration, &latestState)
		if err != nil && !dbconnect.IsNoRows(err) {
			return err
		}
		if err == nil && (latestState == "pending" || latestState == "running" || latestState == "staged" || latestState == "skipped_unavailable" || latestState == "adopted") {
			capture = finishIdleCapture{Generation: latestGeneration, State: latestState}
			_, err = tx.Exec(ctx,
				`UPDATE sandbox_output_capture_operations SET retain_until=GREATEST(retain_until,$4), updated_at=$5
				  WHERE workspace_id=$1 AND session_id=$2 AND finish_idle_write_id=$3 AND capture_generation=$6`,
				request.GetScope().GetWorkspaceId(), request.GetScope().GetSessionId(), key, now.Add(outputCaptureRetainDuration), now, latestGeneration,
			)
			return err
		}
		generation := latestGeneration + 1
		if generation <= 0 {
			generation = 1
		}
		if _, err := tx.Exec(ctx,
			`INSERT INTO sandbox_output_capture_operations (
				workspace_id, session_id, session_thread_id, finish_idle_write_id, capture_generation,
				state, binding_id, binding_generation, retain_until, created_at, updated_at
			) VALUES ($1,$2,$3,$4,$5,'pending',$6,$7,$8,$9,$9)`,
			request.GetScope().GetWorkspaceId(), request.GetScope().GetSessionId(), request.GetScope().GetSessionThreadId(), key, generation,
			request.GetScope().GetBinding().GetBindingId(), request.GetScope().GetBinding().GetBindingGeneration(),
			now.Add(outputCaptureRetainDuration), now,
		); err != nil {
			return err
		}
		payload, err := marshalBridgeJSON(map[string]any{
			"workspace_id": request.GetScope().GetWorkspaceId(), "session_id": request.GetScope().GetSessionId(),
			"finish_idle_write_id": key, "capture_generation": generation,
		})
		if err != nil {
			return err
		}
		workspaceID := workspace.ID(request.GetScope().GetWorkspaceId())
		if _, err := queue.EnqueueTx(ctx, tx, queue.EnqueueRequest{
			ID: queue.NewJobID(), WorkspaceID: workspaceID, Kind: queue.KindSandboxOutputCapture,
			PartitionKey:   queue.FormatSandboxCapturePartitionKey(workspaceID, request.GetScope().GetSessionId(), key),
			DedupeKey:      queue.FormatSandboxOutputCaptureDedupeKey(workspaceID, request.GetScope().GetSessionId(), key, generation),
			PayloadVersion: 1, PayloadJSON: []byte(payload), MaxAttempts: queue.SandboxOutputCaptureMaxAttempts, Now: now,
		}); err != nil {
			return err
		}
		capture = finishIdleCapture{Generation: generation, State: "pending"}
		return nil
	})
	return capture, err
}

func ensureSessionOutputCaptureCleanupTx(ctx context.Context, tx *dbconnect.Tx, workspaceID string, sessionID string, now time.Time) (bool, error) {
	transportOpen, err := hasOpenSessionSandboxQueueJobsTx(ctx, tx, workspaceID, sessionID)
	if err != nil {
		return false, err
	}
	if transportOpen {
		return true, nil
	}
	type capture struct {
		writeID           string
		captureGeneration int64
		state             string
		cleanupGeneration int64
	}
	rows, err := tx.Query(ctx,
		`SELECT finish_idle_write_id, capture_generation, state, cleanup_generation
		   FROM sandbox_output_capture_operations
		  WHERE workspace_id=$1 AND session_id=$2
		  ORDER BY finish_idle_write_id, capture_generation
		  FOR UPDATE`,
		workspaceID, sessionID,
	)
	if err != nil {
		return false, err
	}
	var captures []capture
	for rows.Next() {
		var item capture
		if err := rows.Scan(&item.writeID, &item.captureGeneration, &item.state, &item.cleanupGeneration); err != nil {
			_ = rows.Close()
			return false, err
		}
		captures = append(captures, item)
	}
	if err := rows.Close(); err != nil {
		return false, err
	}
	if err := rows.Err(); err != nil {
		return false, err
	}
	pending := false
	for _, item := range captures {
		switch item.state {
		case "adopted", "cleaned":
			continue
		case "pending", "running", "cleanup_pending":
			pending = true
		case "staged", "skipped_unavailable", "failed":
			nextGeneration := item.cleanupGeneration + 1
			result, err := tx.Exec(ctx,
				`UPDATE sandbox_output_capture_operations
				    SET state='cleanup_pending', cleanup_generation=$5, updated_at=$6
				  WHERE workspace_id=$1 AND session_id=$2 AND finish_idle_write_id=$3 AND capture_generation=$4
				    AND state IN ('staged','skipped_unavailable','failed')`,
				workspaceID, sessionID, item.writeID, item.captureGeneration, nextGeneration, now,
			)
			if err != nil {
				return false, err
			}
			if !rowsAffected(result) {
				return false, errors.New("output capture cleanup lost its state fence")
			}
			if err := queue.EnqueueSandboxOutputCaptureCleanupTx(ctx, tx, workspace.ID(workspaceID), sessionID, item.writeID, item.captureGeneration, nextGeneration, now); err != nil {
				return false, err
			}
			pending = true
		default:
			return false, errors.New("output capture cleanup found an invalid state")
		}
	}
	if pending {
		return true, nil
	}
	_, err = tx.Exec(ctx,
		`DELETE FROM sandbox_output_capture_operations WHERE workspace_id=$1 AND session_id=$2`,
		workspaceID, sessionID,
	)
	return false, err
}

func (s *PostgreSQLBridgeAPIStore) waitForFinishIdleOutputCapture(ctx context.Context, scope *bridgev1.RuntimeScope, key string, capture finishIdleCapture) (finishIdleCapture, error) {
	if capture.Generation == 0 || capture.State == "staged" || capture.State == "skipped_unavailable" || capture.State == "adopted" {
		return capture, nil
	}
	ticker := time.NewTicker(outputCaptureWaitInterval)
	defer ticker.Stop()
	for {
		now := storage.Now()
		if s.Clock != nil {
			now = s.Clock().UTC()
		}
		err := s.Client.WithWorkspaceTx(ctx, scope.GetWorkspaceId(), "agentruntimebridge.await_output_capture", func(tx *dbconnect.Tx) error {
			var failureKind, failureDetail sql.NullString
			if err := tx.QueryRow(ctx,
				`SELECT state, failure_kind, failure_detail FROM sandbox_output_capture_operations
				  WHERE workspace_id=$1 AND session_id=$2 AND finish_idle_write_id=$3 AND capture_generation=$4
				  FOR UPDATE`,
				scope.GetWorkspaceId(), scope.GetSessionId(), key, capture.Generation,
			).Scan(&capture.State, &failureKind, &failureDetail); err != nil {
				return err
			}
			if capture.State == "pending" || capture.State == "running" || capture.State == "staged" || capture.State == "skipped_unavailable" {
				_, err := tx.Exec(ctx,
					`UPDATE sandbox_output_capture_operations SET retain_until=GREATEST(retain_until,$5), updated_at=$6
					  WHERE workspace_id=$1 AND session_id=$2 AND finish_idle_write_id=$3 AND capture_generation=$4`,
					scope.GetWorkspaceId(), scope.GetSessionId(), key, capture.Generation, now.Add(outputCaptureRetainDuration), now,
				)
				return err
			}
			return nil
		})
		if err != nil {
			return capture, err
		}
		switch capture.State {
		case "staged", "skipped_unavailable", "adopted":
			return capture, nil
		case "failed":
			return capture, status.Error(codes.Unavailable, "output capture failed")
		case "cleanup_pending", "cleaned":
			return capture, status.Error(codes.Unavailable, "output capture expired before adoption")
		}
		select {
		case <-ctx.Done():
			return capture, status.Error(codes.DeadlineExceeded, "output capture result is not ready")
		case <-ticker.C:
		}
	}
}

func adoptFinishIdleOutputCaptureTx(ctx context.Context, tx *dbconnect.Tx, scope *bridgev1.RuntimeScope, key string, generation int64, now time.Time) (adoptedOutputCapture, error) {
	if generation == 0 {
		return adoptedOutputCapture{}, nil
	}
	var state, manifestJSON, skippedJSON, scanRecordsJSON string
	if err := tx.QueryRow(ctx,
		`SELECT state, manifest_json, skipped_json, scan_records_json
		   FROM sandbox_output_capture_operations
		  WHERE workspace_id=$1 AND session_id=$2 AND finish_idle_write_id=$3 AND capture_generation=$4
		  FOR UPDATE`,
		scope.GetWorkspaceId(), scope.GetSessionId(), key, generation,
	).Scan(&state, &manifestJSON, &skippedJSON, &scanRecordsJSON); err != nil {
		return adoptedOutputCapture{}, err
	}
	if state == "adopted" {
		return adoptedOutputCapture{}, nil
	}
	if state != "staged" && state != "skipped_unavailable" {
		return adoptedOutputCapture{}, status.Error(codes.Unavailable, "output capture is not ready for adoption")
	}
	var manifest []stagedOutputCaptureEntry
	var adopted adoptedOutputCapture
	if err := json.Unmarshal([]byte(manifestJSON), &manifest); err != nil {
		return adoptedOutputCapture{}, errors.New("output capture manifest is invalid")
	}
	if err := json.Unmarshal([]byte(skippedJSON), &adopted.Skipped); err != nil {
		return adoptedOutputCapture{}, errors.New("output capture skipped records are invalid")
	}
	if err := json.Unmarshal([]byte(scanRecordsJSON), &adopted.ScanRecords); err != nil {
		return adoptedOutputCapture{}, errors.New("output capture scan records are invalid")
	}
	if err := storage.AcquireWorkspaceFilesLock(ctx, tx, scope.GetWorkspaceId()); err != nil {
		return adoptedOutputCapture{}, err
	}
	addFiles, addBytes, err := outputCaptureAdoptionNeeds(manifest)
	if err != nil {
		return adoptedOutputCapture{}, err
	}
	if err := assertOutputCaptureQuotaTx(ctx, tx, scope.GetWorkspaceId(), addFiles, addBytes); err != nil {
		return adoptedOutputCapture{}, err
	}
	for _, entry := range manifest {
		if entry.ExistingFileID != "" {
			var sizeBytes int64
			var digest string
			if err := tx.QueryRow(ctx,
				`SELECT o.size_bytes, o.sha256
				   FROM files f
				   JOIN file_objects o ON o.workspace_id=f.workspace_id AND o.object_id=f.object_id
				  WHERE f.workspace_id=$1 AND f.file_id=$2 AND f.scope_type='session' AND f.scope_id=$3
				  FOR UPDATE OF f, o`,
				scope.GetWorkspaceId(), entry.ExistingFileID, scope.GetSessionId(),
			).Scan(&sizeBytes, &digest); err != nil {
				return adoptedOutputCapture{}, err
			}
			if sizeBytes != entry.SizeBytes || !strings.EqualFold(digest, entry.SHA256) {
				return adoptedOutputCapture{}, errors.New("output capture existing file identity is inconsistent")
			}
			if err := upsertOutputCaptureIndexTx(ctx, tx, scope.GetWorkspaceId(), scope.GetSessionId(), entry, entry.ExistingFileID, now); err != nil {
				return adoptedOutputCapture{}, err
			}
			continue
		}
		var blobState string
		if err := tx.QueryRow(ctx,
			`SELECT state FROM sandbox_output_capture_blobs
			  WHERE workspace_id=$1 AND session_id=$2 AND finish_idle_write_id=$3 AND capture_generation=$4 AND source_path=$5
			  FOR UPDATE`,
			scope.GetWorkspaceId(), scope.GetSessionId(), key, generation, entry.SourcePath,
		).Scan(&blobState); err != nil {
			return adoptedOutputCapture{}, err
		}
		if blobState != "uploaded" {
			return adoptedOutputCapture{}, errors.New("output capture blob is not uploaded")
		}
		fileID := id.New(files.IDPrefix)
		objectID := id.New(files.ObjectIDPrefix)
		if _, err := tx.Exec(ctx,
			`INSERT INTO file_objects (object_id, workspace_id, blob_key, size_bytes, sha256, created_at) VALUES ($1,$2,$3,$4,$5,$6)`,
			objectID, scope.GetWorkspaceId(), entry.BlobPointer, entry.SizeBytes, entry.SHA256, now,
		); err != nil {
			return adoptedOutputCapture{}, err
		}
		if _, err := tx.Exec(ctx,
			`INSERT INTO files (file_id, workspace_id, object_id, filename, mime_type, downloadable, scope_type, scope_id, created_at)
			 VALUES ($1,$2,$3,$4,$5,true,'session',$6,$7)`,
			fileID, scope.GetWorkspaceId(), objectID, entry.Filename, entry.MIMEType, scope.GetSessionId(), now,
		); err != nil {
			return adoptedOutputCapture{}, err
		}
		if err := upsertOutputCaptureIndexTx(ctx, tx, scope.GetWorkspaceId(), scope.GetSessionId(), entry, fileID, now); err != nil {
			return adoptedOutputCapture{}, err
		}
		updated, err := tx.Exec(ctx,
			`UPDATE sandbox_output_capture_blobs SET state='adopted', file_id=$6, adopted_at=$7, updated_at=$7
			  WHERE workspace_id=$1 AND session_id=$2 AND finish_idle_write_id=$3 AND capture_generation=$4 AND source_path=$5 AND state='uploaded'`,
			scope.GetWorkspaceId(), scope.GetSessionId(), key, generation, entry.SourcePath, fileID, now,
		)
		if err != nil {
			return adoptedOutputCapture{}, err
		}
		if !rowsAffected(updated) {
			return adoptedOutputCapture{}, errors.New("output capture blob custody transfer lost its state fence")
		}
	}
	result, err := tx.Exec(ctx,
		`UPDATE sandbox_output_capture_operations SET state='adopted', adopted_at=$5, updated_at=$5
		  WHERE workspace_id=$1 AND session_id=$2 AND finish_idle_write_id=$3 AND capture_generation=$4 AND state IN ('staged','skipped_unavailable')`,
		scope.GetWorkspaceId(), scope.GetSessionId(), key, generation, now,
	)
	if err != nil {
		return adoptedOutputCapture{}, err
	}
	if !rowsAffected(result) {
		return adoptedOutputCapture{}, errors.New("output capture adoption lost its state fence")
	}
	return adopted, nil
}

func outputCaptureAdoptionNeeds(manifest []stagedOutputCaptureEntry) (int, int64, error) {
	var filesNeeded int
	var bytesNeeded int64
	for _, entry := range manifest {
		if (entry.ExistingFileID == "") == (entry.BlobPointer == "") {
			return 0, 0, errors.New("output capture manifest must carry exactly one content owner")
		}
		if entry.ExistingFileID != "" {
			continue
		}
		filesNeeded++
		bytesNeeded += entry.SizeBytes
	}
	return filesNeeded, bytesNeeded, nil
}

func assertOutputCaptureQuotaTx(ctx context.Context, tx *dbconnect.Tx, workspaceID string, addFiles int, addBytes int64) error {
	if addFiles == 0 && addBytes == 0 {
		return nil
	}
	var fileCount int
	if err := tx.QueryRow(ctx, `SELECT count(*) FROM files WHERE workspace_id=$1`, workspaceID).Scan(&fileCount); err != nil {
		return err
	}
	if fileCount+addFiles > files.MaxFileIdentitiesPerWorkspace {
		return status.Error(codes.ResourceExhausted, "workspace file identity quota exceeded")
	}
	var retainedBytes int64
	if err := tx.QueryRow(ctx, `SELECT COALESCE(SUM(size_bytes),0) FROM file_objects WHERE workspace_id=$1`, workspaceID).Scan(&retainedBytes); err != nil {
		return err
	}
	if retainedBytes+addBytes > files.MaxRetainedBytesPerWorkspace {
		return status.Error(codes.ResourceExhausted, "workspace retained file bytes quota exceeded")
	}
	return nil
}

func upsertOutputCaptureIndexTx(ctx context.Context, tx *dbconnect.Tx, workspaceID string, sessionID string, entry stagedOutputCaptureEntry, fileID string, now time.Time) error {
	_, err := tx.Exec(ctx,
		`INSERT INTO session_output_captures (
			workspace_id, session_id, source_path, last_file_id, last_size_bytes, last_sha256, last_captured_at, created_at, updated_at
		) VALUES ($1,$2,$3,$4,$5,$6,$7,$7,$7)
		ON CONFLICT (workspace_id, session_id, source_path) DO UPDATE SET
			last_file_id=EXCLUDED.last_file_id, last_size_bytes=EXCLUDED.last_size_bytes,
			last_sha256=EXCLUDED.last_sha256, last_captured_at=EXCLUDED.last_captured_at, updated_at=EXCLUDED.updated_at`,
		workspaceID, sessionID, entry.SourcePath, fileID, entry.SizeBytes, entry.SHA256, now)
	return err
}
