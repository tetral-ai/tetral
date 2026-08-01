package tetralsandbox

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"strconv"
	"strings"
	"time"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/memory"
	"github.com/tetral-ai/tetral/internal/queue"
	sandboxdriver "github.com/tetral-ai/tetral/internal/sandbox/driver"
	queuev1 "github.com/tetral-ai/tetral/services/queue/gen/tetral/queue/v1"
)

type PostgreSQLSandboxMemoryProjectionStore struct {
	client *dbconnect.Client
}

func NewPostgreSQLSandboxMemoryProjectionStore(client *dbconnect.Client) *PostgreSQLSandboxMemoryProjectionStore {
	return &PostgreSQLSandboxMemoryProjectionStore{client: client}
}

func (s *PostgreSQLSandboxMemoryProjectionStore) LoadProjection(ctx context.Context, job SandboxMemoryProjectionJob) (SandboxMemoryProjectionWork, bool, error) {
	if s == nil || s.client == nil {
		return SandboxMemoryProjectionWork{}, false, errors.New("sandbox memory projection database is required")
	}
	var work SandboxMemoryProjectionWork
	current := false
	err := s.client.WithWorkspaceReadOnlyTx(ctx, job.WorkspaceID, "sandbox.memory_projection.load", func(tx *dbconnect.Tx) error {
		var inputJSON, resultJSON string
		var projectionState sql.NullString
		if err := tx.QueryRow(ctx,
			`SELECT session_id, session_thread_id, input_json, result_json, memory_projection_state
			   FROM session_runtime_tool_results
			  WHERE workspace_id = $1
			    AND tool_use_event_id = $2
			    AND tool_kind = 'memory'`,
			job.WorkspaceID, job.MemoryWriteID,
		).Scan(&work.SessionID, &work.SessionThreadID, &inputJSON, &resultJSON, &projectionState); dbconnect.IsNoRows(err) {
			return nil
		} else if err != nil {
			return err
		}
		if work.SessionID != job.SessionID || !projectionState.Valid || projectionState.String != "pending" {
			return nil
		}
		work.WorkspaceID = job.WorkspaceID
		work.MemoryStoreID = job.MemoryStoreID
		work.MemoryWriteID = job.MemoryWriteID
		if err := tx.QueryRow(ctx,
			`SELECT provider, COALESCE(provider_resource_id, '')
			   FROM session_sandbox_bindings
			  WHERE workspace_id = $1 AND session_id = $2 AND release_requested_at IS NULL`,
			job.WorkspaceID, job.SessionID,
		).Scan(&work.Provider, &work.ProviderResourceID); dbconnect.IsNoRows(err) {
			work.Provider = sandboxdriver.DaytonaProviderName
		} else if err != nil {
			return err
		}
		var resolvedStoreID string
		if err := tx.QueryRow(ctx,
			`SELECT smr.memory_store_id
			   FROM session_memory_store_resources smr
			   JOIN session_resources sr
			     ON sr.workspace_id = smr.workspace_id
			    AND sr.session_id = smr.session_id
			    AND sr.resource_id = smr.resource_id
			    AND sr.type = 'memory_store'
			    AND sr.detached_at IS NULL
			    AND sr.delete_requested_at IS NULL
			  WHERE smr.workspace_id = $1 AND smr.session_id = $2 AND smr.access = 'read_write'`,
			job.WorkspaceID, job.SessionID,
		).Scan(&resolvedStoreID); err != nil {
			return err
		}
		if resolvedStoreID != job.MemoryStoreID {
			return errors.New("sandbox memory projection store identity changed")
		}
		paths := memoryProjectionPlanPaths(inputJSON, resultJSON)
		if len(paths) == 0 {
			current = true
			return nil
		}
		rows, err := tx.Query(ctx,
			`SELECT smr.mount_path
			   FROM session_memory_store_resources smr
			   JOIN session_resources sr
			     ON sr.workspace_id = smr.workspace_id
			    AND sr.session_id = smr.session_id
			    AND sr.resource_id = smr.resource_id
			    AND sr.type = 'memory_store'
			    AND sr.detached_at IS NULL
			    AND sr.delete_requested_at IS NULL
			  WHERE smr.workspace_id = $1 AND smr.session_id = $2 AND smr.memory_store_id = $3
			  ORDER BY smr.resource_id`,
			job.WorkspaceID, job.SessionID, job.MemoryStoreID,
		)
		if err != nil {
			return err
		}
		defer func() { _ = rows.Close() }()
		for rows.Next() {
			var mountPath string
			if err := rows.Scan(&mountPath); err != nil {
				return err
			}
			work.MountPaths = append(work.MountPaths, mountPath)
		}
		if err := rows.Err(); err != nil {
			return err
		}
		heads, err := loadMemoryProjectionHeads(ctx, tx, job.WorkspaceID, job.MemoryStoreID, paths)
		if err != nil {
			return err
		}
		for _, pathValue := range paths {
			if head, ok := heads[pathValue]; ok {
				work.Ops = append(work.Ops, sandboxdriver.MemoryProjectionOp{Kind: "upsert", RelativePath: pathValue, Content: head.content, ContentSHA256: head.digest})
			} else {
				work.Ops = append(work.Ops, sandboxdriver.MemoryProjectionOp{Kind: "remove", RelativePath: pathValue})
			}
		}
		current = true
		return nil
	})
	return work, current, err
}

func (s *PostgreSQLSandboxMemoryProjectionStore) SettleProjection(ctx context.Context, work SandboxMemoryProjectionWork, state string, message string, now time.Time) error {
	if s == nil || s.client == nil {
		return errors.New("sandbox memory projection database is required")
	}
	if state != "refreshed" && state != "skipped_cold" && state != "failed" {
		return errors.New("sandbox memory projection terminal state is invalid")
	}
	return s.client.WithWorkspaceTx(ctx, work.WorkspaceID, "sandbox.memory_projection.settle", func(tx *dbconnect.Tx) error {
		return settleMemoryProjectionTx(ctx, tx, work.MemoryWriteID, state, message, now)
	})
}

func (s *PostgreSQLSandboxMemoryProjectionStore) FinalizeProjectionExhaustion(ctx context.Context, job *queuev1.QueueJob, now time.Time) error {
	if s == nil || s.client == nil {
		return errors.New("sandbox memory projection database is required")
	}
	workspaceID, memoryWriteID, err := decodeMemoryProjectionTransportIdentity(job)
	if err != nil {
		return err
	}
	return s.client.WithWorkspaceTx(ctx, workspaceID, "sandbox.memory_projection.exhaust", func(tx *dbconnect.Tx) error {
		return settleMemoryProjectionTx(ctx, tx, memoryWriteID, "failed", "memory projection attempt budget exhausted", now)
	})
}

func settleMemoryProjectionTx(ctx context.Context, tx *dbconnect.Tx, memoryWriteID string, state string, message string, now time.Time) error {
	var resultJSON string
	var current sql.NullString
	if err := tx.QueryRow(ctx,
		`SELECT result_json, memory_projection_state
		   FROM session_runtime_tool_results
		  WHERE workspace_id = current_setting('tetral.workspace_id', true)
		    AND tool_use_event_id = $1
		    AND tool_kind = 'memory'
		  FOR UPDATE`,
		memoryWriteID,
	).Scan(&resultJSON, &current); dbconnect.IsNoRows(err) {
		return nil
	} else if err != nil {
		return err
	}
	if !current.Valid || current.String != "pending" {
		return nil
	}
	switch state {
	case "refreshed":
		var payload map[string]any
		if err := json.Unmarshal([]byte(resultJSON), &payload); err != nil {
			return err
		}
		payload["projection_refreshed"] = true
		encoded, err := marshalSandboxJSON(payload)
		if err != nil {
			return err
		}
		resultJSON = encoded
	case "failed":
		if strings.TrimSpace(message) == "" {
			message = "memory projection failed"
		}
		encoded, err := marshalSandboxJSON(map[string]any{"status": "runtime_error", "error_code": "projection_refresh_failed", "message": message, "retryable": false})
		if err != nil {
			return err
		}
		resultJSON = encoded
	}
	_, err := tx.Exec(ctx,
		`UPDATE session_runtime_tool_results
		    SET memory_projection_state = $2, result_json = $3, updated_at = $4
		  WHERE workspace_id = current_setting('tetral.workspace_id', true)
		    AND tool_use_event_id = $1
		    AND tool_kind = 'memory'
		    AND memory_projection_state = 'pending'`,
		memoryWriteID, state, resultJSON, now.UTC(),
	)
	return err
}

type projectionHead struct {
	content string
	digest  string
}

func loadMemoryProjectionHeads(ctx context.Context, tx *dbconnect.Tx, workspaceID string, memoryStoreID string, paths []string) (map[string]projectionHead, error) {
	heads := make(map[string]projectionHead, len(paths))
	const batchSize = 10_000
	for offset := 0; offset < len(paths); offset += batchSize {
		end := min(offset+batchSize, len(paths))
		args := []any{workspaceID, memoryStoreID}
		placeholders := make([]string, 0, end-offset)
		for index, pathValue := range paths[offset:end] {
			args = append(args, pathValue)
			placeholders = append(placeholders, "$"+strconv.Itoa(index+3))
		}
		rows, err := tx.Query(ctx,
			`SELECT m.path, v.content, m.content_sha256
			   FROM memories m
			   JOIN memory_versions v
			     ON v.workspace_id = m.workspace_id
			    AND v.memory_store_id = m.memory_store_id
			    AND v.memory_id = m.memory_id
			    AND v.memory_version_id = m.current_version_id
			  WHERE m.workspace_id = $1 AND m.memory_store_id = $2
			    AND m.deleted_at IS NULL AND m.path IN (`+strings.Join(placeholders, ",")+`)`,
			args...,
		)
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			var pathValue string
			var head projectionHead
			if err := rows.Scan(&pathValue, &head.content, &head.digest); err != nil {
				_ = rows.Close()
				return nil, err
			}
			heads[pathValue] = head
		}
		if err := rows.Err(); err != nil {
			_ = rows.Close()
			return nil, err
		}
		if err := rows.Close(); err != nil {
			return nil, err
		}
	}
	return heads, nil
}

type memoryProjectionInput struct {
	Action  string `json:"action"`
	Path    string `json:"path"`
	NewPath string `json:"new_path"`
}

type memoryProjectionResult struct {
	Status         string `json:"status"`
	Action         string `json:"action"`
	Path           string `json:"path"`
	NewPath        string `json:"new_path"`
	ErrorCode      string `json:"error_code"`
	RereadRequired bool   `json:"reread_required"`
	Conflicts      []struct {
		Path string `json:"path"`
	} `json:"conflicts"`
}

func memoryProjectionPlanPaths(inputJSON string, resultJSON string) []string {
	var result memoryProjectionResult
	if json.Unmarshal([]byte(resultJSON), &result) != nil {
		return nil
	}
	var paths []string
	if result.Status == "completed" {
		switch result.Action {
		case "create", "replace", "delete":
			paths = append(paths, result.Path)
		case "rename":
			paths = append(paths, result.Path, result.NewPath)
		}
	} else if result.Status == "tool_error" && result.RereadRequired && isStaleMemoryProjectionError(result.ErrorCode) {
		var input memoryProjectionInput
		if json.Unmarshal([]byte(inputJSON), &input) != nil {
			return nil
		}
		paths = append(paths, input.Path)
		if input.Action == "rename" {
			paths = append(paths, input.NewPath)
		}
		if result.ErrorCode == "path_exists" {
			for _, conflict := range result.Conflicts {
				paths = append(paths, conflict.Path)
			}
		}
	}
	seen := map[string]struct{}{}
	output := make([]string, 0, len(paths))
	for _, relative := range paths {
		pathValue := "/" + strings.TrimPrefix(relative, "/")
		if memory.ValidatePath(pathValue) != nil {
			continue
		}
		if _, exists := seen[pathValue]; exists {
			continue
		}
		seen[pathValue] = struct{}{}
		output = append(output, pathValue)
	}
	return output
}

func isStaleMemoryProjectionError(kind string) bool {
	switch kind {
	case "old_text_not_found", "old_text_not_unique", "expected_text_mismatch", "path_exists", "not_found":
		return true
	default:
		return false
	}
}

func marshalSandboxJSON(value any) (string, error) {
	var body bytes.Buffer
	encoder := json.NewEncoder(&body)
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(value); err != nil {
		return "", err
	}
	return strings.TrimSuffix(body.String(), "\n"), nil
}

func decodeMemoryProjectionTransportIdentity(job *queuev1.QueueJob) (string, string, error) {
	if job == nil || job.GetWorkspaceId() == "" || job.GetKind() != queue.KindSandboxMemoryProjection {
		return "", "", errors.New("sandbox memory projection transport identity is incomplete")
	}
	prefix := queue.KindSandboxMemoryProjection + ":" + job.GetWorkspaceId() + ":"
	if !strings.HasPrefix(job.GetDedupeKey(), prefix) {
		return "", "", errors.New("sandbox memory projection dedupe identity is invalid")
	}
	remainder := strings.TrimPrefix(job.GetDedupeKey(), prefix)
	separator := strings.LastIndex(remainder, ":")
	if separator <= 0 || separator == len(remainder)-1 {
		return "", "", errors.New("sandbox memory projection dedupe identity is invalid")
	}
	memoryStoreID, memoryWriteID := remainder[:separator], remainder[separator+1:]
	workspaceID := queueWorkspaceID(job.GetWorkspaceId())
	if job.GetPartitionKey() != queue.FormatSandboxMemoryPartitionKey(workspaceID, memoryStoreID) || job.GetDedupeKey() != queue.FormatSandboxMemoryProjectionDedupeKey(workspaceID, memoryStoreID, memoryWriteID) {
		return "", "", errors.New("sandbox memory projection transport identity is invalid")
	}
	return job.GetWorkspaceId(), memoryWriteID, nil
}
