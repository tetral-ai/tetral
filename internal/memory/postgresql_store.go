package memory

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/id"
	"github.com/tetral-ai/tetral/internal/storage"
	"github.com/tetral-ai/tetral/internal/workspace"
)

const (
	MaxMemoryStoresPerWorkspace           = 100
	MaxMemoriesPerStore                   = 10000
	MaxMemoryVersionsPerStore             = 50000
	MaxRetainedMemoryPayloadBytesPerStore = 100 * 1024 * 1024
	deleteResultType                      = "memory_deleted"
	memoryType                            = "memory"
	memoryVersionType                     = "memory_version"
	memoryStoreDeletedErrorText           = "memory store not found"
)

type durableQuotaOwner struct{}

// DurableWriteQuotas owns quota admission for every durable Memory write path.
var DurableWriteQuotas durableQuotaOwner

type DurableQuotaTransaction interface {
	QueryRowScanner(ctx context.Context, query string, args ...any) interface{ Scan(dest ...any) error }
}

type PostgreSQLMemoryStore struct {
	client *dbconnect.Client
}

func NewPostgreSQLStore(runtimeClient *dbconnect.Client) *PostgreSQLMemoryStore {
	return &PostgreSQLMemoryStore{client: runtimeClient}
}

func (s *PostgreSQLMemoryStore) CreateStore(ctx context.Context, ws workspace.ID, request CreateStoreRequest) (*Store, error) {
	if ws == "" {
		return nil, &ValidationError{Message: "workspace_id is required"}
	}
	name, err := normalizeStoreName(request.Name)
	if err != nil {
		return nil, err
	}
	description, err := normalizeStoreDescription(request.Description)
	if err != nil {
		return nil, err
	}
	metadata, err := normalizeMetadata(request.Metadata)
	if err != nil {
		return nil, err
	}
	metadataJSON, err := json.Marshal(metadata)
	if err != nil {
		return nil, err
	}
	now := storage.Now().Format(time.RFC3339)
	storeID := id.New("memstore_")
	if err := s.client.WithWorkspaceTx(ctx, string(ws), "memory.transaction", func(tx *dbconnect.Tx) error {
		if err := storage.AcquireWorkspaceMemoryStoreCreateLock(ctx, tx, string(ws)); err != nil {
			return err
		}
		var storeCount int
		if err := tx.QueryRow(ctx,
			`SELECT count(*) FROM memory_stores WHERE workspace_id = $1`,
			string(ws)).Scan(&storeCount); err != nil {
			return err
		}
		if storeCount >= MaxMemoryStoresPerWorkspace {
			return &QuotaError{Message: "memory store quota exceeded"}
		}
		_, execErr := tx.Exec(ctx,
			`INSERT INTO memory_stores (workspace_id, memory_store_id, name, description, metadata_json, created_at, updated_at)
			 VALUES ($1, $2, $3, $4, $5, $6, $6)`,
			string(ws), storeID, name, description, string(metadataJSON), now)
		return execErr
	}); err != nil {
		return nil, err
	}
	return &Store{ID: storeID, Type: "memory_store", Name: name, Description: description, Metadata: metadata, CreatedAt: now, UpdatedAt: now}, nil
}

func (s *PostgreSQLMemoryStore) GetStore(ctx context.Context, ws workspace.ID, storeID string) (*Store, error) {
	if ws == "" {
		return nil, &ValidationError{Message: "workspace_id is required"}
	}
	var store *Store
	err := s.client.WithWorkspaceTx(ctx, string(ws), "memory.transaction", func(tx *dbconnect.Tx) error {
		row := tx.QueryRow(ctx,
			`SELECT memory_store_id, name, description, metadata_json, created_at, updated_at, archived_at
			   FROM memory_stores WHERE workspace_id = $1 AND memory_store_id = $2`,
			string(ws), storeID)
		scanned, scanErr := scanStore(row)
		if scanErr == sql.ErrNoRows {
			return &NotFoundError{Message: "memory store not found"}
		}
		if scanErr != nil {
			return scanErr
		}
		store = scanned
		return nil
	})
	if err != nil {
		return nil, err
	}
	return store, nil
}

func (s *PostgreSQLMemoryStore) UpdateStore(ctx context.Context, ws workspace.ID, storeID string, patch StorePatch) (*Store, error) {
	if ws == "" {
		return nil, &ValidationError{Message: "workspace_id is required"}
	}
	if !patch.MutablePresent {
		return nil, &ValidationError{Message: "at least one mutable field is required"}
	}
	var result *Store
	err := s.client.WithWorkspaceTx(ctx, string(ws), "memory.transaction", func(tx *dbconnect.Tx) error {
		if err := storage.AcquireMemoryStoreMutationLock(ctx, tx, string(ws), storeID); err != nil {
			return err
		}
		row := tx.QueryRow(ctx,
			`SELECT memory_store_id, name, description, metadata_json, created_at, updated_at, archived_at
			   FROM memory_stores WHERE workspace_id = $1 AND memory_store_id = $2 FOR UPDATE`,
			string(ws), storeID)
		current, scanErr := scanStore(row)
		if scanErr == sql.ErrNoRows {
			return &NotFoundError{Message: "memory store not found"}
		}
		if scanErr != nil {
			return scanErr
		}
		if current.ArchivedAt != nil {
			return &ValidationError{Message: "memory store is archived"}
		}
		next, materializeErr := patch.Materialize(*current)
		if materializeErr != nil {
			return materializeErr
		}
		if current.Name == next.Name && current.Description == next.Description && equalMetadata(current.Metadata, next.Metadata) {
			result = current
			return nil
		}
		metadataJSON, marshalErr := json.Marshal(next.Metadata)
		if marshalErr != nil {
			return marshalErr
		}
		now := storage.Now().Format(time.RFC3339)
		row = tx.QueryRow(ctx,
			`UPDATE memory_stores
			    SET name = $1, description = $2, metadata_json = $3, updated_at = $4
			  WHERE workspace_id = $5 AND memory_store_id = $6 AND archived_at IS NULL
			  RETURNING memory_store_id, name, description, metadata_json, created_at, updated_at, archived_at`,
			next.Name, next.Description, string(metadataJSON), now, string(ws), storeID)
		updated, scanErr := scanStore(row)
		if scanErr != nil {
			return scanErr
		}
		result = updated
		return nil
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (s *PostgreSQLMemoryStore) ListStores(ctx context.Context, ws workspace.ID, options ListStoresOptions) (StoreListResult, error) {
	if ws == "" {
		return StoreListResult{}, &ValidationError{Message: "workspace_id is required"}
	}
	if !options.LimitSet && options.Limit == 0 {
		options.Limit = 20
	}
	if options.Limit <= 0 {
		return StoreListResult{}, &ValidationError{Message: "limit must be between 1 and 100"}
	}
	if options.Limit > 100 {
		options.Limit = 100
	}
	var token storePageToken
	if options.Page != "" {
		var err error
		token, err = decodeStorePageToken(options.Page, ws, options)
		if err != nil {
			return StoreListResult{}, err
		}
	}
	var rowsRead []*Store
	err := s.client.WithWorkspaceTx(ctx, string(ws), "memory.transaction", func(tx *dbconnect.Tx) error {
		query := `SELECT memory_store_id, name, description, metadata_json, created_at, updated_at, archived_at
		            FROM memory_stores
		           WHERE workspace_id = $1`
		args := []any{string(ws)}
		if !options.IncludeArchived {
			query += ` AND archived_at IS NULL`
		}
		if options.CreatedAtGTE != "" {
			args = append(args, options.CreatedAtGTE)
			query += ` AND created_at >= $` + sqlArg(len(args))
		}
		if options.CreatedAtLTE != "" {
			args = append(args, options.CreatedAtLTE)
			query += ` AND created_at <= $` + sqlArg(len(args))
		}
		if token.LastCreatedAt != "" {
			args = append(args, token.LastCreatedAt, token.LastID)
			query += ` AND (created_at, memory_store_id) > ($` + sqlArg(len(args)-1) + `, $` + sqlArg(len(args)) + `)`
		}
		args = append(args, options.Limit+1)
		// Placeholder numbers are generated from the internal args slice length; no user input is concatenated into SQL text.
		//nolint:gosec
		query += ` ORDER BY created_at ASC, memory_store_id ASC LIMIT $` + sqlArg(len(args))
		rows, queryErr := tx.Query(ctx, query, args...)
		if queryErr != nil {
			return queryErr
		}
		defer func() { _ = rows.Close() }()
		for rows.Next() {
			store, scanErr := scanStore(rows)
			if scanErr != nil {
				return scanErr
			}
			rowsRead = append(rowsRead, store)
		}
		return rows.Err()
	})
	if err != nil {
		return StoreListResult{}, err
	}
	var nextPage *string
	if len(rowsRead) > options.Limit {
		rowsRead = rowsRead[:options.Limit]
		last := rowsRead[len(rowsRead)-1]
		encoded, err := encodeStorePageToken(storePageToken{
			Version:         storePageTokenVersion,
			Resource:        storePageTokenResource,
			WorkspaceID:     string(ws),
			IncludeArchived: options.IncludeArchived,
			CreatedAtGTE:    options.CreatedAtGTE,
			CreatedAtLTE:    options.CreatedAtLTE,
			LastCreatedAt:   last.CreatedAt,
			LastID:          last.ID,
		})
		if err != nil {
			return StoreListResult{}, err
		}
		nextPage = &encoded
	}
	return StoreListResult{Data: rowsRead, NextPage: nextPage}, nil
}

func (s *PostgreSQLMemoryStore) ArchiveStore(ctx context.Context, ws workspace.ID, storeID string) (*Store, error) {
	if ws == "" {
		return nil, &ValidationError{Message: "workspace_id is required"}
	}
	var result *Store
	err := s.client.WithWorkspaceTx(ctx, string(ws), "memory.transaction", func(tx *dbconnect.Tx) error {
		if err := storage.AcquireMemoryStoreMutationLock(ctx, tx, string(ws), storeID); err != nil {
			return err
		}
		now := storage.Now().Format(time.RFC3339)
		row := tx.QueryRow(ctx,
			`UPDATE memory_stores
			    SET archived_at = COALESCE(archived_at, $1),
			        updated_at = CASE WHEN archived_at IS NULL THEN $1 ELSE updated_at END
			  WHERE workspace_id = $2 AND memory_store_id = $3
			  RETURNING memory_store_id, name, description, metadata_json, created_at, updated_at, archived_at`,
			now, string(ws), storeID)
		archived, scanErr := scanStore(row)
		if scanErr == sql.ErrNoRows {
			return &NotFoundError{Message: "memory store not found"}
		}
		if scanErr != nil {
			return scanErr
		}
		result = archived
		return nil
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (s *PostgreSQLMemoryStore) DeleteStore(ctx context.Context, ws workspace.ID, storeID string) error {
	if ws == "" {
		return &ValidationError{Message: "workspace_id is required"}
	}
	return s.client.WithWorkspaceTx(ctx, string(ws), "memory.transaction", func(tx *dbconnect.Tx) error {
		if err := storage.AcquireMemoryStoreMutationLock(ctx, tx, string(ws), storeID); err != nil {
			return err
		}
		var referenced int
		refErr := tx.QueryRow(ctx,
			`SELECT 1 FROM session_memory_store_resources WHERE workspace_id = $1 AND memory_store_id = $2 LIMIT 1`,
			string(ws), storeID,
		).Scan(&referenced)
		if refErr != nil && refErr != sql.ErrNoRows {
			return refErr
		}
		if refErr == nil {
			return &ValidationError{Message: "memory store has session references"}
		}
		result, execErr := tx.Exec(ctx,
			`DELETE FROM memory_stores WHERE workspace_id = $1 AND memory_store_id = $2`,
			string(ws), storeID)
		if execErr != nil {
			var pgErr *pgconn.PgError
			if errors.As(execErr, &pgErr) && (pgErr.Code == "23503" || pgErr.Code == "23001") {
				return &ValidationError{Message: "memory store has session references"}
			}
			return execErr
		}
		affected, _ := result.RowsAffected()
		if affected == 0 {
			return &NotFoundError{Message: "memory store not found"}
		}
		return nil
	})
}

func (s *PostgreSQLMemoryStore) CreateMemory(ctx context.Context, ws workspace.ID, storeID string, request CreateMemoryRequest, actor Actor) (*Memory, error) {
	if ws == "" {
		return nil, &ValidationError{Message: "workspace_id is required"}
	}
	if !request.ContentSet {
		return nil, &ValidationError{Message: "content is required"}
	}
	if err := ValidatePath(request.Path); err != nil {
		return nil, err
	}
	if len([]byte(request.Content)) > memoryContentMaxBytes {
		return nil, &ValidationError{Message: "content must be at most 102400 bytes"}
	}
	if err := validateActor(actor); err != nil {
		return nil, err
	}
	view := normalizeView(request.View)
	contentHash := sha256Hex(request.Content)
	contentSize := int64(len([]byte(request.Content)))
	memoryID := id.New("mem_")
	versionID := id.New("memver_")
	var now string
	var result *Memory
	err := s.client.WithWorkspaceTx(ctx, string(ws), "memory.transaction", func(tx *dbconnect.Tx) error {
		if err := storage.AcquireMemoryStoreMutationLock(ctx, tx, string(ws), storeID); err != nil {
			return err
		}
		if err := requireActiveStore(ctx, tx, string(ws), storeID); err != nil {
			return err
		}
		conflictID, conflictPath, conflictErr := activeMemoryPathConflict(ctx, tx, string(ws), storeID, request.Path, "")
		if conflictErr == nil {
			return &PathConflictError{Message: "memory path already exists", ConflictingMemoryID: conflictID, ConflictingPath: conflictPath}
		}
		if conflictErr != nil && conflictErr != sql.ErrNoRows {
			return conflictErr
		}
		if err := DurableWriteQuotas.EnforceCreate(ctx, tx, string(ws), storeID, contentSize); err != nil {
			return err
		}
		now = storage.Now().Format(time.RFC3339)
		if _, err := tx.Exec(ctx,
			`INSERT INTO memories (workspace_id, memory_store_id, memory_id, current_version_id, path, content_sha256, content_size_bytes, created_at, updated_at)
			 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $8)`,
			string(ws), storeID, memoryID, versionID, request.Path, contentHash, contentSize, now); err != nil {
			return err
		}
		_, err := tx.Exec(ctx,
			`INSERT INTO memory_versions (workspace_id, memory_store_id, memory_id, memory_version_id, operation, path, content, content_sha256, content_size_bytes, created_at, created_actor_type, created_api_key_id, created_session_id, created_user_id)
			 VALUES ($1, $2, $3, $4, 'created', $5, $6, $7, $8, $9, $10, $11, $12, $13)`,
			string(ws), storeID, memoryID, versionID, request.Path, request.Content, contentHash, contentSize, now,
			actor.Type, nullable(actor.APIKeyID), nullable(actor.SessionID), nullable(actor.UserID))
		return err
	})
	if err != nil {
		return nil, err
	}
	result = &Memory{
		ID:               memoryID,
		Type:             memoryType,
		MemoryStoreID:    storeID,
		MemoryVersionID:  versionID,
		Path:             request.Path,
		ContentSHA256:    contentHash,
		ContentSizeBytes: contentSize,
		CreatedAt:        now,
		UpdatedAt:        now,
	}
	if view == ViewFull {
		result.Content = &request.Content
	}
	return result, nil
}

func (s *PostgreSQLMemoryStore) GetMemory(ctx context.Context, ws workspace.ID, storeID string, memoryID string, view string) (*Memory, error) {
	if ws == "" {
		return nil, &ValidationError{Message: "workspace_id is required"}
	}
	view = normalizeView(view)
	var result *Memory
	err := s.client.WithWorkspaceTx(ctx, string(ws), "memory.transaction", func(tx *dbconnect.Tx) error {
		query := `SELECT m.memory_id, m.memory_store_id, m.current_version_id, m.path, m.content_sha256, m.content_size_bytes, m.created_at, m.updated_at`
		if view == ViewFull {
			query += `, v.content`
		}
		query += ` FROM memories m`
		if view == ViewFull {
			query += ` JOIN memory_versions v ON v.workspace_id = m.workspace_id AND v.memory_store_id = m.memory_store_id AND v.memory_id = m.memory_id AND v.memory_version_id = m.current_version_id`
		}
		query += ` WHERE m.workspace_id = $1 AND m.memory_store_id = $2 AND m.memory_id = $3 AND m.deleted_at IS NULL`
		row := tx.QueryRow(ctx, query, string(ws), storeID, memoryID)
		memory, scanErr := scanMemory(row, view == ViewFull)
		if scanErr == sql.ErrNoRows {
			return &NotFoundError{Message: "memory not found"}
		}
		if scanErr != nil {
			return scanErr
		}
		result = memory
		return nil
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (s *PostgreSQLMemoryStore) UpdateMemory(ctx context.Context, ws workspace.ID, storeID string, memoryID string, request UpdateMemoryRequest, actor Actor) (*Memory, error) {
	if ws == "" {
		return nil, &ValidationError{Message: "workspace_id is required"}
	}
	if request.ContentSet && len([]byte(request.Content)) > memoryContentMaxBytes {
		return nil, &ValidationError{Message: "content must be at most 102400 bytes"}
	}
	if request.Path != nil {
		if err := ValidatePath(*request.Path); err != nil {
			return nil, err
		}
	}
	if request.Precondition != nil && request.Precondition.Type != PreconditionContentSHA256 {
		return nil, &ValidationError{Message: "precondition type must be content_sha256"}
	}
	if request.Precondition != nil {
		if err := validateSHA256Hex(request.Precondition.ContentSHA256); err != nil {
			return nil, err
		}
	}
	if err := validateActor(actor); err != nil {
		return nil, err
	}
	view := normalizeView(request.View)
	versionID := id.New("memver_")
	var now string
	var result *Memory
	err := s.client.WithWorkspaceTx(ctx, string(ws), "memory.transaction", func(tx *dbconnect.Tx) error {
		if err := storage.AcquireMemoryStoreMutationLock(ctx, tx, string(ws), storeID); err != nil {
			return err
		}
		if err := requireActiveStore(ctx, tx, string(ws), storeID); err != nil {
			return err
		}
		current, err := selectCurrentMemoryForUpdate(ctx, tx, string(ws), storeID, memoryID)
		if err != nil {
			return err
		}
		targetPath := current.Path
		if request.Path != nil {
			targetPath = *request.Path
		}
		targetContent := current.Content
		if request.ContentSet {
			targetContent = request.Content
		}
		targetHash := sha256Hex(targetContent)
		targetSize := int64(len([]byte(targetContent)))
		if targetPath == current.Path && targetContent == current.Content {
			result = memoryFromCurrent(current, view)
			return nil
		}
		if request.Precondition != nil && request.Precondition.ContentSHA256 != current.ContentSHA256 {
			return &PreconditionFailedError{Message: "memory precondition failed"}
		}
		if targetPath != current.Path {
			conflictID, conflictPath, conflictErr := activeMemoryPathConflict(ctx, tx, string(ws), storeID, targetPath, memoryID)
			if conflictErr == nil {
				return &PathConflictError{Message: "memory path already exists", ConflictingMemoryID: conflictID, ConflictingPath: conflictPath}
			}
			if conflictErr != nil && conflictErr != sql.ErrNoRows {
				return conflictErr
			}
		}
		if err := DurableWriteQuotas.EnforceContentVersion(ctx, tx, string(ws), storeID, targetSize); err != nil {
			return err
		}
		now = storage.Now().Format(time.RFC3339)
		if _, err := tx.Exec(ctx,
			`UPDATE memories
			    SET current_version_id = $1, path = $2, content_sha256 = $3, content_size_bytes = $4, updated_at = $5
			  WHERE workspace_id = $6 AND memory_store_id = $7 AND memory_id = $8 AND deleted_at IS NULL`,
			versionID, targetPath, targetHash, targetSize, now, string(ws), storeID, memoryID); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx,
			`INSERT INTO memory_versions (workspace_id, memory_store_id, memory_id, memory_version_id, operation, path, content, content_sha256, content_size_bytes, created_at, created_actor_type, created_api_key_id, created_session_id, created_user_id)
			 VALUES ($1, $2, $3, $4, 'modified', $5, $6, $7, $8, $9, $10, $11, $12, $13)`,
			string(ws), storeID, memoryID, versionID, targetPath, targetContent, targetHash, targetSize, now,
			actor.Type, nullable(actor.APIKeyID), nullable(actor.SessionID), nullable(actor.UserID)); err != nil {
			return err
		}
		result = &Memory{
			ID:               memoryID,
			Type:             memoryType,
			MemoryStoreID:    storeID,
			MemoryVersionID:  versionID,
			Path:             targetPath,
			ContentSHA256:    targetHash,
			ContentSizeBytes: targetSize,
			CreatedAt:        current.CreatedAt,
			UpdatedAt:        now,
		}
		if view == ViewFull {
			result.Content = &targetContent
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

func activeMemoryPathConflict(ctx context.Context, tx *dbconnect.Tx, workspaceID string, storeID string, path string, excludeMemoryID string) (string, string, error) {
	var conflictID string
	var conflictPath string
	err := tx.QueryRow(ctx,
		`SELECT memory_id, path FROM memories
		  WHERE workspace_id = $1
		    AND memory_store_id = $2
		    AND deleted_at IS NULL
		    AND ($4 = '' OR memory_id <> $4)
		    AND (path = $3
		      OR left(path, length($3) + 1) = $3 || '/'
		      OR left($3, length(path) + 1) = path || '/')
		  ORDER BY CASE WHEN path = $3 THEN 0 ELSE 1 END, path
		  LIMIT 1`,
		workspaceID,
		storeID,
		path,
		excludeMemoryID,
	).Scan(&conflictID, &conflictPath)
	return conflictID, conflictPath, err
}

func (s *PostgreSQLMemoryStore) DeleteMemory(ctx context.Context, ws workspace.ID, storeID string, memoryID string, expectedHash *string, actor Actor) (*DeleteMemoryResult, error) {
	if ws == "" {
		return nil, &ValidationError{Message: "workspace_id is required"}
	}
	if err := validateActor(actor); err != nil {
		return nil, err
	}
	versionID := id.New("memver_")
	var now string
	err := s.client.WithWorkspaceTx(ctx, string(ws), "memory.transaction", func(tx *dbconnect.Tx) error {
		if err := storage.AcquireMemoryStoreMutationLock(ctx, tx, string(ws), storeID); err != nil {
			return err
		}
		if err := requireActiveStore(ctx, tx, string(ws), storeID); err != nil {
			return err
		}
		var path, currentHash string
		var size int64
		row := tx.QueryRow(ctx,
			`SELECT path, content_sha256, content_size_bytes
			   FROM memories
			  WHERE workspace_id = $1 AND memory_store_id = $2 AND memory_id = $3 AND deleted_at IS NULL
			  FOR UPDATE`,
			string(ws), storeID, memoryID)
		if err := row.Scan(&path, &currentHash, &size); err == sql.ErrNoRows {
			return &NotFoundError{Message: "memory not found"}
		} else if err != nil {
			return err
		}
		if expectedHash != nil && *expectedHash != currentHash {
			return &PreconditionFailedError{Message: "memory precondition failed"}
		}
		if err := DurableWriteQuotas.EnforceDeletionVersion(ctx, tx, string(ws), storeID); err != nil {
			return err
		}
		now = storage.Now().Format(time.RFC3339)
		if _, err := tx.Exec(ctx,
			`UPDATE memories
			    SET current_version_id = $1, content_sha256 = NULL, content_size_bytes = NULL, updated_at = $2, deleted_at = $2
			  WHERE workspace_id = $3 AND memory_store_id = $4 AND memory_id = $5`,
			versionID, now, string(ws), storeID, memoryID); err != nil {
			return err
		}
		_, err := tx.Exec(ctx,
			`INSERT INTO memory_versions (workspace_id, memory_store_id, memory_id, memory_version_id, operation, path, created_at, created_actor_type, created_api_key_id, created_session_id, created_user_id)
			 VALUES ($1, $2, $3, $4, 'deleted', $5, $6, $7, $8, $9, $10)`,
			string(ws), storeID, memoryID, versionID, path, now,
			actor.Type, nullable(actor.APIKeyID), nullable(actor.SessionID), nullable(actor.UserID))
		return err
	})
	if err != nil {
		return nil, err
	}
	return &DeleteMemoryResult{ID: memoryID, Type: deleteResultType}, nil
}

func (s *PostgreSQLMemoryStore) ListMemories(ctx context.Context, ws workspace.ID, storeID string, options ListMemoriesOptions) (MemoryListResult, error) {
	if ws == "" {
		return MemoryListResult{}, &ValidationError{Message: "workspace_id is required"}
	}
	normalized, err := normalizeListMemoriesOptions(options)
	if err != nil {
		return MemoryListResult{}, err
	}
	options = normalized
	var token memoryPageToken
	if options.Page != "" {
		token, err = decodeMemoryPageToken(options.Page, ws, storeID, options)
		if err != nil {
			return MemoryListResult{}, err
		}
	}
	var entries []memoryListProjection
	hasNext := false
	err = s.client.WithWorkspaceTx(ctx, string(ws), "memory.transaction", func(tx *dbconnect.Tx) error {
		if err := requireStoreExists(ctx, tx, string(ws), storeID); err != nil {
			return err
		}
		rows, queryErr := tx.Query(ctx,
			`SELECT memory_id, memory_store_id, current_version_id, path, content_sha256, content_size_bytes, created_at, updated_at
			   FROM memories
			  WHERE workspace_id = $1 AND memory_store_id = $2 AND deleted_at IS NULL
			  ORDER BY path, memory_id`,
			string(ws), storeID)
		if queryErr != nil {
			return queryErr
		}
		defer func() { _ = rows.Close() }()
		var active []memoryListProjection
		for rows.Next() {
			memory, scanErr := scanMemory(rows, false)
			if scanErr != nil {
				return scanErr
			}
			if !strings.HasPrefix(memory.Path, options.PathPrefix) {
				continue
			}
			active = append(active, memoryListProjection{Memory: memory, SortPath: memory.Path, SortCreatedAt: memory.CreatedAt, SortUpdatedAt: memory.UpdatedAt})
		}
		if err := rows.Err(); err != nil {
			return err
		}
		entries = projectMemoryList(active, options)
		sortMemoryList(entries, options)
		start := 0
		if token.LastOrderValue != "" {
			start = findMemoryPageTokenStart(entries, token, options)
		}
		if start > len(entries) {
			start = len(entries)
		}
		end := start + options.Limit + 1
		if end > len(entries) {
			end = len(entries)
		}
		entries = entries[start:end]
		if len(entries) > options.Limit {
			hasNext = true
			entries = entries[:options.Limit]
		}
		if options.View == ViewFull {
			if err := hydrateMemoryListContent(ctx, tx, string(ws), storeID, entries); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return MemoryListResult{}, err
	}
	var nextPage *string
	if hasNext {
		last := entries[len(entries)-1]
		encoded, err := encodeMemoryPageToken(memoryPageToken{
			Version:        memoryPageTokenVersion,
			Resource:       memoryPageTokenResource,
			WorkspaceID:    string(ws),
			MemoryStoreID:  storeID,
			PathPrefix:     options.PathPrefix,
			Depth:          options.Depth,
			DepthSet:       options.DepthSet,
			View:           options.View,
			OrderBy:        options.OrderBy,
			Order:          options.Order,
			LastOrderValue: last.OrderValue(options.OrderBy),
			LastPath:       last.Path(),
			LastType:       last.Type(),
			LastMemoryID:   last.MemoryID(),
		})
		if err != nil {
			return MemoryListResult{}, err
		}
		nextPage = &encoded
	}
	result := MemoryListResult{NextPage: nextPage}
	for _, entry := range entries {
		result.Data = append(result.Data, entry.Entry)
	}
	return result, nil
}

func (s *PostgreSQLMemoryStore) GetMemoryVersion(ctx context.Context, ws workspace.ID, storeID string, versionID string, view string) (*MemoryVersion, error) {
	if ws == "" {
		return nil, &ValidationError{Message: "workspace_id is required"}
	}
	view = normalizeView(view)
	var result *MemoryVersion
	err := s.client.WithWorkspaceTx(ctx, string(ws), "memory.transaction", func(tx *dbconnect.Tx) error {
		row := tx.QueryRow(ctx,
			`SELECT memory_version_id, memory_store_id, memory_id, operation, path, content, content_sha256, content_size_bytes, created_at,
			        created_actor_type, created_api_key_id, created_session_id, created_user_id,
			        redacted_at, redacted_actor_type, redacted_api_key_id, redacted_session_id, redacted_user_id
			   FROM memory_versions
			  WHERE workspace_id = $1 AND memory_store_id = $2 AND memory_version_id = $3`,
			string(ws), storeID, versionID)
		version, scanErr := scanMemoryVersion(row, view == ViewFull)
		if scanErr == sql.ErrNoRows {
			return &NotFoundError{Message: "memory version not found"}
		}
		if scanErr != nil {
			return scanErr
		}
		result = version
		return nil
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (s *PostgreSQLMemoryStore) ListMemoryVersions(ctx context.Context, ws workspace.ID, storeID string, options ListMemoryVersionsOptions) (MemoryVersionListResult, error) {
	if ws == "" {
		return MemoryVersionListResult{}, &ValidationError{Message: "workspace_id is required"}
	}
	normalized, err := normalizeListMemoryVersionsOptions(options)
	if err != nil {
		return MemoryVersionListResult{}, err
	}
	options = normalized
	var token versionPageToken
	if options.Page != "" {
		token, err = decodeVersionPageToken(options.Page, ws, storeID, options)
		if err != nil {
			return MemoryVersionListResult{}, err
		}
	}
	var rowsRead []*MemoryVersion
	err = s.client.WithWorkspaceTx(ctx, string(ws), "memory.transaction", func(tx *dbconnect.Tx) error {
		if err := requireStoreExists(ctx, tx, string(ws), storeID); err != nil {
			return err
		}
		query := `SELECT memory_version_id, memory_store_id, memory_id, operation, path, content, content_sha256, content_size_bytes, created_at,
			        created_actor_type, created_api_key_id, created_session_id, created_user_id,
			        redacted_at, redacted_actor_type, redacted_api_key_id, redacted_session_id, redacted_user_id
			   FROM memory_versions
			  WHERE workspace_id = $1 AND memory_store_id = $2`
		args := []any{string(ws), storeID}
		if options.MemoryID != "" {
			args = append(args, options.MemoryID)
			query += ` AND memory_id = $` + sqlArg(len(args))
		}
		if options.Operation != "" {
			args = append(args, options.Operation)
			query += ` AND operation = $` + sqlArg(len(args))
		}
		if options.SessionID != "" {
			args = append(args, options.SessionID)
			query += ` AND created_session_id = $` + sqlArg(len(args))
		}
		if options.APIKeyID != "" {
			args = append(args, options.APIKeyID)
			query += ` AND created_api_key_id = $` + sqlArg(len(args))
		}
		if options.CreatedAtGTE != "" {
			args = append(args, options.CreatedAtGTE)
			query += ` AND created_at >= $` + sqlArg(len(args))
		}
		if options.CreatedAtLTE != "" {
			args = append(args, options.CreatedAtLTE)
			query += ` AND created_at <= $` + sqlArg(len(args))
		}
		if token.LastCreatedAt != "" {
			args = append(args, token.LastCreatedAt, token.LastID)
			query += ` AND (created_at, memory_version_id) < ($` + sqlArg(len(args)-1) + `, $` + sqlArg(len(args)) + `)`
		}
		args = append(args, options.Limit+1)
		//nolint:gosec // SQL text is built from static fragments and generated placeholders only.
		query += ` ORDER BY created_at DESC, memory_version_id DESC LIMIT $` + sqlArg(len(args))
		rows, queryErr := tx.Query(ctx, query, args...)
		if queryErr != nil {
			return queryErr
		}
		defer func() { _ = rows.Close() }()
		for rows.Next() {
			version, scanErr := scanMemoryVersion(rows, options.View == ViewFull)
			if scanErr != nil {
				return scanErr
			}
			rowsRead = append(rowsRead, version)
		}
		return rows.Err()
	})
	if err != nil {
		return MemoryVersionListResult{}, err
	}
	var nextPage *string
	if len(rowsRead) > options.Limit {
		rowsRead = rowsRead[:options.Limit]
		last := rowsRead[len(rowsRead)-1]
		encoded, err := encodeVersionPageToken(versionPageToken{
			Version:       versionPageTokenVersion,
			Resource:      versionPageTokenResource,
			WorkspaceID:   string(ws),
			MemoryStoreID: storeID,
			MemoryID:      options.MemoryID,
			Operation:     options.Operation,
			SessionID:     options.SessionID,
			APIKeyID:      options.APIKeyID,
			CreatedAtGTE:  options.CreatedAtGTE,
			CreatedAtLTE:  options.CreatedAtLTE,
			View:          options.View,
			LastCreatedAt: last.CreatedAt,
			LastID:        last.ID,
		})
		if err != nil {
			return MemoryVersionListResult{}, err
		}
		nextPage = &encoded
	}
	return MemoryVersionListResult{Data: rowsRead, NextPage: nextPage}, nil
}

func (s *PostgreSQLMemoryStore) RedactMemoryVersion(ctx context.Context, ws workspace.ID, storeID string, versionID string, actor Actor) (*MemoryVersion, error) {
	if ws == "" {
		return nil, &ValidationError{Message: "workspace_id is required"}
	}
	if err := validateActor(actor); err != nil {
		return nil, err
	}
	var now string
	var result *MemoryVersion
	err := s.client.WithWorkspaceTx(ctx, string(ws), "memory.transaction", func(tx *dbconnect.Tx) error {
		if err := storage.AcquireMemoryStoreMutationLock(ctx, tx, string(ws), storeID); err != nil {
			return err
		}
		if err := requireActiveStore(ctx, tx, string(ws), storeID); err != nil {
			return err
		}
		version, err := selectMemoryVersionForUpdate(ctx, tx, string(ws), storeID, versionID)
		if err != nil {
			return err
		}
		if version.RedactedAt != nil {
			result = version
			return nil
		}
		var liveHead bool
		if err := tx.QueryRow(ctx,
			`SELECT EXISTS (
				SELECT 1 FROM memories
				 WHERE workspace_id = $1 AND memory_store_id = $2 AND memory_id = $3
				   AND current_version_id = $4 AND deleted_at IS NULL
			)`, string(ws), storeID, version.MemoryID, versionID).Scan(&liveHead); err != nil {
			return err
		}
		if liveHead {
			return &ValidationError{Message: "cannot redact current live memory head"}
		}
		now = storage.Now().Format(time.RFC3339)
		row := tx.QueryRow(ctx,
			`UPDATE memory_versions
			    SET path = NULL, content = NULL, content_sha256 = NULL, content_size_bytes = NULL,
			        redacted_at = $1, redacted_actor_type = $2, redacted_api_key_id = $3, redacted_session_id = $4, redacted_user_id = $5
			  WHERE workspace_id = $6 AND memory_store_id = $7 AND memory_version_id = $8
			  RETURNING memory_version_id, memory_store_id, memory_id, operation, path, content, content_sha256, content_size_bytes, created_at,
			        created_actor_type, created_api_key_id, created_session_id, created_user_id,
			        redacted_at, redacted_actor_type, redacted_api_key_id, redacted_session_id, redacted_user_id`,
			now, actor.Type, nullable(actor.APIKeyID), nullable(actor.SessionID), nullable(actor.UserID), string(ws), storeID, versionID)
		redacted, scanErr := scanMemoryVersion(row, true)
		if scanErr != nil {
			return scanErr
		}
		result = redacted
		return nil
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

type rowScanner interface {
	Scan(dest ...any) error
}

type memoryListProjection struct {
	Entry         MemoryListEntry
	Memory        *Memory
	SortPath      string
	SortCreatedAt string
	SortUpdatedAt string
}

func (p memoryListProjection) Path() string {
	return p.Entry.Path()
}

func (p memoryListProjection) Type() string {
	return p.Entry.Type()
}

func (p memoryListProjection) MemoryID() string {
	if p.Memory != nil {
		return p.Memory.ID
	}
	return ""
}

func (p memoryListProjection) OrderValue(orderBy string) string {
	switch orderBy {
	case MemoryOrderByCreatedAt:
		return p.SortCreatedAt
	case MemoryOrderByUpdatedAt:
		return p.SortUpdatedAt
	default:
		return p.Path()
	}
}

func normalizeListMemoriesOptions(options ListMemoriesOptions) (ListMemoriesOptions, error) {
	if options.View == "" {
		options.View = ViewBasic
	}
	if options.View != ViewBasic && options.View != ViewFull {
		return ListMemoriesOptions{}, &ValidationError{Message: "view must be basic or full"}
	}
	if options.OrderBy == "" {
		options.OrderBy = MemoryOrderByPath
	}
	switch options.OrderBy {
	case MemoryOrderByPath, MemoryOrderByCreatedAt, MemoryOrderByUpdatedAt:
	default:
		return ListMemoriesOptions{}, &ValidationError{Message: "order_by must be path, created_at, or updated_at"}
	}
	if options.Order == "" {
		options.Order = SortAscending
	}
	if options.Order != SortAscending && options.Order != SortDescending {
		return ListMemoriesOptions{}, &ValidationError{Message: "order must be asc or desc"}
	}
	if options.PathPrefix == "" {
		options.PathPrefix = "/"
	}
	if err := ValidatePathPrefix(options.PathPrefix); err != nil {
		return ListMemoriesOptions{}, err
	}
	if options.DepthSet && options.Depth == 0 {
		options.DepthSet = false
	}
	if options.DepthSet {
		if options.Depth <= 0 {
			return ListMemoriesOptions{}, &ValidationError{Message: "depth must be positive"}
		}
		if options.PathPrefix != "/" && !strings.HasSuffix(options.PathPrefix, "/") {
			return ListMemoriesOptions{}, &ValidationError{Message: "path_prefix must end with / when depth is set"}
		}
	}
	if !options.LimitSet && options.Limit == 0 {
		options.Limit = 20
	}
	if options.Limit <= 0 {
		return ListMemoriesOptions{}, &ValidationError{Message: "limit must be positive"}
	}
	if options.View == ViewFull && options.Limit > 20 {
		options.Limit = 20
	}
	if options.View == ViewBasic && options.Limit > 100 {
		options.Limit = 100
	}
	return options, nil
}

func normalizeListMemoryVersionsOptions(options ListMemoryVersionsOptions) (ListMemoryVersionsOptions, error) {
	if options.View == "" {
		options.View = ViewBasic
	}
	if options.View != ViewBasic && options.View != ViewFull {
		return ListMemoryVersionsOptions{}, &ValidationError{Message: "view must be basic or full"}
	}
	if options.Operation != "" {
		switch options.Operation {
		case OperationCreated, OperationModified, OperationDeleted:
		default:
			return ListMemoryVersionsOptions{}, &ValidationError{Message: "operation must be created, modified, or deleted"}
		}
	}
	if !options.LimitSet && options.Limit == 0 {
		options.Limit = 20
	}
	if options.Limit <= 0 {
		return ListMemoryVersionsOptions{}, &ValidationError{Message: "limit must be positive"}
	}
	if options.View == ViewFull && options.Limit > 20 {
		options.Limit = 20
	}
	if options.View == ViewBasic && options.Limit > 100 {
		options.Limit = 100
	}
	return options, nil
}

func projectMemoryList(active []memoryListProjection, options ListMemoriesOptions) []memoryListProjection {
	if !options.DepthSet {
		for i := range active {
			active[i].Entry = MemoryListEntry{Memory: active[i].Memory}
		}
		return active
	}
	prefixes := map[string]memoryListProjection{}
	var projected []memoryListProjection
	for _, item := range active {
		relative := strings.TrimPrefix(item.Memory.Path, options.PathPrefix)
		segments := strings.Split(relative, "/")
		if len(segments) <= options.Depth {
			item.Entry = MemoryListEntry{Memory: item.Memory}
			projected = append(projected, item)
			continue
		}
		prefixPath := joinMemoryPrefix(options.PathPrefix, segments[:options.Depth])
		existing, ok := prefixes[prefixPath]
		if !ok {
			prefix := &MemoryPrefix{Type: "memory_prefix", Path: prefixPath}
			prefixes[prefixPath] = memoryListProjection{
				Entry:         MemoryListEntry{Prefix: prefix},
				SortPath:      prefixPath,
				SortCreatedAt: item.SortCreatedAt,
				SortUpdatedAt: item.SortUpdatedAt,
			}
			continue
		}
		if item.SortCreatedAt < existing.SortCreatedAt {
			existing.SortCreatedAt = item.SortCreatedAt
		}
		if item.SortUpdatedAt > existing.SortUpdatedAt {
			existing.SortUpdatedAt = item.SortUpdatedAt
		}
		prefixes[prefixPath] = existing
	}
	for _, prefix := range prefixes {
		projected = append(projected, prefix)
	}
	return projected
}

func joinMemoryPrefix(pathPrefix string, segments []string) string {
	joined := strings.Join(segments, "/")
	if pathPrefix == "/" {
		return "/" + joined + "/"
	}
	return pathPrefix + joined + "/"
}

func sortMemoryList(entries []memoryListProjection, options ListMemoriesOptions) {
	sort.Slice(entries, func(i, j int) bool {
		left, right := entries[i], entries[j]
		leftPrimary, rightPrimary := left.OrderValue(options.OrderBy), right.OrderValue(options.OrderBy)
		if leftPrimary != rightPrimary {
			if options.Order == SortDescending {
				return leftPrimary > rightPrimary
			}
			return leftPrimary < rightPrimary
		}
		if left.Path() != right.Path() {
			return left.Path() < right.Path()
		}
		if left.Type() != right.Type() {
			return left.Type() < right.Type()
		}
		return left.MemoryID() < right.MemoryID()
	})
}

func findMemoryPageTokenStart(entries []memoryListProjection, token memoryPageToken, options ListMemoriesOptions) int {
	return sort.Search(len(entries), func(index int) bool {
		return compareMemoryListProjectionToToken(entries[index], token, options) > 0
	})
}

func compareMemoryListProjectionToToken(entry memoryListProjection, token memoryPageToken, options ListMemoriesOptions) int {
	entryPrimary := entry.OrderValue(options.OrderBy)
	if entryPrimary != token.LastOrderValue {
		if options.Order == SortDescending {
			if entryPrimary > token.LastOrderValue {
				return -1
			}
			return 1
		}
		if entryPrimary < token.LastOrderValue {
			return -1
		}
		return 1
	}
	if entry.Path() != token.LastPath {
		if entry.Path() < token.LastPath {
			return -1
		}
		return 1
	}
	if entry.Type() != token.LastType {
		if entry.Type() < token.LastType {
			return -1
		}
		return 1
	}
	if entry.MemoryID() != token.LastMemoryID {
		if entry.MemoryID() < token.LastMemoryID {
			return -1
		}
		return 1
	}
	return 0
}

func hydrateMemoryListContent(ctx context.Context, tx *dbconnect.Tx, workspaceID string, storeID string, entries []memoryListProjection) error {
	for _, entry := range entries {
		if entry.Memory == nil {
			continue
		}
		var content string
		err := tx.QueryRow(ctx,
			`SELECT v.content
			   FROM memories m
			   JOIN memory_versions v
			     ON v.workspace_id = m.workspace_id
			    AND v.memory_store_id = m.memory_store_id
			    AND v.memory_id = m.memory_id
			    AND v.memory_version_id = m.current_version_id
			  WHERE m.workspace_id = $1 AND m.memory_store_id = $2 AND m.memory_id = $3 AND m.deleted_at IS NULL`,
			workspaceID, storeID, entry.Memory.ID).Scan(&content)
		if err != nil {
			return err
		}
		entry.Memory.Content = &content
	}
	return nil
}

func scanMemory(row rowScanner, includeContent bool) (*Memory, error) {
	var memory Memory
	memory.Type = memoryType
	var content sql.NullString
	dest := []any{&memory.ID, &memory.MemoryStoreID, &memory.MemoryVersionID, &memory.Path, &memory.ContentSHA256, &memory.ContentSizeBytes, &memory.CreatedAt, &memory.UpdatedAt}
	if includeContent {
		dest = append(dest, &content)
	}
	if err := row.Scan(dest...); err != nil {
		return nil, err
	}
	if includeContent && content.Valid {
		memory.Content = &content.String
	}
	return &memory, nil
}

func scanMemoryVersion(row rowScanner, includeContent bool) (*MemoryVersion, error) {
	var version MemoryVersion
	version.Type = memoryVersionType
	var path, content, hash sql.NullString
	var size sql.NullInt64
	var apiKeyID, sessionID, userID sql.NullString
	var redactedAt, redactedActorType, redactedAPIKeyID, redactedSessionID, redactedUserID sql.NullString
	var versionCreatedAt time.Time
	if err := row.Scan(&version.ID, &version.MemoryStoreID, &version.MemoryID, &version.Operation, &path, &content, &hash, &size, &versionCreatedAt,
		&version.CreatedBy.Type, &apiKeyID, &sessionID, &userID,
		&redactedAt, &redactedActorType, &redactedAPIKeyID, &redactedSessionID, &redactedUserID); err != nil {
		return nil, err
	}
	version.CreatedAt = versionCreatedAt.UTC().Format(time.RFC3339)
	if path.Valid {
		version.Path = &path.String
	}
	if includeContent && content.Valid {
		version.Content = &content.String
	}
	if hash.Valid {
		value := hash.String
		version.ContentSHA256 = &value
	}
	if size.Valid {
		value := size.Int64
		version.ContentSizeBytes = &value
	}
	if apiKeyID.Valid {
		version.CreatedBy.APIKeyID = apiKeyID.String
	}
	if sessionID.Valid {
		version.CreatedBy.SessionID = sessionID.String
	}
	if userID.Valid {
		version.CreatedBy.UserID = userID.String
	}
	if redactedAt.Valid {
		version.RedactedAt = &redactedAt.String
		actor := Actor{Type: redactedActorType.String}
		if redactedAPIKeyID.Valid {
			actor.APIKeyID = redactedAPIKeyID.String
		}
		if redactedSessionID.Valid {
			actor.SessionID = redactedSessionID.String
		}
		if redactedUserID.Valid {
			actor.UserID = redactedUserID.String
		}
		version.RedactedBy = &actor
	}
	return &version, nil
}

func selectMemoryVersionForUpdate(ctx context.Context, tx *dbconnect.Tx, workspaceID string, storeID string, versionID string) (*MemoryVersion, error) {
	row := tx.QueryRow(ctx,
		`SELECT memory_version_id, memory_store_id, memory_id, operation, path, content, content_sha256, content_size_bytes, created_at,
		        created_actor_type, created_api_key_id, created_session_id, created_user_id,
		        redacted_at, redacted_actor_type, redacted_api_key_id, redacted_session_id, redacted_user_id
		   FROM memory_versions
		  WHERE workspace_id = $1 AND memory_store_id = $2 AND memory_version_id = $3
		  FOR UPDATE`,
		workspaceID, storeID, versionID)
	version, err := scanMemoryVersion(row, true)
	if err == sql.ErrNoRows {
		return nil, &NotFoundError{Message: "memory version not found"}
	}
	return version, err
}

func scanStore(row rowScanner) (*Store, error) {
	var store Store
	var metadataJSON string
	var storeCreatedAt, storeUpdatedAt time.Time
	var storeArchivedAt sql.NullTime
	if err := row.Scan(&store.ID, &store.Name, &store.Description, &metadataJSON, &storeCreatedAt, &storeUpdatedAt, &storeArchivedAt); err != nil {
		return nil, err
	}
	store.CreatedAt = storeCreatedAt.UTC().Format(time.RFC3339)
	store.UpdatedAt = storeUpdatedAt.UTC().Format(time.RFC3339)
	if storeArchivedAt.Valid {
		archived := storeArchivedAt.Time.UTC().Format(time.RFC3339)
		store.ArchivedAt = &archived
	}
	store.Type = "memory_store"
	if err := json.Unmarshal([]byte(metadataJSON), &store.Metadata); err != nil {
		return nil, err
	}
	if store.Metadata == nil {
		store.Metadata = map[string]string{}
	}
	return &store, nil
}

func equalMetadata(a, b map[string]string) bool {
	if len(a) != len(b) {
		return false
	}
	for key, value := range a {
		if b[key] != value {
			return false
		}
	}
	return true
}

func sqlArg(position int) string {
	return strconv.Itoa(position)
}

func normalizeView(view string) string {
	if view == "" {
		return ViewFull
	}
	if view == ViewBasic || view == ViewFull {
		return view
	}
	return ViewFull
}

func validateActor(actor Actor) error {
	switch actor.Type {
	case ActorAPI:
		if actor.APIKeyID == "" || actor.SessionID != "" || actor.UserID != "" {
			return &ValidationError{Message: "invalid actor"}
		}
	case ActorSession:
		if actor.APIKeyID != "" || actor.SessionID == "" || actor.UserID != "" {
			return &ValidationError{Message: "invalid actor"}
		}
	case ActorUser:
		if actor.APIKeyID != "" || actor.SessionID != "" || actor.UserID == "" {
			return &ValidationError{Message: "invalid actor"}
		}
	default:
		return &ValidationError{Message: "invalid actor"}
	}
	return nil
}

func requireActiveStore(ctx context.Context, tx *dbconnect.Tx, workspaceID string, storeID string) error {
	var archivedAt sql.NullTime
	err := tx.QueryRow(ctx,
		`SELECT archived_at FROM memory_stores WHERE workspace_id = $1 AND memory_store_id = $2`,
		workspaceID, storeID).Scan(&archivedAt)
	if err == sql.ErrNoRows {
		return &NotFoundError{Message: memoryStoreDeletedErrorText}
	}
	if err != nil {
		return err
	}
	if archivedAt.Valid {
		return &ValidationError{Message: "memory store is archived"}
	}
	return nil
}

func requireStoreExists(ctx context.Context, tx *dbconnect.Tx, workspaceID string, storeID string) error {
	var exists bool
	err := tx.QueryRow(ctx,
		`SELECT true FROM memory_stores WHERE workspace_id = $1 AND memory_store_id = $2`,
		workspaceID, storeID).Scan(&exists)
	if err == sql.ErrNoRows {
		return &NotFoundError{Message: "memory store not found"}
	}
	return err
}

type currentMemorySnapshot struct {
	ID               string
	MemoryStoreID    string
	MemoryVersionID  string
	Path             string
	Content          string
	ContentSHA256    string
	ContentSizeBytes int64
	CreatedAt        string
	UpdatedAt        string
}

func selectCurrentMemoryForUpdate(ctx context.Context, tx *dbconnect.Tx, workspaceID string, storeID string, memoryID string) (currentMemorySnapshot, error) {
	var current currentMemorySnapshot
	err := tx.QueryRow(ctx,
		`SELECT m.memory_id, m.memory_store_id, m.current_version_id, m.path, v.content, m.content_sha256, m.content_size_bytes, m.created_at, m.updated_at
		   FROM memories m
		   JOIN memory_versions v
		     ON v.workspace_id = m.workspace_id
		    AND v.memory_store_id = m.memory_store_id
		    AND v.memory_id = m.memory_id
		    AND v.memory_version_id = m.current_version_id
		  WHERE m.workspace_id = $1 AND m.memory_store_id = $2 AND m.memory_id = $3 AND m.deleted_at IS NULL
		  FOR UPDATE OF m`,
		workspaceID, storeID, memoryID).Scan(
		&current.ID, &current.MemoryStoreID, &current.MemoryVersionID, &current.Path, &current.Content,
		&current.ContentSHA256, &current.ContentSizeBytes, &current.CreatedAt, &current.UpdatedAt)
	if err == sql.ErrNoRows {
		return currentMemorySnapshot{}, &NotFoundError{Message: "memory not found"}
	}
	if err != nil {
		return currentMemorySnapshot{}, err
	}
	return current, nil
}

func memoryFromCurrent(current currentMemorySnapshot, view string) *Memory {
	result := &Memory{
		ID:               current.ID,
		Type:             memoryType,
		MemoryStoreID:    current.MemoryStoreID,
		MemoryVersionID:  current.MemoryVersionID,
		Path:             current.Path,
		ContentSHA256:    current.ContentSHA256,
		ContentSizeBytes: current.ContentSizeBytes,
		CreatedAt:        current.CreatedAt,
		UpdatedAt:        current.UpdatedAt,
	}
	if view == ViewFull {
		result.Content = &current.Content
	}
	return result
}

func (owner durableQuotaOwner) EnforceCreate(ctx context.Context, tx DurableQuotaTransaction, workspaceID string, storeID string, newContentBytes int64) error {
	var memoryCount int
	if err := tx.QueryRowScanner(ctx,
		`SELECT count(*) FROM memories WHERE workspace_id = $1 AND memory_store_id = $2`,
		workspaceID, storeID).Scan(&memoryCount); err != nil {
		return err
	}
	if memoryCount >= MaxMemoriesPerStore {
		return &QuotaError{Message: "memory quota exceeded"}
	}
	if err := owner.EnforceDeletionVersion(ctx, tx, workspaceID, storeID); err != nil {
		return err
	}
	return enforceRetainedPayloadBytesQuota(ctx, tx, workspaceID, storeID, newContentBytes)
}

func (owner durableQuotaOwner) EnforceContentVersion(ctx context.Context, tx DurableQuotaTransaction, workspaceID string, storeID string, newContentBytes int64) error {
	if err := owner.EnforceDeletionVersion(ctx, tx, workspaceID, storeID); err != nil {
		return err
	}
	return enforceRetainedPayloadBytesQuota(ctx, tx, workspaceID, storeID, newContentBytes)
}

func (durableQuotaOwner) EnforceDeletionVersion(ctx context.Context, tx DurableQuotaTransaction, workspaceID string, storeID string) error {
	var versionCount int
	if err := tx.QueryRowScanner(ctx,
		`SELECT count(*) FROM memory_versions WHERE workspace_id = $1 AND memory_store_id = $2`,
		workspaceID, storeID).Scan(&versionCount); err != nil {
		return err
	}
	if versionCount >= MaxMemoryVersionsPerStore {
		return &QuotaError{Message: "memory version quota exceeded"}
	}
	return nil
}

func enforceRetainedPayloadBytesQuota(ctx context.Context, tx DurableQuotaTransaction, workspaceID string, storeID string, newContentBytes int64) error {
	var retainedBytes sql.NullInt64
	if err := tx.QueryRowScanner(ctx,
		`SELECT COALESCE(sum(octet_length(content)), 0) FROM memory_versions WHERE workspace_id = $1 AND memory_store_id = $2 AND content IS NOT NULL`,
		workspaceID, storeID).Scan(&retainedBytes); err != nil {
		return err
	}
	if retainedBytes.Int64+newContentBytes > MaxRetainedMemoryPayloadBytesPerStore {
		return &RequestTooLargeError{Message: "memory retained payload quota exceeded"}
	}
	return nil
}

func sha256Hex(content string) string {
	sum := sha256.Sum256([]byte(content))
	return hex.EncodeToString(sum[:])
}

func validateSHA256Hex(value string) error {
	if len(value) != 64 {
		return &ValidationError{Message: "precondition content_sha256 must be a hex sha256"}
	}
	decoded, err := hex.DecodeString(value)
	if err != nil || len(decoded) != sha256.Size {
		return &ValidationError{Message: "precondition content_sha256 must be a hex sha256"}
	}
	return nil
}

func nullable(value string) any {
	if value == "" {
		return nil
	}
	return value
}
