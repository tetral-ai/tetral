package session

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/id"
	"github.com/tetral-ai/tetral/internal/queue"
	"github.com/tetral-ai/tetral/internal/storage"
	"github.com/tetral-ai/tetral/internal/workspace"
)

type PostgreSQLSessionStore struct {
	client                      *dbconnect.Client
	pageTokenSecret             []byte
	sessionDeleteSandboxRelease SessionDeleteSandboxReleaseFunc
}

type StoreOption func(*PostgreSQLSessionStore)

// SessionDeleteSandboxReleaseFunc records the Sandbox release fence and its
// durable provider job inside the Session deletion transaction.
type SessionDeleteSandboxReleaseFunc func(context.Context, *dbconnect.Tx, workspace.ID, string, time.Time) error

func WithPageTokenSecret(secret []byte) StoreOption {
	return func(s *PostgreSQLSessionStore) {
		s.pageTokenSecret = append([]byte(nil), secret...)
	}
}

func WithSessionDeleteSandboxRelease(release SessionDeleteSandboxReleaseFunc) StoreOption {
	return func(s *PostgreSQLSessionStore) {
		s.sessionDeleteSandboxRelease = release
	}
}

func NewPostgreSQLSessionStore(runtimeClient *dbconnect.Client, options ...StoreOption) *PostgreSQLSessionStore {
	store := &PostgreSQLSessionStore{client: runtimeClient}
	for _, option := range options {
		option(store)
	}
	if len(store.pageTokenSecret) == 0 {
		store.pageTokenSecret = make([]byte, 32)
		if _, err := rand.Read(store.pageTokenSecret); err != nil {
			panic("session: page token entropy unavailable")
		}
	}
	return store
}

func (s *PostgreSQLSessionStore) WithWorkspaceTx(ctx context.Context, ws workspace.ID, fn func(Transaction) error) error {
	if ws == "" {
		return &ValidationError{Message: "workspace_id is required"}
	}
	return s.client.WithWorkspaceTx(ctx, string(ws), "session.transaction", func(tx *dbconnect.Tx) error {
		return fn(&postgresqlTransaction{
			store:       s,
			workspaceID: ws,
			tx:          postgresqlTxAdapter{tx: tx},
		})
	})
}

func (s *PostgreSQLSessionStore) WithWorkspaceTxAndCleanup(ctx context.Context, ws workspace.ID, fn func(Transaction) error, onCommitFailure func()) error {
	if ws == "" {
		return &ValidationError{Message: "workspace_id is required"}
	}
	return s.client.WithWorkspaceTxAndCleanup(ctx, string(ws), "session.transaction", func(tx *dbconnect.Tx) error {
		return fn(&postgresqlTransaction{
			store:       s,
			workspaceID: ws,
			tx:          postgresqlTxAdapter{tx: tx},
		})
	}, onCommitFailure)
}

func (s *PostgreSQLSessionStore) WithRuntimeMutationLock(ctx context.Context, ws workspace.ID, sessionID string, fn func() error) error {
	resource, err := storage.SessionRuntimeMutationAdvisoryLockResource(string(ws), sessionID)
	if err != nil {
		return &ValidationError{Message: "session runtime mutation lock is invalid"}
	}
	return s.client.WithAdvisoryLock(ctx, "session.runtime_mutation_lock", storage.SessionRuntimeMutationAdvisoryLockCategory, resource, fn)
}

func (s *PostgreSQLSessionStore) Get(ctx context.Context, ws workspace.ID, sessionID string) (*Session, error) {
	var got *Session
	if err := s.WithWorkspaceTx(ctx, ws, func(tx Transaction) error {
		var err error
		got, err = tx.GetSession(ctx, sessionID)
		return err
	}); err != nil {
		return nil, err
	}
	return got, nil
}

func (s *PostgreSQLSessionStore) List(ctx context.Context, ws workspace.ID, options ListOptions) (*StoreListResult, error) {
	var (
		data    []*Session
		hasMore bool
	)
	if err := s.WithWorkspaceTx(ctx, ws, func(tx Transaction) error {
		var err error
		data, hasMore, err = tx.ListSessions(ctx, options)
		return err
	}); err != nil {
		return nil, err
	}
	var nextPage *string
	if hasMore && len(data) > 0 {
		last := data[len(data)-1]
		token, err := encodePageToken(
			s.pageTokenSecret,
			pageTokenPayload{
				Kind:      pageKindSessions,
				CreatedAt: last.CreatedAt.UTC().Format(pageTokenTimeFormat),
				SessionID: last.ID,
			},
			sessionListAssociatedData(ws, options),
		)
		if err != nil {
			return nil, err
		}
		nextPage = &token
	}
	return &StoreListResult{Data: data, NextPage: nextPage}, nil
}

func (s *PostgreSQLSessionStore) ListSessionProviderAuth(ctx context.Context, ws workspace.ID, sessionID string) (ProviderSelectors, error) {
	var selectors ProviderSelectors
	if err := s.WithWorkspaceTx(ctx, ws, func(tx Transaction) error {
		postgresTx, ok := tx.(*postgresqlTransaction)
		if !ok {
			return errors.New("session: postgresql transaction required")
		}
		var err error
		selectors, err = postgresTx.ListSessionProviderAuth(ctx, sessionID)
		return err
	}); err != nil {
		return nil, err
	}
	return selectors, nil
}

func (s *PostgreSQLSessionStore) ListThreads(ctx context.Context, ws workspace.ID, sessionID string, options ThreadListOptions) (*StoreThreadListResult, error) {
	var (
		data    []*Thread
		hasMore bool
	)
	if err := s.WithWorkspaceTx(ctx, ws, func(tx Transaction) error {
		postgresTx, ok := tx.(*postgresqlTransaction)
		if !ok {
			return errors.New("session: postgresql transaction required")
		}
		var err error
		if _, err = tx.GetSession(ctx, sessionID); err != nil {
			return err
		}
		data, hasMore, err = postgresTx.ListThreads(ctx, sessionID, options)
		if err != nil {
			return err
		}
		for _, thread := range data {
			thread.Usage, err = postgresTx.loadThreadUsage(ctx, sessionID, thread.ID)
			if err != nil {
				return err
			}
		}
		return err
	}); err != nil {
		return nil, err
	}
	var nextPage *string
	if hasMore && len(data) > 0 {
		last := data[len(data)-1]
		token, err := encodePageToken(
			s.pageTokenSecret,
			threadPageTokenPayload(last),
			threadListAssociatedData(ws, sessionID),
		)
		if err != nil {
			return nil, err
		}
		nextPage = &token
	}
	return &StoreThreadListResult{Data: data, NextPage: nextPage}, nil
}

func (s *PostgreSQLSessionStore) GetThread(ctx context.Context, ws workspace.ID, sessionID string, threadID string) (*Thread, error) {
	var thread *Thread
	if err := s.WithWorkspaceTx(ctx, ws, func(tx Transaction) error {
		postgresTx, ok := tx.(*postgresqlTransaction)
		if !ok {
			return errors.New("session: postgresql transaction required")
		}
		var err error
		if _, err = tx.GetSession(ctx, sessionID); err != nil {
			return err
		}
		thread, err = postgresTx.GetThread(ctx, sessionID, threadID)
		if err != nil {
			return err
		}
		thread.Usage, err = postgresTx.loadThreadUsage(ctx, sessionID, threadID)
		return err
	}); err != nil {
		return nil, err
	}
	return thread, nil
}

func (s *PostgreSQLSessionStore) ArchiveThread(ctx context.Context, ws workspace.ID, sessionID string, threadID string, archivedAt time.Time) (*Thread, error) {
	var thread *Thread
	if err := s.WithWorkspaceTx(ctx, ws, func(tx Transaction) error {
		postgresTx, ok := tx.(*postgresqlTransaction)
		if !ok {
			return errors.New("session: postgresql transaction required")
		}
		var err error
		if _, err = tx.GetSession(ctx, sessionID); err != nil {
			return err
		}
		thread, err = postgresTx.ArchiveThread(ctx, sessionID, threadID, archivedAt)
		if err != nil {
			return err
		}
		thread.Usage, err = postgresTx.loadThreadUsage(ctx, sessionID, threadID)
		return err
	}); err != nil {
		return nil, err
	}
	return thread, nil
}

func (s *PostgreSQLSessionStore) ListResources(ctx context.Context, ws workspace.ID, sessionID string, options ResourceListOptions) (*StoreResourceListResult, error) {
	var (
		data    []*Resource
		hasMore bool
	)
	if err := s.WithWorkspaceTx(ctx, ws, func(tx Transaction) error {
		var err error
		if _, err = tx.GetSession(ctx, sessionID); err != nil {
			return err
		}
		data, hasMore, err = tx.ListResources(ctx, sessionID, options)
		return err
	}); err != nil {
		return nil, err
	}
	var nextPage *string
	if hasMore && len(data) > 0 {
		last := data[len(data)-1]
		token, err := encodePageToken(
			s.pageTokenSecret,
			resourcePageTokenPayload(last),
			resourceListAssociatedData(ws, sessionID),
		)
		if err != nil {
			return nil, err
		}
		nextPage = &token
	}
	return &StoreResourceListResult{Data: data, NextPage: nextPage}, nil
}

type postgresqlTxAdapter struct {
	tx *dbconnect.Tx
}

func (a postgresqlTxAdapter) Exec(ctx context.Context, query string, args ...any) (ExecResult, error) {
	return a.tx.Exec(ctx, query, args...)
}

func (a postgresqlTxAdapter) QueryRows(ctx context.Context, query string, args ...any) (QueryRows, error) {
	return a.tx.QueryRows(ctx, query, args...)
}

func (a postgresqlTxAdapter) QueryRowScanner(ctx context.Context, query string, args ...any) RowScanner {
	return a.tx.QueryRowScanner(ctx, query, args...)
}

type postgresqlTransaction struct {
	store       *PostgreSQLSessionStore
	workspaceID workspace.ID
	tx          txExecutor
}

func (t *postgresqlTransaction) Exec(ctx context.Context, query string, args ...any) (ExecResult, error) {
	return t.tx.Exec(ctx, query, args...)
}

func (t *postgresqlTransaction) QueryRows(ctx context.Context, query string, args ...any) (QueryRows, error) {
	return t.tx.QueryRows(ctx, query, args...)
}

func (t *postgresqlTransaction) QueryRowScanner(ctx context.Context, query string, args ...any) RowScanner {
	return t.tx.QueryRowScanner(ctx, query, args...)
}

func (t *postgresqlTransaction) CreateSession(ctx context.Context, sess *Session) error {
	if sess == nil {
		return &ValidationError{Message: "session is required"}
	}
	if sess.WorkspaceID == "" {
		sess.WorkspaceID = t.workspaceID
	}
	if sess.WorkspaceID != t.workspaceID {
		return &ValidationError{Message: "workspace_id mismatch"}
	}
	if sess.Type == "" {
		sess.Type = "session"
	}
	if sess.Status == "" {
		sess.Status = StatusIdle
	}
	if err := validateStatus(sess.Status); err != nil {
		return err
	}
	if sess.LifecycleState == "" {
		sess.LifecycleState = LifecycleStateActive
	}
	if err := validateLifecycleState(sess.LifecycleState); err != nil {
		return err
	}
	if sess.ConfigGeneration <= 0 {
		sess.ConfigGeneration = 1
	}
	approvalMode, err := normalizeApprovalMode(sess.ApprovalMode)
	if err != nil {
		return err
	}
	sess.ApprovalMode = approvalMode
	metadataJSON, err := json.Marshal(nonNilMetadata(sess.Metadata))
	if err != nil {
		return err
	}
	vaultIDsJSON, err := json.Marshal(nonNilStringSlice(sess.VaultIDs))
	if err != nil {
		return err
	}
	runtimeAgentConfigJSON, err := encodeRuntimeAgentConfig(sess.RuntimeAgentConfig)
	if err != nil {
		return err
	}
	if sess.AgentVersionID == "" {
		versionID, err := lookupAgentVersionID(ctx, t.tx, t.workspaceID, sess.AgentID, sess.AgentVersion)
		if err != nil {
			return err
		}
		sess.AgentVersionID = versionID
	}
	if _, err := t.tx.Exec(ctx,
		`INSERT INTO sessions (
			workspace_id, id, type, title, metadata_json, status, lifecycle_state, archived_at,
			config_generation, approval_mode, installed_tools_json, agent_id, agent_version_id, agent_version, environment_id, vault_ids_json, created_at, updated_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18)`,
		string(t.workspaceID),
		sess.ID,
		sess.Type,
		nullablePointerString(sess.Title),
		string(metadataJSON),
		string(sess.Status),
		string(sess.LifecycleState),
		sess.ArchivedAt,
		sess.ConfigGeneration,
		string(sess.ApprovalMode),
		runtimeAgentConfigJSON,
		sess.AgentID,
		sess.AgentVersionID,
		sess.AgentVersion,
		sess.EnvironmentID,
		string(vaultIDsJSON),
		sess.CreatedAt,
		sess.UpdatedAt,
	); err != nil {
		return mapPostgreSQLSessionError(err)
	}
	runtimeStatus := "idle"
	var idleSince any = sess.CreatedAt
	if sess.Status == StatusRunning {
		runtimeStatus = "running"
		idleSince = nil
	}
	if _, err := t.tx.Exec(ctx,
		`INSERT INTO session_runtime_status (
			workspace_id, session_id, status, idle_since, created_at, updated_at
		) VALUES ($1, $2, $3, $4, $5, $6)`,
		string(t.workspaceID),
		sess.ID,
		runtimeStatus,
		idleSince,
		sess.CreatedAt,
		sess.UpdatedAt,
	); err != nil {
		return mapPostgreSQLSessionError(err)
	}
	for _, resource := range sess.Resources {
		if resource.WorkspaceID == "" {
			resource.WorkspaceID = t.workspaceID
		}
		if resource.SessionID == "" {
			resource.SessionID = sess.ID
		}
		if err := t.CreateResource(ctx, resource); err != nil {
			return err
		}
	}
	return nil
}

func lookupAgentVersionID(ctx context.Context, tx txExecutor, ws workspace.ID, agentID string, version int) (string, error) {
	var versionID string
	err := tx.QueryRowScanner(ctx,
		`SELECT id
		   FROM agent_versions
		  WHERE workspace_id = $1
		    AND agent_id = $2
		    AND version = $3`,
		string(ws), agentID, version,
	).Scan(&versionID)
	if dbconnect.IsNoRows(err) {
		return "", &ValidationError{Message: "agent version is required"}
	}
	if err != nil {
		return "", err
	}
	if versionID == "" {
		return "", &ValidationError{Message: "agent version is required"}
	}
	return versionID, nil
}

func (t *postgresqlTransaction) CreatePrimaryThread(ctx context.Context, thread *Thread) error {
	if thread == nil {
		return &ValidationError{Message: "session thread is required"}
	}
	if thread.WorkspaceID == "" {
		thread.WorkspaceID = t.workspaceID
	}
	if thread.WorkspaceID != t.workspaceID {
		return &ValidationError{Message: "workspace_id mismatch"}
	}
	if thread.ID == "" {
		return &ValidationError{Message: "thread_id is required"}
	}
	if thread.SessionID == "" {
		return &ValidationError{Message: "session_id is required"}
	}
	if thread.ParentThreadID != nil {
		return &ValidationError{Message: "primary thread cannot have a parent"}
	}
	if thread.Role == "" {
		thread.Role = ThreadRoleMain
	}
	if thread.Role != ThreadRoleMain {
		return &ValidationError{Message: "primary thread role must be main"}
	}
	if err := validateThreadRole(thread.Role); err != nil {
		return err
	}
	if thread.Visibility == "" {
		thread.Visibility = ThreadVisibilityPublic
	}
	if thread.Visibility != ThreadVisibilityPublic {
		return &ValidationError{Message: "primary thread visibility must be public"}
	}
	if err := validateThreadVisibility(thread.Visibility); err != nil {
		return err
	}
	if thread.Status == "" {
		thread.Status = ThreadStatusIdle
	}
	if err := validateThreadStatus(thread.Status); err != nil {
		return err
	}
	if thread.AgentType == "" {
		thread.AgentType = "default"
	}
	if thread.CreatedAt.IsZero() {
		thread.CreatedAt = storage.Now()
	}
	if thread.LastActiveAt.IsZero() {
		thread.LastActiveAt = thread.CreatedAt
	}
	if thread.UpdatedAt.IsZero() {
		thread.UpdatedAt = thread.CreatedAt
	}
	if _, err := t.tx.Exec(ctx,
		`INSERT INTO session_threads (
				workspace_id, id, session_id, parent_thread_id, role, visibility, status,
				agent_type, title, task_name, is_trunk, created_at, last_active_at, closed_at, archived_at, updated_at
			) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16)`,
		string(t.workspaceID),
		thread.ID,
		thread.SessionID,
		nullablePointerString(thread.ParentThreadID),
		string(thread.Role),
		string(thread.Visibility),
		string(thread.Status),
		thread.AgentType,
		nullablePointerString(thread.Title),
		nullablePointerString(thread.TaskName),
		thread.IsTrunk,
		thread.CreatedAt,
		thread.LastActiveAt,
		thread.ClosedAt,
		thread.ArchivedAt,
		thread.UpdatedAt,
	); err != nil {
		return mapPostgreSQLSessionError(err)
	}
	result, err := t.tx.Exec(ctx,
		`UPDATE sessions
		    SET main_thread_id = $1
		  WHERE workspace_id = $2
		    AND id = $3
		    AND main_thread_id IS NULL`,
		thread.ID,
		string(t.workspaceID),
		thread.SessionID,
	)
	if err != nil {
		return mapPostgreSQLSessionError(err)
	}
	if affected, _ := result.RowsAffected(); affected == 0 {
		return &ConflictError{Message: "session primary thread already exists"}
	}
	payload := map[string]any{
		"type":                     "session.thread_created",
		"session_thread_id":        thread.ID,
		"parent_thread_id":         nil,
		"role":                     string(thread.Role),
		"visibility":               string(thread.Visibility),
		"agent_type":               thread.AgentType,
		"task_name":                nil,
		"source_tool_use_event_id": nil,
	}
	return t.appendPublicProcessedSessionEvent(ctx, thread.SessionID, thread.ID, "session.thread_created", payload, thread.CreatedAt)
}

func (t *postgresqlTransaction) GetSession(ctx context.Context, sessionID string) (*Session, error) {
	return t.loadSession(ctx, sessionID)
}

func (t *postgresqlTransaction) LockSession(ctx context.Context, sessionID string) (*Session, error) {
	sess, err := t.loadSessionForUpdate(ctx, sessionID)
	if err != nil {
		return nil, err
	}
	resources, err := t.loadActiveResources(ctx, sessionID)
	if err != nil {
		return nil, err
	}
	sess.Resources = resources
	return sess, nil
}

func (t *postgresqlTransaction) LockSessionForDelete(ctx context.Context, sessionID string) (*Session, error) {
	sess, err := t.loadSessionForUpdate(ctx, sessionID)
	if err != nil {
		return nil, err
	}
	if sess.LifecycleState == LifecycleStateArchiving {
		return nil, &ConflictError{Message: "session lifecycle transition is already in progress", InvalidRequest: true}
	}
	if sess.Status == StatusRunning {
		return nil, &ConflictError{Message: "running sessions cannot be deleted", InvalidRequest: true}
	}
	resources, err := t.loadActiveResources(ctx, sessionID)
	if err != nil {
		return nil, err
	}
	sess.Resources = resources
	return sess, nil
}

func (t *postgresqlTransaction) ListSessions(ctx context.Context, options ListOptions) ([]*Session, bool, error) {
	limit := normalizeLimit(options.Limit)
	if err := validateListOrder(options.Order); err != nil {
		return nil, false, err
	}
	options.Order = normalizeListOrder(options.Order)
	if options.Page != "" {
		payload, err := decodePageToken(t.store.pageTokenSecret, options.Page, sessionListAssociatedData(t.workspaceID, options))
		if err != nil {
			return nil, false, err
		}
		if payload.Kind != pageKindSessions || payload.SessionID == "" || payload.CreatedAt == "" {
			return nil, false, &ValidationError{Message: "invalid page token"}
		}
		createdAt, err := time.Parse(pageTokenTimeFormat, payload.CreatedAt)
		if err != nil {
			return nil, false, &ValidationError{Message: "invalid page token"}
		}
		options.cursorCreatedAt = createdAt
		options.cursorID = payload.SessionID
	}
	query, args := buildListSessionsQuery(t.workspaceID, options, limit+1)
	rows, err := t.tx.QueryRows(ctx, query, args...)
	if err != nil {
		return nil, false, err
	}
	defer func() { _ = rows.Close() }()
	results := []*Session{}
	for rows.Next() {
		sess, err := scanSessionRows(rows)
		if err != nil {
			return nil, false, err
		}
		results = append(results, sess)
	}
	if err := rows.Err(); err != nil {
		return nil, false, err
	}
	if err := rows.Close(); err != nil {
		return nil, false, err
	}
	for _, sess := range results {
		resources, err := t.loadActiveResources(ctx, sess.ID)
		if err != nil {
			return nil, false, err
		}
		sess.Resources = resources
	}
	hasMore := len(results) > limit
	if hasMore {
		results = results[:limit]
	}
	return results, hasMore, nil
}

func (t *postgresqlTransaction) RequireSessionUsableForMutation(ctx context.Context, sessionID string) error {
	usable, err := t.loadSessionUsabilityForUpdate(ctx, sessionID)
	if err != nil {
		return err
	}
	if err := rejectUnusableSession(usable); err != nil {
		return err
	}
	return t.rejectRuntimeConfigUpdateRaces(ctx, sessionID)
}

func (t *postgresqlTransaction) RecordSessionResourceMutation(ctx context.Context, sessionID string, now time.Time) error {
	if sessionID == "" {
		return &ValidationError{Message: "session_id is required"}
	}
	if now.IsZero() {
		now = storage.Now()
	}
	now = now.UTC()
	if err := t.requireIdleRuntimeStatusForResourceMutation(ctx, sessionID); err != nil {
		return err
	}
	var resourceRevision int64
	if err := t.tx.QueryRowScanner(ctx,
		`UPDATE sessions
		    SET sandbox_resource_revision = sandbox_resource_revision + 1,
		        updated_at = $3
		  WHERE workspace_id = $1 AND id = $2
		  RETURNING sandbox_resource_revision`,
		string(t.workspaceID), sessionID, now,
	).Scan(&resourceRevision); err != nil {
		return err
	}
	var logicalSandboxID, providerResourceID string
	var bindingRevision, environmentGeneration int64
	err := t.tx.QueryRowScanner(ctx,
		`SELECT logical_sandbox_id, provider_resource_id, binding_revision, environment_generation
		   FROM session_sandbox_bindings
		  WHERE workspace_id = $1 AND session_id = $2
		    AND provider_resource_id IS NOT NULL
		    AND release_requested_at IS NULL
		  FOR UPDATE`,
		string(t.workspaceID), sessionID,
	).Scan(&logicalSandboxID, &providerResourceID, &bindingRevision, &environmentGeneration)
	if dbconnect.IsNoRows(err) {
		return nil
	}
	if err != nil {
		return err
	}
	var existingOperationID string
	err = t.tx.QueryRowScanner(ctx,
		`SELECT operation_id
		   FROM sandbox_lifecycle_operations
		  WHERE workspace_id = $1 AND logical_sandbox_id = $2
		    AND kind = 'materialize'
		    AND state IN ('pending', 'waiting_activation', 'running')
		  FOR UPDATE`,
		string(t.workspaceID), logicalSandboxID,
	).Scan(&existingOperationID)
	if err == nil {
		return nil
	}
	if !dbconnect.IsNoRows(err) {
		return err
	}
	rawTx, err := t.rawDBTx()
	if err != nil {
		return err
	}
	resources, err := t.loadResourceMaterializationSnapshot(ctx, sessionID)
	if err != nil {
		return err
	}
	resourcesJSON, err := json.Marshal(resources)
	if err != nil {
		return err
	}
	operationID := id.New("sop_")
	queueJobID := queue.NewJobID()
	partitionKey := queue.FormatSandboxLifecyclePartitionKey(t.workspaceID, logicalSandboxID)
	dedupeKey := queue.FormatSandboxLifecycleDedupeKey(queue.KindSandboxMaterialize, t.workspaceID, logicalSandboxID, operationID)
	payloadJSON, err := json.Marshal(map[string]string{
		"workspace_id": string(t.workspaceID), "session_id": sessionID,
		"logical_sandbox_id": logicalSandboxID, "operation_id": operationID,
	})
	if err != nil {
		return err
	}
	if _, err := t.tx.Exec(ctx,
		`INSERT INTO sandbox_lifecycle_operations (
			workspace_id, operation_id, session_id, logical_sandbox_id,
			kind, state, observed_binding_revision, target_environment_generation,
			target_resource_revision, target_provider_resource_id,
			materialization_resources_json, queue_job_id, queue_kind,
			queue_partition_key, queue_dedupe_key, created_at, updated_at
		) VALUES ($1, $2, $3, $4, 'materialize', 'pending', $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $14)`,
		string(t.workspaceID), operationID, sessionID, logicalSandboxID,
		bindingRevision, environmentGeneration, resourceRevision, providerResourceID,
		string(resourcesJSON), queueJobID, queue.KindSandboxMaterialize,
		partitionKey, dedupeKey, now,
	); err != nil {
		return err
	}
	_, err = queue.EnqueueTx(ctx, rawTx, queue.EnqueueRequest{
		ID: queueJobID, WorkspaceID: t.workspaceID, Kind: queue.KindSandboxMaterialize,
		PartitionKey: partitionKey, DedupeKey: dedupeKey, PayloadVersion: 1,
		PayloadJSON: payloadJSON, MaxAttempts: 5, Now: now,
	})
	return err
}

func (t *postgresqlTransaction) rawDBTx() (*dbconnect.Tx, error) {
	adapter, ok := t.tx.(postgresqlTxAdapter)
	if !ok || adapter.tx == nil {
		return nil, errors.New("session: PostgreSQL transaction is unavailable")
	}
	return adapter.tx, nil
}

func (t *postgresqlTransaction) requireIdleRuntimeStatusForResourceMutation(ctx context.Context, sessionID string) error {
	var status string
	err := t.tx.QueryRowScanner(ctx,
		`SELECT status
		   FROM session_runtime_status
		  WHERE workspace_id = $1
		    AND session_id = $2
		  FOR UPDATE`,
		string(t.workspaceID),
		sessionID,
	).Scan(&status)
	if dbconnect.IsNoRows(err) {
		return errors.New("session: session_runtime_status invariant missing")
	}
	if err != nil {
		return err
	}
	switch status {
	case "idle":
		return nil
	case "running":
		return &ConflictError{Message: "session must be idle for resource mutation", InvalidRequest: true}
	default:
		return &ValidationError{Message: "session runtime status is invalid"}
	}
}

func (t *postgresqlTransaction) UpdateSession(ctx context.Context, sessionID string, update UpdateSession) (*Session, error) {
	current, err := t.loadSessionForUpdate(ctx, sessionID)
	if err != nil {
		return nil, err
	}
	if err := rejectUnusableSession(sessionUsabilityFromSession(current)); err != nil {
		return nil, err
	}
	usable, err := t.loadSessionUsabilityForUpdate(ctx, sessionID)
	if err != nil {
		return nil, err
	}
	if err := rejectUnusableSession(usable); err != nil {
		return nil, err
	}
	if err := t.rejectRuntimeConfigUpdateRaces(ctx, sessionID); err != nil {
		return nil, err
	}
	approvalMode := current.ApprovalMode
	runtimeAgentConfig := cloneRuntimeAgentConfigPointer(current.RuntimeAgentConfig)
	runtimeConfigChanged := false
	if update.ApprovalMode != nil {
		nextApprovalMode, err := normalizeApprovalMode(*update.ApprovalMode)
		if err != nil {
			return nil, err
		}
		if nextApprovalMode != current.ApprovalMode {
			approvalMode = nextApprovalMode
			runtimeConfigChanged = true
		}
	}
	if update.RuntimeAgentConfig != nil {
		nextRuntimeAgentConfig := normalizeRuntimeAgentConfig(*update.RuntimeAgentConfig)
		if runtimeAgentConfig == nil || !runtimeAgentConfigEqual(*runtimeAgentConfig, nextRuntimeAgentConfig) {
			runtimeAgentConfig = &nextRuntimeAgentConfig
			runtimeConfigChanged = true
		}
	}
	metadataPresent := update.MetadataPresent || update.MetadataPatch != nil
	nextMetadata := nonNilMetadata(current.Metadata)
	if metadataPresent && update.MetadataPatch == nil {
		nextMetadata = map[string]string{}
	}
	for key, value := range update.MetadataPatch {
		if value == nil {
			delete(nextMetadata, key)
			continue
		}
		nextMetadata[key] = *value
	}
	metadataJSON, err := json.Marshal(nextMetadata)
	if err != nil {
		return nil, err
	}
	titlePresent := update.TitlePresent || update.Title != nil
	title := current.Title
	if titlePresent {
		title = update.Title
	}
	nextConfigGeneration := current.ConfigGeneration
	if nextConfigGeneration <= 0 {
		nextConfigGeneration = 1
	}
	if runtimeConfigChanged {
		nextConfigGeneration++
	}
	var installedToolsJSON any
	if update.RuntimeAgentConfig != nil {
		encodedRuntimeAgentConfig, err := encodeRuntimeAgentConfig(runtimeAgentConfig)
		if err != nil {
			return nil, err
		}
		installedToolsJSON = encodedRuntimeAgentConfig
	}
	updatedAt := update.UpdatedAt
	if updatedAt.IsZero() {
		updatedAt = storage.Now()
	}
	result, err := t.tx.Exec(ctx,
		`UPDATE sessions
		    SET title = $1,
		        metadata_json = $2,
		        approval_mode = $3,
		        config_generation = $4,
		        installed_tools_json = COALESCE($5, installed_tools_json),
		        updated_at = $6
		  WHERE workspace_id = $7 AND id = $8`,
		nullablePointerString(title),
		string(metadataJSON),
		string(approvalMode),
		nextConfigGeneration,
		installedToolsJSON,
		updatedAt,
		string(t.workspaceID),
		sessionID,
	)
	if err != nil {
		return nil, err
	}
	if affected, _ := result.RowsAffected(); affected == 0 {
		return nil, &NotFoundError{Message: "session not found"}
	}
	if runtimeConfigChanged {
		if err := t.enqueueRuntimeConfigUpdate(ctx, sessionID, nextConfigGeneration, updatedAt); err != nil {
			return nil, err
		}
	}
	if err := t.appendPublicProcessedSessionEvent(ctx, sessionID, "", "session.updated", map[string]string{
		"id":   sessionID,
		"type": "session.updated",
	}, updatedAt); err != nil {
		return nil, err
	}
	return t.loadSession(ctx, sessionID)
}

func (t *postgresqlTransaction) rejectRuntimeConfigUpdateRaces(ctx context.Context, sessionID string) error {
	var activeJobKind sql.NullString
	if err := t.tx.QueryRowScanner(ctx,
		`SELECT kind
		   FROM queue_jobs
		  WHERE workspace_id = $1
		    AND partition_key = $2
		    AND kind IN ('runtime_input', 'runtime_config_update', 'cleanup_session')
		    AND status IN ('pending', 'leased')
		  ORDER BY queue_partition_sequence ASC
		  LIMIT 1`,
		string(t.workspaceID),
		queue.FormatSessionPartitionKey(t.workspaceID, sessionID),
	).Scan(&activeJobKind); err != nil && !dbconnect.IsNoRows(err) {
		return err
	}
	if activeJobKind.Valid {
		switch activeJobKind.String {
		case queue.KindRuntimeInput:
			return &ConflictError{Message: "session has pending runtime input", InvalidRequest: true}
		case queue.KindRuntimeConfigUpdate:
			return &ConflictError{Message: "session has pending runtime config update", InvalidRequest: true}
		case queue.KindCleanupSession:
			return &ConflictError{Message: "session cleanup is in progress", InvalidRequest: true}
		default:
			return &ConflictError{Message: "session has conflicting runtime work", InvalidRequest: true}
		}
	}
	var inboxExists bool
	if err := t.tx.QueryRowScanner(ctx,
		`SELECT EXISTS (
			SELECT 1
			  FROM session_runtime_inbox
			 WHERE workspace_id = $1
			   AND session_id = $2
			   AND status IN ('accepted', 'delivering', 'parked')
		)`,
		string(t.workspaceID),
		sessionID,
	).Scan(&inboxExists); err != nil {
		return err
	}
	if inboxExists {
		return &ConflictError{Message: "session has pending runtime input", InvalidRequest: true}
	}
	var runtimeStatus string
	var cleanupClaimedAt sql.NullTime
	var cleanupJobID sql.NullString
	if err := t.tx.QueryRowScanner(ctx,
		`SELECT status, cleanup_claimed_at, cleanup_job_id
		   FROM session_runtime_status
		  WHERE workspace_id = $1 AND session_id = $2
		  FOR UPDATE`,
		string(t.workspaceID),
		sessionID,
	).Scan(&runtimeStatus, &cleanupClaimedAt, &cleanupJobID); dbconnect.IsNoRows(err) {
		return &ConflictError{Message: "session runtime status is unavailable", InvalidRequest: true}
	} else if err != nil {
		return err
	}
	if runtimeStatus == "running" {
		return &ConflictError{Message: "session must be idle for runtime config update", InvalidRequest: true}
	}
	if cleanupClaimedAt.Valid || cleanupJobID.Valid {
		return &ConflictError{Message: "session cleanup is in progress", InvalidRequest: true}
	}
	return nil
}

func (t *postgresqlTransaction) enqueueRuntimeConfigUpdate(ctx context.Context, sessionID string, configGeneration int64, now time.Time) error {
	configGenerationToken := fmt.Sprintf("%d", configGeneration)
	payloadMap := map[string]any{
		"workspace_id":      string(t.workspaceID),
		"session_id":        sessionID,
		"config_generation": configGeneration,
	}
	payload, err := json.Marshal(payloadMap)
	if err != nil {
		return err
	}
	_, err = queue.EnqueueTx(ctx, t.tx, queue.EnqueueRequest{
		WorkspaceID:    t.workspaceID,
		Kind:           queue.KindRuntimeConfigUpdate,
		PartitionKey:   queue.FormatSessionPartitionKey(t.workspaceID, sessionID),
		DedupeKey:      queue.FormatRuntimeConfigUpdateDedupeKey(t.workspaceID, sessionID, configGenerationToken),
		PayloadVersion: 2,
		PayloadJSON:    payload,
		Now:            now,
	})
	return err
}

func (t *postgresqlTransaction) UpsertSessionProviderAuth(ctx context.Context, selector SessionProviderAuthAdmission) error {
	if selector.SessionID == "" {
		return &ValidationError{Message: "session_id is required"}
	}
	if selector.ProviderID == "" {
		return &ValidationError{Message: "providers provider_id is required"}
	}
	if selector.VaultID == "" {
		return &ValidationError{Message: "provider credential vault_id is required"}
	}
	if selector.CredentialID == "" {
		return &ValidationError{Message: "providers credential_id is required"}
	}
	if selector.AccessMode == "" {
		return &ValidationError{Message: "provider credential access_mode is required"}
	}
	updatedAt := selector.UpdatedAt
	if updatedAt.IsZero() {
		updatedAt = storage.Now()
	}
	if _, err := t.tx.Exec(ctx,
		`UPDATE session_provider_auth
		    SET deleted_at = COALESCE(deleted_at, $1),
		        updated_at = $1
		  WHERE workspace_id = $2
		    AND session_id = $3
		    AND provider_id <> $4
		    AND deleted_at IS NULL`,
		updatedAt,
		string(t.workspaceID),
		selector.SessionID,
		selector.ProviderID,
	); err != nil {
		return err
	}
	_, err := t.tx.Exec(ctx,
		`INSERT INTO session_provider_auth (
			workspace_id, session_id, provider_id, vault_id, credential_id, access_mode, created_at, updated_at, deleted_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $7, NULL)
		ON CONFLICT (workspace_id, session_id, provider_id)
		DO UPDATE SET
			vault_id = EXCLUDED.vault_id,
			credential_id = EXCLUDED.credential_id,
			access_mode = EXCLUDED.access_mode,
			updated_at = EXCLUDED.updated_at,
			deleted_at = NULL`,
		string(t.workspaceID),
		selector.SessionID,
		selector.ProviderID,
		selector.VaultID,
		selector.CredentialID,
		selector.AccessMode,
		updatedAt,
	)
	return err
}

func (t *postgresqlTransaction) ListSessionProviderAuth(ctx context.Context, sessionID string) (ProviderSelectors, error) {
	rows, err := t.tx.QueryRows(ctx,
		`SELECT provider_id, credential_id
		   FROM session_provider_auth
		  WHERE workspace_id = $1
		    AND session_id = $2
		    AND deleted_at IS NULL
		  ORDER BY provider_id ASC`,
		string(t.workspaceID),
		sessionID,
	)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	selectors := ProviderSelectors{}
	for rows.Next() {
		var providerID string
		var credentialID string
		if err := rows.Scan(&providerID, &credentialID); err != nil {
			return nil, err
		}
		selectors[providerID] = ProviderCredentialSelector{CredentialID: credentialID}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return selectors, nil
}

func (t *postgresqlTransaction) GetProviderCredentialForAdmission(ctx context.Context, credentialID string, boundVaultIDs []string) (*ProviderCredentialForAdmission, error) {
	boundVaultIDsJSON, err := json.Marshal(boundVaultIDs)
	if err != nil {
		return nil, err
	}
	rows, err := t.tx.QueryRows(ctx,
		`WITH bound_vaults AS (
			SELECT jsonb_array_elements_text($3::jsonb) AS vault_id
		)
		SELECT c.id, c.vault_id, c.auth_type, COALESCE(c.provider_id, ''), COALESCE(c.access_mode, ''), c.archived_at, c.revoked_at
		  FROM credentials c
		  JOIN bound_vaults bv ON bv.vault_id = c.vault_id
		 WHERE c.workspace_id = $1 AND c.id = $2
		 ORDER BY c.vault_id ASC
		 FOR UPDATE`,
		string(t.workspaceID),
		credentialID,
		string(boundVaultIDsJSON),
	)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var matches []ProviderCredentialForAdmission
	for rows.Next() {
		var credential ProviderCredentialForAdmission
		var archivedAt sql.NullTime
		var revokedAt sql.NullTime
		if scanErr := rows.Scan(&credential.ID, &credential.VaultID, &credential.AuthType, &credential.ProviderID, &credential.AccessMode, &archivedAt, &revokedAt); scanErr != nil {
			return nil, scanErr
		}
		credential.Archived = archivedAt.Valid
		credential.Revoked = revokedAt.Valid
		matches = append(matches, credential)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(matches) == 0 {
		var workspaceMatches int
		if countErr := t.tx.QueryRowScanner(ctx,
			`SELECT count(*) FROM credentials WHERE workspace_id = $1 AND id = $2`,
			string(t.workspaceID),
			credentialID,
		).Scan(&workspaceMatches); countErr != nil {
			return nil, countErr
		}
		if workspaceMatches > 0 {
			return nil, &PermissionError{Message: "provider credential is inaccessible"}
		}
		return nil, &NotFoundError{Message: "provider credential not found"}
	}
	if len(matches) > 1 {
		return nil, &PermissionError{Message: "provider credential is inaccessible"}
	}
	return &matches[0], nil
}

func (t *postgresqlTransaction) ArchiveSession(ctx context.Context, sessionID string, archivedAt time.Time) (*Session, error) {
	if archivedAt.IsZero() {
		archivedAt = storage.Now()
	}
	current, err := t.loadSessionForUpdate(ctx, sessionID)
	if err != nil {
		return nil, err
	}
	if current.LifecycleState == LifecycleStateDeleted {
		return nil, &ConflictError{Message: "session lifecycle transition is already in progress", InvalidRequest: true}
	}
	if current.Status == StatusRunning {
		return nil, &ConflictError{Message: "running sessions cannot be archived", InvalidRequest: true}
	}
	if current.LifecycleState == LifecycleStateActive {
		result, err := t.tx.Exec(ctx,
			`UPDATE sessions
			    SET lifecycle_state = 'archiving',
			        updated_at = $1
			  WHERE workspace_id = $2 AND id = $3
			    AND lifecycle_state = 'active'`,
			archivedAt,
			string(t.workspaceID),
			sessionID,
		)
		if err != nil {
			return nil, err
		}
		if affected, _ := result.RowsAffected(); affected == 0 {
			return nil, &NotFoundError{Message: "session not found"}
		}
	}
	result, err := t.tx.Exec(ctx,
		`UPDATE sessions
		    SET archived_at = COALESCE(archived_at, $1),
		        lifecycle_state = 'archived',
		        updated_at = $1
		  WHERE workspace_id = $2 AND id = $3
		    AND lifecycle_state IN ('archiving', 'archived')`,
		archivedAt,
		string(t.workspaceID),
		sessionID,
	)
	if err != nil {
		return nil, err
	}
	if affected, _ := result.RowsAffected(); affected == 0 {
		return nil, &NotFoundError{Message: "session not found"}
	}
	return t.loadSession(ctx, sessionID)
}

func (t *postgresqlTransaction) DeleteSession(ctx context.Context, sessionID string) error {
	current, err := t.loadSessionForUpdate(ctx, sessionID)
	if err != nil {
		return err
	}
	if current.LifecycleState == LifecycleStateArchiving {
		return &ConflictError{Message: "session lifecycle transition is already in progress", InvalidRequest: true}
	}
	if current.LifecycleState == LifecycleStateDeleted {
		return nil
	}
	if current.Status == StatusRunning || current.Status == StatusRescheduling {
		return &ConflictError{Message: "running or rescheduling sessions cannot be deleted", InvalidRequest: true}
	}
	deleteCleanupID := id.New("delcln_")
	timestamp := storage.Now()
	if t.store.sessionDeleteSandboxRelease == nil {
		return errors.New("session: delete sandbox release recorder is unavailable")
	}
	rawTx, err := t.rawDBTx()
	if err != nil {
		return err
	}
	// Deletion commits its Sandbox release fence before cleanup can be leased;
	// a later cleanup pass may join this operation but cannot be its producer.
	if err := t.store.sessionDeleteSandboxRelease(ctx, rawTx, t.workspaceID, sessionID, timestamp); err != nil {
		return err
	}
	if err := t.appendSessionDeletedEvent(ctx, sessionID, timestamp); err != nil {
		return err
	}
	if _, err := t.tx.Exec(ctx,
		`UPDATE session_resources
		    SET detached_at = COALESCE(detached_at, $1),
		        updated_at = $1
		  WHERE workspace_id = $2
		    AND session_id = $3
		    AND detached_at IS NULL`,
		timestamp,
		string(t.workspaceID),
		sessionID,
	); err != nil {
		return err
	}
	result, err := t.tx.Exec(ctx,
		`UPDATE sessions
		    SET lifecycle_state = 'deleted', delete_cleanup_id = $4, updated_at = $1
		  WHERE workspace_id = $2 AND id = $3`,
		timestamp,
		string(t.workspaceID),
		sessionID,
		deleteCleanupID,
	)
	if err != nil {
		return err
	}
	if affected, _ := result.RowsAffected(); affected == 0 {
		return &NotFoundError{Message: "session not found"}
	}
	if err := t.markSessionResourcePrefixGC(ctx, sessionID, timestamp); err != nil {
		return err
	}
	return t.enqueueSessionDeleteCleanup(ctx, sessionID, deleteCleanupID, timestamp)
}

func (t *postgresqlTransaction) enqueueSessionDeleteCleanup(ctx context.Context, sessionID string, deleteCleanupID string, timestamp time.Time) error {
	payload, err := json.Marshal(map[string]string{
		"workspace_id":      string(t.workspaceID),
		"session_id":        sessionID,
		"delete_cleanup_id": deleteCleanupID,
	})
	if err != nil {
		return err
	}
	now := timestamp
	_, err = queue.EnqueueTx(ctx, t.tx, queue.EnqueueRequest{
		WorkspaceID:    t.workspaceID,
		Kind:           queue.KindSessionDeleteCleanup,
		PartitionKey:   queue.FormatSessionPartitionKey(t.workspaceID, sessionID),
		DedupeKey:      queue.FormatSessionDeleteCleanupDedupeKey(t.workspaceID, sessionID, deleteCleanupID),
		PayloadVersion: 1,
		PayloadJSON:    payload,
		MaxAttempts:    0,
		Now:            now,
	})
	return err
}

func (t *postgresqlTransaction) appendSessionDeletedEvent(ctx context.Context, sessionID string, timestamp time.Time) error {
	return t.appendPublicProcessedSessionEvent(ctx, sessionID, "", "session.deleted", map[string]string{
		"id":   sessionID,
		"type": "session.deleted",
	}, timestamp)
}

func (t *postgresqlTransaction) appendPublicProcessedSessionEvent(ctx context.Context, sessionID string, sessionThreadID string, eventType string, payloadValue any, timestamp time.Time) error {
	sequence, err := t.nextSessionEventSequence(ctx, sessionID, sessionThreadID)
	if err != nil {
		return err
	}
	eventID := id.New("sevt_")
	payload, err := json.Marshal(payloadValue)
	if err != nil {
		return err
	}
	sessionVisible := controlPlaneEventSessionVisible(eventType, sessionThreadID)
	eventTimestamp := timestamp
	if _, err := t.tx.Exec(ctx,
		`INSERT INTO session_events (
			workspace_id, session_id, session_thread_id, event_id, sequence, type, payload_json,
			visibility, session_visible, projection_json, created_at, updated_at, processed_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, 'public', $8, $7, $9, $9, $9)`,
		string(t.workspaceID),
		sessionID,
		nullableEmptyString(sessionThreadID),
		eventID,
		sequence,
		eventType,
		string(payload),
		sessionVisible,
		eventTimestamp,
	); err != nil {
		return err
	}
	return t.appendSessionEventStreamChange(ctx, sessionID, sessionThreadID, eventID, sessionVisible, eventTimestamp)
}

func (t *postgresqlTransaction) nextSessionEventSequence(ctx context.Context, sessionID string, sessionThreadID string) (int64, error) {
	var sequence int64
	err := t.tx.QueryRowScanner(ctx,
		`SELECT COALESCE(MAX(sequence), 0) + 1
		   FROM session_events
		  WHERE workspace_id = $1
		    AND session_id = $2
		    AND session_thread_id IS NOT DISTINCT FROM $3`,
		string(t.workspaceID),
		sessionID,
		nullableEmptyString(sessionThreadID),
	).Scan(&sequence)
	return sequence, err
}

func (t *postgresqlTransaction) appendSessionEventStreamChange(ctx context.Context, sessionID string, sessionThreadID string, eventID string, sessionVisible bool, timestamp time.Time) error {
	var streamPosition int64
	if err := t.tx.QueryRowScanner(ctx,
		`INSERT INTO session_event_stream_changes (
			workspace_id, session_id, event_id, session_thread_id, revision, visibility, session_visible, changed_at
		) VALUES ($1, $2, $3, $4, 1, 'public', $5, $6)
		RETURNING stream_position`,
		string(t.workspaceID),
		sessionID,
		eventID,
		nullableEmptyString(sessionThreadID),
		sessionVisible,
		timestamp,
	).Scan(&streamPosition); err != nil {
		return err
	}
	_, err := t.tx.Exec(ctx,
		`UPDATE session_events
		    SET latest_stream_position = $4,
		        insert_stream_position = CASE
		            WHEN insert_stream_position = 0 THEN $4
		            ELSE insert_stream_position
		        END
		  WHERE workspace_id = $1
		    AND session_id = $2
		    AND event_id = $3`,
		string(t.workspaceID),
		sessionID,
		eventID,
		streamPosition,
	)
	return err
}

func controlPlaneEventSessionVisible(eventType string, sessionThreadID string) bool {
	if sessionThreadID == "" {
		return true
	}
	switch eventType {
	case "session.thread_created",
		"session.thread_status_running",
		"session.thread_status_idle",
		"session.thread_status_rescheduled",
		"session.thread_status_terminated",
		"agent.thread_message_sent",
		"agent.thread_message_received":
		return true
	default:
		return false
	}
}

func (t *postgresqlTransaction) markSessionResourcePrefixGC(ctx context.Context, sessionID string, timestamp time.Time) error {
	prefix := "workspaces/" + string(t.workspaceID) + "/sessions/" + sessionID + "/"
	_, err := t.tx.Exec(ctx,
		`INSERT INTO session_resource_prefix_gc (
			workspace_id, session_id, prefix, status, created_at, updated_at
		) VALUES ($1, $2, $3, 'pending', $4, $4)
		ON CONFLICT (workspace_id, session_id) DO UPDATE
		    SET prefix = EXCLUDED.prefix,
		        status = 'pending',
		        next_attempt_at = NULL,
		        completed_at = NULL,
		        last_error_kind = NULL,
		        updated_at = EXCLUDED.updated_at`,
		string(t.workspaceID),
		sessionID,
		prefix,
		timestamp,
	)
	return err
}

func (t *postgresqlTransaction) ListThreads(ctx context.Context, sessionID string, options ThreadListOptions) ([]*Thread, bool, error) {
	limit := normalizeLimit(options.Limit)
	if options.Page != "" {
		payload, err := decodePageToken(t.store.pageTokenSecret, options.Page, threadListAssociatedData(t.workspaceID, sessionID))
		if err != nil {
			return nil, false, err
		}
		if payload.Kind != pageKindThreads || payload.ThreadID == "" {
			return nil, false, &ValidationError{Message: "invalid page token"}
		}
		sequence, err := t.loadThreadCursorSequence(ctx, sessionID, payload.ThreadID)
		if err != nil {
			return nil, false, err
		}
		options.cursorSequence = sequence
		options.cursorID = payload.ThreadID
	}
	rows, err := t.tx.QueryRows(ctx,
		threadSelectSQL+`
		  WHERE workspace_id = $1
		    AND session_id = $2
		    AND visibility = 'public'
		    AND role <> 'approval_reviewer'
		    AND ($3::bigint = 0 OR (storage_sequence, id) > ($3::bigint, $4))
		  ORDER BY storage_sequence ASC, id ASC
		  LIMIT $5`,
		string(t.workspaceID), sessionID, options.cursorSequence, options.cursorID, limit+1,
	)
	if err != nil {
		return nil, false, err
	}
	defer func() { _ = rows.Close() }()
	threads, err := scanThreadRows(rows, t.workspaceID)
	if err != nil {
		return nil, false, err
	}
	hasMore := len(threads) > limit
	if hasMore {
		threads = threads[:limit]
	}
	return threads, hasMore, nil
}

func (t *postgresqlTransaction) GetThread(ctx context.Context, sessionID string, threadID string) (*Thread, error) {
	row := t.tx.QueryRowScanner(ctx,
		threadSelectSQL+`
		  WHERE workspace_id = $1
		    AND session_id = $2
		    AND id = $3
		    AND visibility = 'public'
		    AND role <> 'approval_reviewer'`,
		string(t.workspaceID), sessionID, threadID,
	)
	return scanThreadRow(row, t.workspaceID)
}

func (t *postgresqlTransaction) loadThreadUsage(ctx context.Context, sessionID string, threadID string) (Usage, error) {
	var (
		requestCount int64
		usage        Usage
		cacheWrite   int64
	)
	err := t.tx.QueryRowScanner(ctx,
		`SELECT COUNT(*),
		        COALESCE(SUM(r.input_total_tokens), 0),
		        COALESCE(SUM(r.output_total_tokens), 0),
		        COALESCE(SUM(r.input_cache_read_tokens), 0),
		        COALESCE(SUM(r.input_cache_write_tokens), 0)
		   FROM request_usage_details r
		  WHERE r.workspace_id = $1
		    AND r.session_id = $2
		    AND r.session_thread_id = $3`,
		string(t.workspaceID), sessionID, threadID,
	).Scan(
		&requestCount,
		&usage.InputTokens,
		&usage.OutputTokens,
		&usage.CacheReadInputTokens,
		&cacheWrite,
	)
	if err != nil {
		return Usage{}, err
	}
	if requestCount > 0 {
		usage.CacheCreation.Ephemeral1hInputTokens = &cacheWrite
		ephemeral5m := int64(0)
		usage.CacheCreation.Ephemeral5mInputTokens = &ephemeral5m
	}
	return usage, nil
}

func (t *postgresqlTransaction) ArchiveThread(ctx context.Context, sessionID string, threadID string, archivedAt time.Time) (*Thread, error) {
	if archivedAt.IsZero() {
		archivedAt = storage.Now()
	}
	thread, err := t.lockPublicThreadForArchive(ctx, sessionID, threadID)
	if err != nil {
		return nil, err
	}
	if thread.ArchivedAt != nil {
		return thread, nil
	}
	if err := t.rejectActivePrimaryThreadArchive(ctx, sessionID, threadID); err != nil {
		return nil, err
	}
	if thread.Status == ThreadStatusRunning || thread.Status == ThreadStatusRescheduling {
		return nil, &ConflictError{Message: "running or rescheduling session threads cannot be archived", InvalidRequest: true}
	}
	if err := t.rejectActiveThreadRuntimeInput(ctx, sessionID, threadID); err != nil {
		return nil, err
	}
	if err := t.rejectUnprocessedThreadInput(ctx, sessionID, threadID); err != nil {
		return nil, err
	}
	if err := t.rejectUnresolvedThreadWait(ctx, sessionID, threadID); err != nil {
		return nil, err
	}
	result, err := t.tx.Exec(ctx,
		`UPDATE session_threads
		    SET archived_at = $4,
		        updated_at = $4
		  WHERE workspace_id = $1
		    AND session_id = $2
		    AND id = $3
		    AND archived_at IS NULL`,
		string(t.workspaceID),
		sessionID,
		threadID,
		archivedAt,
	)
	if err != nil {
		return nil, err
	}
	if affected, _ := result.RowsAffected(); affected == 0 {
		return t.GetThread(ctx, sessionID, threadID)
	}
	return t.GetThread(ctx, sessionID, threadID)
}

func (t *postgresqlTransaction) rejectUnprocessedThreadInput(ctx context.Context, sessionID string, threadID string) error {
	var exists bool
	if err := t.tx.QueryRowScanner(ctx,
		`SELECT EXISTS (
			SELECT 1
			  FROM session_events e
			 WHERE e.workspace_id = $1
			   AND e.session_id = $2
			   AND e.session_thread_id = $3
			   AND e.processed_at IS NULL
			   AND e.type IN ('user.message', 'user.interrupt', 'user.tool_confirmation')
			   AND (
			     e.type <> 'user.message'
			     OR NOT EXISTS (
			       SELECT 1
			         FROM session_events interrupt
			        WHERE interrupt.workspace_id = e.workspace_id
			          AND interrupt.session_id = e.session_id
			          AND interrupt.session_thread_id = e.session_thread_id
			          AND interrupt.type = 'user.interrupt'
			          AND interrupt.sequence > e.sequence
			     )
			   )
		)`,
		string(t.workspaceID), sessionID, threadID,
	).Scan(&exists); err != nil {
		return err
	}
	if exists {
		return &ConflictError{Message: "session thread has unprocessed runtime input", InvalidRequest: true}
	}
	return nil
}

func (t *postgresqlTransaction) rejectActivePrimaryThreadArchive(ctx context.Context, sessionID string, threadID string) error {
	var mainThreadID string
	var runtimeStatus string
	if err := t.tx.QueryRowScanner(ctx,
		`SELECT s.main_thread_id, rs.status
		   FROM sessions s
		   JOIN session_runtime_status rs
		     ON rs.workspace_id = s.workspace_id
		    AND rs.session_id = s.id
		  WHERE s.workspace_id = $1
		    AND s.id = $2
		  FOR UPDATE OF s, rs`,
		string(t.workspaceID), sessionID,
	).Scan(&mainThreadID, &runtimeStatus); err != nil {
		return err
	}
	if threadID == mainThreadID && runtimeStatus != "idle" {
		return &ConflictError{Message: "primary session thread can only be archived while runtime is idle", InvalidRequest: true}
	}
	return nil
}

func (t *postgresqlTransaction) lockPublicThreadForArchive(ctx context.Context, sessionID string, threadID string) (*Thread, error) {
	row := t.tx.QueryRowScanner(ctx,
		threadSelectSQL+`
		  WHERE workspace_id = $1
		    AND session_id = $2
		    AND id = $3
		    AND visibility = 'public'
		    AND role <> 'approval_reviewer'
		  FOR UPDATE`,
		string(t.workspaceID), sessionID, threadID,
	)
	return scanThreadRow(row, t.workspaceID)
}

func (t *postgresqlTransaction) rejectActiveThreadRuntimeInput(ctx context.Context, sessionID string, threadID string) error {
	var exists bool
	if err := t.tx.QueryRowScanner(ctx,
		`SELECT EXISTS (
			SELECT 1
			  FROM session_runtime_inbox
			 WHERE workspace_id = $1
			   AND session_id = $2
			   AND session_thread_id = $3
			   AND status IN ('delivering', 'accepted')
		)`,
		string(t.workspaceID), sessionID, threadID,
	).Scan(&exists); err != nil {
		return err
	}
	if exists {
		return &ConflictError{Message: "session thread has active runtime input", InvalidRequest: true}
	}
	if err := t.tx.QueryRowScanner(ctx,
		`SELECT EXISTS (
			SELECT 1
			  FROM queue_jobs
			 WHERE workspace_id = $1
			   AND kind = 'runtime_input'
			   AND status IN ('pending', 'leased')
			   AND payload_json::jsonb ->> 'session_id' = $2
			   AND payload_json::jsonb ->> 'session_thread_id' = $3
		)`,
		string(t.workspaceID), sessionID, threadID,
	).Scan(&exists); err != nil {
		return err
	}
	if exists {
		return &ConflictError{Message: "session thread has active runtime input", InvalidRequest: true}
	}
	return nil
}

func (t *postgresqlTransaction) rejectUnresolvedThreadWait(ctx context.Context, sessionID string, threadID string) error {
	var exists bool
	if err := t.tx.QueryRowScanner(ctx,
		`SELECT EXISTS (
			SELECT 1
			  FROM session_pending_tool_uses
			 WHERE workspace_id = $1
			   AND session_id = $2
			   AND session_thread_id = $3
			   AND status IN ('pending', 'resolving')
		)`,
		string(t.workspaceID), sessionID, threadID,
	).Scan(&exists); err != nil {
		return err
	}
	if exists {
		return &ConflictError{Message: "session thread has unresolved pending external wait", InvalidRequest: true}
	}
	return nil
}

func (t *postgresqlTransaction) CreateResource(ctx context.Context, resource *Resource) error {
	if resource == nil {
		return &ValidationError{Message: "session resource is required"}
	}
	if resource.WorkspaceID == "" {
		resource.WorkspaceID = t.workspaceID
	}
	if resource.WorkspaceID != t.workspaceID {
		return &ValidationError{Message: "workspace_id mismatch"}
	}
	if resource.CreatedAt.IsZero() {
		resource.CreatedAt = storage.Now()
	}
	if resource.UpdatedAt.IsZero() {
		resource.UpdatedAt = resource.CreatedAt
	}
	if _, err := t.tx.Exec(ctx,
		`INSERT INTO session_resources (
			workspace_id, session_id, resource_id, type, created_at, updated_at, detached_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7)`,
		string(t.workspaceID),
		resource.SessionID,
		resource.ID,
		string(resource.Type),
		resource.CreatedAt,
		resource.UpdatedAt,
		resource.DetachedAt,
	); err != nil {
		return mapPostgreSQLSessionError(err)
	}
	switch resource.Type {
	case ResourceTypeFile:
		if resource.File == nil {
			return &ValidationError{Message: "file resource detail is required"}
		}
		_, err := t.tx.Exec(ctx,
			`INSERT INTO session_file_resources (
				workspace_id, session_id, resource_id, source_file_id, file_id, mount_path
			) VALUES ($1, $2, $3, $4, $5, $6)`,
			string(t.workspaceID), resource.SessionID, resource.ID,
			resource.File.SourceFileID, resource.File.FileID, resource.File.MountPath,
		)
		return mapPostgreSQLSessionError(err)
	case ResourceTypeMemoryStore:
		if resource.MemoryStore == nil {
			return &ValidationError{Message: "memory_store resource detail is required"}
		}
		_, err := t.tx.Exec(ctx,
			`INSERT INTO session_memory_store_resources (
				workspace_id, session_id, resource_id, memory_store_id, access, instructions, name, description, mount_path
			) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`,
			string(t.workspaceID), resource.SessionID, resource.ID,
			resource.MemoryStore.MemoryStoreID, resource.MemoryStore.Access, resource.MemoryStore.Instructions,
			resource.MemoryStore.Name, resource.MemoryStore.Description, resource.MemoryStore.MountPath,
		)
		return mapPostgreSQLSessionError(err)
	case ResourceTypeGitHubRepository:
		if resource.GitHubRepository == nil {
			return &ValidationError{Message: "github_repository resource detail is required"}
		}
		_, err := t.tx.Exec(ctx,
			`INSERT INTO session_github_repository_resources (
				workspace_id, session_id, resource_id, url, mount_path, checkout_type, checkout_ref,
				authorization_token_encrypted
			) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
			string(t.workspaceID), resource.SessionID, resource.ID,
			resource.GitHubRepository.URL, resource.GitHubRepository.MountPath,
			nullableEmptyString(resource.GitHubRepository.CheckoutType),
			nullableEmptyString(resource.GitHubRepository.CheckoutRef),
			resource.GitHubRepository.AuthorizationTokenEncrypted,
		)
		return mapPostgreSQLSessionError(err)
	default:
		return &ValidationError{Message: "invalid session resource type"}
	}
}

func (t *postgresqlTransaction) UpdateGitHubRepositoryToken(ctx context.Context, sessionID string, resourceID string, encryptedToken []byte, updatedAt time.Time) (*Resource, error) {
	if updatedAt.IsZero() {
		updatedAt = storage.Now()
	}
	result, err := t.tx.Exec(ctx,
		`UPDATE session_github_repository_resources gr
		    SET authorization_token_encrypted = $1
		   FROM session_resources sr
		  WHERE gr.workspace_id = $2
		    AND gr.session_id = $3
		    AND gr.resource_id = $4
		    AND sr.workspace_id = gr.workspace_id
		    AND sr.session_id = gr.session_id
		    AND sr.resource_id = gr.resource_id
		    AND sr.type = 'github_repository'
		    AND sr.detached_at IS NULL
		    AND sr.delete_requested_at IS NULL`,
		encryptedToken,
		string(t.workspaceID),
		sessionID,
		resourceID,
	)
	if err != nil {
		return nil, err
	}
	if affected, _ := result.RowsAffected(); affected == 0 {
		return nil, &NotFoundError{Message: "session resource not found"}
	}
	if _, err := t.tx.Exec(ctx,
		`UPDATE session_resources
		    SET updated_at = $1
		  WHERE workspace_id = $2 AND session_id = $3 AND resource_id = $4`,
		updatedAt,
		string(t.workspaceID),
		sessionID,
		resourceID,
	); err != nil {
		return nil, err
	}
	return t.GetResource(ctx, sessionID, resourceID)
}

func (t *postgresqlTransaction) ListResources(ctx context.Context, sessionID string, options ResourceListOptions) ([]*Resource, bool, error) {
	limit := normalizeLimit(options.Limit)
	if options.Page != "" {
		payload, err := decodePageToken(t.store.pageTokenSecret, options.Page, resourceListAssociatedData(t.workspaceID, sessionID))
		if err != nil {
			return nil, false, err
		}
		if payload.Kind != pageKindResources || payload.ResourceID == "" {
			return nil, false, &ValidationError{Message: "invalid page token"}
		}
		sequence, err := t.loadResourceCursorSequence(ctx, sessionID, payload.ResourceID)
		if err != nil {
			return nil, false, err
		}
		options.cursorSequence = sequence
		options.cursorID = payload.ResourceID
	}
	rows, err := t.tx.QueryRows(ctx,
		resourceSelectSQL+`
		  WHERE sr.workspace_id = $1
		    AND sr.session_id = $2
		    AND sr.detached_at IS NULL
		    AND sr.delete_requested_at IS NULL
		    AND (
		      sr.type <> 'file'
		      OR EXISTS (
		        SELECT 1
		          FROM files f
		         WHERE f.workspace_id = sr.workspace_id
		           AND f.file_id = sfr.file_id
		           AND f.scope_type = 'session'
		           AND f.scope_id = sr.session_id
		           AND f.deleted_at IS NULL
		      )
		    )
		    AND ($3::bigint = 0 OR (sr.storage_sequence, sr.resource_id) > ($3::bigint, $4))
		  ORDER BY sr.storage_sequence ASC, sr.resource_id ASC
		  LIMIT $5`,
		string(t.workspaceID), sessionID, options.cursorSequence, options.cursorID, limit+1,
	)
	if err != nil {
		return nil, false, err
	}
	defer func() { _ = rows.Close() }()
	resources, err := scanResourceRows(rows, t.workspaceID)
	if err != nil {
		return nil, false, err
	}
	hasMore := len(resources) > limit
	if hasMore {
		resources = resources[:limit]
	}
	return resources, hasMore, nil
}

func (t *postgresqlTransaction) loadResourceCursorSequence(ctx context.Context, sessionID string, resourceID string) (int64, error) {
	row := t.tx.QueryRowScanner(ctx,
		`SELECT storage_sequence
		   FROM session_resources
		  WHERE workspace_id = $1
		    AND session_id = $2
		    AND resource_id = $3`,
		string(t.workspaceID), sessionID, resourceID,
	)
	var sequence int64
	if err := row.Scan(&sequence); dbconnect.IsNoRows(err) {
		return 0, &ValidationError{Message: "invalid page token"}
	} else if err != nil {
		return 0, err
	}
	if sequence <= 0 {
		return 0, &ValidationError{Message: "invalid page token"}
	}
	return sequence, nil
}

func (t *postgresqlTransaction) loadThreadCursorSequence(ctx context.Context, sessionID string, threadID string) (int64, error) {
	row := t.tx.QueryRowScanner(ctx,
		`SELECT storage_sequence
		   FROM session_threads
		  WHERE workspace_id = $1
		    AND session_id = $2
		    AND id = $3
		    AND visibility = 'public'
		    AND role <> 'approval_reviewer'`,
		string(t.workspaceID), sessionID, threadID,
	)
	var sequence int64
	if err := row.Scan(&sequence); dbconnect.IsNoRows(err) {
		return 0, &ValidationError{Message: "invalid page token"}
	} else if err != nil {
		return 0, err
	}
	if sequence <= 0 {
		return 0, &ValidationError{Message: "invalid page token"}
	}
	return sequence, nil
}

func (t *postgresqlTransaction) GetResource(ctx context.Context, sessionID string, resourceID string) (*Resource, error) {
	row := t.tx.QueryRowScanner(ctx,
		resourceSelectSQL+`
		  WHERE sr.workspace_id = $1
		    AND sr.session_id = $2
		    AND sr.resource_id = $3
		    AND sr.detached_at IS NULL
		    AND sr.delete_requested_at IS NULL
		    AND (
		      sr.type <> 'file'
		      OR EXISTS (
		        SELECT 1
		          FROM files f
		         WHERE f.workspace_id = sr.workspace_id
		           AND f.file_id = sfr.file_id
		           AND f.scope_type = 'session'
		           AND f.scope_id = sr.session_id
		           AND f.deleted_at IS NULL
		      )
		    )
		    AND EXISTS (
			SELECT 1 FROM sessions s
			 WHERE s.workspace_id = sr.workspace_id
			   AND s.id = sr.session_id
			   AND `+publicSessionVisiblePredicate("s")+`
		    )`,
		string(t.workspaceID), sessionID, resourceID,
	)
	return scanResourceRow(row, t.workspaceID)
}

func (t *postgresqlTransaction) RequestResourceDelete(ctx context.Context, sessionID string, resourceID string, requestedAt time.Time) (*Resource, error) {
	if err := t.requireIdleRuntimeStatusForResourceMutation(ctx, sessionID); err != nil {
		return nil, err
	}
	row := t.tx.QueryRowScanner(ctx,
		resourceSelectSQL+`
		  WHERE sr.workspace_id = $1
		    AND sr.session_id = $2
		    AND sr.resource_id = $3
		    AND sr.detached_at IS NULL
		    AND (
		      sr.type <> 'file'
		      OR EXISTS (
		        SELECT 1
		          FROM files f
		         WHERE f.workspace_id = sr.workspace_id
		           AND f.file_id = sfr.file_id
		           AND f.scope_type = 'session'
		           AND f.scope_id = sr.session_id
		      )
		    )`,
		string(t.workspaceID), sessionID, resourceID,
	)
	resource, err := scanResourceRow(row, t.workspaceID)
	if err != nil {
		return nil, err
	}
	if resource.DeleteRequestedAt != nil {
		return nil, &ConflictError{Message: "session resource deletion is already in progress", InvalidRequest: true}
	}
	if requestedAt.IsZero() {
		requestedAt = storage.Now()
	}
	var materialized bool
	err = t.tx.QueryRowScanner(ctx,
		`SELECT provider_resource_id IS NOT NULL AND materialized_resource_revision > 0
		   FROM session_sandbox_bindings
		  WHERE workspace_id = $1 AND session_id = $2
		  FOR UPDATE`,
		string(t.workspaceID), sessionID,
	).Scan(&materialized)
	if dbconnect.IsNoRows(err) {
		materialized = false
	} else if err != nil {
		return nil, err
	}
	var result interface {
		RowsAffected() (int64, error)
	}
	if materialized {
		result, err = t.tx.Exec(ctx,
			`UPDATE session_resources
			    SET delete_requested_at = COALESCE(delete_requested_at, $1),
			        updated_at = $1
			  WHERE workspace_id = $2
			    AND session_id = $3
			    AND resource_id = $4
			    AND detached_at IS NULL`,
			requestedAt, string(t.workspaceID), sessionID, resourceID,
		)
	} else {
		result, err = t.tx.Exec(ctx,
			`UPDATE session_resources
			    SET detached_at = COALESCE(detached_at, $1),
			        updated_at = $1
			  WHERE workspace_id = $2
			    AND session_id = $3
			    AND resource_id = $4
			    AND detached_at IS NULL`,
			requestedAt, string(t.workspaceID), sessionID, resourceID,
		)
	}
	if err != nil {
		return nil, err
	}
	if affected, _ := result.RowsAffected(); affected == 0 {
		return nil, &NotFoundError{Message: "session resource not found"}
	}
	if materialized {
		resource.DeleteRequestedAt = &requestedAt
	} else {
		resource.DetachedAt = &requestedAt
	}
	resource.UpdatedAt = requestedAt
	return resource, nil
}

func (t *postgresqlTransaction) DetachResource(ctx context.Context, sessionID string, resourceID string, detachedAt time.Time) (*Resource, error) {
	resource, err := t.GetResource(ctx, sessionID, resourceID)
	if err != nil {
		return nil, err
	}
	if detachedAt.IsZero() {
		detachedAt = storage.Now()
	}
	result, err := t.tx.Exec(ctx,
		`UPDATE session_resources
		    SET detached_at = $1, updated_at = $1
		  WHERE workspace_id = $2 AND session_id = $3 AND resource_id = $4 AND detached_at IS NULL`,
		detachedAt, string(t.workspaceID), sessionID, resourceID,
	)
	if err != nil {
		return nil, err
	}
	if affected, _ := result.RowsAffected(); affected == 0 {
		return nil, &NotFoundError{Message: "session resource not found"}
	}
	resource.DetachedAt = &detachedAt
	resource.UpdatedAt = detachedAt
	return resource, nil
}

func (t *postgresqlTransaction) ReattachResource(ctx context.Context, sessionID string, resourceID string, updatedAt time.Time) (*Resource, error) {
	if updatedAt.IsZero() {
		updatedAt = storage.Now()
	}
	result, err := t.tx.Exec(ctx,
		`UPDATE session_resources
		    SET detached_at = NULL, updated_at = $1
		  WHERE workspace_id = $2 AND session_id = $3 AND resource_id = $4 AND detached_at IS NOT NULL`,
		updatedAt, string(t.workspaceID), sessionID, resourceID,
	)
	if err != nil {
		return nil, err
	}
	if affected, _ := result.RowsAffected(); affected == 0 {
		return nil, &NotFoundError{Message: "session resource not found"}
	}
	return t.GetResource(ctx, sessionID, resourceID)
}

func (t *postgresqlTransaction) loadSession(ctx context.Context, sessionID string) (*Session, error) {
	row := t.tx.QueryRowScanner(ctx,
		sessionSelectSQL("s")+` WHERE s.workspace_id = $1 AND s.id = $2 AND `+publicSessionVisiblePredicate("s"),
		string(t.workspaceID), sessionID,
	)
	sess, err := scanSessionRow(row)
	if err != nil {
		return nil, err
	}
	resources, err := t.loadActiveResources(ctx, sessionID)
	if err != nil {
		return nil, err
	}
	sess.Resources = resources
	return sess, nil
}

func (t *postgresqlTransaction) loadSessionForUpdate(ctx context.Context, sessionID string) (*Session, error) {
	row := t.tx.QueryRowScanner(ctx,
		sessionSelectSQL("s")+` WHERE s.workspace_id = $1 AND s.id = $2 FOR UPDATE OF s`,
		string(t.workspaceID), sessionID,
	)
	return scanSessionRow(row)
}

type sessionUsability struct {
	archivedAt     sql.NullTime
	rawStatus      Status
	status         Status
	lifecycleState LifecycleState
}

func (t *postgresqlTransaction) loadSessionUsabilityForUpdate(ctx context.Context, sessionID string) (sessionUsability, error) {
	row := t.tx.QueryRowScanner(ctx,
		`SELECT s.archived_at,
		        s.status AS raw_status,
		        CASE
		          WHEN s.status IN ('terminated', 'rescheduling') THEN s.status
		          ELSE COALESCE(srs.status, s.status)
		        END AS status,
		        s.lifecycle_state
		   FROM sessions s
		   LEFT JOIN session_runtime_status srs
		     ON srs.workspace_id = s.workspace_id AND srs.session_id = s.id
		  WHERE s.workspace_id = $1 AND s.id = $2
		  FOR UPDATE OF s`,
		string(t.workspaceID), sessionID,
	)
	var archivedAt sql.NullTime
	var rawStatusValue string
	var statusValue string
	var lifecycleStateValue string
	if err := row.Scan(&archivedAt, &rawStatusValue, &statusValue, &lifecycleStateValue); dbconnect.IsNoRows(err) {
		return sessionUsability{}, &NotFoundError{Message: "session not found"}
	} else if err != nil {
		return sessionUsability{}, err
	}
	rawStatus := Status(rawStatusValue)
	if err := validateStatus(rawStatus); err != nil {
		return sessionUsability{}, err
	}
	status := Status(statusValue)
	if err := validateStatus(status); err != nil {
		return sessionUsability{}, err
	}
	lifecycleState := LifecycleState(lifecycleStateValue)
	if err := validateLifecycleState(lifecycleState); err != nil {
		return sessionUsability{}, err
	}
	return sessionUsability{
		archivedAt:     archivedAt,
		rawStatus:      rawStatus,
		status:         status,
		lifecycleState: lifecycleState,
	}, nil
}

func sessionUsabilityFromSession(sess *Session) sessionUsability {
	return sessionUsability{
		archivedAt:     sql.NullTime{Valid: sess.ArchivedAt != nil},
		status:         sess.Status,
		lifecycleState: sess.LifecycleState,
	}
}

func rejectUnusableSession(usable sessionUsability) error {
	status := usable.status
	if usable.rawStatus != "" && usable.rawStatus != StatusIdle {
		status = usable.rawStatus
	}
	if usable.archivedAt.Valid || usable.lifecycleState == LifecycleStateArchived {
		return &ConflictError{Message: "session is archived", InvalidRequest: true}
	}
	if usable.lifecycleState != LifecycleStateActive {
		return &ConflictError{Message: "session lifecycle transition is in progress", InvalidRequest: true}
	}
	if status == StatusTerminated {
		return &ConflictError{Message: "session is terminated", InvalidRequest: true}
	}
	if status != StatusIdle {
		return &ConflictError{Message: "session must be idle for mutation", InvalidRequest: true}
	}
	return nil
}

func (t *postgresqlTransaction) loadActiveResources(ctx context.Context, sessionID string) ([]*Resource, error) {
	rows, err := t.tx.QueryRows(ctx,
		resourceSelectSQL+`
		  WHERE sr.workspace_id = $1 AND sr.session_id = $2 AND sr.detached_at IS NULL AND sr.delete_requested_at IS NULL
		    AND (
		      sr.type <> 'file'
		      OR EXISTS (
		        SELECT 1
		          FROM files f
		         WHERE f.workspace_id = sr.workspace_id
		           AND f.file_id = sfr.file_id
		           AND f.scope_type = 'session'
		           AND f.scope_id = sr.session_id
		           AND f.deleted_at IS NULL
		      )
		    )
		  ORDER BY sr.storage_sequence ASC, sr.resource_id ASC`,
		string(t.workspaceID), sessionID,
	)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	return scanResourceRows(rows, t.workspaceID)
}

func sessionSelectSQL(alias string) string {
	return fmt.Sprintf(`SELECT
	%[1]s.workspace_id, %[1]s.id, %[1]s.type, %[1]s.title, %[1]s.metadata_json,
	CASE
		WHEN %[1]s.status IN ('terminated', 'rescheduling') THEN %[1]s.status
		ELSE COALESCE(srs.status, %[1]s.status)
	END AS status,
	%[1]s.lifecycle_state, %[1]s.archived_at,
	%[1]s.config_generation, %[1]s.approval_mode, %[1]s.installed_tools_json,
	%[1]s.agent_id, %[1]s.agent_version_id, %[1]s.agent_version, %[1]s.environment_id, %[1]s.vault_ids_json, %[1]s.usage_json,
	srs.running_since, COALESCE(srs.active_seconds_total, 0), main_thread.closed_at,
	%[1]s.created_at, %[1]s.updated_at
	FROM sessions %[1]s
	LEFT JOIN session_runtime_status srs
	  ON srs.workspace_id = %[1]s.workspace_id AND srs.session_id = %[1]s.id
	LEFT JOIN session_threads main_thread
	  ON main_thread.workspace_id = %[1]s.workspace_id AND main_thread.session_id = %[1]s.id AND main_thread.role = 'main'`, alias)
}

const resourceSelectSQL = `SELECT
	sr.session_id, sr.resource_id, sr.type, sr.created_at, sr.updated_at, sr.detached_at, sr.delete_requested_at, sr.storage_sequence,
	sfr.source_file_id, sfr.file_id, sfr.mount_path,
	smr.memory_store_id, smr.access, smr.instructions, smr.name, smr.description, smr.mount_path,
	sgr.url, sgr.mount_path, sgr.checkout_type, sgr.checkout_ref
	FROM session_resources sr
	LEFT JOIN session_file_resources sfr
	  ON sfr.workspace_id = sr.workspace_id AND sfr.session_id = sr.session_id AND sfr.resource_id = sr.resource_id
	LEFT JOIN session_memory_store_resources smr
	  ON smr.workspace_id = sr.workspace_id AND smr.session_id = sr.session_id AND smr.resource_id = sr.resource_id
	LEFT JOIN session_github_repository_resources sgr
	  ON sgr.workspace_id = sr.workspace_id AND sgr.session_id = sr.session_id AND sgr.resource_id = sr.resource_id`

const threadSelectSQL = `SELECT
	workspace_id, session_id, id, parent_thread_id, role, visibility, status,
	agent_type, title, task_name, is_trunk, storage_sequence,
	created_at, last_active_at, closed_at, archived_at, updated_at
	FROM session_threads`

func publicSessionVisiblePredicate(alias string) string {
	return fmt.Sprintf("%s.lifecycle_state <> 'deleted'", alias)
}

func buildListSessionsQuery(ws workspace.ID, options ListOptions, limit int) (string, []any) {
	clauses := []string{"s.workspace_id = $1"}
	args := []any{string(ws)}
	add := func(clause string, values ...any) {
		args = append(args, values...)
		clauses = append(clauses, clause)
	}
	if !options.IncludeArchived {
		clauses = append(clauses, "s.archived_at IS NULL")
	}
	clauses = append(clauses, publicSessionVisiblePredicate("s"))
	if options.AgentID != "" {
		add(fmt.Sprintf("s.agent_id = $%d", len(args)+1), options.AgentID)
	}
	if options.AgentVersion > 0 {
		add(fmt.Sprintf("s.agent_version = $%d", len(args)+1), options.AgentVersion)
	}
	if options.MemoryStoreID != "" {
		add(fmt.Sprintf(`EXISTS (
			SELECT 1
			  FROM session_resources sr
			  JOIN session_memory_store_resources smr
			    ON smr.workspace_id = sr.workspace_id AND smr.session_id = sr.session_id AND smr.resource_id = sr.resource_id
			 WHERE sr.workspace_id = s.workspace_id
			   AND sr.session_id = s.id
			   AND sr.detached_at IS NULL
			   AND sr.delete_requested_at IS NULL
			   AND smr.memory_store_id = $%d
		)`, len(args)+1), options.MemoryStoreID)
	}
	if options.DeploymentID != "" {
		clauses = append(clauses, "FALSE")
	}
	if len(options.Statuses) > 0 {
		placeholders := make([]string, 0, len(options.Statuses))
		for _, statusValue := range options.Statuses {
			args = append(args, string(statusValue))
			placeholders = append(placeholders, fmt.Sprintf("$%d", len(args)))
		}
		clauses = append(clauses, fmt.Sprintf(`CASE
			WHEN s.status IN ('terminated', 'rescheduling') THEN s.status
			ELSE COALESCE(srs.status, s.status)
		END IN (%s)`, strings.Join(placeholders, ", ")))
	}
	if options.CreatedAtGT != nil {
		add(fmt.Sprintf("s.created_at > $%d", len(args)+1), *options.CreatedAtGT)
	}
	if options.CreatedAtGTE != nil {
		add(fmt.Sprintf("s.created_at >= $%d", len(args)+1), *options.CreatedAtGTE)
	}
	if options.CreatedAtLT != nil {
		add(fmt.Sprintf("s.created_at < $%d", len(args)+1), *options.CreatedAtLT)
	}
	if options.CreatedAtLTE != nil {
		add(fmt.Sprintf("s.created_at <= $%d", len(args)+1), *options.CreatedAtLTE)
	}
	orderOperator := ">"
	orderDirection := "ASC"
	if normalizeListOrder(options.Order) == ListOrderDescending {
		orderOperator = "<"
		orderDirection = "DESC"
	}
	if options.cursorID != "" {
		add(fmt.Sprintf("(s.created_at, s.id) %s ($%d, $%d)", orderOperator, len(args)+1, len(args)+2), options.cursorCreatedAt, options.cursorID)
	}
	args = append(args, limit)
	return sessionSelectSQL("s") + " WHERE " + strings.Join(clauses, " AND ") +
		fmt.Sprintf(" ORDER BY s.created_at %s, s.id %s LIMIT $%d", orderDirection, orderDirection, len(args)), args
}

func scanSessionRow(row RowScanner) (*Session, error) {
	return scanSession(row)
}

func scanSessionRows(rows QueryRows) (*Session, error) {
	return scanSession(rows)
}

func scanSession(scanner interface{ Scan(dest ...any) error }) (*Session, error) {
	var (
		workspaceIDValue  string
		sessionID         string
		typeValue         string
		title             nullableString
		metadataJSON      string
		status            string
		lifecycleState    string
		archivedAt        sql.NullTime
		configGeneration  int64
		approvalMode      string
		runtimeConfigJSON string
		agentID           string
		agentVersionID    string
		agentVersion      int
		environmentID     string
		vaultIDsJSON      string
		usageJSON         string
		runningSince      sql.NullTime
		activeSeconds     float64
		terminatedAt      sql.NullTime
		createdAt         time.Time
		updatedAt         time.Time
	)
	err := scanner.Scan(
		&workspaceIDValue, &sessionID, &typeValue, &title, &metadataJSON, &status, &lifecycleState, &archivedAt,
		&configGeneration, &approvalMode, &runtimeConfigJSON,
		&agentID, &agentVersionID, &agentVersion, &environmentID, &vaultIDsJSON, &usageJSON,
		&runningSince, &activeSeconds, &terminatedAt, &createdAt, &updatedAt,
	)
	if dbconnect.IsNoRows(err) {
		return nil, &NotFoundError{Message: "session not found"}
	}
	if err != nil {
		return nil, err
	}
	metadata := map[string]string{}
	if metadataJSON != "" {
		if err := json.Unmarshal([]byte(metadataJSON), &metadata); err != nil {
			return nil, err
		}
	}
	vaultIDs := []string{}
	if vaultIDsJSON != "" {
		if err := json.Unmarshal([]byte(vaultIDsJSON), &vaultIDs); err != nil {
			return nil, err
		}
	}
	usage, err := parseUsageJSON(usageJSON)
	if err != nil {
		return nil, err
	}
	runtimeAgentConfig, err := decodeRuntimeAgentConfig(runtimeConfigJSON)
	if err != nil {
		return nil, err
	}
	created := createdAt.UTC()
	updated := updatedAt.UTC()
	var archived *time.Time
	if archivedAt.Valid {
		parsed := archivedAt.Time.UTC()
		archived = &parsed
	}
	var runningSincePtr *time.Time
	if runningSince.Valid {
		parsed := runningSince.Time.UTC()
		runningSincePtr = &parsed
	}
	var terminatedAtPtr *time.Time
	if terminatedAt.Valid {
		parsed := terminatedAt.Time.UTC()
		terminatedAtPtr = &parsed
	}
	var titlePtr *string
	if title.Valid {
		titlePtr = &title.String
	}
	statusValue := Status(status)
	if err := validateStatus(statusValue); err != nil {
		return nil, err
	}
	lifecycleStateValue := LifecycleState(lifecycleState)
	if lifecycleStateValue == "" {
		lifecycleStateValue = LifecycleStateActive
	}
	if err := validateLifecycleState(lifecycleStateValue); err != nil {
		return nil, err
	}
	normalizedApprovalMode, err := normalizeApprovalMode(ApprovalMode(approvalMode))
	if err != nil {
		return nil, err
	}
	return &Session{
		ID:                 sessionID,
		Type:               typeValue,
		WorkspaceID:        workspace.ID(workspaceIDValue),
		Title:              titlePtr,
		Metadata:           metadata,
		Status:             statusValue,
		LifecycleState:     lifecycleStateValue,
		ConfigGeneration:   configGeneration,
		ApprovalMode:       normalizedApprovalMode,
		RuntimeAgentConfig: runtimeAgentConfig,
		ArchivedAt:         archived,
		AgentID:            agentID,
		AgentVersionID:     agentVersionID,
		AgentVersion:       agentVersion,
		EnvironmentID:      environmentID,
		VaultIDs:           vaultIDs,
		Usage:              usage,
		RunningSince:       runningSincePtr,
		ActiveSecondsTotal: activeSeconds,
		TerminatedAt:       terminatedAtPtr,
		CreatedAt:          created,
		UpdatedAt:          updated,
	}, nil
}

func scanThreadRows(rows QueryRows, ws workspace.ID) ([]*Thread, error) {
	threads := []*Thread{}
	for rows.Next() {
		thread, err := scanThread(rows, ws)
		if err != nil {
			return nil, err
		}
		threads = append(threads, thread)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return threads, nil
}

func scanThreadRow(row RowScanner, ws workspace.ID) (*Thread, error) {
	return scanThread(row, ws)
}

func scanThread(scanner interface{ Scan(dest ...any) error }, ws workspace.ID) (*Thread, error) {
	var (
		workspaceIDValue string
		sessionID        string
		threadID         string
		parentThreadID   nullableString
		role             string
		visibility       string
		status           string
		agentType        string
		title            nullableString
		taskName         nullableString
		isTrunk          bool
		sequence         int64
		createdAt        time.Time
		lastActiveAt     sql.NullTime
		closedAt         sql.NullTime
		archivedAt       sql.NullTime
		updatedAt        time.Time
	)
	err := scanner.Scan(
		&workspaceIDValue, &sessionID, &threadID, &parentThreadID, &role, &visibility, &status,
		&agentType, &title, &taskName, &isTrunk, &sequence,
		&createdAt, &lastActiveAt, &closedAt, &archivedAt, &updatedAt,
	)
	if dbconnect.IsNoRows(err) {
		return nil, &NotFoundError{Message: "session thread not found"}
	}
	if err != nil {
		return nil, err
	}
	if workspace.ID(workspaceIDValue) != ws {
		return nil, &ValidationError{Message: "workspace_id mismatch"}
	}
	created := createdAt.UTC()
	lastActive := created
	if lastActiveAt.Valid {
		lastActive = lastActiveAt.Time.UTC()
	}
	updated := updatedAt.UTC()
	var closed *time.Time
	if closedAt.Valid {
		parsed := closedAt.Time.UTC()
		closed = &parsed
	}
	var archived *time.Time
	if archivedAt.Valid {
		parsed := archivedAt.Time.UTC()
		archived = &parsed
	}
	var parent *string
	if parentThreadID.Valid {
		parent = &parentThreadID.String
	}
	var titlePtr *string
	if title.Valid {
		titlePtr = &title.String
	}
	var taskNamePtr *string
	if taskName.Valid {
		taskNamePtr = &taskName.String
	}
	roleValue := ThreadRole(role)
	if err := validateThreadRole(roleValue); err != nil {
		return nil, err
	}
	visibilityValue := ThreadVisibility(visibility)
	if err := validateThreadVisibility(visibilityValue); err != nil {
		return nil, err
	}
	statusValue := ThreadStatus(status)
	if err := validateThreadStatus(statusValue); err != nil {
		return nil, err
	}
	if agentType == "" {
		agentType = "default"
	}
	return &Thread{
		ID:              threadID,
		WorkspaceID:     workspace.ID(workspaceIDValue),
		SessionID:       sessionID,
		ParentThreadID:  parent,
		Role:            roleValue,
		Visibility:      visibilityValue,
		Status:          statusValue,
		AgentType:       agentType,
		Title:           titlePtr,
		TaskName:        taskNamePtr,
		IsTrunk:         isTrunk,
		StorageSequence: sequence,
		CreatedAt:       created,
		LastActiveAt:    lastActive,
		ClosedAt:        closed,
		ArchivedAt:      archived,
		UpdatedAt:       updated,
	}, nil
}

func scanResourceRows(rows QueryRows, ws workspace.ID) ([]*Resource, error) {
	resources := []*Resource{}
	for rows.Next() {
		resource, err := scanResource(rows, ws)
		if err != nil {
			return nil, err
		}
		resources = append(resources, resource)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return resources, nil
}

func scanResourceRow(row RowScanner, ws workspace.ID) (*Resource, error) {
	return scanResource(row, ws)
}

func scanResource(scanner interface{ Scan(dest ...any) error }, ws workspace.ID) (*Resource, error) {
	var (
		sessionID    string
		resourceID   string
		typeValue    string
		createdAt    time.Time
		updatedAt    time.Time
		detachedAt   sql.NullTime
		deleteAt     sql.NullTime
		sequence     int64
		sourceFileID nullableString
		fileID       nullableString
		fileMount    nullableString
		memoryID     nullableString
		access       nullableString
		instructions nullableString
		memoryName   nullableString
		description  nullableString
		memoryMount  nullableString
		githubURL    nullableString
		githubMount  nullableString
		checkoutType nullableString
		checkoutRef  nullableString
	)
	err := scanner.Scan(
		&sessionID, &resourceID, &typeValue, &createdAt, &updatedAt, &detachedAt, &deleteAt, &sequence,
		&sourceFileID, &fileID, &fileMount,
		&memoryID, &access, &instructions, &memoryName, &description, &memoryMount,
		&githubURL, &githubMount, &checkoutType, &checkoutRef,
	)
	if dbconnect.IsNoRows(err) {
		return nil, &NotFoundError{Message: "session resource not found"}
	}
	if err != nil {
		return nil, err
	}
	created := createdAt.UTC()
	updated := updatedAt.UTC()
	var detached *time.Time
	if detachedAt.Valid {
		parsed := detachedAt.Time.UTC()
		detached = &parsed
	}
	var deleteRequested *time.Time
	if deleteAt.Valid {
		parsed := deleteAt.Time.UTC()
		deleteRequested = &parsed
	}
	resource := &Resource{
		ID:                resourceID,
		SessionID:         sessionID,
		WorkspaceID:       ws,
		StorageSequence:   sequence,
		Type:              ResourceType(typeValue),
		CreatedAt:         created,
		UpdatedAt:         updated,
		DetachedAt:        detached,
		DeleteRequestedAt: deleteRequested,
	}
	switch resource.Type {
	case ResourceTypeFile:
		resource.File = &FileResource{SourceFileID: sourceFileID.String, FileID: fileID.String, MountPath: fileMount.String}
	case ResourceTypeMemoryStore:
		resource.MemoryStore = &MemoryStoreResource{
			MemoryStoreID: memoryID.String,
			Access:        access.String,
			Instructions:  instructions.String,
			Name:          memoryName.String,
			Description:   description.String,
			MountPath:     memoryMount.String,
		}
	case ResourceTypeGitHubRepository:
		resource.GitHubRepository = &GitHubRepositoryResource{
			URL:          githubURL.String,
			MountPath:    githubMount.String,
			CheckoutType: checkoutType.String,
			CheckoutRef:  checkoutRef.String,
		}
	default:
		return nil, &ValidationError{Message: "invalid session resource type"}
	}
	return resource, nil
}

type nullableString struct {
	String string
	Valid  bool
}

func (n *nullableString) Scan(value any) error {
	if value == nil {
		n.String = ""
		n.Valid = false
		return nil
	}
	switch typed := value.(type) {
	case string:
		n.String = typed
	case []byte:
		n.String = string(typed)
	default:
		return fmt.Errorf("session: unsupported nullable string type %T", value)
	}
	n.Valid = true
	return nil
}

func nullablePointerString(value *string) any {
	if value == nil {
		return nil
	}
	return *value
}

func encodeRuntimeAgentConfig(config *RuntimeAgentConfig) (string, error) {
	if config == nil {
		return "[]", nil
	}
	encoded, err := json.Marshal(normalizeRuntimeAgentConfig(*config))
	if err != nil {
		return "", err
	}
	return string(encoded), nil
}

func decodeRuntimeAgentConfig(raw string) (*RuntimeAgentConfig, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" || trimmed == "[]" {
		return nil, nil
	}
	if !strings.HasPrefix(trimmed, "{") {
		return nil, nil
	}
	var config RuntimeAgentConfig
	if err := json.Unmarshal([]byte(trimmed), &config); err != nil {
		return nil, err
	}
	normalized := normalizeRuntimeAgentConfig(config)
	return &normalized, nil
}

func nullableEmptyString(value string) any {
	if value == "" {
		return nil
	}
	return value
}

// nullableNullTime hands a nullable durable timestamp to the driver as either a
// time value or SQL NULL, without a string detour that would carry the process
// timezone into the column.

func nonNilMetadata(metadata map[string]string) map[string]string {
	out := map[string]string{}
	for key, value := range metadata {
		out[key] = value
	}
	return out
}

func nonNilStringSlice(values []string) []string {
	if values == nil {
		return []string{}
	}
	return append([]string(nil), values...)
}

func normalizeLimit(limit int) int {
	if limit <= 0 {
		return 20
	}
	if limit > 100 {
		return 100
	}
	return limit
}

func mapPostgreSQLSessionError(err error) error {
	if err == nil {
		return nil
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == "23505" {
		return &ConflictError{Message: "session unique constraint violated"}
	}
	return err
}
