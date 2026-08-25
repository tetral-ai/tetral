package agentruntimebridge

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"log/slog"
	"strings"
	"time"

	"github.com/tetral-ai/tetral/internal/storage"

	"github.com/tetral-ai/tetral/internal/blob"
	"github.com/tetral-ai/tetral/internal/dbconnect"
	bridgev1 "github.com/tetral-ai/tetral/services/bridge/gen/tetral/bridge/v1"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// MaxProviderRequestAttachments is the Bridge terminal settlement backstop.
const MaxProviderRequestAttachments = 32

// MaxStableReasoningPartsPerRequest and MaxStableReasoningBytesPerRequest bound
// the atomic request-end settlement before its transaction starts.
//
// The two limits are ONE budget counted across BOTH stable-reasoning commit
// vectors of a single model request: the anchored prefix merged inside a public
// tool event's WriteEvent transaction, and the full ordered set carried by the
// successful WriteRequestEnd settlement. The 16-part count and the 2 MiB
// aggregate (per-part content and metadata caps unchanged) apply to the union,
// not to each vector separately. An attached set or a settlement that breaches
// either limit is rejected as a contract violation before any durable write.
// UPDATE-WITH: services/agent-runtime/packages/core/src/contracts/runtime.ts
const (
	MaxStableReasoningPartsPerRequest = 16
	MaxStableReasoningBytesPerRequest = 2 * 1024 * 1024
)

// MaxMemoryPathConflicts bounds the Memory-specific path conflict wire.
//
// It caps the model-visible conflict wire (conflicts[], conflict_total,
// conflicts_truncated). Derivation: worst case ~32 entries x ~1024 B each is
// ~32 KiB, so the explicit 64 KiB cap leaves room for the enclosing wire. The value is
// coincidentally equal to MaxProviderRequestAttachments but is NOT coupled with
// it; the two move independently. The conflict query's ORDER BY (conflict-kind
// priority, then length(path) DESC, then memory_id ASC) makes truncation
// deterministic, so the exact-path head is never dropped, and a truncated set
// is still a hard path_exists rejection rather than a silent success.
const MaxMemoryPathConflicts = 32

const (
	bridgeAckCommitted = "committed"
	bridgeAckRejected  = "rejected"

	bridgeOpCommitInputs                   = "commit_inputs"
	bridgeOpCommitTaskNotificationResult   = "commit_task_notification_result"
	runtimeTaskNotificationPayloadMaxBytes = 16 * 1024
	bridgeOpWriteEvent                     = "write_event"
	bridgeOpSettleToolResult               = "settle_tool_result"
	bridgeOpWriteRequestEnd                = "write_request_end"
	bridgeOpFinishIdle                     = "finish_idle"
	bridgeOpCreateChildThread              = "create_child_thread"
	bridgeOpDeliverInterAgentMail          = "deliver_inter_agent_mail"
	bridgeOpResolveChildThread             = "resolve_child_thread"
	bridgeOpListChildThreads               = "list_child_threads"
	bridgeOpCloseChildControl              = "close_child_control"
	bridgeOpCloseApprovalReviewer          = "close_approval_reviewer"
	bridgeOpMarkChildThreadActive          = "mark_child_thread_active"
	bridgeOpReadCommandResult              = "read_command_result"
	bridgeOpSendCommandInput               = "send_command_input"
	bridgeOpCancelCommand                  = "cancel_command"
	bridgeOpRunMemory                      = "run_memory"
	bridgeOpMcpManifestChanged             = "mcp_manifest_changed"
	bridgeOpCommitMcpToolResult            = "commit_mcp_tool_result"
	bridgeOpRelinquishMcpToolResult        = "relinquish_mcp_tool_result"
	bridgeOpCommitInternalToolRepair       = "commit_internal_tool_repair"
	bridgeOpCommitRuntimeTermination       = "commit_runtime_termination"
	mcpManifestAcceptanceLockCategory      = int32(0x6D63_7061) // "mcpa"

	bridgeToolKindSandbox           = "sandbox_tool"
	bridgeToolKindSandboxBackground = "sandbox_background"
	bridgeToolKindMemory            = "memory"
	bridgeToolKindMCP               = "mcp"

	// mcpClaimLeaseTTL bounds an MCP tool-call reservation. Derivation: the
	// 120s MCP call timeout plus margin, so a connector crash mid-call cannot
	// strand the call past the point the lease frees it. The reservation is the
	// concurrency fence: it prevents two replicas from executing one
	// tool_use_event_id's side effect at once (a timeout-retry racing the
	// still-running original). An expired reservation is superseded by the next
	// Claim, a deterministic pre-commit failure relinquishes the exact claim,
	// and a replay after Commit is served from the stored result.
	mcpClaimStatusStored   = "stored"
	mcpClaimStatusInFlight = "in_flight"
	mcpClaimStatusConsumed = "consumed"
	mcpClaimLeaseTTL       = 180 * time.Second
	mcpClaimInFlightCode   = "mcp_claim_in_flight"
	mcpClaimNotOwnedCode   = "mcp_claim_not_owned"

	requestKindAgentProviderRequest = "agent_provider_request"
	requestKindCompactionSummary    = "compaction_summary"
	requestKindApprovalReviewer     = "approval_reviewer"

	// memoryToolContentMaxBytes caps the content of a memory tool write. It MUST
	// stay equal to internal/memory's memoryContentMaxBytes, which caps the
	// public create/update path; the identical 102400-byte bound is enforced at
	// both layers, and at tool replace on the final post-replacement content.
	// UPDATE-WITH: internal/memory/input.go (memoryContentMaxBytes).
	//
	// session_runtime_tool_results.memory_projection_state — the durable
	// refresh state of a memory tool result's projection push. State table:
	//
	//   NULL          non-memory result, or a memory result whose refresh plan
	//                 is empty. Terminal.
	//   pending       durable mutation committed; Sandbox projection job not yet
	//                 settled. -> refreshed | skipped_cold | failed.
	//   refreshed     projection push confirmed. Terminal.
	//   skipped_cold  no reachable sandbox at push time. Terminal, superseded
	//                 only by cold-return re-materialization.
	//   failed        projection retries exhausted or failed terminally. Terminal;
	//                 result_json carries projection_refresh_failed.
	//
	//   writers: RunMemory sets NULL, pending, or skipped_cold; Sandbox Service
	//     moves pending to a terminal state.
	//   reader: completePendingMemoryProjection replay dispatch.
	memoryProjectionStatePending     = "pending"
	memoryProjectionStateRefreshed   = "refreshed"
	memoryProjectionStateSkippedCold = "skipped_cold"
	memoryToolContentMaxBytes        = 102400
	transientAttachmentMaxBytes      = 10 * 1024 * 1024
	providerCommandMetadataMaxBytes  = 4 * 1024
	internalToolRepairIDMaxBytes     = 128
	maxResourceHelperRecoveries      = 1

	// maxRescheduleBackoff clamps the reschedule backoff and deadline a pod
	// reports for a retryable request-end. There is no queue job on this
	// surface: the pod waits the backoff out in-run while holding its binding
	// and sandbox, so this clamp is the only bound on how long a rescheduling
	// turn parks those resources. Worst-case hold for a run is the reschedule
	// budget times this value (budget x backoff), not a retry cadence.
	defaultRuntimeBindingTokenTTL = 5 * time.Minute
	defaultIdleCleanupDelay       = 30 * time.Minute
	defaultTransientAttachmentTTL = 15 * time.Minute
	maxRescheduleBackoff          = 120 * time.Second
)

type PostgreSQLBridgeAPIStore struct {
	Client                     *dbconnect.Client
	Logger                     *slog.Logger
	Clock                      func() time.Time
	AttachmentBlobStore        blob.BlobStore
	FileBlobStore              blob.BlobStore
	MCPManifestLister          MCPManifestLister
	RuntimeBindingTokenHMACKey []byte
	RuntimeBindingTokenTTL     time.Duration
	ProviderRescheduleBudget   int64
	CompactionRescheduleBudget int64
}

type TransientAttachmentGCResult struct {
	Marked  int
	Deleted int
	Failed  int
}

type transientAttachmentGCRow struct {
	WorkspaceID    string
	AttachmentRef  string
	BlobPointer    string
	PreviousStatus string
}

func NewPostgreSQLBridgeAPIStore(client *dbconnect.Client) *PostgreSQLBridgeAPIStore {
	return &PostgreSQLBridgeAPIStore{
		Client:                     client,
		Clock:                      func() time.Time { return storage.Now() },
		RuntimeBindingTokenTTL:     defaultRuntimeBindingTokenTTL,
		ProviderRescheduleBudget:   defaultProviderRescheduleBudget,
		CompactionRescheduleBudget: defaultCompactionRescheduleBudget,
	}
}

func (s *PostgreSQLBridgeAPIStore) withScopeTx(ctx context.Context, scope *bridgev1.RuntimeScope, operation string, fn func(*dbconnect.Tx) error) error {
	if s == nil || s.Client == nil {
		return status.Error(codes.FailedPrecondition, "bridge API store is unavailable")
	}
	if err := validateRuntimeScope(scope); err != nil {
		return err
	}
	return s.Client.WithWorkspaceTx(ctx, scope.GetWorkspaceId(), operation, fn)
}

func (s *PostgreSQLBridgeAPIStore) withScopeReadOnlyTx(ctx context.Context, scope *bridgev1.RuntimeScope, operation string, fn func(*dbconnect.Tx) error) error {
	if s == nil || s.Client == nil {
		return status.Error(codes.FailedPrecondition, "bridge API store is unavailable")
	}
	if err := validateRuntimeScope(scope); err != nil {
		return err
	}
	return s.Client.WithWorkspaceReadOnlyTx(ctx, scope.GetWorkspaceId(), operation, fn)
}

func (s *PostgreSQLBridgeAPIStore) withScopeTxAndCleanup(ctx context.Context, scope *bridgev1.RuntimeScope, operation string, fn func(*dbconnect.Tx) error, onCommitFailure func()) error {
	if s == nil || s.Client == nil {
		return status.Error(codes.FailedPrecondition, "bridge API store is unavailable")
	}
	if err := validateRuntimeScope(scope); err != nil {
		return err
	}
	return s.Client.WithWorkspaceTxAndCleanup(ctx, scope.GetWorkspaceId(), operation, fn, onCommitFailure)
}

func (s *PostgreSQLBridgeAPIStore) now() time.Time {
	if s != nil && s.Clock != nil {
		return s.Clock().UTC()
	}
	return storage.Now()
}

func (s *PostgreSQLBridgeAPIStore) providerRescheduleBudget() int64 {
	if s != nil && s.ProviderRescheduleBudget >= 0 {
		return s.ProviderRescheduleBudget
	}
	return defaultProviderRescheduleBudget
}

func (s *PostgreSQLBridgeAPIStore) compactionRescheduleBudget() int64 {
	if s != nil && s.CompactionRescheduleBudget >= 0 {
		return s.CompactionRescheduleBudget
	}
	return defaultCompactionRescheduleBudget
}

type bridgeOperation struct {
	RequestHash   string
	AckStatus     string
	ErrorCode     string
	ResultJSON    string
	StdinWriteSeq sql.NullInt64
}

type bridgeOperationInsert struct {
	Operation      string
	SourceKind     string
	IdempotencyKey string
	RequestHash    string
	AckStatus      string
	RuntimeInputID sql.NullString
	RuntimeWriteID sql.NullString
	ErrorCode      sql.NullString
	ResultJSON     string
	StdinWriteSeq  sql.NullInt64
	Now            time.Time
}

type bridgeDeclarationOperation struct {
	DeclarationDigest string
	ReceiptJSON       string
}

func readBridgeOperationTx(ctx context.Context, tx *dbconnect.Tx, scope *bridgev1.RuntimeScope, operation string, key string) (bridgeOperation, bool, error) {
	return readBridgeOperationBySourceTx(ctx, tx, scope, operation, operation, key)
}

func readBridgeOperationBySourceTx(
	ctx context.Context,
	tx *dbconnect.Tx,
	scope *bridgev1.RuntimeScope,
	operation string,
	sourceKind string,
	key string,
) (bridgeOperation, bool, error) {
	row := tx.QueryRow(ctx,
		`SELECT request_hash, ack_status, COALESCE(error_code, ''), result_json, stdin_write_seq
		   FROM session_bridge_operations
		  WHERE workspace_id = $1
		    AND session_id = $2
		    AND session_thread_id = $3
		    AND operation = $4
		    AND source_kind = $5
		    AND idempotency_key = $6
		  FOR UPDATE`,
		scope.GetWorkspaceId(),
		scope.GetSessionId(),
		scope.GetSessionThreadId(),
		operation,
		sourceKind,
		key,
	)
	var existing bridgeOperation
	if err := row.Scan(&existing.RequestHash, &existing.AckStatus, &existing.ErrorCode, &existing.ResultJSON, &existing.StdinWriteSeq); dbconnect.IsNoRows(err) {
		return bridgeOperation{}, false, nil
	} else if err != nil {
		return bridgeOperation{}, false, err
	}
	return existing, true, nil
}

func insertBridgeOperationTx(ctx context.Context, tx *dbconnect.Tx, scope *bridgev1.RuntimeScope, op bridgeOperationInsert) error {
	if op.ResultJSON == "" {
		op.ResultJSON = "{}"
	}
	if op.SourceKind == "" {
		op.SourceKind = op.Operation
	}
	_, err := tx.Exec(ctx,
		`INSERT INTO session_bridge_operations (
			workspace_id, session_id, session_thread_id, operation, source_kind, idempotency_key,
			request_hash, ack_status, runtime_input_id, runtime_write_id, error_code,
			result_json, stdin_write_seq, created_at, updated_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $14)`,
		scope.GetWorkspaceId(),
		scope.GetSessionId(),
		scope.GetSessionThreadId(),
		op.Operation,
		op.SourceKind,
		op.IdempotencyKey,
		op.RequestHash,
		op.AckStatus,
		op.RuntimeInputID,
		op.RuntimeWriteID,
		op.ErrorCode,
		op.ResultJSON,
		op.StdinWriteSeq,
		op.Now,
	)
	return err
}

func readBridgeDeclarationOperationTx(
	ctx context.Context,
	tx *dbconnect.Tx,
	scope *bridgev1.RuntimeScope,
	operation string,
	sourceKind string,
	sourceID string,
) (bridgeDeclarationOperation, bool, error) {
	row := tx.QueryRow(ctx,
		`SELECT COALESCE(declaration_digest, ''), COALESCE(receipt_json, '')
		   FROM session_bridge_operations
		  WHERE workspace_id = $1
		    AND session_id = $2
		    AND session_thread_id = $3
		    AND operation = $4
		    AND source_kind = $5
		    AND idempotency_key = $6
		  FOR UPDATE`,
		scope.GetWorkspaceId(),
		scope.GetSessionId(),
		scope.GetSessionThreadId(),
		operation,
		sourceKind,
		sourceID,
	)
	var existing bridgeDeclarationOperation
	if err := row.Scan(&existing.DeclarationDigest, &existing.ReceiptJSON); dbconnect.IsNoRows(err) {
		return bridgeDeclarationOperation{}, false, nil
	} else if err != nil {
		return bridgeDeclarationOperation{}, false, err
	}
	return existing, true, nil
}

func insertBridgeDeclarationOperationTx(
	ctx context.Context,
	tx *dbconnect.Tx,
	scope *bridgev1.RuntimeScope,
	operation string,
	sourceKind string,
	sourceID string,
	declarationDigest string,
	receiptJSON string,
	now time.Time,
) error {
	_, err := tx.Exec(ctx,
		`INSERT INTO session_bridge_operations (
			workspace_id, session_id, session_thread_id, operation, source_kind,
			idempotency_key, request_hash, declaration_digest, receipt_json,
			ack_status, runtime_input_id, result_json, created_at, updated_at
		) VALUES (
			$1, $2, $3, $4, $5, $6, $7, $7, $8, $9, $6, '{}', $10, $10
		)`,
		scope.GetWorkspaceId(),
		scope.GetSessionId(),
		scope.GetSessionThreadId(),
		operation,
		sourceKind,
		sourceID,
		declarationDigest,
		receiptJSON,
		bridgeAckCommitted,
		now,
	)
	return err
}

type runtimeToolResult struct {
	ToolKind               string
	NormalizedInputHash    string
	ToolName               string
	InputJSON              string
	AckStatus              string
	ResultJSON             string
	ResultDigest           string
	ModelToolCallID        sql.NullString
	ExecutionState         sql.NullString
	BackgroundTaskStarted  bool
	TaskID                 sql.NullString
	MemoryProjectionState  sql.NullString
	MCPClaimStatus         sql.NullString
	MCPClaimID             sql.NullString
	MCPClaimLeaseExpiresAt sql.NullString
}

type runtimeToolResultInsert struct {
	ToolUseEventID         string
	ToolKind               string
	NormalizedInputHash    string
	ToolName               string
	InputJSON              string
	AckStatus              string
	ResultJSON             string
	BackgroundTaskStarted  bool
	TaskID                 sql.NullString
	MemoryProjectionState  sql.NullString
	MCPClaimStatus         string
	MCPClaimID             sql.NullString
	MCPClaimLeaseExpiresAt sql.NullString
	Now                    time.Time
}

func readRuntimeToolResultTx(ctx context.Context, tx *dbconnect.Tx, scope *bridgev1.RuntimeScope, toolUseEventID string) (runtimeToolResult, bool, error) {
	return readRuntimeToolResult(ctx, tx, scope, toolUseEventID, true)
}

func readRuntimeToolResultReadOnlyTx(ctx context.Context, tx *dbconnect.Tx, scope *bridgev1.RuntimeScope, toolUseEventID string) (runtimeToolResult, bool, error) {
	return readRuntimeToolResult(ctx, tx, scope, toolUseEventID, false)
}

func readRuntimeToolResult(ctx context.Context, tx *dbconnect.Tx, scope *bridgev1.RuntimeScope, toolUseEventID string, forUpdate bool) (runtimeToolResult, bool, error) {
	query := `SELECT tool_kind, normalized_input_hash, tool_name, input_json, ack_status,
	               COALESCE(result_json, ''), COALESCE(result_digest, ''), model_tool_call_id, execution_state,
	               background_task_started, task_id, memory_projection_state,
	               mcp_claim_status, mcp_claim_id, mcp_claim_lease_expires_at
		   FROM session_runtime_tool_results
		  WHERE workspace_id = $1
		    AND session_id = $2
		    AND session_thread_id = $3
		    AND tool_use_event_id = $4`
	if forUpdate {
		query += `
		  FOR UPDATE`
	}
	row := tx.QueryRow(ctx,
		query,
		scope.GetWorkspaceId(),
		scope.GetSessionId(),
		scope.GetSessionThreadId(),
		toolUseEventID,
	)
	var existing runtimeToolResult
	if err := row.Scan(
		&existing.ToolKind, &existing.NormalizedInputHash, &existing.ToolName, &existing.InputJSON,
		&existing.AckStatus, &existing.ResultJSON, &existing.ResultDigest, &existing.ModelToolCallID, &existing.ExecutionState,
		&existing.BackgroundTaskStarted, &existing.TaskID, &existing.MemoryProjectionState,
		&existing.MCPClaimStatus, &existing.MCPClaimID, &existing.MCPClaimLeaseExpiresAt,
	); dbconnect.IsNoRows(err) {
		return runtimeToolResult{}, false, nil
	} else if err != nil {
		return runtimeToolResult{}, false, err
	}
	return existing, true, nil
}

func insertRuntimeToolResultTx(ctx context.Context, tx *dbconnect.Tx, scope *bridgev1.RuntimeScope, tool runtimeToolResultInsert) error {
	claimStatus := nullableSQLString(tool.MCPClaimStatus)
	_, err := tx.Exec(ctx,
		`INSERT INTO session_runtime_tool_results (
			workspace_id, session_id, session_thread_id, tool_use_event_id, tool_kind,
			normalized_input_hash, tool_name, input_json, ack_status, result_json,
			background_task_started, task_id, memory_projection_state, mcp_claim_status,
			mcp_claim_id, mcp_claim_lease_expires_at, created_at, updated_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $17)`,
		scope.GetWorkspaceId(),
		scope.GetSessionId(),
		scope.GetSessionThreadId(),
		tool.ToolUseEventID,
		tool.ToolKind,
		tool.NormalizedInputHash,
		tool.ToolName,
		tool.InputJSON,
		tool.AckStatus,
		tool.ResultJSON,
		tool.BackgroundTaskStarted,
		tool.TaskID,
		tool.MemoryProjectionState,
		claimStatus,
		tool.MCPClaimID,
		tool.MCPClaimLeaseExpiresAt,
		tool.Now,
	)
	return err
}

func validateRuntimeScope(scope *bridgev1.RuntimeScope) error {
	if scope == nil || scope.GetWorkspaceId() == "" || scope.GetSessionId() == "" || scope.GetSessionThreadId() == "" || scope.GetBinding() == nil ||
		scope.GetBinding().GetBindingId() == "" || scope.GetBinding().GetBindingGeneration() <= 0 || scope.GetBinding().GetTargetPodUid() == "" {
		return closeoutUnrepairableError(status.Error(codes.InvalidArgument, "invalid runtime scope"))
	}
	return nil
}

func verifyRuntimeScopeTx(ctx context.Context, tx *dbconnect.Tx, scope *bridgev1.RuntimeScope) error {
	if err := validateRuntimeScope(scope); err != nil {
		return err
	}
	if err := verifyRuntimeCallerPodUID(ctx, scope); err != nil {
		return err
	}
	if err := lockRuntimeMutationSessionTx(ctx, tx, scope.GetWorkspaceId(), scope.GetSessionId()); err != nil {
		return err
	}
	if err := verifyRuntimeSessionNonTerminalTx(ctx, tx, scope); err != nil {
		return err
	}
	return verifyRuntimeBindingTx(ctx, tx, scope)
}

func verifyRuntimeSessionNonTerminalTx(ctx context.Context, tx *dbconnect.Tx, scope *bridgev1.RuntimeScope) error {
	var sessionStatus string
	if err := tx.QueryRow(ctx,
		`SELECT status
		   FROM sessions
		  WHERE workspace_id = $1
		    AND id = $2`,
		scope.GetWorkspaceId(),
		scope.GetSessionId(),
	).Scan(&sessionStatus); dbconnect.IsNoRows(err) {
		return scopeSupersededError(status.Error(codes.NotFound, "session not found"))
	} else if err != nil {
		return err
	}
	if sessionStatus == "terminated" {
		return scopeSupersededError(status.Error(codes.FailedPrecondition, "runtime session is terminal"))
	}
	return nil
}

func verifyRuntimeBindingTx(ctx context.Context, tx *dbconnect.Tx, scope *bridgev1.RuntimeScope) error {
	row := tx.QueryRow(ctx,
		`SELECT agent_runtime_pod_uid
		   FROM session_runtime_bindings
		  WHERE workspace_id = $1
		    AND session_id = $2
		    AND binding_id = $3
		    AND binding_generation = $4
		  FOR UPDATE`,
		scope.GetWorkspaceId(),
		scope.GetSessionId(),
		scope.GetBinding().GetBindingId(),
		scope.GetBinding().GetBindingGeneration(),
	)
	var podUID string
	if err := row.Scan(&podUID); dbconnect.IsNoRows(err) {
		return scopeSupersededError(status.Error(codes.FailedPrecondition, "runtime binding is stale"))
	} else if err != nil {
		return err
	}
	if podUID != scope.GetBinding().GetTargetPodUid() {
		return scopeSupersededError(status.Error(codes.FailedPrecondition, "runtime binding is stale"))
	}
	return nil
}

func verifyRuntimeDeclarationCaller(ctx context.Context, scope *bridgev1.RuntimeScope) error {
	if err := validateRuntimeScope(scope); err != nil {
		return err
	}
	return verifyRuntimeCallerPodUID(ctx, scope)
}

func lockSessionRuntimeArbitrationTx(ctx context.Context, tx *dbconnect.Tx, workspaceID string, sessionID string) (string, error) {
	if err := storage.AcquireSessionRuntimeMutationLock(ctx, tx, workspaceID, sessionID); err != nil {
		return "", err
	}
	var lifecycleState string
	if err := tx.QueryRow(ctx,
		`SELECT lifecycle_state
		   FROM sessions
		  WHERE workspace_id = $1
		    AND id = $2
		  FOR UPDATE`,
		workspaceID,
		sessionID,
	).Scan(&lifecycleState); dbconnect.IsNoRows(err) {
		return "", scopeSupersededError(status.Error(codes.NotFound, "session not found"))
	} else if err != nil {
		return "", err
	}
	return lifecycleState, nil
}

func lockRuntimeMutationSessionTx(ctx context.Context, tx *dbconnect.Tx, workspaceID string, sessionID string) error {
	lifecycleState, err := lockSessionRuntimeArbitrationTx(ctx, tx, workspaceID, sessionID)
	if err != nil {
		return err
	}
	if lifecycleState == "deleted" {
		return scopeSupersededError(status.Error(codes.FailedPrecondition, "session is deleted"))
	}
	return nil
}

func verifyRuntimeScopeReadOnlyTx(ctx context.Context, tx *dbconnect.Tx, scope *bridgev1.RuntimeScope) error {
	if err := validateRuntimeScope(scope); err != nil {
		return err
	}
	if err := verifyRuntimeCallerPodUID(ctx, scope); err != nil {
		return err
	}
	row := tx.QueryRow(ctx,
		`SELECT agent_runtime_pod_uid
		   FROM session_runtime_bindings
		  WHERE workspace_id = $1
		    AND session_id = $2
		    AND binding_id = $3
		    AND binding_generation = $4`,
		scope.GetWorkspaceId(),
		scope.GetSessionId(),
		scope.GetBinding().GetBindingId(),
		scope.GetBinding().GetBindingGeneration(),
	)
	var podUID string
	if err := row.Scan(&podUID); dbconnect.IsNoRows(err) {
		return scopeSupersededError(status.Error(codes.FailedPrecondition, "runtime binding is stale"))
	} else if err != nil {
		return err
	}
	if podUID != scope.GetBinding().GetTargetPodUid() {
		return scopeSupersededError(status.Error(codes.FailedPrecondition, "runtime binding is stale"))
	}
	return nil
}

func verifyRuntimeThreadScopeTx(ctx context.Context, tx *dbconnect.Tx, scope *bridgev1.RuntimeScope) error {
	row := tx.QueryRow(ctx,
		`SELECT id
		   FROM session_threads
		  WHERE workspace_id = $1
		    AND session_id = $2
		    AND id = $3`,
		scope.GetWorkspaceId(),
		scope.GetSessionId(),
		scope.GetSessionThreadId(),
	)
	var threadID string
	if err := row.Scan(&threadID); dbconnect.IsNoRows(err) {
		return closeoutUnrepairableError(status.Error(codes.FailedPrecondition, "runtime thread is stale"))
	} else if err != nil {
		return err
	}
	return nil
}

func nullableInt64(value *int64) sql.NullInt64 {
	if value == nil {
		return sql.NullInt64{}
	}
	return sql.NullInt64{Int64: *value, Valid: true}
}

func rowsAffected(result sql.Result) bool {
	if result == nil {
		return false
	}
	count, err := result.RowsAffected()
	return err == nil && count > 0
}

// defaultTime parses a wire timestamp, falling back when the caller omitted it.
// Wire timestamps are RFC 3339; durable columns are native timestamps, so an
// unparsable value is rejected here rather than stored.

// Truncate like a minted timestamp: the column keeps microseconds, so an
// untruncated wire value would be echoed and hashed at nanosecond precision
// while the stored row holds something else.

func defaultString(value string, fallback string) string {
	if value == "" {
		return fallback
	}
	return value
}

func bridgeRawJSON(value string, fallback string) json.RawMessage {
	if json.Valid([]byte(value)) {
		return json.RawMessage(value)
	}
	return json.RawMessage(fallback)
}

func bridgeJSONFieldRaw(raw json.RawMessage, field string, fallback string) json.RawMessage {
	var object map[string]json.RawMessage
	if err := json.Unmarshal(raw, &object); err != nil {
		return json.RawMessage(fallback)
	}
	value, ok := object[field]
	if !ok || !json.Valid(value) {
		return json.RawMessage(fallback)
	}
	return value
}

func nullableSQLString(value string) sql.NullString {
	return sql.NullString{String: value, Valid: value != ""}
}

func bridgeRequestHash(parts ...string) string {
	digest := sha256.Sum256([]byte(strings.Join(parts, "\x00")))
	return hex.EncodeToString(digest[:])
}

func nullableJSONString(value sql.NullString) any {
	if !value.Valid {
		return nil
	}
	return value.String
}

var _ BridgeAPIStore = (*PostgreSQLBridgeAPIStore)(nil)
