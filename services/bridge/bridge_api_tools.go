package agentruntimebridge

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"strconv"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/id"
	"github.com/tetral-ai/tetral/internal/memory"
	"github.com/tetral-ai/tetral/internal/pathvalidation"
	"github.com/tetral-ai/tetral/internal/queue"
	"github.com/tetral-ai/tetral/internal/storage"
	"github.com/tetral-ai/tetral/internal/workspace"
	bridgev1 "github.com/tetral-ai/tetral/services/bridge/gen/tetral/bridge/v1"
)

// This file owns the Bridge tools protocol-family boundary.

const (
	sandboxToolExecuteMaxAttempts = 5
	runtimeToolResultPollInterval = 25 * time.Millisecond
	sandboxExecutionWaitTimeout   = 30 * time.Second
)

// AcceptSandboxExecution durably transfers one already-authored Tool Use to
// Sandbox Service. The execution row and refs-only Queue job become visible
// together, and the ACK returns immediately after that transaction commits.
func (s *PostgreSQLBridgeAPIStore) AcceptSandboxExecution(ctx context.Context, request *bridgev1.AcceptSandboxExecutionRequest) (*bridgev1.AcceptSandboxExecutionResponse, error) {
	if err := validateDurableToolTarget(request.GetScope(), request.GetToolUseEventId()); err != nil {
		return nil, err
	}
	created := false
	if err := s.withScopeTx(ctx, request.GetScope(), "agentruntimebridge.accept_sandbox_execution", func(tx *dbconnect.Tx) error {
		if err := verifyRuntimeScopeTx(ctx, tx, request.GetScope()); err != nil {
			return err
		}
		if err := lockSandboxExecutionThreadTx(ctx, tx, request.GetScope()); err != nil {
			return err
		}
		if _, settled, err := toolResultForToolUseExistsTx(ctx, tx, request.GetScope().GetWorkspaceId(), request.GetScope().GetSessionId(), request.GetScope().GetSessionThreadId(), "agent.tool_use", request.GetToolUseEventId()); err != nil {
			return err
		} else if settled {
			return status.Error(codes.FailedPrecondition, "sandbox tool use is already settled")
		}
		tool, err := loadDurableToolExecutionTx(ctx, tx, request.GetScope(), request.GetToolUseEventId(), "agent.tool_use", true)
		if err != nil {
			return err
		}
		if existing, ok, err := readRuntimeToolResultTx(ctx, tx, request.GetScope(), request.GetToolUseEventId()); err != nil {
			return err
		} else if ok {
			if !sandboxExecutionIdentityMatches(existing, tool) {
				return status.Error(codes.AlreadyExists, "tool use id conflicts with existing result")
			}
			if existing.ExecutionState.String == "consumed" {
				return status.Error(codes.FailedPrecondition, "sandbox tool result is already consumed")
			}
			return nil
		}
		if err := requireSessionMutationAllowedTx(ctx, tx, request.GetScope()); err != nil {
			return err
		}
		if err := rejectSandboxExecutionAfterReleaseFenceTx(ctx, tx, request.GetScope()); err != nil {
			return err
		}
		if err := lockExecutableSandboxToolRouteTx(ctx, tx, request.GetScope(), request.GetToolUseEventId()); err != nil {
			return err
		}
		now := s.now()
		if _, err := tx.Exec(ctx,
			`INSERT INTO session_runtime_tool_results (
				workspace_id, session_id, session_thread_id, tool_use_event_id, tool_kind,
				normalized_input_hash, tool_name, input_json, ack_status, result_json,
				model_tool_call_id, execution_state, execution_attempt_generation,
				background_task_started, created_at, updated_at
			) VALUES ($1, $2, $3, $4, 'sandbox_tool', $5, $6, $7, 'committed', NULL,
				$8, 'pending', 1, FALSE, $9, $9)`,
			request.GetScope().GetWorkspaceId(), request.GetScope().GetSessionId(),
			request.GetScope().GetSessionThreadId(), request.GetToolUseEventId(),
			tool.NormalizedInputHash, tool.ToolName, tool.InputJSON,
			tool.ModelToolCallID, now,
		); err != nil {
			return err
		}
		payload, err := marshalBridgeJSON(map[string]string{
			"workspace_id": request.GetScope().GetWorkspaceId(), "session_id": request.GetScope().GetSessionId(),
			"session_thread_id": request.GetScope().GetSessionThreadId(), "tool_use_event_id": request.GetToolUseEventId(),
		})
		if err != nil {
			return err
		}
		workspaceID := workspace.ID(request.GetScope().GetWorkspaceId())
		if _, err := queue.EnqueueTx(ctx, tx, queue.EnqueueRequest{
			ID: queue.NewJobID(), WorkspaceID: workspaceID, Kind: queue.KindSandboxToolExecute,
			PartitionKey:   queue.FormatSandboxExecutionPartitionKey(workspaceID, request.GetScope().GetSessionId(), request.GetScope().GetSessionThreadId(), request.GetToolUseEventId()),
			DedupeKey:      queue.FormatSandboxToolExecuteDedupeKey(workspaceID, request.GetScope().GetSessionId(), request.GetScope().GetSessionThreadId(), request.GetToolUseEventId(), 1),
			PayloadVersion: 1, PayloadJSON: []byte(payload), MaxAttempts: sandboxToolExecuteMaxAttempts, Now: now,
		}); err != nil {
			return err
		}
		created = true
		return nil
	}); err != nil {
		if isConversationMutationStaleError(err) {
			return &bridgev1.AcceptSandboxExecutionResponse{Outcome: &bridgev1.AcceptSandboxExecutionResponse_Stale{Stale: &bridgev1.SandboxExecutionStale{}}}, nil
		}
		return nil, err
	}
	if created {
		return &bridgev1.AcceptSandboxExecutionResponse{Outcome: &bridgev1.AcceptSandboxExecutionResponse_Committed{Committed: &bridgev1.SandboxExecutionCommitted{}}}, nil
	}
	return &bridgev1.AcceptSandboxExecutionResponse{Outcome: &bridgev1.AcceptSandboxExecutionResponse_Duplicate{Duplicate: &bridgev1.SandboxExecutionDuplicate{}}}, nil
}

func rejectSandboxExecutionAfterReleaseFenceTx(ctx context.Context, tx *dbconnect.Tx, scope *bridgev1.RuntimeScope) error {
	var releaseRequested bool
	if err := tx.QueryRow(ctx,
		`SELECT EXISTS (
			SELECT 1 FROM session_sandbox_bindings
			 WHERE workspace_id = $1 AND session_id = $2 AND release_requested_at IS NOT NULL
		)`,
		scope.GetWorkspaceId(), scope.GetSessionId(),
	).Scan(&releaseRequested); err != nil {
		return err
	}
	if releaseRequested {
		return status.Error(codes.FailedPrecondition, "sandbox release is already requested")
	}
	return nil
}

// AwaitSandboxExecution reads one accepted execution until Sandbox Service
// settles it. It never creates an execution row or Queue job.
func (s *PostgreSQLBridgeAPIStore) AwaitSandboxExecution(ctx context.Context, request *bridgev1.AwaitSandboxExecutionRequest) (*bridgev1.AwaitSandboxExecutionResponse, error) {
	if err := validateDurableToolTarget(request.GetScope(), request.GetToolUseEventId()); err != nil {
		return nil, err
	}
	if err := s.withScopeReadOnlyTx(ctx, request.GetScope(), "agentruntimebridge.await_sandbox_execution", func(tx *dbconnect.Tx) error {
		return verifyRuntimeScopeReadOnlyTx(ctx, tx, request.GetScope())
	}); err != nil {
		if isScopeSupersededError(err) {
			return &bridgev1.AwaitSandboxExecutionResponse{Outcome: &bridgev1.AwaitSandboxExecutionResponse_Stale{Stale: &bridgev1.SandboxExecutionAwaitStale{}}}, nil
		}
		return nil, err
	}
	terminal, err := s.waitForSandboxExecutionResult(ctx, request)
	if err != nil {
		if isScopeSupersededError(err) {
			return &bridgev1.AwaitSandboxExecutionResponse{Outcome: &bridgev1.AwaitSandboxExecutionResponse_Stale{Stale: &bridgev1.SandboxExecutionAwaitStale{}}}, nil
		}
		return nil, err
	}
	return &bridgev1.AwaitSandboxExecutionResponse{
		Outcome: &bridgev1.AwaitSandboxExecutionResponse_Completed{Completed: &bridgev1.SandboxExecutionCompleted{
			ResultJson: terminal.ResultJSON,
			TaskId:     terminal.TaskID.String,
		}},
	}, nil
}

func validateDurableToolTarget(scope *bridgev1.RuntimeScope, toolUseEventID string) error {
	if err := validateRuntimeScope(scope); err != nil {
		return err
	}
	if toolUseEventID == "" {
		return status.Error(codes.InvalidArgument, "tool use event id is required")
	}
	return nil
}

type durableToolExecution struct {
	ModelRequestID      string
	ModelToolCallID     string
	ToolName            string
	MCPServerName       string
	ProviderInputJSON   string
	InputJSON           string
	NormalizedInputHash string
	EvaluatedPermission string
	RouteCapability     string
}

func loadDurableToolExecutionTx(
	ctx context.Context,
	tx *dbconnect.Tx,
	scope *bridgev1.RuntimeScope,
	toolUseEventID string,
	eventType string,
	lock bool,
) (durableToolExecution, error) {
	query := `SELECT projection_json, model_request_id
		  FROM session_events
		 WHERE workspace_id = $1 AND session_id = $2 AND session_thread_id = $3
		   AND event_id = $4 AND type = $5`
	if lock {
		query += ` FOR UPDATE`
	}
	var projectionJSON string
	var modelRequestID sql.NullString
	if err := tx.QueryRow(ctx, query,
		scope.GetWorkspaceId(), scope.GetSessionId(), scope.GetSessionThreadId(), toolUseEventID, eventType,
	).Scan(&projectionJSON, &modelRequestID); dbconnect.IsNoRows(err) {
		return durableToolExecution{}, status.Error(codes.FailedPrecondition, "durable tool use is missing")
	} else if err != nil {
		return durableToolExecution{}, err
	}
	var projection struct {
		ModelToolCallID         string          `json:"model_tool_call_id"`
		ToolName                string          `json:"tool_name"`
		ProviderInput           json.RawMessage `json:"provider_input"`
		CanonicalExecutionInput json.RawMessage `json:"canonical_execution_input"`
		RouteCapability         string          `json:"route_capability"`
		EvaluatedPermission     string          `json:"evaluated_permission"`
		EventType               string          `json:"event_type"`
		MCPServerName           string          `json:"mcp_server_name"`
	}
	if err := json.Unmarshal([]byte(projectionJSON), &projection); err != nil {
		return durableToolExecution{}, status.Error(codes.FailedPrecondition, "durable tool use projection is invalid")
	}
	inputJSON, inputHash, err := canonicalRunToolInput(string(projection.CanonicalExecutionInput))
	if err != nil || !modelRequestID.Valid || modelRequestID.String == "" || projection.ModelToolCallID == "" ||
		projection.ToolName == "" || len(projection.ProviderInput) == 0 ||
		!runtimeToolRouteCapabilityAllowed(projection.RouteCapability) ||
		projection.EventType != eventType ||
		(projection.EvaluatedPermission != "allow" && projection.EvaluatedPermission != "ask" && projection.EvaluatedPermission != "deny") {
		return durableToolExecution{}, status.Error(codes.FailedPrecondition, "durable tool execution facts are incomplete")
	}
	return durableToolExecution{
		ModelRequestID:      modelRequestID.String,
		ModelToolCallID:     projection.ModelToolCallID,
		ToolName:            projection.ToolName,
		MCPServerName:       projection.MCPServerName,
		ProviderInputJSON:   string(projection.ProviderInput),
		InputJSON:           inputJSON,
		NormalizedInputHash: inputHash,
		EvaluatedPermission: projection.EvaluatedPermission,
		RouteCapability:     projection.RouteCapability,
	}, nil
}

func lockSandboxExecutionThreadTx(ctx context.Context, tx *dbconnect.Tx, scope *bridgev1.RuntimeScope) error {
	var threadID string
	if err := tx.QueryRow(ctx,
		`SELECT id FROM session_threads
		  WHERE workspace_id = $1 AND session_id = $2 AND id = $3
		  FOR UPDATE`,
		scope.GetWorkspaceId(), scope.GetSessionId(), scope.GetSessionThreadId(),
	).Scan(&threadID); dbconnect.IsNoRows(err) {
		return closeoutUnrepairableError(status.Error(codes.FailedPrecondition, "runtime thread is stale"))
	} else if err != nil {
		return err
	}
	return nil
}

// lockExecutableToolRouteTx is the mechanical first-effect gate shared by
// Bridge-owned executors. It locks the exact durable route and its Runtime-
// declared endpoint capability without interpreting Tool identity, arguments,
// or policy.
func lockExecutableToolRouteTx(
	ctx context.Context,
	tx *dbconnect.Tx,
	scope *bridgev1.RuntimeScope,
	toolUseEventID string,
	expectedCapabilities ...string,
) error {
	var statusValue string
	var decision sql.NullString
	var resultEventID sql.NullString
	var routeCapability string
	var terminalResultExists bool
	err := tx.QueryRow(ctx,
		`SELECT route.status, route.decision, route.result_event_id,
		        source.projection_json::jsonb ->> 'route_capability',
		        EXISTS (
		          SELECT 1 FROM session_events result
		           WHERE result.workspace_id=route.workspace_id
		             AND result.session_id=route.session_id
		             AND result.session_thread_id=route.session_thread_id
		             AND result.type IN ('agent.tool_result','agent.mcp_tool_result')
		             AND COALESCE(
		                   result.payload_json::jsonb ->> 'tool_use_event_id',
		                   result.payload_json::jsonb ->> 'tool_use_id',
		                   result.payload_json::jsonb ->> 'mcp_tool_use_id'
		                 ) = route.tool_use_event_id
		        )
		   FROM session_pending_tool_uses route
		   JOIN session_events source
		     ON source.workspace_id=route.workspace_id AND source.session_id=route.session_id
		    AND source.session_thread_id=route.session_thread_id AND source.event_id=route.tool_use_event_id
		  WHERE route.workspace_id = $1 AND route.session_id = $2 AND route.session_thread_id = $3
		    AND route.tool_use_event_id = $4
		  FOR UPDATE`,
		scope.GetWorkspaceId(), scope.GetSessionId(), scope.GetSessionThreadId(), toolUseEventID,
	).Scan(&statusValue, &decision, &resultEventID, &routeCapability, &terminalResultExists)
	if dbconnect.IsNoRows(err) {
		return status.Error(codes.FailedPrecondition, "durable Tool route is missing")
	}
	if err != nil {
		return err
	}
	if statusValue != "resolving" || !decision.Valid || decision.String != "allow" ||
		resultEventID.Valid || terminalResultExists {
		return status.Error(codes.FailedPrecondition, "durable Tool route is not executable")
	}
	if len(expectedCapabilities) > 0 {
		matched := false
		for _, expected := range expectedCapabilities {
			matched = matched || routeCapability == expected
		}
		if !matched {
			return status.Error(codes.FailedPrecondition, "durable Tool route capability does not match the effect endpoint")
		}
	}
	return nil
}

// lockExecutableSandboxToolRouteTx binds Sandbox admission to the capability
// declared by Runtime for this exact Tool route.
func lockExecutableSandboxToolRouteTx(
	ctx context.Context,
	tx *dbconnect.Tx,
	scope *bridgev1.RuntimeScope,
	toolUseEventID string,
) error {
	return lockExecutableToolRouteTx(ctx, tx, scope, toolUseEventID, "sandbox_execute")
}

// lockSettleableToolRouteTx separates terminal Result authority from executor
// admission. Both an allowed execution and a policy-owned denial may settle,
// but a pending, cancelled, resolved, missing, or conflicting route may not.
func lockSettleableToolRouteTx(
	ctx context.Context,
	tx *dbconnect.Tx,
	scope *bridgev1.RuntimeScope,
	toolUseEventID string,
) error {
	var statusValue string
	var decision sql.NullString
	var resultEventID sql.NullString
	err := tx.QueryRow(ctx,
		`SELECT status, decision, result_event_id
		   FROM session_pending_tool_uses
		  WHERE workspace_id=$1 AND session_id=$2 AND session_thread_id=$3
		    AND tool_use_event_id=$4
		  FOR UPDATE`,
		scope.GetWorkspaceId(), scope.GetSessionId(), scope.GetSessionThreadId(), toolUseEventID,
	).Scan(&statusValue, &decision, &resultEventID)
	if dbconnect.IsNoRows(err) {
		return status.Error(codes.FailedPrecondition, "durable Tool settlement route is missing")
	}
	if err != nil {
		return err
	}
	if statusValue != "resolving" || !decision.Valid ||
		(decision.String != "allow" && decision.String != "deny") || resultEventID.Valid {
		return status.Error(codes.FailedPrecondition, "durable Tool route is not settleable")
	}
	return nil
}

func sandboxExecutionIdentityMatches(existing runtimeToolResult, tool durableToolExecution) bool {
	return existing.ToolKind == bridgeToolKindSandbox && existing.NormalizedInputHash == tool.NormalizedInputHash &&
		existing.ToolName == tool.ToolName && existing.InputJSON == tool.InputJSON &&
		existing.ModelToolCallID.Valid && existing.ModelToolCallID.String == tool.ModelToolCallID
}

func (s *PostgreSQLBridgeAPIStore) waitForSandboxExecutionResult(ctx context.Context, request *bridgev1.AwaitSandboxExecutionRequest) (runtimeToolResult, error) {
	waitCtx, cancel := context.WithTimeout(ctx, sandboxExecutionWaitTimeout)
	defer cancel()
	ticker := time.NewTicker(runtimeToolResultPollInterval)
	defer ticker.Stop()
	for {
		var stored runtimeToolResult
		var tool durableToolExecution
		var found bool
		err := s.Client.WithWorkspaceReadOnlyTx(waitCtx, request.GetScope().GetWorkspaceId(), "agentruntimebridge.wait_sandbox_tool", func(tx *dbconnect.Tx) error {
			var err error
			tool, err = loadDurableToolExecutionTx(waitCtx, tx, request.GetScope(), request.GetToolUseEventId(), "agent.tool_use", false)
			if err != nil {
				return err
			}
			stored, found, err = readRuntimeToolResultReadOnlyTx(waitCtx, tx, request.GetScope(), request.GetToolUseEventId())
			return err
		})
		if err != nil {
			if waitCtx.Err() != nil {
				return runtimeToolResult{}, status.Error(codes.DeadlineExceeded, "sandbox tool result is not ready")
			}
			return runtimeToolResult{}, err
		}
		if !found || !sandboxExecutionIdentityMatches(stored, tool) {
			return runtimeToolResult{}, status.Error(codes.FailedPrecondition, "sandbox tool execution identity changed while waiting")
		}
		if stored.ExecutionState.Valid && stored.ExecutionState.String == "terminal_unconsumed" {
			if stored.ResultJSON == "" || !json.Valid([]byte(stored.ResultJSON)) {
				return runtimeToolResult{}, status.Error(codes.FailedPrecondition, "sandbox tool execution result is invalid")
			}
			return stored, nil
		}
		if stored.ExecutionState.Valid && stored.ExecutionState.String == "consumed" {
			return runtimeToolResult{}, status.Error(codes.FailedPrecondition, "sandbox tool result is already consumed")
		}
		select {
		case <-waitCtx.Done():
			return runtimeToolResult{}, status.Error(codes.DeadlineExceeded, "sandbox tool result is not ready")
		case <-ticker.C:
		}
	}
}

// RunMemory is commit-then-project. The mutation and, when a live Sandbox is
// currently bound, its projection job commit atomically. Bridge never calls a
// Sandbox provider; it waits on the durable projection state written by the
// Sandbox worker.
//
//	phase 1  (withScopeTx here): scope-verify, idempotency read, applyMemoryToolTx,
//	         compute the refresh plan, and insert the tool result with
//	         memory_projection_state = pending (non-empty plan) or NULL (empty).
//	         On replay of a row already at pending, the mutation is SKIPPED (it is
//	         already committed) and control drops straight into phase 2.
//	phase 2  (completePendingMemoryProjection): runs with no open transaction and
//	         waits for Sandbox Service to settle the durable projection row. A
//	         disconnect leaves the row pending for the Runtime's normal replay.
//
// INVARIANTS:
//   - The Queue job carries only durable identities. Sandbox Service derives the
//     current committed heads and provider target after leasing the job.
//   - Scope is deliberately narrow: RunMemory refreshes ONLY the mutating session's
//     own bound sandbox projection. Other sessions that attach the same store are
//     not fanned out and see the change only at their own next materialization
//     (cold return); this package must not grow a cross-session fan-out.
func (s *PostgreSQLBridgeAPIStore) RunMemory(ctx context.Context, request *bridgev1.RunMemoryRequest) (*bridgev1.RunMemoryResponse, error) {
	if err := validateDurableToolTarget(request.GetScope(), request.GetToolUseEventId()); err != nil {
		return nil, err
	}
	now := s.now()
	var response *bridgev1.RunMemoryResponse
	phaseTwoDuplicate := false
	if err := s.withScopeTx(ctx, request.GetScope(), "agentruntimebridge.execute_memory", func(tx *dbconnect.Tx) error {
		if err := verifyRuntimeScopeTx(ctx, tx, request.GetScope()); err != nil {
			return err
		}
		tool, err := loadDurableToolExecutionTx(ctx, tx, request.GetScope(), request.GetToolUseEventId(), "agent.tool_use", true)
		if err != nil {
			return err
		}
		var memoryInput memoryToolInput
		if err := json.Unmarshal([]byte(tool.InputJSON), &memoryInput); err != nil || memoryInput.Action == "" {
			return status.Error(codes.FailedPrecondition, "durable memory input is invalid")
		}
		if existing, ok, err := readRuntimeToolResultTx(ctx, tx, request.GetScope(), request.GetToolUseEventId()); err != nil {
			return err
		} else if ok {
			if existing.ToolKind != bridgeToolKindMemory || existing.NormalizedInputHash != tool.NormalizedInputHash {
				return status.Error(codes.AlreadyExists, "memory tool use id conflicts with existing result")
			}
			if existing.MemoryProjectionState.Valid && existing.MemoryProjectionState.String == memoryProjectionStatePending {
				phaseTwoDuplicate = true
				return nil
			}
			response = duplicateMemoryRunResponse(existing.ResultJSON)
			return nil
		}
		if err := lockExecutableToolRouteTx(ctx, tx, request.GetScope(), request.GetToolUseEventId(), "memory_execute"); err != nil {
			return err
		}
		if err := requireSessionMutationAllowedTx(ctx, tx, request.GetScope()); err != nil {
			return err
		}
		resultJSON, err := applyMemoryToolTx(ctx, tx, request.GetScope(), memoryInput.Action, tool.InputJSON)
		if err != nil {
			return err
		}
		var projectionState sql.NullString
		if len(memoryProjectionPlanPaths(tool.InputJSON, resultJSON)) > 0 {
			storeID, bindingResultJSON, err := resolveWritableMemoryStoreTx(ctx, tx, request.GetScope())
			if err != nil {
				return err
			}
			if bindingResultJSON != "" {
				return status.Error(codes.FailedPrecondition, "writable memory store binding changed during mutation")
			}
			var providerResourceID sql.NullString
			if err := tx.QueryRow(ctx,
				`SELECT provider_resource_id
				   FROM session_sandbox_bindings
				  WHERE workspace_id = $1 AND session_id = $2 AND release_requested_at IS NULL`,
				request.GetScope().GetWorkspaceId(), request.GetScope().GetSessionId(),
			).Scan(&providerResourceID); err != nil && !dbconnect.IsNoRows(err) {
				return err
			}
			if providerResourceID.Valid && providerResourceID.String != "" {
				projectionState = sql.NullString{String: memoryProjectionStatePending, Valid: true}
				payload, err := marshalBridgeJSON(map[string]string{
					"workspace_id": request.GetScope().GetWorkspaceId(), "session_id": request.GetScope().GetSessionId(),
					"memory_store_id": storeID, "memory_write_id": request.GetToolUseEventId(),
				})
				if err != nil {
					return err
				}
				workspaceID := workspace.ID(request.GetScope().GetWorkspaceId())
				if _, err := queue.EnqueueTx(ctx, tx, queue.EnqueueRequest{
					ID: queue.NewJobID(), WorkspaceID: workspaceID, Kind: queue.KindSandboxMemoryProjection,
					PartitionKey:   queue.FormatSandboxMemoryPartitionKey(workspaceID, storeID),
					DedupeKey:      queue.FormatSandboxMemoryProjectionDedupeKey(workspaceID, storeID, request.GetToolUseEventId()),
					PayloadVersion: 1, PayloadJSON: []byte(payload), MaxAttempts: queue.SandboxMemoryProjectionMaxAttempts, Now: now,
				}); err != nil {
					return err
				}
			} else {
				projectionState = sql.NullString{String: memoryProjectionStateSkippedCold, Valid: true}
			}
		}
		if err := insertRuntimeToolResultTx(ctx, tx, request.GetScope(), runtimeToolResultInsert{
			ToolUseEventID:        request.GetToolUseEventId(),
			ToolKind:              bridgeToolKindMemory,
			NormalizedInputHash:   tool.NormalizedInputHash,
			ToolName:              "memory",
			InputJSON:             tool.InputJSON,
			AckStatus:             bridgeAckCommitted,
			ResultJSON:            resultJSON,
			MemoryProjectionState: projectionState,
			Now:                   now,
		}); err != nil {
			return err
		}
		if projectionState.Valid && projectionState.String == memoryProjectionStatePending {
			return nil
		}
		response = committedMemoryRunResponse(resultJSON)
		return nil
	}); err != nil {
		if isConversationMutationStaleError(err) {
			return &bridgev1.RunMemoryResponse{Outcome: &bridgev1.RunMemoryResponse_Stale{Stale: &bridgev1.MemoryRunStale{}}}, nil
		}
		return nil, err
	}
	if response == nil {
		response, err := s.completePendingMemoryProjection(ctx, request, phaseTwoDuplicate)
		if isScopeSupersededError(err) {
			return &bridgev1.RunMemoryResponse{Outcome: &bridgev1.RunMemoryResponse_Stale{Stale: &bridgev1.MemoryRunStale{}}}, nil
		}
		return response, err
	}
	return response, nil
}

func committedMemoryRunResponse(resultJSON string) *bridgev1.RunMemoryResponse {
	return &bridgev1.RunMemoryResponse{Outcome: &bridgev1.RunMemoryResponse_Committed{Committed: &bridgev1.MemoryRunCommitted{ResultJson: resultJSON}}}
}

func duplicateMemoryRunResponse(resultJSON string) *bridgev1.RunMemoryResponse {
	return &bridgev1.RunMemoryResponse{Outcome: &bridgev1.RunMemoryResponse_Duplicate{Duplicate: &bridgev1.MemoryRunDuplicate{ResultJson: resultJSON}}}
}

func (s *PostgreSQLBridgeAPIStore) CommitInternalToolRepair(ctx context.Context, request *bridgev1.CommitInternalToolRepairRequest) (*bridgev1.CommitInternalToolRepairResponse, error) {
	if err := validateInternalToolRepairRequest(request); err != nil {
		return nil, err
	}
	repairKey := request.GetRepairKey()
	declarationDigest, err := internalToolRepairDeclarationDigest(request, repairKey)
	if err != nil {
		return nil, err
	}
	now := s.now()
	var (
		facts     internalToolRepairDurableFacts
		duplicate bool
	)
	if err := s.withScopeTx(ctx, request.GetScope(), "agentruntimebridge.commit_internal_tool_repair", func(tx *dbconnect.Tx) error {
		if err := lockRuntimeMutationSessionTx(
			ctx,
			tx,
			request.GetScope().GetWorkspaceId(),
			request.GetScope().GetSessionId(),
		); err != nil {
			return err
		}
		if err := verifyRuntimeDeclarationCaller(ctx, request.GetScope()); err != nil {
			return err
		}
		if err := verifyRuntimeScopeTx(ctx, tx, request.GetScope()); err != nil {
			return err
		}
		if existing, ok, err := readBridgeDeclarationOperationTx(
			ctx,
			tx,
			request.GetScope(),
			bridgeOpCommitInternalToolRepair,
			"internal_tool_repair",
			repairKey,
		); err != nil {
			return err
		} else if ok {
			if existing.DeclarationDigest == "" || existing.ReceiptJSON == "" {
				return status.Error(codes.FailedPrecondition, "stored internal tool repair result is invalid")
			}
			if existing.DeclarationDigest != declarationDigest {
				return status.Error(codes.AlreadyExists, "internal tool repair idempotency conflict")
			}
			if err := json.Unmarshal([]byte(existing.ReceiptJSON), &facts); err != nil || facts.RepairEventID == "" || facts.AssignedMessageSequence <= 0 {
				return status.Error(codes.FailedPrecondition, "stored internal tool repair result is invalid")
			}
			duplicate = true
			return nil
		}
		threadScope, err := lockThreadMutationTx(ctx, tx, request.GetScope())
		if err != nil {
			return err
		}
		if err := verifyModelRequestAcceptsMembersTx(ctx, tx, request.GetScope(), request.GetModelRequestId()); err != nil {
			return err
		}
		if err := verifyModelToolCallIDAvailableTx(
			ctx, tx, request.GetScope(), request.GetModelToolCallId(),
		); err != nil {
			return err
		}
		eventID, _, err := insertInternalToolRepairEventTx(ctx, tx, request, threadScope, repairKey, now)
		if err != nil {
			return err
		}
		facts, err = commitInternalToolRepairContextTx(
			ctx,
			tx,
			request.GetScope(),
			eventID,
			request.GetModelRequestId(),
			request.GetModelToolCallId(),
			request.GetToolName(),
			request.GetCanonicalInputJson(),
			request.GetError(),
			now,
		)
		if err != nil {
			return err
		}
		if err := verifyModelToolCallIDUniqueTx(
			ctx,
			tx,
			request.GetScope(),
			request.GetModelToolCallId(),
		); err != nil {
			return err
		}
		resultJSON, err := marshalBridgeJSON(facts)
		if err != nil {
			return err
		}
		return insertBridgeDeclarationOperationTx(
			ctx,
			tx,
			request.GetScope(),
			bridgeOpCommitInternalToolRepair,
			"internal_tool_repair",
			repairKey,
			declarationDigest,
			resultJSON,
			now,
		)
	}); err != nil {
		if isConversationMutationStaleError(err) {
			return &bridgev1.CommitInternalToolRepairResponse{Outcome: &bridgev1.CommitInternalToolRepairResponse_Stale{Stale: &bridgev1.CommitInternalToolRepairStale{}}}, nil
		}
		return nil, err
	}
	if facts.RepairEventID == "" || facts.AssignedMessageSequence <= 0 {
		return nil, status.Error(codes.FailedPrecondition, "internal tool repair result is invalid")
	}
	observation, err := s.declarationApplicationObservation(ctx, request.GetScope())
	if err != nil {
		return nil, err
	}
	logRuntimeDeclaration(
		s.Logger,
		request.GetScope(),
		bridgeOpCommitInternalToolRepair,
		"internal_tool_repair",
		repairKey,
		declarationDigest,
		duplicate,
		observation,
	)
	if !observation.Current {
		return &bridgev1.CommitInternalToolRepairResponse{Outcome: &bridgev1.CommitInternalToolRepairResponse_Stale{Stale: &bridgev1.CommitInternalToolRepairStale{}}}, nil
	}
	if duplicate {
		return &bridgev1.CommitInternalToolRepairResponse{Outcome: &bridgev1.CommitInternalToolRepairResponse_Duplicate{Duplicate: &bridgev1.CommitInternalToolRepairDuplicate{
			RepairEventId: facts.RepairEventID, AssignedMessageSequence: facts.AssignedMessageSequence,
		}}}, nil
	}
	return &bridgev1.CommitInternalToolRepairResponse{Outcome: &bridgev1.CommitInternalToolRepairResponse_Committed{Committed: &bridgev1.CommitInternalToolRepairCommitted{
		RepairEventId: facts.RepairEventID, AssignedMessageSequence: facts.AssignedMessageSequence,
	}}}, nil
}

func validateInternalToolRepairRequest(request *bridgev1.CommitInternalToolRepairRequest) error {
	if err := validateRuntimeScope(request.GetScope()); err != nil {
		return err
	}
	if request.GetModelRequestId() == "" || request.GetModelToolCallId() == "" || request.GetToolName() == "" || request.GetCanonicalInputJson() == "" || request.GetError() == nil || request.GetRepairKey() == "" {
		return status.Error(codes.InvalidArgument, "invalid internal tool repair request")
	}
	if len([]byte(request.GetModelToolCallId())) > internalToolRepairIDMaxBytes || len([]byte(request.GetToolName())) > internalToolRepairIDMaxBytes {
		return status.Error(codes.InvalidArgument, "internal tool repair identifiers are too large")
	}
	if request.GetRepairKey() != internalToolRepairKey(request.GetModelRequestId(), request.GetModelToolCallId(), request.GetToolName()) {
		return status.Error(codes.InvalidArgument, "internal tool repair key is invalid")
	}
	if _, err := canonicalRunToolJSON(request.GetCanonicalInputJson()); err != nil {
		return status.Error(codes.InvalidArgument, "internal tool repair canonical input is invalid")
	}
	if _, err := canonicalRuntimeToolError(request.GetError()); err != nil {
		return err
	}
	return nil
}

func insertInternalToolRepairEventTx(
	ctx context.Context,
	tx *dbconnect.Tx,
	request *bridgev1.CommitInternalToolRepairRequest,
	threadScope threadMutationScope,
	repairKey string,
	now time.Time,
) (string, int64, error) {
	scope := request.GetScope()
	payloadJSON, err := marshalBridgeJSON(map[string]any{
		"type":               "agent.tool_result",
		"model_tool_call_id": request.GetModelToolCallId(),
		"tool_name":          request.GetToolName(),
		"repair_kind":        "invalid_tool",
	})
	if err != nil {
		return "", 0, err
	}
	eventID := id.New("evt_")
	sequence, err := nextSessionEventSequenceTx(ctx, tx, scope)
	if err != nil {
		return "", 0, err
	}
	visibility, sessionVisible := threadScope.publicProjection("agent.tool_result")
	errorValue, err := canonicalRuntimeToolError(request.GetError())
	if err != nil {
		return "", 0, err
	}
	projection := runtimeToolProjectionFromDurableTool(durableToolExecution{
		ModelRequestID:    request.GetModelRequestId(),
		ModelToolCallID:   request.GetModelToolCallId(),
		ToolName:          request.GetToolName(),
		ProviderInputJSON: request.GetCanonicalInputJson(),
		InputJSON:         request.GetCanonicalInputJson(),
	}, map[string]any{"type": "error", "error": errorValue})
	projectionJSON, err := marshalBridgeJSON(projection)
	if err != nil {
		return "", 0, err
	}
	if _, err := tx.Exec(ctx,
		`INSERT INTO session_events (
			workspace_id, session_id, session_thread_id, event_id, sequence, type, payload_json,
			visibility, session_visible, runtime_write_id, model_request_id, projection_json,
			created_at, updated_at, processed_at
		) VALUES ($1, $2, $3, $4, $5, 'agent.tool_result', $6, $7, $8, $9, $10, $11, $12, $12, $12)`,
		scope.GetWorkspaceId(),
		scope.GetSessionId(),
		scope.GetSessionThreadId(),
		eventID,
		sequence,
		payloadJSON,
		visibility,
		sessionVisible,
		repairKey,
		request.GetModelRequestId(),
		projectionJSON,
		now,
	); err != nil {
		return "", 0, err
	}
	if _, err := appendSessionEventStreamChangeTx(ctx, tx, scope, eventID, visibility, sessionVisible, now); err != nil {
		return "", 0, err
	}
	return eventID, sequence, nil
}

func internalToolRepairKey(modelRequestID string, modelToolCallID string, toolName string) string {
	hash := sha256.New()
	for _, value := range []string{modelRequestID, modelToolCallID, toolName} {
		_, _ = io.WriteString(hash, strconv.Itoa(len([]byte(value))))
		_, _ = io.WriteString(hash, ":")
		_, _ = io.WriteString(hash, value)
	}
	return "internal_invalid_tool_" + hex.EncodeToString(hash.Sum(nil))
}

func (s *PostgreSQLBridgeAPIStore) completePendingMemoryProjection(ctx context.Context, request *bridgev1.RunMemoryRequest, duplicate bool) (*bridgev1.RunMemoryResponse, error) {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		var response *bridgev1.RunMemoryResponse
		if err := s.withScopeTx(ctx, request.GetScope(), "agentruntimebridge.await_memory_projection", func(tx *dbconnect.Tx) error {
			if err := verifyRuntimeScopeReadOnlyTx(ctx, tx, request.GetScope()); err != nil {
				return err
			}
			existing, ok, err := readRuntimeToolResultReadOnlyTx(ctx, tx, request.GetScope(), request.GetToolUseEventId())
			if err != nil {
				return err
			}
			if !ok {
				return status.Error(codes.FailedPrecondition, "memory tool result is missing")
			}
			tool, err := loadDurableToolExecutionTx(ctx, tx, request.GetScope(), request.GetToolUseEventId(), "agent.tool_use", false)
			if err != nil {
				return err
			}
			if existing.ToolKind != bridgeToolKindMemory || existing.NormalizedInputHash != tool.NormalizedInputHash {
				return status.Error(codes.AlreadyExists, "memory tool use id conflicts with existing result")
			}
			if !existing.MemoryProjectionState.Valid || existing.MemoryProjectionState.String != memoryProjectionStatePending {
				if duplicate {
					response = duplicateMemoryRunResponse(existing.ResultJSON)
				} else {
					response = committedMemoryRunResponse(existing.ResultJSON)
				}
			}
			return nil
		}); err != nil {
			return nil, err
		}
		if response != nil {
			return response, nil
		}
		select {
		case <-ctx.Done():
			return nil, status.Error(codes.DeadlineExceeded, "memory projection result is not ready")
		case <-ticker.C:
		}
	}
}

type memoryToolInput struct {
	Action       string  `json:"action"`
	Path         string  `json:"path"`
	NewPath      string  `json:"new_path"`
	Content      *string `json:"content"`
	OldText      *string `json:"old_text"`
	NewText      *string `json:"new_text"`
	ReplaceAll   *bool   `json:"replace_all"`
	ExpectedText *string `json:"expected_text"`
}

type memoryToolResultShape struct {
	Status          string                     `json:"status"`
	Action          string                     `json:"action"`
	Path            string                     `json:"path"`
	NewPath         string                     `json:"new_path"`
	ErrorCode       string                     `json:"error_code"`
	Conflicts       []activeMemoryPathConflict `json:"conflicts"`
	RereadRequired  bool                       `json:"reread_required"`
	ProjectionReady bool                       `json:"projection_refreshed"`
}

// memoryProjectionPlanPaths derives the refresh plan (the paths to re-sync) per
// action and outcome:
//
//	action / outcome                          plan
//	create | replace | delete (completed)     path
//	rename (completed)                         path AND new_path
//	stale tool_error (old_text_not_found,      every path referenced by the request
//	  old_text_not_unique, expected_text_        — pushing current truth is what makes
//	  mismatch, not_found)                       the model's mandated re-read see fresh state
//	stale tool_error (path_exists)             referenced paths PLUS every conflicting
//	                                             durable head from the conflict check
//	                                             (a conflicting head need not be
//	                                             request-referenced)
//	validation / binding tool_error            empty — nothing was learned about
//	                                             divergence
//
// The two non-obvious rows: path_exists pulls in conflicting heads that the
// request never named, and stale errors push truth precisely so the re-read
// observes fresh state.
func memoryProjectionPlanPaths(inputJSON string, resultJSON string) []string {
	var result memoryToolResultShape
	if err := json.Unmarshal([]byte(resultJSON), &result); err != nil {
		return nil
	}
	switch result.Status {
	case "completed":
		return memoryCompletedProjectionPaths(result)
	case "tool_error":
		if !isMemoryStaleErrorCode(result.ErrorCode) {
			return nil
		}
		var input memoryToolInput
		if err := json.Unmarshal([]byte(inputJSON), &input); err != nil {
			return nil
		}
		paths := memoryReferencedProjectionPaths(input.Action, input)
		if result.ErrorCode == "path_exists" {
			for _, conflict := range result.Conflicts {
				paths = append(paths, conflict.Path)
			}
		}
		return compactMemoryProjectionPaths(paths...)
	default:
		return nil
	}
}

func memoryCompletedProjectionPaths(result memoryToolResultShape) []string {
	switch result.Action {
	case "create", "replace", "delete":
		return compactMemoryProjectionPaths(memoryProjectionPathFromResult(result.Path))
	case "rename":
		return compactMemoryProjectionPaths(memoryProjectionPathFromResult(result.Path), memoryProjectionPathFromResult(result.NewPath))
	default:
		return nil
	}
}

func memoryReferencedProjectionPaths(action string, input memoryToolInput) []string {
	switch action {
	case "create", "replace", "delete":
		return compactMemoryProjectionPaths(memoryProjectionPathFromInput(input.Path))
	case "rename":
		return compactMemoryProjectionPaths(memoryProjectionPathFromInput(input.Path), memoryProjectionPathFromInput(input.NewPath))
	default:
		return nil
	}
}

func memoryProjectionPathFromResult(relative string) string {
	if err := validateMemoryToolRelativePath(relative); err != nil {
		return ""
	}
	return "/" + relative
}

func memoryProjectionPathFromInput(relative string) string {
	if err := validateMemoryToolRelativePath(relative); err != nil {
		return ""
	}
	return "/" + relative
}

func compactMemoryProjectionPaths(paths ...string) []string {
	seen := map[string]struct{}{}
	out := make([]string, 0, len(paths))
	for _, pathValue := range paths {
		if pathValue == "" {
			continue
		}
		if _, ok := seen[pathValue]; ok {
			continue
		}
		seen[pathValue] = struct{}{}
		out = append(out, pathValue)
	}
	return out
}

func isMemoryStaleErrorCode(errorCode string) bool {
	switch errorCode {
	case "old_text_not_found", "old_text_not_unique", "expected_text_mismatch", "path_exists", "not_found":
		return true
	default:
		return false
	}
}

func applyMemoryToolTx(ctx context.Context, tx *dbconnect.Tx, scope *bridgev1.RuntimeScope, requestedOperation string, inputJSON string) (string, error) {
	var inputShape map[string]json.RawMessage
	if err := json.Unmarshal([]byte(inputJSON), &inputShape); err != nil || inputShape == nil {
		return marshalMemoryToolError("invalid_input", "memory input must be an object", false)
	}
	var input memoryToolInput
	if err := json.Unmarshal([]byte(inputJSON), &input); err != nil {
		return marshalMemoryToolError("invalid_input", "memory input must be an object", false)
	}
	action, resultJSON, err := canonicalMemoryToolAction(requestedOperation, input.Action)
	if err != nil || resultJSON != "" {
		return resultJSON, err
	}
	storeID, resultJSON, err := resolveWritableMemoryStoreTx(ctx, tx, scope)
	if err != nil || resultJSON != "" {
		return resultJSON, err
	}
	if err := storage.AcquireMemoryStoreMutationLock(ctx, tx, scope.GetWorkspaceId(), storeID); err != nil {
		return "", err
	}
	if err := requireActiveMemoryStoreTx(ctx, tx, scope.GetWorkspaceId(), storeID); err != nil {
		return "", err
	}
	switch action {
	case "create":
		return createMemoryByToolTx(ctx, tx, scope, storeID, input)
	case "replace":
		return replaceMemoryByToolTx(ctx, tx, scope, storeID, input)
	case "delete":
		return deleteMemoryByToolTx(ctx, tx, scope, storeID, input)
	case "rename":
		return renameMemoryByToolTx(ctx, tx, scope, storeID, input)
	default:
		return marshalMemoryToolError("invalid_action", "memory action must be create, replace, delete, or rename", false)
	}
}

func canonicalMemoryToolAction(requestedOperation string, inputAction string) (string, string, error) {
	if inputAction == "" {
		result, err := marshalMemoryToolError("invalid_action", "memory action must be create, replace, delete, or rename", false)
		return "", result, err
	}
	if requestedOperation != "" && requestedOperation != inputAction {
		return "", "", status.Error(codes.InvalidArgument, "memory operation does not match input action")
	}
	return inputAction, "", nil
}

func resolveWritableMemoryStoreTx(ctx context.Context, tx *dbconnect.Tx, scope *bridgev1.RuntimeScope) (string, string, error) {
	rows, err := tx.Query(ctx,
		`SELECT smr.memory_store_id
		   FROM session_memory_store_resources smr
		   JOIN session_resources sr
		     ON sr.workspace_id = smr.workspace_id
		    AND sr.session_id = smr.session_id
		    AND sr.resource_id = smr.resource_id
		    AND sr.type = 'memory_store'
		    AND sr.detached_at IS NULL
		    AND sr.delete_requested_at IS NULL
		  WHERE smr.workspace_id = $1
		    AND smr.session_id = $2
		    AND smr.access = 'read_write'
		  ORDER BY smr.resource_id ASC`,
		scope.GetWorkspaceId(),
		scope.GetSessionId(),
	)
	if err != nil {
		return "", "", err
	}
	defer func() { _ = rows.Close() }()
	var stores []string
	for rows.Next() {
		var storeID string
		if err := rows.Scan(&storeID); err != nil {
			return "", "", err
		}
		stores = append(stores, storeID)
	}
	if err := rows.Err(); err != nil {
		return "", "", err
	}
	if len(stores) == 1 {
		return stores[0], "", nil
	}
	errorCode := "memory_store_not_configured"
	message := "session has no writable memory store binding"
	if len(stores) > 1 {
		errorCode = "memory_store_ambiguous"
		message = "session has more than one writable memory store binding"
	}
	result, err := marshalMemoryToolError(errorCode, message, false)
	return "", result, err
}

func requireActiveMemoryStoreTx(ctx context.Context, tx *dbconnect.Tx, workspaceID string, storeID string) error {
	var archivedAt sql.NullTime
	if err := tx.QueryRow(ctx,
		`SELECT archived_at
		   FROM memory_stores
		  WHERE workspace_id = $1 AND memory_store_id = $2
		  FOR UPDATE`,
		workspaceID,
		storeID,
	).Scan(&archivedAt); dbconnect.IsNoRows(err) {
		return status.Error(codes.FailedPrecondition, "memory store not found")
	} else if err != nil {
		return err
	}
	if archivedAt.Valid {
		return status.Error(codes.FailedPrecondition, "memory store is archived")
	}
	return nil
}

func createMemoryByToolTx(ctx context.Context, tx *dbconnect.Tx, scope *bridgev1.RuntimeScope, storeID string, input memoryToolInput) (string, error) {
	path, toolErr, err := memoryToolPath(input.Path)
	if err != nil || toolErr != "" {
		return toolErr, err
	}
	if input.Content == nil {
		return marshalMemoryToolError("missing_content", "create requires content", false)
	}
	if len([]byte(*input.Content)) > memoryToolContentMaxBytes {
		return marshalMemoryToolError("content_too_large", "content must be at most 102400 bytes", false)
	}
	if conflicts, err := activeMemoryPathConflictTx(ctx, tx, scope.GetWorkspaceId(), storeID, path, ""); err != nil {
		return "", err
	} else if len(conflicts.Conflicts) > 0 {
		return marshalMemoryPathConflictToolError(memoryPathConflictCreateMessage(conflicts.Conflicts[0].Kind), conflicts)
	}
	now := storage.Now()
	memoryID := id.New("mem_")
	versionID := id.New("memver_")
	hash := sha256Hex(*input.Content)
	size := int64(len([]byte(*input.Content)))
	if err := memory.DurableWriteQuotas.EnforceCreate(ctx, tx, scope.GetWorkspaceId(), storeID, size); err != nil {
		return "", err
	}
	if _, err := tx.Exec(ctx,
		`INSERT INTO memories (workspace_id, memory_store_id, memory_id, current_version_id, path, content_sha256, content_size_bytes, created_at, updated_at)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $8)`,
		scope.GetWorkspaceId(), storeID, memoryID, versionID, path, hash, size, now); err != nil {
		return "", err
	}
	if _, err := tx.Exec(ctx,
		`INSERT INTO memory_versions (
			workspace_id, memory_store_id, memory_id, memory_version_id, operation, path, content,
			content_sha256, content_size_bytes, created_at, created_actor_type, created_session_id
		) VALUES ($1, $2, $3, $4, 'created', $5, $6, $7, $8, $9, 'session_actor', $10)`,
		scope.GetWorkspaceId(), storeID, memoryID, versionID, path, *input.Content, hash, size, now, scope.GetSessionId()); err != nil {
		return "", err
	}
	return marshalMemoryCompleted("create", strings.TrimPrefix(path, "/"))
}

func replaceMemoryByToolTx(ctx context.Context, tx *dbconnect.Tx, scope *bridgev1.RuntimeScope, storeID string, input memoryToolInput) (string, error) {
	path, toolErr, err := memoryToolPath(input.Path)
	if err != nil || toolErr != "" {
		return toolErr, err
	}
	if input.OldText == nil || input.NewText == nil {
		return marshalMemoryToolError("missing_replace_text", "replace requires old_text and new_text", false)
	}
	current, resultJSON, err := readActiveMemoryByPathTx(ctx, tx, scope.GetWorkspaceId(), storeID, path)
	if err != nil || resultJSON != "" {
		return resultJSON, err
	}
	count := strings.Count(current.Content, *input.OldText)
	replaceAll := input.ReplaceAll != nil && *input.ReplaceAll
	if count == 0 {
		return marshalMemoryStaleToolError("old_text_not_found", "old_text is absent from current content")
	}
	if count > 1 && !replaceAll {
		return marshalMemoryStaleToolError("old_text_not_unique", "old_text matches more than once")
	}
	nextContent := strings.Replace(current.Content, *input.OldText, *input.NewText, replaceCount(replaceAll))
	if len([]byte(nextContent)) > memoryToolContentMaxBytes {
		return marshalMemoryToolError("content_too_large", "content must be at most 102400 bytes", false)
	}
	if err := updateMemoryContentTx(ctx, tx, scope, storeID, current, path, nextContent, "modified"); err != nil {
		return "", err
	}
	return marshalMemoryCompleted("replace", strings.TrimPrefix(path, "/"))
}

func deleteMemoryByToolTx(ctx context.Context, tx *dbconnect.Tx, scope *bridgev1.RuntimeScope, storeID string, input memoryToolInput) (string, error) {
	path, toolErr, err := memoryToolPath(input.Path)
	if err != nil || toolErr != "" {
		return toolErr, err
	}
	if input.ExpectedText == nil {
		return marshalMemoryToolError("missing_expected_text", "delete requires expected_text", false)
	}
	current, resultJSON, err := readActiveMemoryByPathTx(ctx, tx, scope.GetWorkspaceId(), storeID, path)
	if err != nil || resultJSON != "" {
		return resultJSON, err
	}
	if current.Content != *input.ExpectedText {
		return marshalMemoryStaleToolError("expected_text_mismatch", "expected_text does not match current content")
	}
	if err := memory.DurableWriteQuotas.EnforceDeletionVersion(ctx, tx, scope.GetWorkspaceId(), storeID); err != nil {
		return "", err
	}
	now := storage.Now()
	versionID := id.New("memver_")
	if _, err := tx.Exec(ctx,
		`UPDATE memories
		    SET current_version_id = $1, content_sha256 = NULL, content_size_bytes = NULL, updated_at = $2, deleted_at = $2
		  WHERE workspace_id = $3 AND memory_store_id = $4 AND memory_id = $5`,
		versionID, now, scope.GetWorkspaceId(), storeID, current.MemoryID); err != nil {
		return "", err
	}
	if _, err := tx.Exec(ctx,
		`INSERT INTO memory_versions (
			workspace_id, memory_store_id, memory_id, memory_version_id, operation, path,
			created_at, created_actor_type, created_session_id
		) VALUES ($1, $2, $3, $4, 'deleted', $5, $6, 'session_actor', $7)`,
		scope.GetWorkspaceId(), storeID, current.MemoryID, versionID, path, now, scope.GetSessionId()); err != nil {
		return "", err
	}
	return marshalMemoryCompleted("delete", strings.TrimPrefix(path, "/"))
}

func renameMemoryByToolTx(ctx context.Context, tx *dbconnect.Tx, scope *bridgev1.RuntimeScope, storeID string, input memoryToolInput) (string, error) {
	path, toolErr, err := memoryToolPath(input.Path)
	if err != nil || toolErr != "" {
		return toolErr, err
	}
	newPath, toolErr, err := memoryToolPath(input.NewPath)
	if err != nil || toolErr != "" {
		return toolErr, err
	}
	if input.ExpectedText == nil {
		return marshalMemoryToolError("missing_expected_text", "rename requires expected_text", false)
	}
	current, resultJSON, err := readActiveMemoryByPathTx(ctx, tx, scope.GetWorkspaceId(), storeID, path)
	if err != nil || resultJSON != "" {
		return resultJSON, err
	}
	if current.Content != *input.ExpectedText {
		return marshalMemoryStaleToolError("expected_text_mismatch", "expected_text does not match current content")
	}
	if conflicts, err := activeMemoryPathConflictTx(ctx, tx, scope.GetWorkspaceId(), storeID, newPath, current.MemoryID); err != nil {
		return "", err
	} else if len(conflicts.Conflicts) > 0 {
		return marshalMemoryPathConflictToolError(memoryPathConflictRenameMessage(conflicts.Conflicts[0].Kind), conflicts)
	}
	if err := updateMemoryContentTx(ctx, tx, scope, storeID, current, newPath, current.Content, "modified"); err != nil {
		return "", err
	}
	return marshalBridgeJSON(map[string]any{"status": "completed", "action": "rename", "path": strings.TrimPrefix(path, "/"), "new_path": strings.TrimPrefix(newPath, "/")})
}

type currentMemory struct {
	MemoryID string
	Path     string
	Content  string
	Created  string
}

func readActiveMemoryByPathTx(ctx context.Context, tx *dbconnect.Tx, workspaceID string, storeID string, path string) (currentMemory, string, error) {
	row := tx.QueryRow(ctx,
		`SELECT m.memory_id, m.path, v.content, m.created_at
		   FROM memories m
		   JOIN memory_versions v
		     ON v.workspace_id = m.workspace_id
		    AND v.memory_store_id = m.memory_store_id
		    AND v.memory_id = m.memory_id
		    AND v.memory_version_id = m.current_version_id
		  WHERE m.workspace_id = $1
		    AND m.memory_store_id = $2
		    AND m.path = $3
		    AND m.deleted_at IS NULL
		  FOR UPDATE OF m`,
		workspaceID,
		storeID,
		path,
	)
	var current currentMemory
	if err := row.Scan(&current.MemoryID, &current.Path, &current.Content, &current.Created); dbconnect.IsNoRows(err) {
		result, marshalErr := marshalMemoryToolError("not_found", "memory path was not found", true)
		return currentMemory{}, result, marshalErr
	} else if err != nil {
		return currentMemory{}, "", err
	}
	return current, "", nil
}

type memoryPathConflict string

const (
	memoryPathConflictNone       memoryPathConflict = ""
	memoryPathConflictExact      memoryPathConflict = "exact"
	memoryPathConflictAncestor   memoryPathConflict = "ancestor"
	memoryPathConflictDescendant memoryPathConflict = "descendant"
)

type activeMemoryPathConflict struct {
	MemoryID string             `json:"memory_id"`
	Path     string             `json:"path"`
	Kind     memoryPathConflict `json:"-"`
}

type activeMemoryPathConflicts struct {
	Conflicts []activeMemoryPathConflict
	Total     int
	Truncated bool
}

func activeMemoryPathConflictTx(ctx context.Context, tx *dbconnect.Tx, workspaceID string, storeID string, path string, exceptMemoryID string) (activeMemoryPathConflicts, error) {
	rows, err := tx.Query(ctx,
		`SELECT memory_id, path, count(*) OVER ()
		   FROM memories
		  WHERE workspace_id = $1
		    AND memory_store_id = $2
		    AND deleted_at IS NULL
		    AND ($4 = '' OR memory_id <> $4)
		    AND (
				path = $3
				OR left(path, length($3) + 1) = $3 || '/'
				OR left($3, length(path) + 1) = path || '/'
			)
		  ORDER BY CASE
			   WHEN path = $3 THEN 0
			   WHEN left($3, length(path) + 1) = path || '/' THEN 1
			   ELSE 2
		   END, length(path) DESC, memory_id ASC
		 LIMIT $5`,
		workspaceID,
		storeID,
		path,
		exceptMemoryID,
		MaxMemoryPathConflicts,
	)
	if err != nil {
		return activeMemoryPathConflicts{}, err
	}
	defer func() { _ = rows.Close() }()
	result := activeMemoryPathConflicts{Conflicts: make([]activeMemoryPathConflict, 0, MaxMemoryPathConflicts)}
	for rows.Next() {
		var memoryID, existingPath string
		if err := rows.Scan(&memoryID, &existingPath, &result.Total); err != nil {
			return activeMemoryPathConflicts{}, err
		}
		kind := memoryPathConflictNone
		switch {
		case existingPath == path:
			kind = memoryPathConflictExact
		case strings.HasPrefix(path, existingPath+"/"):
			kind = memoryPathConflictDescendant
		case strings.HasPrefix(existingPath, path+"/"):
			kind = memoryPathConflictAncestor
		}
		if kind != memoryPathConflictNone {
			result.Conflicts = append(result.Conflicts, activeMemoryPathConflict{MemoryID: memoryID, Path: existingPath, Kind: kind})
		}
	}
	if err := rows.Err(); err != nil {
		return activeMemoryPathConflicts{}, err
	}
	result.Truncated = result.Total > MaxMemoryPathConflicts
	return result, nil
}

func memoryPathConflictCreateMessage(conflict memoryPathConflict) string {
	switch conflict {
	case memoryPathConflictAncestor:
		return "memory path would contain an existing memory"
	case memoryPathConflictDescendant:
		return "memory path is inside an existing memory"
	default:
		return "memory path already exists"
	}
}

func memoryPathConflictRenameMessage(conflict memoryPathConflict) string {
	switch conflict {
	case memoryPathConflictAncestor:
		return "memory target path would contain an existing memory"
	case memoryPathConflictDescendant:
		return "memory target path is inside an existing memory"
	default:
		return "memory target path already exists"
	}
}

func updateMemoryContentTx(ctx context.Context, tx *dbconnect.Tx, scope *bridgev1.RuntimeScope, storeID string, current currentMemory, targetPath string, targetContent string, operation string) error {
	now := storage.Now()
	versionID := id.New("memver_")
	hash := sha256Hex(targetContent)
	size := int64(len([]byte(targetContent)))
	if err := memory.DurableWriteQuotas.EnforceContentVersion(ctx, tx, scope.GetWorkspaceId(), storeID, size); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx,
		`UPDATE memories
		    SET current_version_id = $1, path = $2, content_sha256 = $3, content_size_bytes = $4, updated_at = $5
		  WHERE workspace_id = $6 AND memory_store_id = $7 AND memory_id = $8 AND deleted_at IS NULL`,
		versionID, targetPath, hash, size, now, scope.GetWorkspaceId(), storeID, current.MemoryID); err != nil {
		return err
	}
	_, err := tx.Exec(ctx,
		`INSERT INTO memory_versions (
			workspace_id, memory_store_id, memory_id, memory_version_id, operation, path, content,
			content_sha256, content_size_bytes, created_at, created_actor_type, created_session_id
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, 'session_actor', $11)`,
		scope.GetWorkspaceId(), storeID, current.MemoryID, versionID, operation, targetPath, targetContent,
		hash, size, now, scope.GetSessionId())
	return err
}

func memoryToolPath(relative string) (string, string, error) {
	if err := validateMemoryToolRelativePath(relative); err != nil {
		result, marshalErr := marshalMemoryToolError("invalid_path", err.Error(), false)
		return "", result, marshalErr
	}
	return "/" + relative, "", nil
}

func validateMemoryToolRelativePath(path string) error {
	if path == "" {
		return errors.New("path is required")
	}
	if strings.HasPrefix(path, "/") {
		return errors.New("path must be relative")
	}
	if strings.HasPrefix(path, "mnt/memory") {
		return errors.New("path must not use runtime projection path")
	}
	if len(path) > 1023 {
		return errors.New("path must be at most 1023 bytes")
	}
	if !utf8.ValidString(path) {
		return errors.New("path must be valid UTF-8")
	}
	if !pathvalidation.IsNFC(path) {
		return errors.New("path must be NFC normalized")
	}
	for _, r := range path {
		if unicode.IsControl(r) || unicode.Is(unicode.Cf, r) {
			return errors.New("path must not contain control or format characters")
		}
	}
	for _, segment := range strings.Split(path, "/") {
		if segment == "" {
			return errors.New("path must not contain empty segments")
		}
		if segment == "." || segment == ".." {
			return errors.New("path must not contain . or .. segments")
		}
	}
	return nil
}

func marshalBridgeJSON(value any) (string, error) {
	body, err := json.Marshal(value)
	if err != nil {
		return "", err
	}
	return string(body), nil
}

func marshalBridgeDataJSON(value any) (string, error) {
	var body strings.Builder
	encoder := json.NewEncoder(&body)
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(value); err != nil {
		return "", err
	}
	return strings.TrimSuffix(body.String(), "\n"), nil
}

func marshalMemoryToolError(errorCode string, message string, rereadRequired bool) (string, error) {
	return marshalBridgeJSON(map[string]any{
		"status":               "tool_error",
		"error_code":           errorCode,
		"message":              message,
		"reread_required":      rereadRequired,
		"projection_refreshed": false,
	})
}

func marshalMemoryStaleToolError(errorCode string, message string) (string, error) {
	return marshalMemoryToolError(errorCode, message, true)
}

func marshalMemoryPathConflictToolError(message string, conflicts activeMemoryPathConflicts) (string, error) {
	return marshalBridgeJSON(map[string]any{
		"status":               "tool_error",
		"error_code":           "path_exists",
		"message":              message,
		"reread_required":      true,
		"projection_refreshed": false,
		"conflicts":            conflicts.Conflicts,
		"conflict_total":       conflicts.Total,
		"conflicts_truncated":  conflicts.Truncated,
	})
}

func marshalMemoryCompleted(action string, relativePath string) (string, error) {
	return marshalBridgeJSON(map[string]any{
		"status": "completed",
		"action": action,
		"path":   relativePath,
	})
}

func replaceCount(replaceAll bool) int {
	if replaceAll {
		return -1
	}
	return 1
}
