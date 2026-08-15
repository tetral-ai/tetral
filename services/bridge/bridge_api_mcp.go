package agentruntimebridge

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/binary"
	"encoding/json"
	"log/slog"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	bridgev1 "github.com/tetral-ai/tetral/services/bridge/gen/tetral/bridge/v1"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/id"
	"github.com/tetral-ai/tetral/internal/queue"
	"github.com/tetral-ai/tetral/internal/workspace"
)

// This file owns the Bridge mcp protocol-family boundary.

func (s *PostgreSQLBridgeAPIStore) McpManifestChanged(ctx context.Context, request *bridgev1.McpManifestChangedRequest) (*bridgev1.McpManifestChangedResponse, error) {
	if s == nil || s.Client == nil {
		return nil, status.Error(codes.FailedPrecondition, "bridge API store is unavailable")
	}
	if err := validateMCPManifestChangedRequest(request); err != nil {
		return nil, err
	}
	workspaceID := request.GetWorkspaceId()
	sessionID := request.GetSessionId()
	mcpServerName := request.GetMcpServerName()
	manifestETag := request.GetManifestEtag()
	if response, ok, err := s.replayedMCPManifestChanged(ctx, workspaceID, sessionID, mcpServerName, manifestETag); err != nil || ok {
		return response, err
	}
	if s.MCPManifestLister == nil {
		return nil, errMCPManifestListerUnavailable()
	}
	manifest, err := s.MCPManifestLister.ListMCPTools(ctx, MCPManifestListRequest{
		WorkspaceID:   workspaceID,
		SessionID:     sessionID,
		MCPServerName: mcpServerName,
		ManifestETag:  manifestETag,
	})
	if err != nil {
		return nil, err
	}
	if manifest.ManifestETag != manifestETag {
		return nil, status.Error(codes.FailedPrecondition, "mcp manifest etag changed during delivery")
	}
	var acceptance mcpManifestAcceptance
	err = s.Client.WithWorkspaceTx(ctx, workspaceID, "agentruntimebridge.mcp_manifest_changed", func(tx *dbconnect.Tx) error {
		var err error
		acceptance, err = captureMCPManifestAcceptanceTx(ctx, tx, workspaceID, sessionID, mcpServerName, manifestETag, manifest.Tools, s.now())
		return err
	})
	if err != nil {
		return nil, err
	}
	logMCPManifestTransitionCommitted(s.Logger, ServiceNameBridgeAPI, workspaceID, sessionID, mcpServerName, acceptance, false)
	if !acceptance.Duplicate {
		logMCPManifestOmissions(s.Logger, ServiceNameBridgeAPI, workspaceID, sessionID, mcpServerName, acceptance.BuiltinFamily, acceptance.Omissions)
	}
	if acceptance.Duplicate {
		return &bridgev1.McpManifestChangedResponse{Outcome: &bridgev1.McpManifestChangedResponse_Duplicate{Duplicate: &bridgev1.McpManifestDuplicate{}}}, nil
	}
	return &bridgev1.McpManifestChangedResponse{Outcome: &bridgev1.McpManifestChangedResponse_Committed{Committed: &bridgev1.McpManifestCommitted{}}}, nil
}

func (s *PostgreSQLBridgeAPIStore) ClaimMcpToolResult(ctx context.Context, request *bridgev1.ClaimMcpToolResultRequest) (*bridgev1.ClaimMcpToolResultResponse, error) {
	if err := validateMCPClaimTarget(request.GetScope(), request.GetToolUseEventId(), request.GetClaimId()); err != nil {
		return nil, err
	}
	now := s.now()
	var response *bridgev1.ClaimMcpToolResultResponse
	if err := s.withScopeTx(ctx, request.GetScope(), "agentruntimebridge.claim_mcp_tool_result", func(tx *dbconnect.Tx) error {
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
		tool, err := loadDurableToolExecutionTx(ctx, tx, request.GetScope(), request.GetToolUseEventId(), "agent.mcp_tool_use", true)
		if err != nil {
			return err
		}
		if tool.MCPServerName == "" {
			return status.Error(codes.FailedPrecondition, "durable tool is not an MCP operation")
		}
		if err := verifyApprovedToolExecutionHandoffTx(ctx, tx, request.GetScope(), request.GetToolUseEventId(), tool); err != nil {
			return err
		}
		response, err = claimMCPToolResultTx(ctx, tx, request, tool, now)
		return err
	}); err != nil {
		if isScopeSupersededError(err) {
			return &bridgev1.ClaimMcpToolResultResponse{Outcome: &bridgev1.ClaimMcpToolResultResponse_Stale{Stale: &bridgev1.McpToolClaimStale{}}}, nil
		}
		return nil, err
	}
	return response, nil
}

// RelinquishMcpToolResult releases one exact, known-not-to-have-committed MCP
// execution attempt. Ambiguous commit outcomes never use this operation: a
// stored/consumed result or a different active claim is returned as stale and
// remains authoritative.
func (s *PostgreSQLBridgeAPIStore) RelinquishMcpToolResult(ctx context.Context, request *bridgev1.RelinquishMcpToolResultRequest) (*bridgev1.RelinquishMcpToolResultResponse, error) {
	if err := validateMCPClaimTarget(request.GetScope(), request.GetToolUseEventId(), request.GetClaimId()); err != nil {
		return nil, err
	}
	declarationDigest, err := mcpToolRelinquishDeclarationDigest(request)
	if err != nil {
		return nil, err
	}
	var response *bridgev1.RelinquishMcpToolResultResponse
	err = s.withScopeTx(ctx, request.GetScope(), "agentruntimebridge.relinquish_mcp_tool_result", func(tx *dbconnect.Tx) error {
		if err := lockRuntimeMutationSessionTx(ctx, tx, request.GetScope().GetWorkspaceId(), request.GetScope().GetSessionId()); err != nil {
			return err
		}
		if err := verifyRuntimeDeclarationCaller(ctx, request.GetScope()); err != nil {
			return err
		}
		existingOperation, ok, err := readBridgeDeclarationOperationTx(
			ctx,
			tx,
			request.GetScope(),
			bridgeOpRelinquishMcpToolResult,
			"mcp_tool_execution",
			request.GetClaimId(),
		)
		if err != nil {
			return err
		}
		if ok {
			if existingOperation.DeclarationDigest != declarationDigest {
				return status.Error(codes.AlreadyExists, "mcp tool relinquish idempotency conflict")
			}
			response = duplicateMCPRelinquishResponse()
			return nil
		}
		if err := verifyRuntimeScopeTx(ctx, tx, request.GetScope()); err != nil {
			return err
		}
		tool, err := loadDurableToolExecutionTx(ctx, tx, request.GetScope(), request.GetToolUseEventId(), "agent.mcp_tool_use", true)
		if err != nil {
			return err
		}
		if tool.MCPServerName == "" {
			return status.Error(codes.FailedPrecondition, "durable tool is not an MCP operation")
		}
		existing, ok, err := readRuntimeToolResultTx(ctx, tx, request.GetScope(), request.GetToolUseEventId())
		if err != nil {
			return err
		}
		if !ok || !sameMCPToolResult(existing, tool) ||
			existing.MCPClaimStatus.String != mcpClaimStatusInFlight ||
			!existing.MCPClaimID.Valid || existing.MCPClaimID.String != request.GetClaimId() {
			response = staleMCPRelinquishResponse()
			return nil
		}
		result, err := tx.Exec(ctx,
			`DELETE FROM session_runtime_tool_results
			  WHERE workspace_id = $1
			    AND session_id = $2
			    AND session_thread_id = $3
			    AND tool_use_event_id = $4
			    AND tool_kind = 'mcp'
			    AND mcp_claim_status = 'in_flight'
			    AND mcp_claim_id = $5`,
			request.GetScope().GetWorkspaceId(),
			request.GetScope().GetSessionId(),
			request.GetScope().GetSessionThreadId(),
			request.GetToolUseEventId(),
			request.GetClaimId(),
		)
		if err != nil {
			return err
		}
		if !rowsAffected(result) {
			response = staleMCPRelinquishResponse()
			return nil
		}
		if err := insertBridgeDeclarationOperationTx(
			ctx,
			tx,
			request.GetScope(),
			bridgeOpRelinquishMcpToolResult,
			"mcp_tool_execution",
			request.GetClaimId(),
			declarationDigest,
			`{}`,
			s.now(),
		); err != nil {
			return err
		}
		response = &bridgev1.RelinquishMcpToolResultResponse{Outcome: &bridgev1.RelinquishMcpToolResultResponse_Relinquished{Relinquished: &bridgev1.McpToolRelinquishRelinquished{}}}
		return nil
	})
	if err != nil {
		if isScopeSupersededError(err) {
			return staleMCPRelinquishResponse(), nil
		}
		return nil, err
	}
	return response, nil
}

func (s *PostgreSQLBridgeAPIStore) CommitMcpToolResult(ctx context.Context, request *bridgev1.CommitMcpToolResultRequest) (*bridgev1.CommitMcpToolResultResponse, error) {
	if err := validateMCPClaimTarget(request.GetScope(), request.GetToolUseEventId(), request.GetClaimId()); err != nil {
		return nil, err
	}
	if err := validateMCPCommitPayload(request); err != nil {
		return nil, err
	}
	sourceID := request.GetClaimId()
	declarationDigest, err := mcpToolCommitDeclarationDigest(request)
	if err != nil {
		return nil, err
	}
	var response *bridgev1.CommitMcpToolResultResponse
	var tool durableToolExecution
	if err := s.withScopeTx(ctx, request.GetScope(), "agentruntimebridge.commit_mcp_tool_result_preflight", func(tx *dbconnect.Tx) error {
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
		var replayed bool
		response, replayed, err = replayMCPCommitTx(ctx, tx, request, sourceID, declarationDigest)
		if err != nil || replayed {
			return err
		}
		if err := verifyRuntimeScopeTx(ctx, tx, request.GetScope()); err != nil {
			return err
		}
		tool, err = loadDurableToolExecutionTx(ctx, tx, request.GetScope(), request.GetToolUseEventId(), "agent.mcp_tool_use", true)
		if err != nil {
			return err
		}
		if existing, ok, err := readRuntimeToolResultTx(ctx, tx, request.GetScope(), request.GetToolUseEventId()); err != nil {
			return err
		} else if ok {
			if !sameMCPToolResult(existing, tool) {
				return status.Error(codes.AlreadyExists, "mcp tool use id conflicts with existing result")
			}
			switch existing.MCPClaimStatus.String {
			case mcpClaimStatusStored, mcpClaimStatusConsumed:
				return status.Error(codes.FailedPrecondition, "mcp tool result commit operation is missing")
			case mcpClaimStatusInFlight:
				if !existing.MCPClaimID.Valid || existing.MCPClaimID.String != request.GetClaimId() {
					response = staleMCPCommitResponse()
				}
			default:
				return status.Error(codes.Internal, "invalid mcp claim state")
			}
			return nil
		}
		return status.Error(codes.FailedPrecondition, "mcp tool claim is missing")
	}); err != nil || response != nil {
		if isScopeSupersededError(err) {
			return staleMCPCommitResponse(), nil
		}
		return response, err
	}

	var attachment *bridgev1.TransientAttachmentRef
	var blobPointer string
	blobStored := false
	cleanupBlob := func() {
		if blobStored {
			_ = s.AttachmentBlobStore.Delete(context.WithoutCancel(ctx), blobPointer)
			blobStored = false
		}
	}
	if len(request.GetInlineMedia()) == 1 {
		if s == nil || s.AttachmentBlobStore == nil {
			return nil, status.Error(codes.FailedPrecondition, "transient attachment blob store is unavailable")
		}
		media := request.GetInlineMedia()[0]
		attachmentCreate := mcpTransientAttachmentCreate(request, tool, media)
		var err error
		attachment, err = newTransientAttachmentRef(attachmentCreate)
		if err != nil {
			return nil, status.Error(codes.Internal, "transient attachment ref generation failed")
		}
		blobPointer = transientAttachmentBlobPointer(request.GetScope(), attachment.GetAttachmentRef())
		if err := s.AttachmentBlobStore.Put(ctx, blobPointer, bytes.NewReader(media.GetData()), int64(len(media.GetData()))); err != nil {
			return nil, status.Error(codes.Unavailable, "transient attachment upload failed")
		}
		blobStored = true
	}
	refsOnlyResultJSON, err := completeMCPAttachmentRefs(request.GetResultJson(), request.GetInlineMedia(), attachment)
	if err != nil {
		cleanupBlob()
		if isScopeSupersededError(err) {
			return staleMCPCommitResponse(), nil
		}
		return nil, err
	}
	now := s.now()
	commitOutcomeUnknown := false
	err = s.withScopeTxAndCleanup(ctx, request.GetScope(), "agentruntimebridge.commit_mcp_tool_result", func(tx *dbconnect.Tx) error {
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
		var replayed bool
		response, replayed, err = replayMCPCommitTx(ctx, tx, request, sourceID, declarationDigest)
		if err != nil {
			return err
		}
		if replayed {
			// The deterministic blob pointer may already be referenced by the
			// committed replay. Never delete it after observing durable success.
			blobStored = false
			return nil
		}
		if err := verifyRuntimeScopeTx(ctx, tx, request.GetScope()); err != nil {
			return err
		}
		tool, err = loadDurableToolExecutionTx(ctx, tx, request.GetScope(), request.GetToolUseEventId(), "agent.mcp_tool_use", true)
		if err != nil {
			return err
		}
		existing, ok, err := readRuntimeToolResultTx(ctx, tx, request.GetScope(), request.GetToolUseEventId())
		if err != nil {
			return err
		}
		if !ok {
			return status.Error(codes.FailedPrecondition, "mcp tool claim is missing")
		}
		if !sameMCPToolResult(existing, tool) {
			return status.Error(codes.AlreadyExists, "mcp tool use id conflicts with existing result")
		}
		switch existing.MCPClaimStatus.String {
		case mcpClaimStatusStored, mcpClaimStatusConsumed:
			return status.Error(codes.FailedPrecondition, "mcp tool result commit operation is missing")
		case mcpClaimStatusInFlight:
			if !existing.MCPClaimID.Valid || existing.MCPClaimID.String != request.GetClaimId() {
				response = staleMCPCommitResponse()
				cleanupBlob()
				return nil
			}
		default:
			return status.Error(codes.Internal, "invalid mcp claim state")
		}
		if attachment != nil {
			if err := insertStagedTransientAttachmentTx(ctx, tx, mcpTransientAttachmentCreate(request, tool, request.GetInlineMedia()[0]), attachment, blobPointer, now); err != nil {
				return err
			}
		}
		if err := storeMCPToolResultTx(ctx, tx, request.GetScope(), request.GetToolUseEventId(), request.GetClaimId(), refsOnlyResultJSON, now); err != nil {
			return err
		}
		attachmentRef := ""
		if attachment != nil {
			attachmentRef = attachment.GetAttachmentRef()
		}
		commitResultJSON, err := marshalBridgeJSON(mcpToolCommitResult{AttachmentRef: &attachmentRef})
		if err != nil {
			return err
		}
		if err := insertBridgeDeclarationOperationTx(
			ctx,
			tx,
			request.GetScope(),
			bridgeOpCommitMcpToolResult,
			"mcp_tool_execution",
			sourceID,
			declarationDigest,
			commitResultJSON,
			now,
		); err != nil {
			return err
		}
		response = &bridgev1.CommitMcpToolResultResponse{Outcome: &bridgev1.CommitMcpToolResultResponse_Committed{Committed: &bridgev1.McpToolCommitCommitted{AttachmentRef: attachmentRef}}}
		return nil
	}, func() { commitOutcomeUnknown = true })
	if err != nil {
		if !commitOutcomeUnknown {
			cleanupBlob()
		}
		return nil, err
	}
	blobStored = false
	return response, nil
}

type mcpToolCommitResult struct {
	AttachmentRef *string `json:"attachment_ref"`
}

func replayMCPCommitTx(
	ctx context.Context,
	tx *dbconnect.Tx,
	request *bridgev1.CommitMcpToolResultRequest,
	sourceID string,
	declarationDigest string,
) (*bridgev1.CommitMcpToolResultResponse, bool, error) {
	existingOperation, ok, err := readBridgeDeclarationOperationTx(
		ctx,
		tx,
		request.GetScope(),
		bridgeOpCommitMcpToolResult,
		"mcp_tool_execution",
		sourceID,
	)
	if err != nil || !ok {
		return nil, false, err
	}
	if existingOperation.DeclarationDigest == "" {
		return nil, false, status.Error(codes.FailedPrecondition, "mcp tool commit operation is invalid")
	}
	if existingOperation.DeclarationDigest != declarationDigest {
		return nil, false, status.Error(codes.AlreadyExists, "mcp tool commit idempotency conflict")
	}
	var result mcpToolCommitResult
	if existingOperation.ReceiptJSON == "" || json.Unmarshal([]byte(existingOperation.ReceiptJSON), &result) != nil ||
		result.AttachmentRef == nil {
		return nil, false, status.Error(codes.FailedPrecondition, "mcp tool commit result is invalid")
	}
	return &bridgev1.CommitMcpToolResultResponse{
		Outcome: &bridgev1.CommitMcpToolResultResponse_Duplicate{Duplicate: &bridgev1.McpToolCommitDuplicate{AttachmentRef: *result.AttachmentRef}},
	}, true, nil
}

type storedMCPAttachment struct {
	AttachmentRef string
	Mime          string
	Filename      string
}

func storedMCPAttachmentMetadata(resultJSON string) ([]storedMCPAttachment, bool) {
	decoder := json.NewDecoder(strings.NewReader(resultJSON))
	decoder.UseNumber()
	var root map[string]any
	if err := decoder.Decode(&root); err != nil || root == nil {
		return nil, false
	}
	response, ok := root["response"].(map[string]any)
	if !ok {
		return nil, false
	}
	rawAttachments, ok := response["attachments"].([]any)
	if !ok {
		return nil, false
	}
	attachments := make([]storedMCPAttachment, 0, len(rawAttachments))
	for _, raw := range rawAttachments {
		metadata, ok := raw.(map[string]any)
		if !ok {
			return nil, false
		}
		attachmentRef, refOK := metadata["attachment_ref"].(string)
		mime, mimeOK := metadata["mime"].(string)
		filename, filenameOK := metadata["suggested_filename"].(string)
		size, sizeOK := metadata["size_bytes"].(json.Number)
		sizeBytes, sizeErr := size.Int64()
		if !refOK || attachmentRef == "" || !mimeOK || mime == "" ||
			!filenameOK || filename == "" || !sizeOK || sizeErr != nil || sizeBytes < 0 {
			return nil, false
		}
		attachments = append(attachments, storedMCPAttachment{
			AttachmentRef: attachmentRef,
			Mime:          mime,
			Filename:      filename,
		})
	}
	return attachments, true
}

func claimMCPToolResultTx(ctx context.Context, tx *dbconnect.Tx, request *bridgev1.ClaimMcpToolResultRequest, tool durableToolExecution, now time.Time) (*bridgev1.ClaimMcpToolResultResponse, error) {
	existing, ok, err := readRuntimeToolResultTx(ctx, tx, request.GetScope(), request.GetToolUseEventId())
	if err != nil {
		return nil, err
	}
	if ok {
		return claimExistingMCPToolResultTx(ctx, tx, request, tool, existing, now)
	}
	inserted, err := insertMCPToolResultClaimTx(ctx, tx, request, tool, now)
	if err != nil {
		return nil, err
	}
	if inserted {
		return acquiredMCPClaimResponse(tool), nil
	}
	existing, ok, err = readRuntimeToolResultTx(ctx, tx, request.GetScope(), request.GetToolUseEventId())
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, status.Error(codes.Internal, "mcp tool claim race was not stored")
	}
	return claimExistingMCPToolResultTx(ctx, tx, request, tool, existing, now)
}

func claimExistingMCPToolResultTx(ctx context.Context, tx *dbconnect.Tx, request *bridgev1.ClaimMcpToolResultRequest, tool durableToolExecution, existing runtimeToolResult, now time.Time) (*bridgev1.ClaimMcpToolResultResponse, error) {
	if !sameMCPToolResult(existing, tool) {
		return nil, status.Error(codes.AlreadyExists, "mcp tool use id conflicts with existing result")
	}
	switch existing.MCPClaimStatus.String {
	case mcpClaimStatusStored, mcpClaimStatusConsumed:
		if _, ok := storedMCPAttachmentMetadata(existing.ResultJSON); !ok {
			return nil, status.Error(codes.FailedPrecondition, "stored mcp tool result is invalid")
		}
		return &bridgev1.ClaimMcpToolResultResponse{Outcome: &bridgev1.ClaimMcpToolResultResponse_AlreadyCompleted{AlreadyCompleted: &bridgev1.McpToolAlreadyCompleted{ResultJson: existing.ResultJSON}}}, nil
	case mcpClaimStatusInFlight:
		if existing.MCPClaimID.Valid && existing.MCPClaimID.String == request.GetClaimId() {
			if err := renewMCPToolResultClaimTx(ctx, tx, request.GetScope(), request.GetToolUseEventId(), request.GetClaimId(), now); err != nil {
				return nil, err
			}
			return acquiredMCPClaimResponse(tool), nil
		}
		active, err := mcpClaimLeaseActive(existing, now)
		if err != nil {
			return nil, err
		}
		if active {
			return &bridgev1.ClaimMcpToolResultResponse{Outcome: &bridgev1.ClaimMcpToolResultResponse_InFlight{InFlight: &bridgev1.McpToolClaimInFlight{}}}, nil
		}
		if err := renewMCPToolResultClaimTx(ctx, tx, request.GetScope(), request.GetToolUseEventId(), request.GetClaimId(), now); err != nil {
			return nil, err
		}
		return acquiredMCPClaimResponse(tool), nil
	default:
		return nil, status.Error(codes.Internal, "invalid mcp claim state")
	}
}

func acquiredMCPClaimResponse(tool durableToolExecution) *bridgev1.ClaimMcpToolResultResponse {
	return &bridgev1.ClaimMcpToolResultResponse{Outcome: &bridgev1.ClaimMcpToolResultResponse_Acquired{Acquired: &bridgev1.McpToolClaimAcquired{
		McpServerName: tool.MCPServerName,
		ToolName:      tool.ToolName,
		InputJson:     tool.InputJSON,
	}}}
}

func insertMCPToolResultClaimTx(ctx context.Context, tx *dbconnect.Tx, request *bridgev1.ClaimMcpToolResultRequest, tool durableToolExecution, now time.Time) (bool, error) {
	result, err := tx.Exec(ctx,
		`INSERT INTO session_runtime_tool_results (
			workspace_id, session_id, session_thread_id, tool_use_event_id, tool_kind,
			normalized_input_hash, tool_name, input_json, ack_status, result_json,
			mcp_claim_status, mcp_claim_id, mcp_claim_lease_expires_at,
			created_at, updated_at
		) VALUES ($1, $2, $3, $4, 'mcp', $5, $6, $7, 'committed', '{}', 'in_flight', $8, $9, $10, $10)
		ON CONFLICT (workspace_id, session_id, session_thread_id, tool_use_event_id) DO NOTHING`,
		request.GetScope().GetWorkspaceId(),
		request.GetScope().GetSessionId(),
		request.GetScope().GetSessionThreadId(),
		request.GetToolUseEventId(),
		tool.NormalizedInputHash,
		mcpRuntimeToolName(tool.MCPServerName, tool.ToolName),
		tool.InputJSON,
		request.GetClaimId(),
		now.Add(mcpClaimLeaseTTL),
		now,
	)
	if err != nil {
		return false, err
	}
	return rowsAffected(result), nil
}

func renewMCPToolResultClaimTx(ctx context.Context, tx *dbconnect.Tx, scope *bridgev1.RuntimeScope, toolUseEventID string, claimID string, now time.Time) error {
	result, err := tx.Exec(ctx,
		`UPDATE session_runtime_tool_results
		    SET mcp_claim_id = $5,
		        mcp_claim_lease_expires_at = $6,
		        updated_at = $7
		  WHERE workspace_id = $1
		    AND session_id = $2
		    AND session_thread_id = $3
		    AND tool_use_event_id = $4
		    AND tool_kind = 'mcp'
		    AND mcp_claim_status = 'in_flight'`,
		scope.GetWorkspaceId(),
		scope.GetSessionId(),
		scope.GetSessionThreadId(),
		toolUseEventID,
		claimID,
		now.Add(mcpClaimLeaseTTL),
		now,
	)
	if err != nil {
		return err
	}
	if !rowsAffected(result) {
		return status.Error(codes.FailedPrecondition, "mcp tool claim renewal failed")
	}
	return nil
}

func storeMCPToolResultTx(ctx context.Context, tx *dbconnect.Tx, scope *bridgev1.RuntimeScope, toolUseEventID string, claimID string, resultJSON string, now time.Time) error {
	result, err := tx.Exec(ctx,
		`UPDATE session_runtime_tool_results
		    SET result_json = $5,
		        mcp_claim_status = 'stored',
		        mcp_claim_id = NULL,
		        mcp_claim_lease_expires_at = NULL,
		        updated_at = $6
		  WHERE workspace_id = $1
		    AND session_id = $2
		    AND session_thread_id = $3
		    AND tool_use_event_id = $4
		    AND tool_kind = 'mcp'
		    AND mcp_claim_status = 'in_flight'
		    AND mcp_claim_id = $7`,
		scope.GetWorkspaceId(),
		scope.GetSessionId(),
		scope.GetSessionThreadId(),
		toolUseEventID,
		resultJSON,
		now,
		claimID,
	)
	if err != nil {
		return err
	}
	if !rowsAffected(result) {
		return status.Error(codes.FailedPrecondition, "mcp tool result commit failed")
	}
	return nil
}

func mcpClaimLeaseActive(existing runtimeToolResult, now time.Time) (bool, error) {
	if !existing.MCPClaimLeaseExpiresAt.Valid {
		return false, status.Error(codes.Internal, "mcp claim lease is missing")
	}
	expiresAt, err := time.Parse(time.RFC3339Nano, existing.MCPClaimLeaseExpiresAt.String)
	if err != nil {
		return false, status.Error(codes.Internal, "invalid mcp claim lease")
	}
	return now.Before(expiresAt), nil
}

func validateMCPClaimTarget(scope *bridgev1.RuntimeScope, toolUseEventID string, claimID string) error {
	if err := validateRuntimeScope(scope); err != nil {
		return err
	}
	if toolUseEventID == "" || claimID == "" {
		return status.Error(codes.InvalidArgument, "invalid mcp tool result request")
	}
	return nil
}

func sameMCPToolResult(existing runtimeToolResult, tool durableToolExecution) bool {
	return existing.ToolKind == bridgeToolKindMCP &&
		existing.NormalizedInputHash == tool.NormalizedInputHash &&
		existing.ToolName == mcpRuntimeToolName(tool.MCPServerName, tool.ToolName) &&
		existing.InputJSON == tool.InputJSON
}

func staleMCPCommitResponse() *bridgev1.CommitMcpToolResultResponse {
	return &bridgev1.CommitMcpToolResultResponse{Outcome: &bridgev1.CommitMcpToolResultResponse_Stale{Stale: &bridgev1.McpToolCommitStale{}}}
}

func duplicateMCPRelinquishResponse() *bridgev1.RelinquishMcpToolResultResponse {
	return &bridgev1.RelinquishMcpToolResultResponse{Outcome: &bridgev1.RelinquishMcpToolResultResponse_Duplicate{Duplicate: &bridgev1.McpToolRelinquishDuplicate{}}}
}

func staleMCPRelinquishResponse() *bridgev1.RelinquishMcpToolResultResponse {
	return &bridgev1.RelinquishMcpToolResultResponse{Outcome: &bridgev1.RelinquishMcpToolResultResponse_Stale{Stale: &bridgev1.McpToolRelinquishStale{}}}
}

func mcpRuntimeToolName(mcpServerName string, toolName string) string {
	return mcpServerName + "/" + toolName
}

func (s *PostgreSQLBridgeAPIStore) replayedMCPManifestChanged(ctx context.Context, workspaceID string, sessionID string, mcpServerName string, manifestETag string) (*bridgev1.McpManifestChangedResponse, bool, error) {
	var response *bridgev1.McpManifestChangedResponse
	var restored mcpManifestAcceptance
	err := s.Client.WithWorkspaceTx(ctx, workspaceID, "agentruntimebridge.mcp_manifest_changed_replay", func(tx *dbconnect.Tx) error {
		if err := acquireMCPManifestAcceptanceLockTx(ctx, tx, workspaceID, sessionID, mcpServerName); err != nil {
			return err
		}
		if _, err := loadMainSessionThreadIDTx(ctx, tx, workspaceID, sessionID); err != nil {
			return err
		}
		row, found, err := loadMCPManifestRowForUpdateTx(ctx, tx, workspaceID, sessionID, mcpServerName)
		if err != nil {
			return err
		}
		if !found || !row.ManifestETag.Valid || row.ManifestETag.String != manifestETag {
			return nil
		}
		if row.Readiness == mcpManifestReadinessUnready {
			if !row.ToolsJSON.Valid {
				return nil
			}
			toolset, err := mcpManifestToolsetConfigTx(ctx, tx, workspaceID, sessionID, mcpServerName)
			if err != nil {
				return err
			}
			acceptance, err := commitMCPManifestReadyTx(ctx, tx, workspaceID, sessionID, mcpServerName, manifestETag, row.ToolsJSON.String, row.Generation+1, toolset, s.now())
			if err != nil {
				return err
			}
			acceptance.PreviousGeneration = row.Generation
			acceptance.Readiness = mcpManifestReadinessReady
			acceptance.QueueCustody = "created"
			acceptance.Transitioned = true
			restored = acceptance
			response = &bridgev1.McpManifestChangedResponse{Outcome: &bridgev1.McpManifestChangedResponse_Committed{Committed: &bridgev1.McpManifestCommitted{}}}
			return nil
		}
		response = &bridgev1.McpManifestChangedResponse{Outcome: &bridgev1.McpManifestChangedResponse_Duplicate{Duplicate: &bridgev1.McpManifestDuplicate{}}}
		return nil
	})
	if err != nil {
		return nil, false, err
	}
	logMCPManifestTransitionCommitted(s.Logger, ServiceNameBridgeAPI, workspaceID, sessionID, mcpServerName, restored, false)
	return response, response != nil, nil
}

func validateMCPManifestChangedRequest(request *bridgev1.McpManifestChangedRequest) error {
	if request.GetWorkspaceId() == "" || request.GetSessionId() == "" || request.GetMcpServerName() == "" || request.GetManifestEtag() == "" {
		return status.Error(codes.InvalidArgument, "invalid mcp manifest change request")
	}
	return nil
}

func validateMCPCommitPayload(request *bridgev1.CommitMcpToolResultRequest) error {
	if request.GetResultJson() == "" || !json.Valid([]byte(request.GetResultJson())) {
		return status.Error(codes.InvalidArgument, "mcp tool result must be JSON")
	}
	if len(request.GetInlineMedia()) > 1 {
		return status.Error(codes.InvalidArgument, "mcp tool result has too many attachments")
	}
	for _, media := range request.GetInlineMedia() {
		if media == nil || len(media.GetData()) == 0 || len(media.GetData()) > transientAttachmentMaxBytes {
			return status.Error(codes.InvalidArgument, "mcp attachment size is invalid")
		}
		if !validTransientAttachmentMime(media.GetMime()) {
			return status.Error(codes.InvalidArgument, "mcp attachment mime is not supported")
		}
		if media.GetSuggestedFilename() == "" || len(media.GetSuggestedFilename()) > 1024 || !utf8.ValidString(media.GetSuggestedFilename()) {
			return status.Error(codes.InvalidArgument, "mcp attachment filename is invalid")
		}
	}
	_, err := completeMCPAttachmentRefs(request.GetResultJson(), request.GetInlineMedia(), nil)
	return err
}

func mcpTransientAttachmentCreate(request *bridgev1.CommitMcpToolResultRequest, tool durableToolExecution, media *bridgev1.McpInlineMedia) transientAttachmentCreate {
	return transientAttachmentCreate{
		Scope:                request.GetScope(),
		SourceToolUseEventID: request.GetToolUseEventId(),
		Mime:                 media.GetMime(),
		Filename:             media.GetSuggestedFilename(),
		SourcePath:           "mcp:" + tool.MCPServerName + "/" + media.GetSuggestedFilename(),
		Detail:               "auto",
		Data:                 media.GetData(),
	}
}

func completeMCPAttachmentRefs(resultJSON string, media []*bridgev1.McpInlineMedia, attachment *bridgev1.TransientAttachmentRef) (string, error) {
	decoder := json.NewDecoder(strings.NewReader(resultJSON))
	decoder.UseNumber()
	var root map[string]any
	if err := decoder.Decode(&root); err != nil || root == nil {
		return "", status.Error(codes.InvalidArgument, "mcp tool result must be a JSON object")
	}
	response, ok := root["response"].(map[string]any)
	if !ok {
		return "", status.Error(codes.InvalidArgument, "mcp tool result response is missing")
	}
	rawAttachments, ok := response["attachments"].([]any)
	if !ok || len(rawAttachments) != len(media) {
		return "", status.Error(codes.InvalidArgument, "mcp attachment metadata does not match inline media")
	}
	for index, raw := range rawAttachments {
		metadata, ok := raw.(map[string]any)
		if !ok {
			return "", status.Error(codes.InvalidArgument, "mcp attachment metadata is invalid")
		}
		if _, present := metadata["data_base64"]; present {
			return "", status.Error(codes.InvalidArgument, "mcp attachment bytes must use the inline media leg")
		}
		if ref, present := metadata["attachment_ref"]; present && ref != "" {
			return "", status.Error(codes.InvalidArgument, "mcp attachment ref must be assigned by Bridge")
		}
		inline := media[index]
		mime, mimeOK := metadata["mime"].(string)
		filename, filenameOK := metadata["suggested_filename"].(string)
		size, sizeOK := metadata["size_bytes"].(json.Number)
		sizeBytes, sizeErr := size.Int64()
		if !mimeOK || !filenameOK || !sizeOK || sizeErr != nil || mime != inline.GetMime() || filename != inline.GetSuggestedFilename() || sizeBytes != int64(len(inline.GetData())) {
			return "", status.Error(codes.InvalidArgument, "mcp attachment metadata does not match inline media")
		}
		if attachment != nil {
			metadata["attachment_ref"] = attachment.GetAttachmentRef()
		}
	}
	if attachment == nil {
		return resultJSON, nil
	}
	completed, err := json.Marshal(root)
	if err != nil {
		return "", status.Error(codes.Internal, "mcp refs-only result encoding failed")
	}
	return string(completed), nil
}

func loadMainSessionThreadIDTx(ctx context.Context, tx *dbconnect.Tx, workspaceID string, sessionID string) (string, error) {
	row := tx.QueryRow(ctx,
		`SELECT id
		   FROM session_threads
		  WHERE workspace_id = $1
		    AND session_id = $2
		    AND role = 'main'
		  FOR UPDATE`,
		workspaceID,
		sessionID,
	)
	var sessionThreadID string
	if err := row.Scan(&sessionThreadID); dbconnect.IsNoRows(err) {
		return "", status.Error(codes.FailedPrecondition, "session main thread is unavailable")
	} else if err != nil {
		return "", err
	}
	return sessionThreadID, nil
}

func bridgeSessionScope(workspaceID string, sessionID string, sessionThreadID string) *bridgev1.RuntimeScope {
	return &bridgev1.RuntimeScope{
		WorkspaceId:     workspaceID,
		SessionId:       sessionID,
		SessionThreadId: sessionThreadID,
	}
}

// runtimeMCPManifestUpdatePayload builds the manifest patch that rides a
// runtime_config_update job. That job kind is shared, but the MCP manifest path
// must NOT reuse config_generation: config_generation is owned exclusively by
// api session admission, and a GitHub-driven manifest change never passes
// through api. The payload therefore carries manifest_generation only, and
// Runtime applies the patch independently of config_generation, gated solely on
// manifest_generation monotonicity. Incrementing config_generation on this path
// would collapse the two-generation separation.
func runtimeMCPManifestUpdatePayload(workspaceID string, sessionID string, mcpServerName string, manifestGeneration int64) (string, error) {
	payload, err := json.Marshal(map[string]any{
		"workspace_id":        workspaceID,
		"session_id":          sessionID,
		"mcp_server_name":     mcpServerName,
		"manifest_generation": manifestGeneration,
	})
	if err != nil {
		return "", err
	}
	return string(payload), nil
}

func runtimeMCPManifestCommandPayload(workspaceID string, sessionID string, mcpServerName string, row mcpManifestRow) (string, error) {
	manifest := map[string]any{
		"mcp_server_name":     mcpServerName,
		"manifest_generation": row.Generation,
		"readiness":           row.Readiness,
		"diagnostic":          nil,
	}
	if row.Readiness == mcpManifestReadinessReady {
		if !row.ToolsJSON.Valid || !row.ManifestETag.Valid || !json.Valid([]byte(row.ToolsJSON.String)) {
			return "", status.Error(codes.Internal, "mcp manifest ready content is invalid")
		}
		manifest["manifest_etag"] = row.ManifestETag.String
		manifest["tools"] = json.RawMessage(row.ToolsJSON.String)
	} else {
		if !row.Diagnostic.Valid || row.Diagnostic.String == "" {
			return "", status.Error(codes.Internal, "mcp manifest unready diagnostic is invalid")
		}
		manifest["diagnostic"] = row.Diagnostic.String
	}
	payload, err := marshalBridgeDataJSON(map[string]any{
		"workspace_id": workspaceID,
		"session_id":   sessionID,
		"mcp_manifest": manifest,
	})
	if err != nil {
		return "", err
	}
	return payload, nil
}

type mcpManifestAcceptance struct {
	RuntimeInputID     string
	PreviousGeneration int64
	Generation         int64
	Readiness          string
	Diagnostic         string
	QueueCustody       string
	BuiltinFamily      string
	Omissions          []mcpManifestOmission
	Duplicate          bool
	Transitioned       bool
}

// session_mcp_manifests carries readiness and diagnostic ORTHOGONALLY to the
// accepted content (tools_json, manifest_etag):
//
//	readiness   meaning                                writer
//	ready       tools_json/manifest_etag are the       commitMCPManifestReadyTx
//	            latest accepted, within-cap manifest
//	unready     server's toolset is closed; diagnostic transitionMCPManifestUnreadyTx
//	            (manifest_too_large | delivery_exhausted)  (content columns untouched)
//
// Rules the transition helpers enforce:
//   - manifest_generation increments on EVERY accepted content change AND every
//     readiness transition; a repeated same-class failure report is an
//     increment-free no-op.
//   - Supersession keys SOLELY on generation monotonicity, never on etag (a
//     flapping A->B->A etag must not clobber newer state).
//   - An over-cap or delivery-exhausted report flips to unready WITHOUT touching
//     the accepted content columns.
//   - A re-notify matching the STORED etag while the row is unready RESTORES ready
//     (generation+1, diagnostic cleared) rather than short-circuiting as a
//     duplicate no-op — otherwise a server reverting to its last-accepted manifest
//     would stay unready forever.
const (
	mcpManifestReadinessReady              = "ready"
	mcpManifestReadinessUnready            = "unready"
	mcpManifestDiagnosticTooLarge          = "manifest_too_large"
	mcpManifestDiagnosticDeliveryExhausted = "delivery_exhausted"
	//nolint:gosec // This is a public readiness diagnostic token, not credential material.
	mcpManifestDiagnosticCredentialUnavailable = "credential_unavailable"
	mcpManifestDiagnosticDiscoveryUnavailable  = "discovery_unavailable"
	mcpManifestDiagnosticInvalid               = "manifest_invalid"
	runtimeMCPManifestDeliveryMaxAttempts      = 5
)

type mcpManifestRow struct {
	ToolsJSON    sql.NullString
	ManifestETag sql.NullString
	Generation   int64
	Readiness    string
	Diagnostic   sql.NullString
}

func loadMCPManifestRowForUpdateTx(ctx context.Context, tx *dbconnect.Tx, workspaceID string, sessionID string, mcpServerName string) (mcpManifestRow, bool, error) {
	var row mcpManifestRow
	err := tx.QueryRow(ctx, `SELECT tools_json, manifest_etag, manifest_generation, readiness, diagnostic
		FROM session_mcp_manifests
		WHERE workspace_id = $1 AND session_id = $2 AND mcp_server_name = $3
		FOR UPDATE`, workspaceID, sessionID, mcpServerName).Scan(
		&row.ToolsJSON, &row.ManifestETag, &row.Generation, &row.Readiness, &row.Diagnostic,
	)
	if dbconnect.IsNoRows(err) {
		return mcpManifestRow{}, false, nil
	}
	return row, err == nil, err
}

func captureMCPManifestAcceptanceTx(
	ctx context.Context,
	tx *dbconnect.Tx,
	workspaceID string,
	sessionID string,
	mcpServerName string,
	manifestETag string,
	tools []MCPManifestTool,
	now time.Time,
) (mcpManifestAcceptance, error) {
	if err := acquireMCPManifestAcceptanceLockTx(ctx, tx, workspaceID, sessionID, mcpServerName); err != nil {
		return mcpManifestAcceptance{}, err
	}
	if _, err := loadMainSessionThreadIDTx(ctx, tx, workspaceID, sessionID); err != nil {
		return mcpManifestAcceptance{}, err
	}
	toolsetConfig, err := mcpManifestToolsetConfigTx(ctx, tx, workspaceID, sessionID, mcpServerName)
	if err != nil {
		return mcpManifestAcceptance{}, err
	}
	filteredTools, omissions := filterMCPManifestCollisions(toolsetConfig.BuiltinFamily, tools)
	toolsJSON, err := canonicalMCPManifestToolsJSON(filteredTools)
	if err != nil {
		return mcpManifestAcceptance{}, err
	}
	current, rowExists, err := loadMCPManifestRowForUpdateTx(ctx, tx, workspaceID, sessionID, mcpServerName)
	if err != nil {
		return mcpManifestAcceptance{}, err
	}
	if len([]byte(toolsJSON)) > MaxMcpManifestBytes {
		transitioned := !rowExists || current.Readiness != mcpManifestReadinessUnready || !current.Diagnostic.Valid || current.Diagnostic.String != mcpManifestDiagnosticTooLarge
		generation, err := transitionMCPManifestUnreadyTx(ctx, tx, workspaceID, sessionID, mcpServerName, current, rowExists, mcpManifestDiagnosticTooLarge, toolsetConfig, now)
		return mcpManifestAcceptance{
			PreviousGeneration: current.Generation,
			Generation:         generation,
			Readiness:          mcpManifestReadinessUnready,
			Diagnostic:         mcpManifestDiagnosticTooLarge,
			QueueCustody:       "created",
			BuiltinFamily:      toolsetConfig.BuiltinFamily,
			Duplicate:          !transitioned,
			Transitioned:       transitioned,
		}, err
	}
	if rowExists && current.ManifestETag.Valid && current.ManifestETag.String == manifestETag && current.Readiness == mcpManifestReadinessReady {
		return mcpManifestAcceptance{
			RuntimeInputID: runtimeMCPManifestInputID(sessionID, mcpServerName, current.Generation),
			Generation:     current.Generation, BuiltinFamily: toolsetConfig.BuiltinFamily, Duplicate: true,
		}, nil
	}
	generation := int64(1)
	if rowExists {
		generation = current.Generation + 1
	}
	acceptance, err := commitMCPManifestReadyTx(ctx, tx, workspaceID, sessionID, mcpServerName, manifestETag, toolsJSON, generation, toolsetConfig, now)
	acceptance.PreviousGeneration = current.Generation
	acceptance.Readiness = mcpManifestReadinessReady
	acceptance.QueueCustody = "created"
	acceptance.Transitioned = true
	acceptance.BuiltinFamily = toolsetConfig.BuiltinFamily
	acceptance.Omissions = omissions
	return acceptance, err
}

func commitMCPManifestReadyTx(ctx context.Context, tx *dbconnect.Tx, workspaceID string, sessionID string, mcpServerName string, manifestETag string, toolsJSON string, generation int64, toolsetConfig MCPManifestToolsetConfig, now time.Time) (mcpManifestAcceptance, error) {
	sessionThreadID, err := loadMainSessionThreadIDTx(ctx, tx, workspaceID, sessionID)
	if err != nil {
		return mcpManifestAcceptance{}, err
	}
	payloadJSON, err := runtimeMCPManifestUpdatePayload(workspaceID, sessionID, mcpServerName, generation)
	if err != nil {
		return mcpManifestAcceptance{}, err
	}
	formattedNow := now
	_, err = tx.Exec(ctx, `INSERT INTO session_mcp_manifests (
		workspace_id, session_id, mcp_server_name, tools_json, manifest_etag,
		manifest_generation, readiness, diagnostic, created_at, updated_at
	) VALUES ($1, $2, $3, $4, $5, $6, 'ready', NULL, $7, $7)
	ON CONFLICT (workspace_id, session_id, mcp_server_name) DO UPDATE SET
		tools_json = EXCLUDED.tools_json, manifest_etag = EXCLUDED.manifest_etag,
		manifest_generation = EXCLUDED.manifest_generation, readiness = 'ready',
		diagnostic = NULL, updated_at = EXCLUDED.updated_at`,
		workspaceID, sessionID, mcpServerName, toolsJSON, manifestETag, generation, formattedNow)
	if err != nil {
		return mcpManifestAcceptance{}, err
	}
	if err := enqueueRuntimeMCPManifestUpdateTx(ctx, tx, workspaceID, sessionID, mcpServerName, generation, payloadJSON, now); err != nil {
		return mcpManifestAcceptance{}, err
	}
	runtimeInputID := runtimeMCPManifestInputID(sessionID, mcpServerName, generation)
	if err := insertBridgeOperationTx(ctx, tx, bridgeSessionScope(workspaceID, sessionID, sessionThreadID), bridgeOperationInsert{
		Operation:      bridgeOpMcpManifestChanged,
		IdempotencyKey: mcpServerName + ":" + strconv.FormatInt(generation, 10),
		RequestHash:    bridgeRequestHash(bridgeOpMcpManifestChanged, workspaceID, sessionID, mcpServerName, manifestETag, strconv.FormatInt(generation, 10)),
		AckStatus:      bridgeAckCommitted,
		RuntimeInputID: sql.NullString{String: runtimeInputID, Valid: true},
		Now:            now,
	}); err != nil {
		return mcpManifestAcceptance{}, err
	}
	return mcpManifestAcceptance{
		RuntimeInputID: runtimeInputID, Generation: generation,
	}, nil
}

func transitionMCPManifestUnreadyTx(ctx context.Context, tx *dbconnect.Tx, workspaceID string, sessionID string, mcpServerName string, current mcpManifestRow, rowExists bool, diagnostic string, toolsetConfig MCPManifestToolsetConfig, now time.Time) (int64, error) {
	return transitionMCPManifestUnreadyWithDeliveryTx(
		ctx, tx, workspaceID, sessionID, mcpServerName, current, rowExists, diagnostic, toolsetConfig, now, true,
	)
}

func transitionMCPManifestDeliveryExhaustedTx(ctx context.Context, tx *dbconnect.Tx, workspaceID string, sessionID string, mcpServerName string, current mcpManifestRow, toolsetConfig MCPManifestToolsetConfig, now time.Time) (int64, error) {
	// The leased job that exhausted delivery remains the queue barrier and
	// carries the new unready generation. A replacement job would move the
	// barrier behind later Session input.
	return transitionMCPManifestUnreadyWithDeliveryTx(
		ctx, tx, workspaceID, sessionID, mcpServerName, current, true, mcpManifestDiagnosticDeliveryExhausted, toolsetConfig, now, false,
	)
}

func transitionMCPManifestUnreadyWithDeliveryTx(ctx context.Context, tx *dbconnect.Tx, workspaceID string, sessionID string, mcpServerName string, current mcpManifestRow, rowExists bool, diagnostic string, toolsetConfig MCPManifestToolsetConfig, now time.Time, enqueueDelivery bool) (int64, error) {
	if rowExists && current.Readiness == mcpManifestReadinessUnready && current.Diagnostic.Valid && current.Diagnostic.String == diagnostic {
		return current.Generation, nil
	}
	generation := int64(1)
	if rowExists {
		generation = current.Generation + 1
	}
	row := current
	row.Generation = generation
	row.Readiness = mcpManifestReadinessUnready
	row.Diagnostic = sql.NullString{String: diagnostic, Valid: true}
	formattedNow := now
	if !rowExists {
		_, err := tx.Exec(ctx, `INSERT INTO session_mcp_manifests
			(workspace_id, session_id, mcp_server_name, tools_json, manifest_etag, manifest_generation, readiness, diagnostic, created_at, updated_at)
			VALUES ($1, $2, $3, NULL, NULL, $4, 'unready', $5, $6, $6)`, workspaceID, sessionID, mcpServerName, generation, diagnostic, formattedNow)
		if err != nil {
			return 0, err
		}
	} else {
		_, err := tx.Exec(ctx, `UPDATE session_mcp_manifests
			SET manifest_generation = $4, readiness = 'unready', diagnostic = $5, updated_at = $6
			WHERE workspace_id = $1 AND session_id = $2 AND mcp_server_name = $3`, workspaceID, sessionID, mcpServerName, generation, diagnostic, formattedNow)
		if err != nil {
			return 0, err
		}
	}
	if enqueueDelivery {
		payloadJSON, err := runtimeMCPManifestUpdatePayload(workspaceID, sessionID, mcpServerName, generation)
		if err != nil {
			return 0, err
		}
		if err := enqueueRuntimeMCPManifestUpdateTx(ctx, tx, workspaceID, sessionID, mcpServerName, generation, payloadJSON, now); err != nil {
			return 0, err
		}
	}
	return generation, nil
}

func acquireMCPManifestAcceptanceLockTx(ctx context.Context, tx *dbconnect.Tx, workspaceID string, sessionID string, mcpServerName string) error {
	sum := sha256.Sum256([]byte(workspaceID + "\x00" + sessionID + "\x00" + mcpServerName))
	resource := int32(binary.BigEndian.Uint32(sum[:4]))
	_, err := tx.Exec(ctx,
		"SELECT pg_advisory_xact_lock($1, $2)",
		mcpManifestAcceptanceLockCategory,
		resource,
	)
	return err
}

type mcpManifestOmission struct {
	ToolName string
}

func filterMCPManifestCollisions(family string, tools []MCPManifestTool) ([]MCPManifestTool, []mcpManifestOmission) {
	blocked := make(map[string]struct{}, 6)
	switch family {
	case "claude":
		for _, name := range []string{"Bash", "Read", "Write", "Edit", "Glob", "Grep"} {
			blocked[name] = struct{}{}
		}
	case "gpt":
		for _, name := range []string{"exec_command", "write_stdin", "view_image", "apply_patch"} {
			blocked[name] = struct{}{}
		}
	}
	filtered := make([]MCPManifestTool, 0, len(tools))
	omissions := make([]mcpManifestOmission, 0)
	for _, tool := range tools {
		if _, collision := blocked[tool.Name]; collision {
			omissions = append(omissions, mcpManifestOmission{ToolName: tool.Name})
			continue
		}
		filtered = append(filtered, tool)
	}
	return filtered, omissions
}

func logMCPManifestOmissions(logger *slog.Logger, component string, workspaceID string, sessionID string, mcpServerName string, family string, omissions []mcpManifestOmission) {
	if logger == nil {
		return
	}
	defer func() { _ = recover() }()
	for _, omission := range omissions {
		logger.Warn("bridge.mcp_manifest.tool_omitted",
			slog.String("operation", "mcp_manifest.filter"),
			slog.String("event.kind", "mcp_manifest.tool_omitted"),
			slog.String("component", component),
			slog.String("workspace.id", workspaceID),
			slog.String("session.id", sessionID),
			slog.String("mcp.server.name", mcpServerName),
			slog.String("mcp.tool.name", omission.ToolName),
			slog.String("mcp.tool.family", family),
			slog.String("mcp.omission.reason", "builtin_name_collision"),
		)
	}
}

// The transaction result is the sole source of this event. Keeping the logger
// at the post-commit boundary prevents telemetry from becoming manifest state
// or Queue custody evidence.
func logMCPManifestTransitionCommitted(logger *slog.Logger, component string, workspaceID string, sessionID string, mcpServerName string, acceptance mcpManifestAcceptance, inputContinued bool) {
	if logger == nil || !acceptance.Transitioned {
		return
	}
	defer func() {
		_ = recover()
	}()
	logger.Info("bridge.mcp_manifest.transition_committed",
		slog.String("operation", "mcp_manifest.transition"),
		slog.String("event.kind", "mcp_manifest_transition_committed"),
		slog.String("component", component),
		slog.String("workspace.id", workspaceID),
		slog.String("session.id", sessionID),
		slog.String("mcp.server.name", mcpServerName),
		slog.Int64("mcp.manifest.previous_generation", acceptance.PreviousGeneration),
		slog.Int64("mcp.manifest.generation", acceptance.Generation),
		slog.String("mcp.manifest.readiness", acceptance.Readiness),
		slog.String("mcp.manifest.diagnostic", acceptance.Diagnostic),
		slog.String("queue.custody", acceptance.QueueCustody),
		slog.Bool("runtime.input.continued", inputContinued),
	)
}

func enqueueRuntimeMCPManifestUpdateTx(ctx context.Context, tx *dbconnect.Tx, workspaceID string, sessionID string, mcpServerName string, manifestGeneration int64, payloadJSON string, now time.Time) error {
	ws := workspace.ID(workspaceID)
	_, err := queue.EnqueueTx(ctx, tx, queue.EnqueueRequest{
		ID:             id.New("qjob_"),
		WorkspaceID:    ws,
		Kind:           queue.KindRuntimeConfigUpdate,
		PartitionKey:   queue.FormatSessionPartitionKey(ws, sessionID),
		DedupeKey:      queue.FormatRuntimeMCPManifestUpdateDedupeKey(ws, sessionID, mcpServerName, strconv.FormatInt(manifestGeneration, 10)),
		PayloadVersion: 2,
		PayloadJSON:    []byte(payloadJSON),
		MaxAttempts:    runtimeMCPManifestDeliveryMaxAttempts,
		Now:            now,
	})
	return err
}
