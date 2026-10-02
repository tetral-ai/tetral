package agentruntimebridge

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/tetral-ai/tetral/internal/mcpmanifest"

	"github.com/tetral-ai/tetral/internal/runtimecontrol"

	bridgev1 "github.com/tetral-ai/tetral/services/bridge/gen/tetral/bridge/v1"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/tetral-ai/tetral/internal/dbconnect"
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
		return nil, mcpmanifest.ListerUnavailableError()
	}
	manifest, err := s.MCPManifestLister.ListMCPTools(ctx, mcpmanifest.ListRequest{
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
	var acceptance mcpmanifest.Acceptance
	err = s.Client.WithWorkspaceTx(ctx, workspaceID, "agentruntimebridge.mcp_manifest_changed", func(tx *dbconnect.Tx) error {
		var err error
		acceptance, err = mcpmanifest.CaptureAcceptanceTx(ctx, tx, workspaceID, sessionID, mcpServerName, manifestETag, manifest.Tools, s.now())
		return err
	})
	if err != nil {
		return nil, err
	}
	mcpmanifest.LogTransitionCommitted(s.Logger, ServiceNameBridgeAPI, workspaceID, sessionID, mcpServerName, acceptance, false)
	if !acceptance.Duplicate {
		mcpmanifest.LogOmissions(s.Logger, ServiceNameBridgeAPI, workspaceID, sessionID, mcpServerName, acceptance.BuiltinFamily, acceptance.Omissions)
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
		if err := runtimecontrol.LockRuntimeMutationSessionTx(
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
		if err := verifyRuntimeReceiptScopeTx(ctx, tx, request.GetScope()); err != nil {
			return err
		}
		tool, err := runtimecontrol.LoadDurableToolExecutionTx(ctx, tx, request.GetScope(), request.GetToolUseEventId(), "agent.mcp_tool_use", true)
		if err != nil {
			return err
		}
		if tool.MCPServerName == "" {
			return status.Error(codes.FailedPrecondition, "durable tool is not an MCP operation")
		}
		if existing, ok, err := readRuntimeToolResultTx(ctx, tx, request.GetScope(), request.GetToolUseEventId()); err != nil {
			return err
		} else if ok {
			response, err = claimExistingMCPToolResultTx(ctx, tx, request, tool, existing, now)
			return err
		}
		if err := requireRuntimeProcessCurrentTx(ctx, tx, request.GetScope()); err != nil {
			return err
		}
		if err := lockExecutableToolRouteTx(ctx, tx, request.GetScope(), request.GetToolUseEventId(), "mcp_execute"); err != nil {
			return err
		}
		response, err = claimMCPToolResultTx(ctx, tx, request, tool, now)
		return err
	}); err != nil {
		if runtimecontrol.IsScopeSupersededError(err) {
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
		if err := runtimecontrol.LockRuntimeMutationSessionTx(ctx, tx, request.GetScope().GetWorkspaceId(), request.GetScope().GetSessionId()); err != nil {
			return err
		}
		if err := verifyRuntimeDeclarationCaller(ctx, request.GetScope()); err != nil {
			return err
		}
		if err := verifyRuntimeReceiptScopeTx(ctx, tx, request.GetScope()); err != nil {
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
		if err := requireRuntimeProcessCurrentTx(ctx, tx, request.GetScope()); err != nil {
			return err
		}
		tool, err := runtimecontrol.LoadDurableToolExecutionTx(ctx, tx, request.GetScope(), request.GetToolUseEventId(), "agent.mcp_tool_use", true)
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
		if !runtimecontrol.RowsAffected(result) {
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
		if runtimecontrol.IsScopeSupersededError(err) {
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
	var tool runtimecontrol.DurableToolExecution
	if err := s.withScopeTx(ctx, request.GetScope(), "agentruntimebridge.commit_mcp_tool_result_preflight", func(tx *dbconnect.Tx) error {
		if err := runtimecontrol.LockRuntimeMutationSessionTx(
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
		if err := verifyRuntimeReceiptScopeTx(ctx, tx, request.GetScope()); err != nil {
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
		tool, err = runtimecontrol.LoadDurableToolExecutionTx(ctx, tx, request.GetScope(), request.GetToolUseEventId(), "agent.mcp_tool_use", true)
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
		if runtimecontrol.IsScopeSupersededError(err) {
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
		if runtimecontrol.IsScopeSupersededError(err) {
			return staleMCPCommitResponse(), nil
		}
		return nil, err
	}
	now := s.now()
	commitOutcomeUnknown := false
	cleanupReplayedBlob := false
	err = s.withScopeTxAndCleanup(ctx, request.GetScope(), "agentruntimebridge.commit_mcp_tool_result", func(tx *dbconnect.Tx) error {
		if err := runtimecontrol.LockRuntimeMutationSessionTx(
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
		if err := verifyRuntimeReceiptScopeTx(ctx, tx, request.GetScope()); err != nil {
			return err
		}
		var replayed bool
		response, replayed, err = replayMCPCommitTx(ctx, tx, request, sourceID, declarationDigest)
		if err != nil {
			return err
		}
		if replayed {
			// This invocation generated its attachment after preflight. A concurrent
			// commit won the durable identity, so only the attachment named by the
			// replay response remains authoritative.
			cleanupReplayedBlob = attachment != nil &&
				response.GetDuplicate().GetAttachmentRef() != attachment.GetAttachmentRef()
			return nil
		}
		if err := verifyRuntimeScopeTx(ctx, tx, request.GetScope()); err != nil {
			return err
		}
		tool, err = runtimecontrol.LoadDurableToolExecutionTx(ctx, tx, request.GetScope(), request.GetToolUseEventId(), "agent.mcp_tool_use", true)
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
		commitResultJSON, err := runtimecontrol.MarshalJSON(mcpToolCommitResult{AttachmentRef: &attachmentRef})
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
	if cleanupReplayedBlob {
		cleanupBlob()
		return response, nil
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

func claimMCPToolResultTx(ctx context.Context, tx *dbconnect.Tx, request *bridgev1.ClaimMcpToolResultRequest, tool runtimecontrol.DurableToolExecution, now time.Time) (*bridgev1.ClaimMcpToolResultResponse, error) {
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

func claimExistingMCPToolResultTx(ctx context.Context, tx *dbconnect.Tx, request *bridgev1.ClaimMcpToolResultRequest, tool runtimecontrol.DurableToolExecution, existing runtimeToolResult, now time.Time) (*bridgev1.ClaimMcpToolResultResponse, error) {
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
		if err := requireRuntimeProcessCurrentTx(ctx, tx, request.GetScope()); err != nil {
			return nil, err
		}
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
		if err := lockExecutableToolRouteTx(ctx, tx, request.GetScope(), request.GetToolUseEventId(), "mcp_execute"); err != nil {
			if status.Code(err) == codes.FailedPrecondition {
				return &bridgev1.ClaimMcpToolResultResponse{Outcome: &bridgev1.ClaimMcpToolResultResponse_Stale{Stale: &bridgev1.McpToolClaimStale{}}}, nil
			}
			return nil, err
		}
		if err := renewMCPToolResultClaimTx(ctx, tx, request.GetScope(), request.GetToolUseEventId(), request.GetClaimId(), now); err != nil {
			return nil, err
		}
		return acquiredMCPClaimResponse(tool), nil
	default:
		return nil, status.Error(codes.Internal, "invalid mcp claim state")
	}
}

func acquiredMCPClaimResponse(tool runtimecontrol.DurableToolExecution) *bridgev1.ClaimMcpToolResultResponse {
	return &bridgev1.ClaimMcpToolResultResponse{Outcome: &bridgev1.ClaimMcpToolResultResponse_Acquired{Acquired: &bridgev1.McpToolClaimAcquired{
		McpServerName: tool.MCPServerName,
		ToolName:      tool.ToolName,
		InputJson:     tool.InputJSON,
	}}}
}

func insertMCPToolResultClaimTx(ctx context.Context, tx *dbconnect.Tx, request *bridgev1.ClaimMcpToolResultRequest, tool runtimecontrol.DurableToolExecution, now time.Time) (bool, error) {
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
	return runtimecontrol.RowsAffected(result), nil
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
	if !runtimecontrol.RowsAffected(result) {
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
	if !runtimecontrol.RowsAffected(result) {
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

func sameMCPToolResult(existing runtimeToolResult, tool runtimecontrol.DurableToolExecution) bool {
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
	var restored mcpmanifest.Acceptance
	err := s.Client.WithWorkspaceTx(ctx, workspaceID, "agentruntimebridge.mcp_manifest_changed_replay", func(tx *dbconnect.Tx) error {
		if err := mcpmanifest.AcquireAcceptanceLockTx(ctx, tx, workspaceID, sessionID, mcpServerName); err != nil {
			return err
		}
		if _, err := runtimecontrol.LockMainThreadIDTx(ctx, tx, workspaceID, sessionID); err != nil {
			return err
		}
		row, found, err := mcpmanifest.LoadRowForUpdateTx(ctx, tx, workspaceID, sessionID, mcpServerName)
		if err != nil {
			return err
		}
		if !found || !row.ManifestETag.Valid || row.ManifestETag.String != manifestETag {
			return nil
		}
		if row.Readiness == mcpmanifest.ReadinessUnready {
			if !row.ToolsJSON.Valid {
				return nil
			}
			toolset, err := mcpmanifest.ToolsetConfigTx(ctx, tx, workspaceID, sessionID, mcpServerName)
			if err != nil {
				return err
			}
			acceptance, err := mcpmanifest.CommitReadyTx(ctx, tx, workspaceID, sessionID, mcpServerName, manifestETag, row.ToolsJSON.String, row.Generation+1, toolset, s.now())
			if err != nil {
				return err
			}
			acceptance.PreviousGeneration = row.Generation
			acceptance.Readiness = mcpmanifest.ReadinessReady
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
	mcpmanifest.LogTransitionCommitted(s.Logger, ServiceNameBridgeAPI, workspaceID, sessionID, mcpServerName, restored, false)
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

func mcpTransientAttachmentCreate(request *bridgev1.CommitMcpToolResultRequest, tool runtimecontrol.DurableToolExecution, media *bridgev1.McpInlineMedia) transientAttachmentCreate {
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
