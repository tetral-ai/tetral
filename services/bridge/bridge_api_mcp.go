package agentruntimebridge

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/binary"
	"encoding/json"
	"fmt"
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
	if acceptance.ManifestTooLarge {
		logMCPManifestReadiness(s.Logger, ServiceNameBridgeAPI, workspaceID, sessionID, mcpServerName, mcpManifestReadinessUnready, mcpManifestDiagnosticTooLarge, acceptance.Generation)
		return nil, status.Error(codes.ResourceExhausted, "mcp manifest tools exceed the accepted byte limit")
	}
	if !acceptance.Duplicate {
		logMCPManifestOmissions(s.Logger, ServiceNameBridgeAPI, workspaceID, sessionID, mcpServerName, acceptance.BuiltinFamily, acceptance.Omissions)
	}
	ack := committedAck(acceptance.RuntimeInputID, "")
	if acceptance.Duplicate {
		ack = duplicateAck(acceptance.RuntimeInputID, "")
	}
	return &bridgev1.McpManifestChangedResponse{Ack: ack}, nil
}

func (s *PostgreSQLBridgeAPIStore) ClaimMcpToolResult(ctx context.Context, request *bridgev1.ClaimMcpToolResultRequest) (*bridgev1.ClaimMcpToolResultResponse, error) {
	if err := validateMCPToolResultIdentity(request.GetScope(), request.GetToolUseEventId(), request.GetNormalizedInputHash(), request.GetMcpServerName(), request.GetToolName(), request.GetInputJson()); err != nil {
		return nil, err
	}
	now := s.now()
	var response *bridgev1.ClaimMcpToolResultResponse
	if err := s.withScopeTx(ctx, request.GetScope(), "agentruntimebridge.claim_mcp_tool_result", func(tx *dbconnect.Tx) error {
		if err := verifyRuntimeScopeTx(ctx, tx, request.GetScope()); err != nil {
			return err
		}
		claimed, err := claimMCPToolResultTx(ctx, tx, request, now)
		if err != nil {
			return err
		}
		response = claimed
		return nil
	}); err != nil {
		return nil, err
	}
	return response, nil
}

func (s *PostgreSQLBridgeAPIStore) CommitMcpToolResult(ctx context.Context, request *bridgev1.CommitMcpToolResultRequest) (*bridgev1.CommitMcpToolResultResponse, error) {
	if err := validateMCPToolResultIdentity(request.GetScope(), request.GetToolUseEventId(), request.GetNormalizedInputHash(), request.GetMcpServerName(), request.GetToolName(), request.GetInputJson()); err != nil {
		return nil, err
	}
	if err := validateMCPCommitPayload(request); err != nil {
		return nil, err
	}
	var response *bridgev1.CommitMcpToolResultResponse
	if err := s.withScopeTx(ctx, request.GetScope(), "agentruntimebridge.commit_mcp_tool_result_preflight", func(tx *dbconnect.Tx) error {
		if err := verifyRuntimeScopeTx(ctx, tx, request.GetScope()); err != nil {
			return err
		}
		if existing, ok, err := readRuntimeToolResultTx(ctx, tx, request.GetScope(), request.GetToolUseEventId()); err != nil {
			return err
		} else if ok {
			if !sameMCPToolResult(existing, request.GetNormalizedInputHash(), request.GetMcpServerName(), request.GetToolName()) {
				return status.Error(codes.AlreadyExists, "mcp tool use id conflicts with existing result")
			}
			switch existing.MCPClaimStatus.String {
			case mcpClaimStatusStored:
				replayJSON, err := replayMCPToolResultTx(ctx, tx, request.GetScope(), request.GetToolUseEventId(), existing.ResultJSON, s.now())
				if err != nil {
					return err
				}
				response = &bridgev1.CommitMcpToolResultResponse{Ack: duplicateAck("", ""), RefsOnlyResultJson: replayJSON}
			case mcpClaimStatusInFlight:
				if !existing.MCPClaimOwnerRequestID.Valid || existing.MCPClaimOwnerRequestID.String != request.GetScope().GetRequestId() {
					response = &bridgev1.CommitMcpToolResultResponse{Ack: rejectedAck(mcpClaimNotOwnedCode)}
				}
			default:
				return status.Error(codes.Internal, "invalid mcp claim state")
			}
			return nil
		}
		return status.Error(codes.FailedPrecondition, "mcp tool claim is missing")
	}); err != nil || response != nil {
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
		attachmentCreate := mcpTransientAttachmentCreate(request, media)
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
		return nil, err
	}
	now := s.now()
	err = s.withScopeTxAndCleanup(ctx, request.GetScope(), "agentruntimebridge.commit_mcp_tool_result", func(tx *dbconnect.Tx) error {
		if err := verifyRuntimeScopeTx(ctx, tx, request.GetScope()); err != nil {
			return err
		}
		existing, ok, err := readRuntimeToolResultTx(ctx, tx, request.GetScope(), request.GetToolUseEventId())
		if err != nil {
			return err
		}
		if !ok {
			return status.Error(codes.FailedPrecondition, "mcp tool claim is missing")
		}
		if !sameMCPToolResult(existing, request.GetNormalizedInputHash(), request.GetMcpServerName(), request.GetToolName()) {
			return status.Error(codes.AlreadyExists, "mcp tool use id conflicts with existing result")
		}
		switch existing.MCPClaimStatus.String {
		case mcpClaimStatusStored:
			replayJSON, err := replayMCPToolResultTx(ctx, tx, request.GetScope(), request.GetToolUseEventId(), existing.ResultJSON, now)
			if err != nil {
				return err
			}
			response = &bridgev1.CommitMcpToolResultResponse{Ack: duplicateAck("", ""), RefsOnlyResultJson: replayJSON}
			cleanupBlob()
			return nil
		case mcpClaimStatusInFlight:
			if !existing.MCPClaimOwnerRequestID.Valid || existing.MCPClaimOwnerRequestID.String != request.GetScope().GetRequestId() {
				response = &bridgev1.CommitMcpToolResultResponse{Ack: rejectedAck(mcpClaimNotOwnedCode)}
				cleanupBlob()
				return nil
			}
		default:
			return status.Error(codes.Internal, "invalid mcp claim state")
		}
		if attachment != nil {
			if err := insertTransientAttachmentTx(ctx, tx, mcpTransientAttachmentCreate(request, request.GetInlineMedia()[0]), attachment, blobPointer, now); err != nil {
				return err
			}
		}
		if err := storeMCPToolResultTx(ctx, tx, request.GetScope(), request.GetToolUseEventId(), refsOnlyResultJSON, now); err != nil {
			return err
		}
		response = &bridgev1.CommitMcpToolResultResponse{Ack: committedAck("", ""), RefsOnlyResultJson: refsOnlyResultJSON}
		return nil
	}, cleanupBlob)
	if err != nil {
		cleanupBlob()
		return nil, err
	}
	blobStored = false
	return response, nil
}

func claimMCPToolResultTx(ctx context.Context, tx *dbconnect.Tx, request *bridgev1.ClaimMcpToolResultRequest, now time.Time) (*bridgev1.ClaimMcpToolResultResponse, error) {
	existing, ok, err := readRuntimeToolResultTx(ctx, tx, request.GetScope(), request.GetToolUseEventId())
	if err != nil {
		return nil, err
	}
	if ok {
		return claimExistingMCPToolResultTx(ctx, tx, request, existing, now)
	}
	inserted, err := insertMCPToolResultClaimTx(ctx, tx, request, now)
	if err != nil {
		return nil, err
	}
	if inserted {
		return &bridgev1.ClaimMcpToolResultResponse{Ack: committedAck("", "")}, nil
	}
	existing, ok, err = readRuntimeToolResultTx(ctx, tx, request.GetScope(), request.GetToolUseEventId())
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, status.Error(codes.Internal, "mcp tool claim race was not materialized")
	}
	return claimExistingMCPToolResultTx(ctx, tx, request, existing, now)
}

func claimExistingMCPToolResultTx(ctx context.Context, tx *dbconnect.Tx, request *bridgev1.ClaimMcpToolResultRequest, existing runtimeToolResult, now time.Time) (*bridgev1.ClaimMcpToolResultResponse, error) {
	if !sameMCPToolResult(existing, request.GetNormalizedInputHash(), request.GetMcpServerName(), request.GetToolName()) {
		return nil, status.Error(codes.AlreadyExists, "mcp tool use id conflicts with existing result")
	}
	switch existing.MCPClaimStatus.String {
	case mcpClaimStatusStored:
		replayJSON, err := replayMCPToolResultTx(ctx, tx, request.GetScope(), request.GetToolUseEventId(), existing.ResultJSON, now)
		if err != nil {
			return nil, err
		}
		return &bridgev1.ClaimMcpToolResultResponse{Ack: duplicateAck("", ""), ResultJson: replayJSON}, nil
	case mcpClaimStatusInFlight:
		active, err := mcpClaimLeaseActive(existing, now)
		if err != nil {
			return nil, err
		}
		if active {
			return &bridgev1.ClaimMcpToolResultResponse{Ack: rejectedAck(mcpClaimInFlightCode)}, nil
		}
		if err := renewMCPToolResultClaimTx(ctx, tx, request.GetScope(), request.GetToolUseEventId(), now); err != nil {
			return nil, err
		}
		return &bridgev1.ClaimMcpToolResultResponse{Ack: committedAck("", "")}, nil
	default:
		return nil, status.Error(codes.Internal, "invalid mcp claim state")
	}
}

func replayMCPToolResultTx(ctx context.Context, tx *dbconnect.Tx, scope *bridgev1.RuntimeScope, toolUseEventID string, resultJSON string, now time.Time) (string, error) {
	decoder := json.NewDecoder(strings.NewReader(resultJSON))
	decoder.UseNumber()
	var root map[string]any
	if err := decoder.Decode(&root); err != nil || root == nil {
		return "", status.Error(codes.Internal, "stored mcp tool result is invalid")
	}
	response, ok := root["response"].(map[string]any)
	if !ok {
		return "", status.Error(codes.Internal, "stored mcp tool response is invalid")
	}
	rawAttachments, ok := response["attachments"].([]any)
	if !ok || len(rawAttachments) == 0 {
		return resultJSON, nil
	}
	available := make([]any, 0, len(rawAttachments))
	omissions := make([]string, 0, len(rawAttachments))
	for _, raw := range rawAttachments {
		metadata, ok := raw.(map[string]any)
		if !ok {
			return "", status.Error(codes.Internal, "stored mcp attachment metadata is invalid")
		}
		attachmentRef, refOK := metadata["attachment_ref"].(string)
		mime, mimeOK := metadata["mime"].(string)
		size, sizeOK := metadata["size_bytes"].(json.Number)
		sizeBytes, sizeErr := size.Int64()
		if !refOK || attachmentRef == "" || !mimeOK || !sizeOK || sizeErr != nil || sizeBytes < 0 {
			return "", status.Error(codes.Internal, "stored mcp attachment metadata is incomplete")
		}
		var active bool
		err := tx.QueryRow(ctx,
			`SELECT status = 'active' AND expires_at > $6
			   FROM session_transient_attachments
			  WHERE workspace_id = $1
			    AND session_id = $2
			    AND session_thread_id = $3
			    AND source_tool_use_event_id = $4
			    AND attachment_ref = $5`,
			scope.GetWorkspaceId(), scope.GetSessionId(), scope.GetSessionThreadId(), toolUseEventID, attachmentRef, now,
		).Scan(&active)
		if err != nil && !dbconnect.IsNoRows(err) {
			return "", err
		}
		if err == nil && active {
			available = append(available, raw)
			continue
		}
		omissions = append(omissions, fmt.Sprintf("[MCP attachment unavailable: %s (%d)]", mime, sizeBytes))
	}
	if len(omissions) == 0 {
		return resultJSON, nil
	}
	response["attachments"] = available
	resultText, _ := response["result_text"].(string)
	if resultText == "" {
		response["result_text"] = strings.Join(omissions, "\n")
	} else {
		response["result_text"] = resultText + "\n" + strings.Join(omissions, "\n")
	}
	replayed, err := json.Marshal(root)
	if err != nil {
		return "", status.Error(codes.Internal, "mcp replay result encoding failed")
	}
	return string(replayed), nil
}

func insertMCPToolResultClaimTx(ctx context.Context, tx *dbconnect.Tx, request *bridgev1.ClaimMcpToolResultRequest, now time.Time) (bool, error) {
	result, err := tx.Exec(ctx,
		`INSERT INTO session_runtime_tool_results (
			workspace_id, session_id, session_thread_id, tool_use_event_id, tool_kind,
			normalized_input_hash, tool_name, input_json, ack_status, result_json,
			mcp_claim_status, mcp_claim_owner_request_id, mcp_claim_lease_expires_at,
			created_at, updated_at
		) VALUES ($1, $2, $3, $4, 'mcp', $5, $6, $7, 'committed', '{}', 'in_flight', $8, $9, $10, $10)
		ON CONFLICT (workspace_id, session_id, tool_use_event_id) DO NOTHING`,
		request.GetScope().GetWorkspaceId(),
		request.GetScope().GetSessionId(),
		request.GetScope().GetSessionThreadId(),
		request.GetToolUseEventId(),
		request.GetNormalizedInputHash(),
		mcpRuntimeToolName(request.GetMcpServerName(), request.GetToolName()),
		request.GetInputJson(),
		request.GetScope().GetRequestId(),
		now.Add(mcpClaimLeaseTTL),
		now,
	)
	if err != nil {
		return false, err
	}
	return rowsAffected(result), nil
}

func renewMCPToolResultClaimTx(ctx context.Context, tx *dbconnect.Tx, scope *bridgev1.RuntimeScope, toolUseEventID string, now time.Time) error {
	result, err := tx.Exec(ctx,
		`UPDATE session_runtime_tool_results
		    SET mcp_claim_owner_request_id = $4,
		        mcp_claim_lease_expires_at = $5,
		        updated_at = $6
		  WHERE workspace_id = $1
		    AND session_id = $2
		    AND tool_use_event_id = $3
		    AND tool_kind = 'mcp'
		    AND mcp_claim_status = 'in_flight'`,
		scope.GetWorkspaceId(),
		scope.GetSessionId(),
		toolUseEventID,
		scope.GetRequestId(),
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

func storeMCPToolResultTx(ctx context.Context, tx *dbconnect.Tx, scope *bridgev1.RuntimeScope, toolUseEventID string, resultJSON string, now time.Time) error {
	result, err := tx.Exec(ctx,
		`UPDATE session_runtime_tool_results
		    SET result_json = $4,
		        mcp_claim_status = 'stored',
		        mcp_claim_owner_request_id = NULL,
		        mcp_claim_lease_expires_at = NULL,
		        updated_at = $5
		  WHERE workspace_id = $1
		    AND session_id = $2
		    AND tool_use_event_id = $3
		    AND tool_kind = 'mcp'
		    AND mcp_claim_status = 'in_flight'`,
		scope.GetWorkspaceId(),
		scope.GetSessionId(),
		toolUseEventID,
		resultJSON,
		now,
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

func validateMCPToolResultIdentity(scope *bridgev1.RuntimeScope, toolUseEventID string, normalizedInputHash string, mcpServerName string, toolName string, inputJSON string) error {
	if err := validateRuntimeScope(scope); err != nil {
		return err
	}
	if toolUseEventID == "" || normalizedInputHash == "" || mcpServerName == "" || toolName == "" || inputJSON == "" {
		return status.Error(codes.InvalidArgument, "invalid mcp tool result request")
	}
	if !json.Valid([]byte(inputJSON)) {
		return status.Error(codes.InvalidArgument, "mcp tool input must be JSON")
	}
	return nil
}

func sameMCPToolResult(existing runtimeToolResult, normalizedInputHash string, mcpServerName string, toolName string) bool {
	return existing.ToolKind == bridgeToolKindMCP &&
		existing.NormalizedInputHash == normalizedInputHash &&
		existing.ToolName == mcpRuntimeToolName(mcpServerName, toolName)
}

func mcpRuntimeToolName(mcpServerName string, toolName string) string {
	return mcpServerName + "/" + toolName
}

func (s *PostgreSQLBridgeAPIStore) replayedMCPManifestChanged(ctx context.Context, workspaceID string, sessionID string, mcpServerName string, manifestETag string) (*bridgev1.McpManifestChangedResponse, bool, error) {
	var response *bridgev1.McpManifestChangedResponse
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
			response = &bridgev1.McpManifestChangedResponse{Ack: committedAck(acceptance.RuntimeInputID, "")}
			return nil
		}
		response = &bridgev1.McpManifestChangedResponse{
			Ack: duplicateAck(runtimeMCPManifestInputID(sessionID, mcpServerName, row.Generation), ""),
		}
		return nil
	})
	if err != nil {
		return nil, false, err
	}
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

func mcpTransientAttachmentCreate(request *bridgev1.CommitMcpToolResultRequest, media *bridgev1.McpInlineMedia) transientAttachmentCreate {
	return transientAttachmentCreate{
		Scope:                request.GetScope(),
		SourceToolUseEventID: request.GetToolUseEventId(),
		Mime:                 media.GetMime(),
		Filename:             media.GetSuggestedFilename(),
		SourcePath:           "mcp:" + request.GetMcpServerName() + "/" + media.GetSuggestedFilename(),
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
	RuntimeInputID   string
	Generation       int64
	BuiltinFamily    string
	Omissions        []mcpManifestOmission
	Duplicate        bool
	ManifestTooLarge bool
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
	runtimeMCPManifestDeliveryMaxAttempts  = 5
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
		generation, err := transitionMCPManifestUnreadyTx(ctx, tx, workspaceID, sessionID, mcpServerName, current, rowExists, mcpManifestDiagnosticTooLarge, toolsetConfig, now)
		return mcpManifestAcceptance{Generation: generation, BuiltinFamily: toolsetConfig.BuiltinFamily, ManifestTooLarge: true}, err
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
	payloadJSON, err := runtimeMCPManifestUpdatePayload(workspaceID, sessionID, mcpServerName, generation)
	if err != nil {
		return 0, err
	}
	if err := enqueueRuntimeMCPManifestUpdateTx(ctx, tx, workspaceID, sessionID, mcpServerName, generation, payloadJSON, now); err != nil {
		return 0, err
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

func logMCPManifestReadiness(logger *slog.Logger, component string, workspaceID string, sessionID string, mcpServerName string, readiness string, diagnostic string, generation int64) {
	if logger == nil {
		return
	}
	logger.Warn("bridge.mcp_manifest.readiness_changed",
		slog.String("operation", "mcp_manifest.readiness"),
		slog.String("event.kind", "mcp_manifest.readiness_changed"),
		slog.String("component", component),
		slog.String("workspace.id", workspaceID),
		slog.String("session.id", sessionID),
		slog.String("mcp.server.name", mcpServerName),
		slog.String("mcp.manifest.readiness", readiness),
		slog.String("mcp.manifest.diagnostic", diagnostic),
		slog.Int64("mcp.manifest.generation", generation),
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
