package agentruntimebridge

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"path"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	bridgev1 "github.com/tetral-ai/tetral/services/bridge/gen/tetral/bridge/v1"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/tetral-ai/tetral/internal/blob"
	"github.com/tetral-ai/tetral/internal/dbconnect"
)

// This file owns the Bridge attachments protocol-family boundary.

const fileAttachmentChunkMaxBytes = 8 * 1024 * 1024

var errFileAttachmentBlobRangeLengthMismatch = errors.New("file attachment blob range length mismatch")

type fileAttachmentRangeReader interface {
	GetRange(context.Context, string, int64, int64) (io.ReadCloser, error)
}

func (s *PostgreSQLBridgeAPIStore) ResolveTransientAttachment(ctx context.Context, request *bridgev1.ResolveTransientAttachmentRequest) (*bridgev1.ResolveTransientAttachmentResponse, error) {
	if err := validateResolveTransientAttachmentRequest(request); err != nil {
		return nil, err
	}
	if s == nil || s.AttachmentBlobStore == nil {
		return nil, status.Error(codes.Unavailable, "transient attachment blob store is unavailable")
	}
	var row transientAttachmentIndexRow
	var unavailable bool
	err := s.withScopeReadOnlyTx(ctx, request.GetScope(), "agentruntimebridge.resolve_transient_attachment", func(tx *dbconnect.Tx) error {
		if err := verifyAttachmentResolverScopeReadOnlyTx(ctx, tx, request.GetScope()); err != nil {
			return err
		}
		loaded, err := readTransientAttachmentIndexTx(ctx, tx, request)
		if err != nil {
			return err
		}
		if loaded.Status != "active" {
			unavailable = true
			return nil
		}
		now := s.now()
		if loaded.ExpiresAt.Before(now) || loaded.ExpiresAt.Equal(now) {
			unavailable = true
			return nil
		}
		row = loaded
		return nil
	})
	if err != nil {
		return nil, err
	}
	if unavailable {
		return &bridgev1.ResolveTransientAttachmentResponse{
			Outcome: &bridgev1.ResolveTransientAttachmentResponse_Unavailable{
				Unavailable: &bridgev1.TransientAttachmentUnavailable{},
			},
		}, nil
	}
	expectedPointer := transientAttachmentBlobPointer(request.GetScope(), request.GetAttachmentRef())
	if row.BlobPointer != expectedPointer {
		return nil, status.Error(codes.Internal, "transient attachment blob integrity check failed")
	}
	objectMetadata, err := s.AttachmentBlobStore.HeadObject(ctx, row.BlobPointer)
	if err != nil {
		return nil, status.Error(codes.Unavailable, "transient attachment blob metadata read failed")
	}
	if objectMetadata.SizeBytes != row.SizeBytes {
		return nil, status.Error(codes.Internal, "transient attachment blob integrity check failed")
	}
	rc, err := s.AttachmentBlobStore.Get(ctx, row.BlobPointer)
	if err != nil {
		return nil, status.Error(codes.Unavailable, "transient attachment blob read failed")
	}
	defer func() { _ = rc.Close() }()
	data, err := io.ReadAll(io.LimitReader(rc, transientAttachmentMaxBytes+1))
	if err != nil {
		return nil, status.Error(codes.Unavailable, "transient attachment blob read failed")
	}
	if len(data) > transientAttachmentMaxBytes {
		return nil, status.Error(codes.Internal, "transient attachment blob exceeds size limit")
	}
	if int64(len(data)) != row.SizeBytes || transientAttachmentDigest(data) != row.Digest {
		return nil, status.Error(codes.Internal, "transient attachment blob integrity check failed")
	}
	return &bridgev1.ResolveTransientAttachmentResponse{
		Outcome: &bridgev1.ResolveTransientAttachmentResponse_Resolved{
			Resolved: &bridgev1.ResolvedTransientAttachment{
				AttachmentRef: row.AttachmentRef,
				Mime:          row.Mime,
				Filename:      row.Filename,
				SourcePath:    row.SourcePath,
				PageRange:     row.PageRange,
				Detail:        row.Detail,
				Data:          data,
			},
		},
	}, nil
}

func (s *PostgreSQLBridgeAPIStore) ResolveFileAttachmentMetadata(ctx context.Context, request *bridgev1.ResolveFileAttachmentMetadataRequest) (*bridgev1.ResolveFileAttachmentMetadataResponse, error) {
	if err := validateFileAttachmentPairs(request.GetAttachments()); err != nil {
		return nil, err
	}
	response := &bridgev1.ResolveFileAttachmentMetadataResponse{
		Attachments: make([]*bridgev1.FileAttachmentMetadataResult, 0, len(request.GetAttachments())),
	}
	if err := s.withScopeReadOnlyTx(ctx, request.GetScope(), "agentruntimebridge.resolve_file_attachment_metadata", func(tx *dbconnect.Tx) error {
		if err := verifyAttachmentResolverScopeReadOnlyTx(ctx, tx, request.GetScope()); err != nil {
			return err
		}
		for _, pair := range request.GetAttachments() {
			result, _, err := readFileAttachmentMetadataTx(ctx, tx, request.GetScope(), pair)
			if err != nil {
				return err
			}
			response.Attachments = append(response.Attachments, result)
		}
		return nil
	}); err != nil {
		return nil, err
	}
	return response, nil
}

func (s *PostgreSQLBridgeAPIStore) ReadFileAttachmentChunk(ctx context.Context, request *bridgev1.ReadFileAttachmentChunkRequest) (*bridgev1.ReadFileAttachmentChunkResponse, error) {
	if request == nil || request.GetAttachment() == nil {
		return nil, status.Error(codes.InvalidArgument, "file attachment is required")
	}
	if err := validateFileAttachmentPairs([]*bridgev1.FileAttachmentPair{request.GetAttachment()}); err != nil {
		return nil, err
	}
	if request.GetOffset() < 0 || request.GetLength() <= 0 || request.GetLength() > fileAttachmentChunkMaxBytes {
		return nil, status.Error(codes.InvalidArgument, "file attachment chunk range is invalid")
	}
	if s == nil || s.FileBlobStore == nil {
		return nil, status.Error(codes.Unavailable, "file attachment blob store is unavailable")
	}
	var metadata *bridgev1.FileAttachmentMetadata
	var rejected *bridgev1.FileAttachmentRejection
	var blobPointer string
	if err := s.withScopeReadOnlyTx(ctx, request.GetScope(), "agentruntimebridge.read_file_attachment_chunk", func(tx *dbconnect.Tx) error {
		if err := verifyAttachmentResolverScopeReadOnlyTx(ctx, tx, request.GetScope()); err != nil {
			return err
		}
		var err error
		var result *bridgev1.FileAttachmentMetadataResult
		result, blobPointer, err = readFileAttachmentMetadataTx(ctx, tx, request.GetScope(), request.GetAttachment())
		if err != nil {
			return err
		}
		if rejected = result.GetRejected(); rejected != nil {
			return nil
		}
		metadata = result.GetMetadata()
		if request.GetOffset() > metadata.GetSizeBytes() {
			return status.Error(codes.InvalidArgument, "file attachment chunk offset exceeds file size")
		}
		return nil
	}); err != nil {
		return nil, err
	}
	if rejected != nil {
		return &bridgev1.ReadFileAttachmentChunkResponse{
			Outcome: &bridgev1.ReadFileAttachmentChunkResponse_Rejected{Rejected: rejected},
		}, nil
	}
	length := request.GetLength()
	if remaining := metadata.GetSizeBytes() - request.GetOffset(); length > remaining {
		length = remaining
	}
	if length == 0 {
		return &bridgev1.ReadFileAttachmentChunkResponse{
			Outcome: &bridgev1.ReadFileAttachmentChunkResponse_Data{Data: []byte{}},
		}, nil
	}
	data, err := readFileAttachmentBlobRange(ctx, s.FileBlobStore, blobPointer, request.GetOffset(), length)
	if err != nil {
		if errors.Is(err, errFileAttachmentBlobRangeLengthMismatch) {
			return nil, status.Error(codes.Internal, "file attachment blob range is corrupt")
		}
		return nil, status.Error(codes.Unavailable, "file attachment blob read failed")
	}
	return &bridgev1.ReadFileAttachmentChunkResponse{
		Outcome: &bridgev1.ReadFileAttachmentChunkResponse_Data{Data: data},
	}, nil
}

func verifyAttachmentResolverScopeReadOnlyTx(
	ctx context.Context,
	tx *dbconnect.Tx,
	scope *bridgev1.RuntimeScope,
) error {
	err := verifyRuntimeScopeReadOnlyTx(ctx, tx, scope)
	if status.Code(err) == codes.FailedPrecondition {
		return status.Error(codes.InvalidArgument, "attachment resolver scope is invalid")
	}
	return err
}

func validateFileAttachmentPairs(pairs []*bridgev1.FileAttachmentPair) error {
	if len(pairs) == 0 {
		return status.Error(codes.InvalidArgument, "file attachments are required")
	}
	_, err := normalizeConsumedFileAttachments(pairs)
	return err
}

func readFileAttachmentMetadataTx(
	ctx context.Context,
	tx *dbconnect.Tx,
	scope *bridgev1.RuntimeScope,
	pair *bridgev1.FileAttachmentPair,
) (*bridgev1.FileAttachmentMetadataResult, string, error) {
	metadata := &bridgev1.FileAttachmentMetadata{
		Attachment: &bridgev1.FileAttachmentPair{
			SourceEventId: pair.GetSourceEventId(),
			FileId:        pair.GetFileId(),
		},
	}
	var eventType, payloadJSON string
	err := tx.QueryRow(ctx,
		`SELECT type, payload_json
		   FROM session_events
		  WHERE workspace_id = $1
		    AND session_id = $2
		    AND session_thread_id = $3
		    AND event_id = $4`,
		scope.GetWorkspaceId(),
		scope.GetSessionId(),
		scope.GetSessionThreadId(),
		pair.GetSourceEventId(),
	).Scan(&eventType, &payloadJSON)
	if dbconnect.IsNoRows(err) {
		return nil, "", status.Error(codes.InvalidArgument, "file attachment source event is invalid")
	}
	if err != nil {
		return nil, "", err
	}
	blockType, ok := fileAttachmentBlockType(payloadJSON, pair.GetFileId())
	if eventType != "user.message" || !ok {
		return nil, "", status.Error(codes.InvalidArgument, "file attachment does not match its source event")
	}
	var filename, mime, blobPointer string
	var sizeBytes int64
	var deletedAt sql.NullTime
	err = tx.QueryRow(ctx,
		`SELECT f.filename, f.mime_type, f.deleted_at, o.size_bytes, o.blob_key
		   FROM files f
		   JOIN file_objects o
		     ON o.workspace_id = f.workspace_id
		    AND o.object_id = f.object_id
		  WHERE f.workspace_id = $1
		    AND f.file_id = $2`,
		scope.GetWorkspaceId(),
		pair.GetFileId(),
	).Scan(&filename, &mime, &deletedAt, &sizeBytes, &blobPointer)
	if dbconnect.IsNoRows(err) || deletedAt.Valid {
		return &bridgev1.FileAttachmentMetadataResult{
			Outcome: &bridgev1.FileAttachmentMetadataResult_Rejected{
				Rejected: &bridgev1.FileAttachmentRejection{
					Attachment: metadata.GetAttachment(),
					Reason:     bridgev1.FileAttachmentRejectionReason_FILE_ATTACHMENT_REJECTION_REASON_DELETED,
				},
			},
		}, "", nil
	}
	if err != nil {
		return nil, "", err
	}
	if !fileAttachmentMIMEMatchesBlock(blockType, mime) {
		return nil, "", status.Error(codes.InvalidArgument, "file attachment does not match its source event")
	}
	metadata.Mime = mime
	metadata.SizeBytes = sizeBytes
	metadata.Filename = filename
	return &bridgev1.FileAttachmentMetadataResult{
		Outcome: &bridgev1.FileAttachmentMetadataResult_Metadata{Metadata: metadata},
	}, blobPointer, nil
}

func fileAttachmentBlockType(payloadJSON, fileID string) (string, bool) {
	var payload struct {
		Content []struct {
			Type   string `json:"type"`
			Source *struct {
				Type   string `json:"type"`
				FileID string `json:"file_id"`
			} `json:"source"`
		} `json:"content"`
	}
	if err := json.Unmarshal([]byte(payloadJSON), &payload); err != nil {
		return "", false
	}
	for _, block := range payload.Content {
		if block.Source != nil && block.Source.Type == "file" && block.Source.FileID == fileID &&
			(block.Type == "image" || block.Type == "document") {
			return block.Type, true
		}
	}
	return "", false
}

func fileAttachmentMIMEMatchesBlock(blockType, mime string) bool {
	switch blockType {
	case "image":
		return mime == "image/jpeg" || mime == "image/png" || mime == "image/gif" || mime == "image/webp"
	case "document":
		return mime == "application/pdf" || mime == "text/plain"
	default:
		return false
	}
}

func readFileAttachmentBlobRange(ctx context.Context, store blob.BlobStore, key string, offset, length int64) ([]byte, error) {
	var reader io.ReadCloser
	var err error
	if ranged, ok := store.(fileAttachmentRangeReader); ok {
		reader, err = ranged.GetRange(ctx, key, offset, length)
	} else {
		reader, err = store.Get(ctx, key)
		if err == nil && offset > 0 {
			_, err = io.CopyN(io.Discard, reader, offset)
		}
	}
	if err != nil {
		if reader != nil {
			_ = reader.Close()
		}
		return nil, err
	}
	defer func() { _ = reader.Close() }()
	data, err := io.ReadAll(io.LimitReader(reader, length+1))
	if err != nil {
		return nil, err
	}
	if int64(len(data)) != length {
		return nil, errFileAttachmentBlobRangeLengthMismatch
	}
	return data, nil
}

// ReconcileTransientAttachments is the business GC for transient media. The
// object bucket's lifecycle rule is only a safety net. Uploading and staged
// media remain protected while their source execution is unconsumed. GC
// deletes are ordered: the index row leaves its readable state and commits
// before its Blob is deleted, so a resolver cannot serve bytes after custody
// is released.
func (s *PostgreSQLBridgeAPIStore) ReconcileTransientAttachments(ctx context.Context, limit int) (TransientAttachmentGCResult, error) {
	if s == nil || s.AttachmentBlobStore == nil {
		return TransientAttachmentGCResult{}, status.Error(codes.FailedPrecondition, "transient attachment blob store is unavailable")
	}
	if limit <= 0 {
		limit = 100
	}
	now := s.now()
	rows, err := s.markTransientAttachmentsForDeletion(ctx, limit, now)
	if err != nil {
		return TransientAttachmentGCResult{}, err
	}
	result := TransientAttachmentGCResult{Marked: len(rows)}
	for _, row := range rows {
		if err := s.AttachmentBlobStore.Delete(ctx, row.BlobPointer); err != nil {
			var notFound *blob.NotFoundError
			if !errors.As(err, &notFound) {
				result.Failed++
				continue
			}
		}
		if err := s.markTransientAttachmentDeleted(ctx, row, s.now()); err != nil {
			result.Failed++
			continue
		}
		result.Deleted++
	}
	return result, nil
}

func (s *PostgreSQLBridgeAPIStore) markTransientAttachmentsForDeletion(ctx context.Context, limit int, now time.Time) ([]transientAttachmentGCRow, error) {
	var out []transientAttachmentGCRow
	err := s.Client.WithTx(ctx, "agentruntimebridge.transient_attachment_gc_mark", nil, func(tx *dbconnect.Tx) error {
		if _, err := tx.Exec(ctx, "SELECT set_config('tetral.transient_attachment_gc', 'true', true)"); err != nil {
			return err
		}
		rows, err := tx.Query(ctx,
			`WITH candidates AS (
				SELECT workspace_id, attachment_ref, blob_pointer, status
				  FROM session_transient_attachments
				 WHERE status = 'deleting'
				    OR status = 'consumed'
				    OR (status IN ('uploading', 'staged', 'active') AND expires_at <= $1
				        AND NOT (status IN ('uploading', 'staged') AND EXISTS (
				          SELECT 1
				            FROM session_runtime_tool_results AS execution
				           WHERE execution.workspace_id = session_transient_attachments.workspace_id
				             AND execution.session_id = session_transient_attachments.session_id
				             AND execution.session_thread_id = session_transient_attachments.session_thread_id
				             AND execution.tool_use_event_id = session_transient_attachments.source_tool_use_event_id
				             AND execution.tool_kind = 'sandbox_tool'
				             AND execution.execution_state <> 'consumed'
				        )))
				 ORDER BY storage_sequence
				 LIMIT $2
				 FOR UPDATE SKIP LOCKED
			)
			UPDATE session_transient_attachments AS a
			   SET status = 'deleting',
			       updated_at = $1
			  FROM candidates
			 WHERE a.workspace_id = candidates.workspace_id
			   AND a.attachment_ref = candidates.attachment_ref
			RETURNING a.workspace_id, a.attachment_ref, a.blob_pointer, candidates.status`,
			now,
			limit,
		)
		if err != nil {
			return err
		}
		defer func() { _ = rows.Close() }()
		for rows.Next() {
			var row transientAttachmentGCRow
			if err := rows.Scan(&row.WorkspaceID, &row.AttachmentRef, &row.BlobPointer, &row.PreviousStatus); err != nil {
				return err
			}
			out = append(out, row)
		}
		return rows.Err()
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (s *PostgreSQLBridgeAPIStore) markTransientAttachmentDeleted(ctx context.Context, row transientAttachmentGCRow, now time.Time) error {
	// This settles one already-identified row and holds its workspace, so it runs
	// under ordinary workspace isolation. The cross-workspace sweep policy stays
	// closed here: opening it on a path the tool runner also reaches would switch
	// off the database-side isolation backstop for this table on the hot path.
	return s.Client.WithWorkspaceTx(ctx, row.WorkspaceID, "agentruntimebridge.transient_attachment_deleted", func(tx *dbconnect.Tx) error {
		_, err := tx.Exec(ctx,
			`UPDATE session_transient_attachments
			    SET status = 'deleted',
			        updated_at = $3
			  WHERE workspace_id = $1
			    AND attachment_ref = $2
			    AND status = 'deleting'`,
			row.WorkspaceID,
			row.AttachmentRef,
			now,
		)
		return err
	})
}

func StartTransientAttachmentGC(ctx context.Context, store *PostgreSQLBridgeAPIStore, logger *slog.Logger, interval time.Duration, limit int) {
	if store == nil || interval <= 0 {
		return
	}
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				result, err := store.ReconcileTransientAttachments(ctx, limit)
				logTransientAttachmentGC(logger, result, err)
			}
		}
	}()
}

func logTransientAttachmentGC(logger *slog.Logger, result TransientAttachmentGCResult, err error) {
	if logger == nil {
		return
	}
	attrs := []any{
		slog.String("operation", "transient_attachment.gc"),
		slog.String("event.kind", "transient_attachment.gc"),
		slog.String("component", ServiceNameBridgeAPI),
		slog.Int("attachment.marked", result.Marked),
		slog.Int("attachment.deleted", result.Deleted),
		slog.Int("attachment.failed", result.Failed),
	}
	if err != nil {
		logger.Error("bridge.transient_attachment.gc_failed", append(attrs,
			slog.Bool("retryable", true),
			slog.Bool("terminal", false),
			slog.String("error.class", "transient_attachment_gc_error"),
			slog.String("error.code", "scan_failed"),
			slog.String("error.message_safe", "transient attachment GC scan failed"),
		)...)
		return
	}
	if result.Failed > 0 {
		logger.Warn("bridge.transient_attachment.gc_partial", append(attrs,
			slog.Bool("retryable", true),
			slog.Bool("terminal", false),
			slog.String("error.class", "transient_attachment_gc_error"),
			slog.String("error.code", "delete_failed"),
			slog.String("error.message_safe", "one or more transient attachment deletes failed"),
		)...)
		return
	}
	if result.Marked > 0 {
		logger.Info("bridge.transient_attachment.gc_completed", attrs...)
	}
}

// Provider execution metadata is not part of a durable Tool output or its
// declaration digest; Bridge removes it before comparing or storing output.
var internalProviderPayloadFields = map[string]struct{}{
	"background_task":                {},
	"engine_sandbox_id":              {},
	"provider_sandbox_id":            {},
	"provider_session_id":            {},
	"provider_command_id":            {},
	"provider_command_metadata":      {},
	"provider_command_metadata_json": {},
	"provider_metadata":              {},
	"provider_metadata_json":         {},
}

func stripInternalProviderFields(raw string) string {
	stripped, err := canonicalRunToolJSONWithoutObjectFields(raw, internalProviderPayloadFields)
	if err != nil {
		return raw
	}
	return stripped
}

type transientAttachmentCreate struct {
	Scope                *bridgev1.RuntimeScope
	SourceToolUseEventID string
	Data                 []byte
	Mime                 string
	Filename             string
	SourcePath           string
	PageRange            string
	Detail               string
}

type uploadedTransientAttachment struct {
	Create      transientAttachmentCreate
	Attachment  *bridgev1.TransientAttachmentRef
	BlobPointer string
}

func validateTransientAttachmentCreate(create transientAttachmentCreate) error {
	if create.SourceToolUseEventID == "" || len(create.Data) == 0 || create.Filename == "" {
		return status.Error(codes.InvalidArgument, "invalid transient attachment")
	}
	if err := validateRuntimeScope(create.Scope); err != nil {
		return err
	}
	if len(create.Data) > transientAttachmentMaxBytes {
		return status.Error(codes.InvalidArgument, "transient attachment exceeds size limit")
	}
	if !validTransientAttachmentMime(create.Mime) {
		return status.Error(codes.InvalidArgument, "transient attachment mime is not supported")
	}
	for _, value := range []string{create.SourceToolUseEventID, create.Mime, create.Filename, create.SourcePath, create.PageRange, create.Detail} {
		if len(value) > 1024 || !utf8.ValidString(value) {
			return status.Error(codes.InvalidArgument, "transient attachment metadata is invalid")
		}
	}
	return nil
}

func validateResolveTransientAttachmentRequest(request *bridgev1.ResolveTransientAttachmentRequest) error {
	if request.GetAttachmentRef() == "" {
		return status.Error(codes.InvalidArgument, "invalid transient attachment resolve request")
	}
	if err := validateRuntimeScope(request.GetScope()); err != nil {
		return err
	}
	if len(request.GetAttachmentRef()) > 1024 || !utf8.ValidString(request.GetAttachmentRef()) {
		return status.Error(codes.InvalidArgument, "transient attachment resolve metadata is invalid")
	}
	return nil
}

type normalizedTransientAttachmentSet struct {
	Refs          []string
	CanonicalJSON string
}

func normalizeConsumedAttachmentRefs(refs []string) (normalizedTransientAttachmentSet, error) {
	if len(refs) == 0 {
		return normalizedTransientAttachmentSet{CanonicalJSON: "[]"}, nil
	}
	if len(refs) > MaxProviderRequestAttachments {
		return normalizedTransientAttachmentSet{}, status.Error(codes.InvalidArgument, "too many consumed transient attachments")
	}
	seen := make(map[string]struct{}, len(refs))
	out := make([]string, 0, len(refs))
	for _, ref := range refs {
		if ref == "" || len(ref) > 1024 || !utf8.ValidString(ref) {
			return normalizedTransientAttachmentSet{}, status.Error(codes.InvalidArgument, "consumed transient attachment ref is invalid")
		}
		if _, ok := seen[ref]; ok {
			return normalizedTransientAttachmentSet{}, status.Error(codes.InvalidArgument, "duplicate consumed transient attachment ref")
		}
		seen[ref] = struct{}{}
		out = append(out, ref)
	}
	body, err := json.Marshal(out)
	if err != nil {
		return normalizedTransientAttachmentSet{}, err
	}
	return normalizedTransientAttachmentSet{Refs: out, CanonicalJSON: string(body)}, nil
}

type normalizedFileAttachmentSet struct {
	Pairs         []*bridgev1.FileAttachmentPair
	CanonicalJSON string
}

func normalizeConsumedFileAttachments(pairs []*bridgev1.FileAttachmentPair) (normalizedFileAttachmentSet, error) {
	if len(pairs) == 0 {
		return normalizedFileAttachmentSet{CanonicalJSON: "[]"}, nil
	}
	if len(pairs) > MaxProviderRequestAttachments {
		return normalizedFileAttachmentSet{}, status.Error(codes.InvalidArgument, "too many consumed file attachments")
	}
	type canonicalPair struct {
		SourceEventID string `json:"source_event_id"`
		FileID        string `json:"file_id"`
	}
	canonical := make([]canonicalPair, 0, len(pairs))
	normalized := make([]*bridgev1.FileAttachmentPair, 0, len(pairs))
	seen := make(map[canonicalPair]struct{}, len(pairs))
	for _, pair := range pairs {
		if pair == nil || pair.GetSourceEventId() == "" || pair.GetFileId() == "" {
			return normalizedFileAttachmentSet{}, status.Error(codes.InvalidArgument, "file attachment identity is incomplete")
		}
		if len(pair.GetSourceEventId()) > 1024 || len(pair.GetFileId()) > 1024 ||
			!utf8.ValidString(pair.GetSourceEventId()) || !utf8.ValidString(pair.GetFileId()) {
			return normalizedFileAttachmentSet{}, status.Error(codes.InvalidArgument, "file attachment identity is invalid")
		}
		item := canonicalPair{SourceEventID: pair.GetSourceEventId(), FileID: pair.GetFileId()}
		if _, ok := seen[item]; ok {
			return normalizedFileAttachmentSet{}, status.Error(codes.InvalidArgument, "duplicate consumed file attachment")
		}
		seen[item] = struct{}{}
		canonical = append(canonical, item)
		normalized = append(normalized, &bridgev1.FileAttachmentPair{
			SourceEventId: item.SourceEventID,
			FileId:        item.FileID,
		})
	}
	body, err := json.Marshal(canonical)
	if err != nil {
		return normalizedFileAttachmentSet{}, err
	}
	return normalizedFileAttachmentSet{Pairs: normalized, CanonicalJSON: string(body)}, nil
}

func validateFileAttachmentConsumptionsTx(
	ctx context.Context,
	tx *dbconnect.Tx,
	scope *bridgev1.RuntimeScope,
	pairs []*bridgev1.FileAttachmentPair,
) error {
	for _, pair := range pairs {
		var eventType, payloadJSON string
		err := tx.QueryRow(ctx,
			`SELECT event.type, event.payload_json
			   FROM session_events event
			   JOIN files file
			     ON file.workspace_id = event.workspace_id
			    AND file.file_id = $5
			  WHERE event.workspace_id = $1
			    AND event.session_id = $2
			    AND event.session_thread_id = $3
			    AND event.event_id = $4`,
			scope.GetWorkspaceId(),
			scope.GetSessionId(),
			scope.GetSessionThreadId(),
			pair.GetSourceEventId(),
			pair.GetFileId(),
		).Scan(&eventType, &payloadJSON)
		if dbconnect.IsNoRows(err) {
			return status.Error(codes.InvalidArgument, "consumed file attachment source event is invalid")
		}
		if err != nil {
			return err
		}
		if eventType != "user.message" {
			return status.Error(codes.InvalidArgument, "consumed file attachment source event is invalid")
		}
		if _, ok := fileAttachmentBlockType(payloadJSON, pair.GetFileId()); !ok {
			return status.Error(codes.InvalidArgument, "consumed file attachment does not match its source event")
		}
	}
	return nil
}

// insertFileAttachmentConsumptionsTx records file-backed consumption durably.
// A thread's pending file-backed media is never stored: it is always DERIVED as
// the media blocks of processed user input events MINUS the rows written here.
// That derived-not-stored invariant is what makes hot assembly (a live pod) and
// cold LoadContext reconstruction run the identical derivation, so pod memory is
// disposable for user media by construction.
func insertFileAttachmentConsumptionsTx(
	ctx context.Context,
	tx *dbconnect.Tx,
	scope *bridgev1.RuntimeScope,
	requestStartEventID string,
	pairs []*bridgev1.FileAttachmentPair,
) error {
	for _, pair := range pairs {
		if _, err := tx.Exec(ctx,
			`INSERT INTO session_file_attachment_consumptions (
				workspace_id, session_id, session_thread_id,
				request_start_event_id, source_event_id, file_id
			) VALUES ($1, $2, $3, $4, $5, $6)`,
			scope.GetWorkspaceId(),
			scope.GetSessionId(),
			scope.GetSessionThreadId(),
			requestStartEventID,
			pair.GetSourceEventId(),
			pair.GetFileId(),
		); err != nil {
			return err
		}
	}
	return nil
}

func validTransientAttachmentMime(mime string) bool {
	switch mime {
	case "application/pdf", "image/png", "image/jpeg", "image/gif", "image/webp":
		return true
	default:
		return false
	}
}

func newTransientAttachmentRef(create transientAttachmentCreate) (*bridgev1.TransientAttachmentRef, error) {
	var randomBytes [16]byte
	if _, err := rand.Read(randomBytes[:]); err != nil {
		return nil, err
	}
	return &bridgev1.TransientAttachmentRef{
		AttachmentRef: "att_" + base64.RawURLEncoding.EncodeToString(randomBytes[:]),
		Mime:          create.Mime,
		Filename:      create.Filename,
		SourcePath:    create.SourcePath,
		PageRange:     create.PageRange,
		Detail:        create.Detail,
	}, nil
}

func (s *PostgreSQLBridgeAPIStore) uploadTransientAttachment(ctx context.Context, create transientAttachmentCreate) (*uploadedTransientAttachment, error) {
	if err := validateTransientAttachmentCreate(create); err != nil {
		return nil, err
	}
	if s == nil || s.AttachmentBlobStore == nil {
		return nil, status.Error(codes.FailedPrecondition, "transient attachment blob store is unavailable")
	}
	attachment, err := newTransientAttachmentRef(create)
	if err != nil {
		return nil, status.Error(codes.Internal, "transient attachment ref generation failed")
	}
	blobPointer := transientAttachmentBlobPointer(create.Scope, attachment.GetAttachmentRef())
	if err := s.AttachmentBlobStore.Put(ctx, blobPointer, bytes.NewReader(create.Data), int64(len(create.Data))); err != nil {
		return nil, status.Error(codes.Unavailable, "transient attachment upload failed")
	}
	return &uploadedTransientAttachment{Create: create, Attachment: attachment, BlobPointer: blobPointer}, nil
}

func insertTransientAttachmentTx(ctx context.Context, tx *dbconnect.Tx, create transientAttachmentCreate, attachment *bridgev1.TransientAttachmentRef, blobPointer string, now time.Time) error {
	metadataJSON, err := transientAttachmentMetadataJSON(create)
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx,
		`INSERT INTO session_transient_attachments (
			workspace_id, attachment_ref, session_id, session_thread_id,
			source_tool_use_event_id, blob_pointer, mime, metadata_json,
			status, expires_at, created_at, updated_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 'active', $9, $10, $10)`,
		create.Scope.GetWorkspaceId(), attachment.GetAttachmentRef(), create.Scope.GetSessionId(),
		create.Scope.GetSessionThreadId(), create.SourceToolUseEventID, blobPointer, create.Mime, metadataJSON,
		now.Add(defaultTransientAttachmentTTL), now)
	return err
}

func insertStagedTransientAttachmentTx(ctx context.Context, tx *dbconnect.Tx, create transientAttachmentCreate, attachment *bridgev1.TransientAttachmentRef, blobPointer string, now time.Time) error {
	metadataJSON, err := transientAttachmentMetadataJSON(create)
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx,
		`INSERT INTO session_transient_attachments (
			workspace_id, attachment_ref, session_id, session_thread_id,
			source_tool_use_event_id, blob_pointer, mime, metadata_json,
			status, expires_at, created_at, updated_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 'staged', $9, $10, $10)`,
		create.Scope.GetWorkspaceId(), attachment.GetAttachmentRef(), create.Scope.GetSessionId(),
		create.Scope.GetSessionThreadId(), create.SourceToolUseEventID, blobPointer, create.Mime, metadataJSON,
		now.Add(defaultTransientAttachmentTTL), now)
	return err
}

func transientAttachmentBlobPointer(scope *bridgev1.RuntimeScope, attachmentRef string) string {
	return path.Join("transient-attachments", scope.GetWorkspaceId(), scope.GetSessionId(), attachmentRef)
}

type transientAttachmentMetadata struct {
	Filename   string `json:"filename"`
	SourcePath string `json:"source_path"`
	PageRange  string `json:"page_range"`
	Detail     string `json:"detail"`
	SizeBytes  int64  `json:"size_bytes"`
	Digest     string `json:"sha256"`
}

type transientAttachmentIndexRow struct {
	AttachmentRef string
	Mime          string
	Filename      string
	SourcePath    string
	PageRange     string
	Detail        string
	BlobPointer   string
	Status        string
	ExpiresAt     time.Time
	SizeBytes     int64
	Digest        string
}

func transientAttachmentMetadataJSON(create transientAttachmentCreate) (string, error) {
	body, err := json.Marshal(transientAttachmentMetadata{
		Filename:   create.Filename,
		SourcePath: create.SourcePath,
		PageRange:  create.PageRange,
		Detail:     create.Detail,
		SizeBytes:  int64(len(create.Data)),
		Digest:     transientAttachmentDigest(create.Data),
	})
	if err != nil {
		return "", err
	}
	return string(body), nil
}

func readTransientAttachmentIndexTx(ctx context.Context, tx *dbconnect.Tx, request *bridgev1.ResolveTransientAttachmentRequest) (transientAttachmentIndexRow, error) {
	row := tx.QueryRow(ctx,
		`SELECT attachment.blob_pointer, attachment.mime, attachment.metadata_json,
		        attachment.status, attachment.expires_at
		   FROM session_transient_attachments AS attachment
		   JOIN session_runtime_tool_results AS source_tool
		     ON source_tool.workspace_id = attachment.workspace_id
		    AND source_tool.session_id = attachment.session_id
		    AND source_tool.session_thread_id = attachment.session_thread_id
		    AND source_tool.tool_use_event_id = attachment.source_tool_use_event_id
		  WHERE attachment.workspace_id = $1
		    AND attachment.attachment_ref = $2
		    AND attachment.session_id = $3
		    AND attachment.session_thread_id = $4`,
		request.GetScope().GetWorkspaceId(),
		request.GetAttachmentRef(),
		request.GetScope().GetSessionId(),
		request.GetScope().GetSessionThreadId(),
	)
	var blobPointer string
	var mime string
	var metadataJSON string
	var statusValue string
	var expiresAtRaw string
	if err := row.Scan(&blobPointer, &mime, &metadataJSON, &statusValue, &expiresAtRaw); dbconnect.IsNoRows(err) {
		return transientAttachmentIndexRow{}, status.Error(codes.InvalidArgument, "transient attachment not found")
	} else if err != nil {
		return transientAttachmentIndexRow{}, err
	}
	var metadata transientAttachmentMetadata
	if err := json.Unmarshal([]byte(defaultString(metadataJSON, "{}")), &metadata); err != nil {
		return transientAttachmentIndexRow{}, err
	}
	if metadata.SizeBytes <= 0 || metadata.SizeBytes > transientAttachmentMaxBytes || len(metadata.Digest) != sha256.Size*2 {
		return transientAttachmentIndexRow{}, status.Error(codes.Internal, "transient attachment blob metadata is invalid")
	}
	if _, err := hex.DecodeString(metadata.Digest); err != nil {
		return transientAttachmentIndexRow{}, status.Error(codes.Internal, "transient attachment blob metadata is invalid")
	}
	expiresAt, err := time.Parse(time.RFC3339Nano, expiresAtRaw)
	if err != nil {
		return transientAttachmentIndexRow{}, status.Error(codes.Internal, "transient attachment expiry is invalid")
	}
	return transientAttachmentIndexRow{
		AttachmentRef: request.GetAttachmentRef(),
		Mime:          mime,
		Filename:      metadata.Filename,
		SourcePath:    metadata.SourcePath,
		PageRange:     metadata.PageRange,
		Detail:        metadata.Detail,
		BlobPointer:   blobPointer,
		Status:        statusValue,
		ExpiresAt:     expiresAt,
		SizeBytes:     metadata.SizeBytes,
		Digest:        metadata.Digest,
	}, nil
}

func transientAttachmentDigest(data []byte) string {
	digest := sha256.Sum256(data)
	return hex.EncodeToString(digest[:])
}

// validateTransientAttachmentsForConsumptionTx returns the subset of the
// snapshotted refs still in status 'active' (the consumable set). Its caller in
// WriteRequestEnd consumes that set IFF the request-end commits settled model
// output — the durable predicate is exactly IsError == false (the completed and
// waiting_external dispositions, success being structurally exclusive with
// reschedule on the wire). Failed, interrupted, and rescheduled ends consume
// nothing, so user media re-derives and rides the next attempt or turn. This
// durable gate and the pod's hot ride disposition express ONE act and must
// change together; if they diverge, a live pod and a cold re-derivation disagree
// about the same ride (media silently spent, or double-ridden).
func validateTransientAttachmentsForConsumptionTx(
	ctx context.Context,
	tx *dbconnect.Tx,
	scope *bridgev1.RuntimeScope,
	refs []string,
) ([]string, error) {
	if len(refs) == 0 {
		return nil, nil
	}
	if len(refs) > MaxProviderRequestAttachments {
		return nil, status.Error(codes.InvalidArgument, "too many consumed transient attachments")
	}
	placeholders := make([]string, 0, len(refs))
	args := []any{
		scope.GetWorkspaceId(),
		scope.GetSessionId(),
		scope.GetSessionThreadId(),
	}
	for i, ref := range refs {
		placeholders = append(placeholders, "$"+strconv.Itoa(i+4))
		args = append(args, ref)
	}
	rows, err := tx.Query(ctx,
		`SELECT attachment_ref, status
		   FROM session_transient_attachments
		  WHERE workspace_id = $1
		    AND session_id = $2
		    AND session_thread_id = $3
		    AND attachment_ref IN (`+strings.Join(placeholders, ",")+`)
		  FOR UPDATE`,
		args...,
	)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	found := make(map[string]struct{}, len(refs))
	active := make([]string, 0, len(refs))
	for rows.Next() {
		var ref, attachmentStatus string
		if err := rows.Scan(&ref, &attachmentStatus); err != nil {
			return nil, err
		}
		found[ref] = struct{}{}
		if attachmentStatus == "active" {
			active = append(active, ref)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	for _, ref := range refs {
		if _, ok := found[ref]; !ok {
			return nil, status.Error(codes.FailedPrecondition, "transient attachment cannot be consumed")
		}
	}
	return active, nil
}

// markTransientAttachmentsConsumedTx is Bridge's conversation-side transition
// site for the transient attachment status machine. Sandbox Service owns the
// uploading-to-staged handoff; Bridge owns activation and consumption:
//
//	status      meaning                                     transitions
//	uploading   bytes being written through blob APIs        -> staged | deleting
//	staged      committed by a tool, not yet model-visible    -> active | deleting
//	active      resolvable by the Gateway; may be consumed    -> consumed | deleting
//	consumed    spent by a settled-output request-end        (GC-eligible)
//	expired     TTL passed before consumption                (never written)
//	deleting    GC has claimed the row                       -> deleted
//	deleted     blob released                                 (terminal)
//	failed      write failed                                  (never written)
//
// The GC sweep reclaims deleting and consumed rows, plus expired uploading,
// staged, or active rows. Uploading and staged Sandbox attachments remain
// protected while their execution result is still unconsumed.
//
// The UPDATE flips 'active' -> 'consumed' ONLY (WHERE status = 'active'). A ref
// the caller already validated as present but in ANY non-active in-scope status
// (including uploading) settles as a status-preserving no-op, so a GC that raced
// ahead of a still-in-flight request can never roll back the request-end
// transaction; consumed-marking is bookkeeping, never a liveness gate on the
// settlement.
func markTransientAttachmentsConsumedTx(
	ctx context.Context,
	tx *dbconnect.Tx,
	scope *bridgev1.RuntimeScope,
	active []string,
	now time.Time,
) error {
	if len(active) == 0 {
		return nil
	}
	activePlaceholders := make([]string, 0, len(active))
	updateArgs := []any{
		now,
		scope.GetWorkspaceId(),
		scope.GetSessionId(),
		scope.GetSessionThreadId(),
	}
	for index, ref := range active {
		activePlaceholders = append(activePlaceholders, "$"+strconv.Itoa(index+5))
		updateArgs = append(updateArgs, ref)
	}
	_, err := tx.Exec(ctx,
		`UPDATE session_transient_attachments
		    SET status = 'consumed', updated_at = $1
		  WHERE workspace_id = $2
		    AND session_id = $3
		    AND session_thread_id = $4
		    AND status = 'active'
		    AND attachment_ref IN (`+strings.Join(activePlaceholders, ",")+`)`,
		updateArgs...,
	)
	if err != nil {
		return err
	}
	return nil
}
