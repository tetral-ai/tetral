package tetralsandbox

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"path"
	"strings"
	"time"

	"github.com/tetral-ai/tetral/internal/blob"
	"github.com/tetral-ai/tetral/internal/dbconnect"
)

const (
	SandboxTransientAttachmentMaxBytes = 10 * 1024 * 1024
	sandboxTransientAttachmentTTL      = 15 * time.Minute
)

var (
	errSandboxMediaCustodyConflict = errors.New("sandbox media custody identity conflicts with its execution")
	errSandboxMediaStateConflict   = errors.New("sandbox media custody did not enter staged state")
)

type PostgreSQLSandboxMediaMaterializer struct {
	client *dbconnect.Client
	blobs  blob.BlobStore
}

func NewPostgreSQLSandboxMediaMaterializer(client *dbconnect.Client, blobs blob.BlobStore) *PostgreSQLSandboxMediaMaterializer {
	return &PostgreSQLSandboxMediaMaterializer{client: client, blobs: blobs}
}

// RecoverResult closes the settlement window after media custody became
// durable but before the execution row reached its terminal state.
func (m *PostgreSQLSandboxMediaMaterializer) RecoverResult(ctx context.Context, ref SandboxExecutionRef) (SandboxMediaRecovery, error) {
	if m == nil || m.client == nil || m.blobs == nil {
		return SandboxMediaRecovery{}, errors.New("sandbox media materializer is unavailable")
	}
	attachmentRef := sandboxAttachmentRef(ref)
	var statusValue, storedSessionID, storedThreadID, storedSourceID, blobPointer, mime, metadataJSON string
	err := m.client.WithWorkspaceTx(ctx, ref.WorkspaceID, "sandbox.media.recover", func(tx *dbconnect.Tx) (txErr error) {
		defer finishSandboxQueueAuthorityTx(ctx, tx, &txErr)
		return tx.QueryRow(ctx,
			`SELECT session_id, session_thread_id, source_tool_use_event_id,
			        blob_pointer, mime, metadata_json, status
			   FROM session_transient_attachments
			  WHERE workspace_id=$1 AND attachment_ref=$2`,
			ref.WorkspaceID, attachmentRef,
		).Scan(&storedSessionID, &storedThreadID, &storedSourceID, &blobPointer, &mime, &metadataJSON, &statusValue)
	})
	if dbconnect.IsNoRows(err) {
		return SandboxMediaRecovery{}, nil
	}
	if err != nil {
		return SandboxMediaRecovery{}, err
	}
	if storedSessionID != ref.SessionID || storedThreadID != ref.SessionThreadID || storedSourceID != ref.ToolUseEventID {
		return SandboxMediaRecovery{}, errSandboxMediaCustodyConflict
	}
	if statusValue == "uploading" {
		return SandboxMediaRecovery{Found: true}, nil
	}
	if statusValue != "staged" || !validSandboxTransientAttachmentMIME(mime) {
		return SandboxMediaRecovery{}, errSandboxMediaStateConflict
	}
	var attachmentMetadata map[string]string
	if err := json.Unmarshal([]byte(metadataJSON), &attachmentMetadata); err != nil {
		return SandboxMediaRecovery{}, errSandboxMediaStateConflict
	}
	blobMetadata, err := m.blobs.HeadObject(ctx, blobPointer)
	if err != nil {
		var notFound *blob.NotFoundError
		if errors.As(err, &notFound) {
			return SandboxMediaRecovery{Found: true}, nil
		}
		return SandboxMediaRecovery{}, err
	}
	result := map[string]any{
		"attachment_ref":           attachmentRef,
		"mime":                     mime,
		"filename":                 attachmentMetadata["filename"],
		"source_path":              attachmentMetadata["source_path"],
		"source_tool_use_event_id": ref.ToolUseEventID,
		"detail":                   attachmentMetadata["detail"],
		"size_bytes":               blobMetadata.SizeBytes,
	}
	if pageRange := attachmentMetadata["page_range"]; pageRange != "" {
		result["page_range"] = pageRange
	}
	encoded, err := json.Marshal(map[string]any{"status": "success", "result": result})
	if err != nil {
		return SandboxMediaRecovery{}, err
	}
	return SandboxMediaRecovery{Found: true, Ready: true, ResultJSON: string(encoded)}, nil
}

// MaterializeResult replaces helper-returned media bytes with a scoped Blob
// reference. The durable attachment row is allocated before upload so failure
// always leaves an indexed cleanup owner; media bytes remain worker-local until
// the row reaches staged.
func (m *PostgreSQLSandboxMediaMaterializer) MaterializeResult(ctx context.Context, ref SandboxExecutionRef, toolName string, inputJSON string, raw string, now time.Time) (string, error) {
	if !sandboxToolCanReturnMedia(toolName) {
		return raw, nil
	}
	if m == nil || m.client == nil || m.blobs == nil {
		return "", errors.New("sandbox media materializer is unavailable")
	}
	var value map[string]any
	if err := json.Unmarshal([]byte(raw), &value); err != nil {
		return raw, nil
	}
	if sandboxString(value["status"]) != "success" {
		return raw, nil
	}
	source := value
	if result, ok := value["result"].(map[string]any); ok {
		source = result
	}
	if sandboxString(source["attachment_ref"]) != "" {
		return raw, nil
	}
	encodedBody := sandboxString(source["data_base64"])
	if encodedBody == "" {
		return raw, nil
	}
	body, err := base64.StdEncoding.DecodeString(encodedBody)
	if err != nil || len(body) == 0 || len(body) > SandboxTransientAttachmentMaxBytes {
		return "", errors.New("sandbox helper returned invalid media data")
	}
	mime := sandboxString(source["mime"])
	if !validSandboxTransientAttachmentMIME(mime) {
		return "", errors.New("sandbox helper returned an unsupported media type")
	}
	sourcePath, filename, err := sandboxMediaSource(inputJSON)
	if err != nil {
		return "", err
	}
	attachmentRef := sandboxAttachmentRef(ref)
	blobPointer := path.Join("transient-attachments", ref.WorkspaceID, ref.SessionID, attachmentRef)
	metadataJSON, err := json.Marshal(map[string]string{
		"filename": filename, "source_path": sourcePath,
		"page_range": sandboxString(source["page_range"]), "detail": "auto",
	})
	if err != nil {
		return "", err
	}
	statusValue := ""
	if err := m.client.WithWorkspaceTx(ctx, ref.WorkspaceID, "sandbox.media.ensure", func(tx *dbconnect.Tx) (txErr error) {
		defer finishSandboxQueueAuthorityTx(ctx, tx, &txErr)
		if _, err := tx.Exec(ctx,
			`INSERT INTO session_transient_attachments (
				workspace_id, attachment_ref, session_id, session_thread_id,
				source_tool_use_event_id, blob_pointer, mime, metadata_json,
				status, expires_at, created_at, updated_at
			) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,'uploading',$9,$10,$10)
			ON CONFLICT (workspace_id, attachment_ref) DO NOTHING`,
			ref.WorkspaceID, attachmentRef, ref.SessionID, ref.SessionThreadID,
			ref.ToolUseEventID, blobPointer, mime, string(metadataJSON), now.Add(sandboxTransientAttachmentTTL), now.UTC(),
		); err != nil {
			return err
		}
		var storedSessionID, storedThreadID, storedSourceID, storedPointer, storedMIME string
		if err := tx.QueryRow(ctx,
			`SELECT session_id, session_thread_id, source_tool_use_event_id, blob_pointer, mime, status
			   FROM session_transient_attachments
			  WHERE workspace_id=$1 AND attachment_ref=$2 FOR UPDATE`,
			ref.WorkspaceID, attachmentRef,
		).Scan(&storedSessionID, &storedThreadID, &storedSourceID, &storedPointer, &storedMIME, &statusValue); err != nil {
			return err
		}
		if storedSessionID != ref.SessionID || storedThreadID != ref.SessionThreadID || storedSourceID != ref.ToolUseEventID || storedPointer != blobPointer || storedMIME != mime || (statusValue != "uploading" && statusValue != "staged") {
			return errSandboxMediaCustodyConflict
		}
		return nil
	}); err != nil {
		if errors.Is(err, errSandboxMediaCustodyConflict) {
			return "", err
		}
		return "", err
	}
	if statusValue == "uploading" {
		if err := m.blobs.Put(ctx, blobPointer, bytes.NewReader(body), int64(len(body))); err != nil {
			var duplicate *blob.DuplicateKeyError
			if !errors.As(err, &duplicate) {
				return "", err
			}
			metadata, headErr := m.blobs.HeadObject(ctx, blobPointer)
			if headErr != nil {
				return "", headErr
			}
			if metadata.SizeBytes != int64(len(body)) {
				return "", errors.New("sandbox media Blob custody could not be verified")
			}
		}
		if err := m.client.WithWorkspaceTx(ctx, ref.WorkspaceID, "sandbox.media.stage", func(tx *dbconnect.Tx) (txErr error) {
			defer finishSandboxQueueAuthorityTx(ctx, tx, &txErr)
			result, err := tx.Exec(ctx,
				`UPDATE session_transient_attachments
				    SET status='staged', updated_at=$3
				  WHERE workspace_id=$1 AND attachment_ref=$2 AND status='uploading'`,
				ref.WorkspaceID, attachmentRef, now.UTC(),
			)
			if err != nil {
				return err
			}
			if !transitionRowsAffected(result) {
				return errSandboxMediaStateConflict
			}
			return nil
		}); err != nil {
			if errors.Is(err, errSandboxMediaStateConflict) {
				return "", err
			}
			return "", err
		}
	}
	delete(source, "data_base64")
	source["attachment_ref"] = attachmentRef
	source["filename"] = filename
	source["source_tool_use_event_id"] = ref.ToolUseEventID
	source["source_path"] = sourcePath
	source["detail"] = "auto"
	encoded, err := json.Marshal(value)
	if err != nil {
		return "", err
	}
	return string(encoded), nil
}

func sandboxToolCanReturnMedia(toolName string) bool {
	return toolName == "view_image" || toolName == "Read" || toolName == "read"
}

func validSandboxTransientAttachmentMIME(mime string) bool {
	switch mime {
	case "application/pdf", "image/png", "image/jpeg", "image/gif", "image/webp":
		return true
	default:
		return false
	}
}

func sandboxMediaSource(inputJSON string) (string, string, error) {
	var input map[string]any
	if err := json.Unmarshal([]byte(inputJSON), &input); err != nil {
		return "", "", errors.New("sandbox media input must be JSON")
	}
	sourcePath := sandboxString(input["path"])
	if sourcePath == "" {
		sourcePath = sandboxString(input["file_path"])
	}
	if sourcePath == "" {
		sourcePath = "image"
	}
	filename := path.Base(strings.TrimSpace(sourcePath))
	if filename == "." || filename == "/" || filename == "" {
		filename = "attachment"
	}
	return sourcePath, filename, nil
}

func sandboxAttachmentRef(ref SandboxExecutionRef) string {
	digest := sha256.Sum256([]byte(ref.WorkspaceID + "\x00" + ref.SessionID + "\x00" + ref.SessionThreadID + "\x00" + ref.ToolUseEventID))
	return "att_" + hex.EncodeToString(digest[:16])
}

func sandboxString(value any) string {
	result, _ := value.(string)
	return result
}
