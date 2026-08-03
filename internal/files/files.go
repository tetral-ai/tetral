package files

import (
	"context"
	"io"
	"time"

	"github.com/tetral-ai/tetral/internal/workspace"
)

// IDPrefix is the stable prefix every public Files API identity uses.
const IDPrefix = "file_"

// ObjectIDPrefix is the stable prefix for internal stored-byte
// identities. Object ids are not public API file ids.
const ObjectIDPrefix = "fobj_"

// FileMetadata is the JSON-compatible Files metadata envelope.
type FileMetadata struct {
	ID           string     `json:"id"`
	Type         string     `json:"type"`
	Filename     string     `json:"filename"`
	MIMEType     string     `json:"mime_type"`
	SizeBytes    int64      `json:"size_bytes"`
	CreatedAt    time.Time  `json:"created_at"`
	Downloadable bool       `json:"downloadable"`
	Scope        *FileScope `json:"scope,omitempty"`
}

// FileScope is present on future session-scoped file identities.
type FileScope struct {
	Type string `json:"type"`
	ID   string `json:"id"`
}

// DeleteResponse is the public delete envelope for file identities.
type DeleteResponse struct {
	ID   string `json:"id"`
	Type string `json:"type"`
}

// ListOptions carries Files cursor/filter inputs.
type ListOptions struct {
	Limit    int
	AfterID  string
	BeforeID string
	ScopeID  string
}

// ListResult is the Files-specific list envelope.
type ListResult struct {
	Data    []*FileMetadata `json:"data"`
	FirstID *string         `json:"first_id"`
	LastID  *string         `json:"last_id"`
	HasMore bool            `json:"has_more"`
}

// ContentStream is the public downloadable content stream.
type ContentStream struct {
	Metadata *FileMetadata
	Reader   io.ReadCloser
}

// Source is the internal non-HTTP resolver output for future session
// copy code. Open streams bytes from BlobStore when called.
type Source struct {
	Metadata *FileMetadata
	Open     func(ctx context.Context) (io.ReadCloser, error)
}

// MountSource is the internal resolver output for Sandbox resource
// materialization. ObjectKey is storage-internal and must never appear in
// public HTTP DTOs.
type MountSource struct {
	WorkspaceID workspace.ID
	FileID      string
	ObjectKey   string
	SizeBytes   int64
	SHA256      string
}

type SessionFileIdentityRequest struct {
	SourceFileID  string
	SessionID     string
	SessionFileID string
	CreatedAt     time.Time
}

// EventAttachmentReference is the Files-owned admission input for one
// user.message media block. BlockType is "image" or "document".
type EventAttachmentReference struct {
	BlockType string
	FileID    string
}
