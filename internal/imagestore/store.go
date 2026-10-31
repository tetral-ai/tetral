package imagestore

import (
	"crypto/sha256"
	"encoding/hex"
	"hash/fnv"
)

// Reference is what a caller keeps of a stored attachment.
type Reference struct {
	AttachmentID string `json:"attachment_id"`
	MediaType    string `json:"media_type"`
	SizeBytes    int64  `json:"size_bytes"`
}

// Uploaded is an upload whose bytes are stored and whose entry keeps them
// until Store records it in the caller's transaction.
type Uploaded struct {
	UploadID      string
	ContentSHA256 string
	MediaType     string
	SizeBytes     int64
}

// Attachment is a resolved attachment's media type and bytes.
type Attachment struct {
	MediaType string
	Data      []byte
}

// mediaTypes are the media types an attachment may have.
var mediaTypes = map[string]bool{
	"image/jpeg":      true,
	"image/png":       true,
	"image/gif":       true,
	"image/webp":      true,
	"application/pdf": true,
	"text/plain":      true,
}

// SupportedMediaType reports whether an attachment may have mediaType.
func SupportedMediaType(mediaType string) bool { return mediaTypes[mediaType] }

// contentHash is the hex SHA-256 of data.
func contentHash(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

// blobKey is where the bytes of contentSHA256 in workspaceID live.
func blobKey(workspaceID, contentSHA256 string) string {
	return "attachments/" + workspaceID + "/" + contentSHA256
}

// contentLockCategory partitions the advisory locks of attachment contents.
const contentLockCategory int32 = 0x6174_6368 // "atch"

// contentLockResource is the advisory lock resource of contentSHA256 in
// workspaceID; two contents that share one only wait for each other.
func contentLockResource(workspaceID, contentSHA256 string) int32 {
	h := fnv.New32a()
	_, _ = h.Write([]byte(workspaceID))
	_, _ = h.Write([]byte{0})
	_, _ = h.Write([]byte(contentSHA256))
	return int32(h.Sum32()) //nolint:gosec // a lock key; wrapping is intended
}
