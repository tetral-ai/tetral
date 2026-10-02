package integration

import (
	"context"
	"database/sql"
	"strings"
	"testing"

	"github.com/tetral-ai/tetral/internal/blob"
)

func walkPostgreSQLPlan(node map[string]any, visit func(map[string]any)) {
	visit(node)
	children, _ := node["Plans"].([]any)
	for _, child := range children {
		childNode, _ := child.(map[string]any)
		if childNode != nil {
			walkPostgreSQLPlan(childNode, visit)
		}
	}
}

func seedBridgeAPIFileAttachment(t *testing.T, db *sql.DB, blobStore blob.BlobStore, fileID, filename, mime, body string) {
	t.Helper()
	objectID := "fobj_" + fileID
	blobKey := "files/default/" + objectID
	if _, err := db.ExecContext(context.Background(),
		`INSERT INTO file_objects (workspace_id, object_id, blob_key, size_bytes, sha256, created_at)
		 VALUES ('default', $1, $2, $3, $4, '2026-01-01T00:00:00Z')`,
		objectID, blobKey, len(body), strings.Repeat("a", 64)); err != nil {
		t.Fatalf("seed file object %s: %v", fileID, err)
	}
	if _, err := db.ExecContext(context.Background(),
		`INSERT INTO files (workspace_id, file_id, object_id, filename, mime_type, downloadable, created_at)
		 VALUES ('default', $1, $2, $3, $4, TRUE, '2026-01-01T00:00:00Z')`,
		fileID, objectID, filename, mime); err != nil {
		t.Fatalf("seed file %s: %v", fileID, err)
	}
	if err := blobStore.Put(context.Background(), blobKey, strings.NewReader(body), int64(len(body))); err != nil {
		t.Fatalf("seed file blob %s: %v", fileID, err)
	}
}

func seedBridgeAPIProjectedUserMessage(t *testing.T, db *sql.DB, sessionID, threadID, messageID, sourceEventID string, sequence int64) {
	t.Helper()
	dataJSON := `{"parts":[]}`
	if _, err := db.ExecContext(context.Background(),
		`INSERT INTO session_messages (
			workspace_id, session_id, session_thread_id, message_id, sequence, kind,
			data_json, source_event_id, created_at, updated_at
		) VALUES ('default', $1, $2, $3, $4, 'user', $5, $6, '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z')`,
		sessionID, threadID, messageID, sequence, dataJSON, sourceEventID); err != nil {
		t.Fatalf("seed projected user message %s: %v", sourceEventID, err)
	}
}
