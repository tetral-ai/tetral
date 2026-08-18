package files

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"

	"github.com/tetral-ai/tetral/internal/workspace"
)

func TestEventAttachmentValidationLocksFilesThenObjectsInStableOrder(t *testing.T) {
	tx := &attachmentLockRecordingTransaction{
		files: map[string]eventAttachmentFileRecord{
			"file_a": {objectID: "object_z", mimeType: "image/png"},
			"file_b": {objectID: "object_a", mimeType: "image/png"},
			"file_z": {objectID: "object_a", mimeType: "image/png"},
		},
		objects: map[string]eventAttachmentObjectRecord{
			"object_a": {sizeBytes: 1, blobKey: "files/wksp/object_a"},
			"object_z": {sizeBytes: 1, blobKey: "files/wksp/object_z"},
		},
	}
	store := &PostgreSQLFileStore{}
	err := store.ValidateEventAttachments(
		context.Background(),
		tx,
		workspace.DefaultID,
		"sesn_attachment_locks",
		[]EventAttachmentReference{
			{BlockType: "image", FileID: "file_z"},
			{BlockType: "image", FileID: "file_a"},
			{BlockType: "image", FileID: "file_z"},
			{BlockType: "image", FileID: "file_b"},
		},
	)
	if err != nil {
		t.Fatalf("ValidateEventAttachments: %v", err)
	}
	want := []string{
		"file:file_a",
		"file:file_b",
		"file:file_z",
		"object:object_a",
		"object:object_z",
	}
	if fmt.Sprint(tx.lockOrder) != fmt.Sprint(want) {
		t.Fatalf("lock order = %#v; want %#v", tx.lockOrder, want)
	}
}

type attachmentLockRecordingTransaction struct {
	files     map[string]eventAttachmentFileRecord
	objects   map[string]eventAttachmentObjectRecord
	lockOrder []string
}

func (t *attachmentLockRecordingTransaction) ExecContext(context.Context, string, ...any) (interface {
	RowsAffected() (int64, error)
}, error) {
	return nil, fmt.Errorf("unexpected ExecContext")
}

func (t *attachmentLockRecordingTransaction) Query(context.Context, string, ...any) (Rows, error) {
	return nil, fmt.Errorf("unexpected Query")
}

func (t *attachmentLockRecordingTransaction) QueryRow(_ context.Context, query string, args ...any) Row {
	id, _ := args[1].(string)
	switch {
	case strings.Contains(query, "FROM files"):
		t.lockOrder = append(t.lockOrder, "file:"+id)
		record, ok := t.files[id]
		if !ok {
			return attachmentLockRow{err: sql.ErrNoRows}
		}
		return attachmentLockRow{values: []any{record.objectID, record.mimeType}}
	case strings.Contains(query, "FROM file_objects"):
		t.lockOrder = append(t.lockOrder, "object:"+id)
		record, ok := t.objects[id]
		if !ok {
			return attachmentLockRow{err: sql.ErrNoRows}
		}
		return attachmentLockRow{values: []any{record.sizeBytes, record.pdfPageCount, record.blobKey}}
	default:
		return attachmentLockRow{err: fmt.Errorf("unexpected query: %s", query)}
	}
}

type attachmentLockRow struct {
	values []any
	err    error
}

func (r attachmentLockRow) Scan(dest ...any) error {
	if r.err != nil {
		return r.err
	}
	if len(dest) != len(r.values) {
		return fmt.Errorf("scan destinations = %d; want %d", len(dest), len(r.values))
	}
	for index, value := range r.values {
		switch target := dest[index].(type) {
		case *string:
			*target = value.(string)
		case *int64:
			*target = value.(int64)
		case *sql.NullInt64:
			*target = value.(sql.NullInt64)
		default:
			return fmt.Errorf("unsupported scan destination %T", dest[index])
		}
	}
	return nil
}
