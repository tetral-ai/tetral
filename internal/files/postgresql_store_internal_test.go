package files

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"path/filepath"
	"strings"
	"testing"
	"testing/iotest"

	"github.com/tetral-ai/tetral/internal/blob"
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

// A legacy PDF that cannot be staged for counting fails with the error that
// stopped it, which the request's failure record carries as its cause.
func TestCountStoredPDFKeepsTheErrorThatStoppedIt(t *testing.T) {
	readFailure := errors.New("blob stream reset")
	for _, test := range []struct {
		name   string
		source io.Reader
		tmpDir string
		want   error
	}{
		{name: "the staging file", source: strings.NewReader("%PDF"), tmpDir: filepath.Join(t.TempDir(), "missing"), want: fs.ErrNotExist},
		{name: "the stored bytes", source: iotest.ErrReader(readFailure), tmpDir: t.TempDir(), want: readFailure},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Setenv("TMPDIR", test.tmpDir)
			store := &PostgreSQLFileStore{blob: readerBlobStore{source: test.source}}
			_, err := store.countStoredPDF(context.Background(), "files/wksp/object_a", 4)
			if !errors.Is(err, test.want) {
				t.Fatalf("countStoredPDF error = %v; want %v in its chain", err, test.want)
			}
		})
	}
}

type readerBlobStore struct {
	blob.BlobStore
	source io.Reader
}

func (s readerBlobStore) Get(context.Context, string) (io.ReadCloser, error) {
	return io.NopCloser(s.source), nil
}
