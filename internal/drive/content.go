package drive

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"io"
	"os"
	"path/filepath"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/id"
	"github.com/tetral-ai/tetral/internal/workspace"
)

// Content is stored content of a drive.
type Content struct {
	SHA256    string
	SizeBytes int64
}

// PutContent stores a body as content of the drive. size is the declared
// length, or negative when unknown; expectedSHA256 is the hash the caller
// declared, or "". Content the drive already holds is kept once: the new blob
// is left for the cleanup to delete.
func (s *Service) PutContent(ctx context.Context, ws workspace.ID, driveID string, body io.Reader, size int64, expectedSHA256 string) (Content, error) {
	if size > MaxFileBytes {
		return Content{}, ErrContentTooLarge
	}
	if size < 0 {
		staged, err := s.stage(body)
		if err != nil {
			return Content{}, err
		}
		defer staged.remove()
		file, err := staged.open()
		if err != nil {
			return Content{}, err
		}
		defer func() { _ = file.Close() }()
		body, size = file, staged.size
	}
	key := fmt.Sprintf("drives/%s/%s/%s", ws, driveID, id.New(""))
	// The cleanup entry exists before the blob does, so a crash at any later
	// step leaves only an entry the daily cleanup deletes.
	if err := s.db.WithWorkspaceTx(ctx, string(ws), "drive.content_reserve", func(tx *dbconnect.Tx) error {
		_, err := tx.ExecContext(ctx, `INSERT INTO drive_blob_cleanup(workspace_id,blob_key,delete_after) VALUES ($1,$2,clock_timestamp()+`+uploadGraceInterval+`)`, string(ws), key)
		return err
	}); err != nil {
		return Content{}, err
	}
	counted := &countingReader{reader: body, limit: size, hash: sha256.New()}
	if err := s.blobs.Put(ctx, key, counted, size); err != nil {
		return Content{}, err
	}
	if counted.count != size || counted.over {
		return Content{}, s.discardBlob(ctx, ws, key, errors.New("drive: content length differs from its declared size"))
	}
	content := Content{SHA256: hex.EncodeToString(counted.hash.Sum(nil)), SizeBytes: size}
	if expectedSHA256 != "" && expectedSHA256 != content.SHA256 {
		return Content{}, s.discardBlob(ctx, ws, key, ErrContentHashMismatch)
	}
	err := s.db.WithWorkspaceTx(ctx, string(ws), "drive.content_record", func(tx *dbconnect.Tx) error {
		var active bool
		if err := tx.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM drives WHERE workspace_id=$1 AND drive_id=$2 AND deleted_at IS NULL)`, string(ws), driveID).Scan(&active); err != nil {
			return err
		}
		if !active {
			return ErrDriveNotFound
		}
		inserted, err := tx.ExecContext(ctx, `INSERT INTO drive_contents(workspace_id,drive_id,content_sha256,size_bytes,blob_key,uploaded_at)
			VALUES ($1,$2,$3,$4,$5,clock_timestamp()) ON CONFLICT (workspace_id,drive_id,content_sha256) DO NOTHING`,
			string(ws), driveID, content.SHA256, content.SizeBytes, key)
		if err != nil {
			return err
		}
		if count, err := inserted.RowsAffected(); err != nil {
			return err
		} else if count == 1 {
			_, err = tx.ExecContext(ctx, `DELETE FROM drive_blob_cleanup WHERE workspace_id=$1 AND blob_key=$2`, string(ws), key)
			return err
		}
		if _, err := tx.ExecContext(ctx, `UPDATE drive_contents SET uploaded_at=clock_timestamp() WHERE workspace_id=$1 AND drive_id=$2 AND content_sha256=$3`,
			string(ws), driveID, content.SHA256); err != nil {
			return err
		}
		_, err = tx.ExecContext(ctx, `UPDATE drive_blob_cleanup SET delete_after=clock_timestamp() WHERE workspace_id=$1 AND blob_key=$2`, string(ws), key)
		return err
	})
	if err != nil {
		return Content{}, err
	}
	return content, nil
}

// discardBlob deletes a blob no content row will name, and its cleanup entry;
// when the delete fails, the entry stays for the daily cleanup.
func (s *Service) discardBlob(ctx context.Context, ws workspace.ID, key string, cause error) error {
	if err := s.blobs.Delete(ctx, key); err != nil {
		return cause
	}
	_ = s.db.WithWorkspaceTx(ctx, string(ws), "drive.content_discard", func(tx *dbconnect.Tx) error {
		_, err := tx.ExecContext(ctx, `DELETE FROM drive_blob_cleanup WHERE workspace_id=$1 AND blob_key=$2`, string(ws), key)
		return err
	})
	return cause
}

// countingReader hashes and counts what it passes on, and stops past limit.
type countingReader struct {
	reader io.Reader
	limit  int64
	count  int64
	over   bool
	hash   hash.Hash
}

func (r *countingReader) Read(p []byte) (int, error) {
	n, err := r.reader.Read(p)
	r.count += int64(n)
	if r.count > r.limit {
		r.over = true
		return 0, errors.New("drive: content longer than its declared size")
	}
	r.hash.Write(p[:n])
	return n, err
}

// stagedBody is a body of unknown size, copied to a server-named file in the
// service's data directory.
type stagedBody struct {
	path string
	size int64
}

func newStageDir(dataDir string) (string, error) {
	if dataDir == "" {
		return "", errors.New("drive: data directory is required")
	}
	dir := filepath.Join(dataDir, "drive-upload-stage")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return "", fmt.Errorf("drive: create stage directory: %w", err)
	}
	return dir, nil
}

func (s *Service) stage(body io.Reader) (*stagedBody, error) {
	file, err := os.CreateTemp(s.stageDir, "upload-*")
	if err != nil {
		return nil, fmt.Errorf("drive: stage upload: %w", err)
	}
	staged := &stagedBody{path: file.Name()}
	written, copyErr := io.Copy(file, io.LimitReader(body, MaxFileBytes+1))
	closeErr := file.Close()
	switch {
	case copyErr != nil:
		staged.remove()
		return nil, fmt.Errorf("drive: read upload: %w", copyErr)
	case closeErr != nil:
		staged.remove()
		return nil, closeErr
	case written > MaxFileBytes:
		staged.remove()
		return nil, ErrContentTooLarge
	}
	staged.size = written
	return staged, nil
}

func (b *stagedBody) open() (*os.File, error) {
	return os.Open(b.path) //nolint:gosec // G304: the path is server-named in the stage directory.
}

func (b *stagedBody) remove() { _ = os.Remove(b.path) }
