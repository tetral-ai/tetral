package imagestore

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/tetral-ai/tetral/internal/blob"
	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/id"
	"github.com/tetral-ai/tetral/internal/workspace"
)

// uploadCheckDelay is how long after an upload its entry keeps the bytes
// for a Store that has not committed.
const uploadCheckDelay = time.Hour

// Upload puts data, of mediaType, in the blob store of ws before the caller's
// transaction. Its own transaction, under the advisory lock of the content,
// records an upload entry that keeps the bytes until uploadCheckDelay after
// the database clock, and commits; it then puts the bytes at their blob key,
// an existing object counting as stored. A failed put leaves the entry to
// the collection.
func Upload(ctx context.Context, client *dbconnect.Client, blobs blob.BlobStore, ws workspace.ID, mediaType string, data []byte) (Uploaded, error) {
	if !mediaTypes[mediaType] {
		return Uploaded{}, fmt.Errorf("imagestore: unsupported media type %q", mediaType)
	}
	upload := Uploaded{UploadID: id.New("atu_"), ContentSHA256: contentHash(data), MediaType: mediaType, SizeBytes: int64(len(data))}
	if err := client.WithWorkspaceTx(ctx, string(ws), "imagestore.upload", func(tx *dbconnect.Tx) error {
		if err := lockContent(ctx, tx, string(ws), upload.ContentSHA256); err != nil {
			return err
		}
		_, err := tx.Exec(ctx, `INSERT INTO session_attachment_uploads (workspace_id,upload_id,content_sha256,check_after)
			VALUES ($1,$2,$3,now()+make_interval(secs => $4))`, string(ws), upload.UploadID, upload.ContentSHA256, uploadCheckDelay.Seconds())
		return err
	}); err != nil {
		return Uploaded{}, err
	}
	if err := blobs.Put(ctx, blobKey(string(ws), upload.ContentSHA256), bytes.NewReader(data), upload.SizeBytes); err != nil {
		var duplicate *blob.DuplicateKeyError
		if !errors.As(err, &duplicate) {
			return Uploaded{}, err
		}
	}
	return upload, nil
}

// Store records upload as an attachment of thread threadID of sessionID in
// tx, the caller's transaction, and returns its reference: it inserts the row
// and deletes the upload's entry, so the row takes over keeping the bytes
// when tx commits. It takes no content lock.
func Store(ctx context.Context, tx *dbconnect.Tx, ws workspace.ID, upload Uploaded, sessionID, threadID string) (Reference, error) {
	reference := Reference{AttachmentID: id.New("att_"), MediaType: upload.MediaType, SizeBytes: upload.SizeBytes}
	if _, err := tx.Exec(ctx, `INSERT INTO session_attachments (workspace_id,attachment_id,session_id,session_thread_id,media_type,size_bytes,content_sha256)
		VALUES ($1,$2,$3,$4,$5,$6,$7)`, string(ws), reference.AttachmentID, sessionID, threadID, upload.MediaType, upload.SizeBytes, upload.ContentSHA256); err != nil {
		return Reference{}, err
	}
	if _, err := tx.Exec(ctx, `DELETE FROM session_attachment_uploads WHERE workspace_id=$1 AND upload_id=$2`, string(ws), upload.UploadID); err != nil {
		return Reference{}, err
	}
	return reference, nil
}

// Resolve reads, in tx, attachmentID of sessionID while it is live, and its
// bytes. found is false for an unknown ID, another Session's attachment and
// an expired one alike.
func Resolve(ctx context.Context, tx *dbconnect.Tx, blobs blob.BlobStore, ws workspace.ID, sessionID, attachmentID string, lifetime time.Duration) (attachment Attachment, found bool, err error) {
	var hash string
	err = tx.QueryRow(ctx, `SELECT media_type, content_sha256 FROM session_attachments
		WHERE workspace_id=$1 AND attachment_id=$2 AND session_id=$3 AND created_at + make_interval(secs => $4) > clock_timestamp()`,
		string(ws), attachmentID, sessionID, lifetime.Seconds()).Scan(&attachment.MediaType, &hash)
	if errors.Is(err, sql.ErrNoRows) {
		return Attachment{}, false, nil
	}
	if err != nil {
		return Attachment{}, false, err
	}
	reader, err := blobs.Get(ctx, blobKey(string(ws), hash))
	var notFound *blob.NotFoundError
	if errors.As(err, &notFound) {
		// The row expired after the read and its bytes were collected.
		return Attachment{}, false, nil
	}
	if err != nil {
		return Attachment{}, false, err
	}
	defer func() { _ = reader.Close() }()
	if attachment.Data, err = io.ReadAll(reader); err != nil {
		return Attachment{}, false, err
	}
	return attachment, true, nil
}

// CollectResult counts what one collection pass deleted: rows, and contents
// or upload entries it could not release.
type CollectResult struct {
	Deleted, Failed int
}

// CollectExpired deletes at most limit rows past lifetime, across
// Workspaces, and the blob of each of their contents that no row or upload
// entry of its Workspace names any more. Each content's expired rows and its
// blob go in one transaction of its Workspace under the content's lock, so a
// pass that stops midway leaves every unreleased blob still named. It then
// releases at most limit upload entries past check_after, oldest first, the
// same way.
func CollectExpired(ctx context.Context, client *dbconnect.Client, blobs blob.BlobStore, lifetime time.Duration, limit int) (CollectResult, error) {
	type content struct{ workspaceID, hash string }
	expired := map[content][]string{}
	var order []content
	err := client.WithTx(ctx, "imagestore.collect_expired", nil, func(tx *dbconnect.Tx) error {
		if _, err := tx.Exec(ctx, "SELECT set_config('tetral.attachment_gc', 'true', true)"); err != nil {
			return err
		}
		rows, err := tx.Query(ctx, `SELECT workspace_id, content_sha256, attachment_id FROM session_attachments
			WHERE created_at + make_interval(secs => $1) <= clock_timestamp() ORDER BY created_at LIMIT $2`, lifetime.Seconds(), limit)
		if err != nil {
			return err
		}
		defer func() { _ = rows.Close() }()
		for rows.Next() {
			var c content
			var attachmentID string
			if err := rows.Scan(&c.workspaceID, &c.hash, &attachmentID); err != nil {
				return err
			}
			if _, seen := expired[c]; !seen {
				order = append(order, c)
			}
			expired[c] = append(expired[c], attachmentID)
		}
		return rows.Err()
	})
	if err != nil {
		return CollectResult{}, err
	}
	var result CollectResult
	for _, c := range order {
		var deleted int64
		err := client.WithWorkspaceTx(ctx, c.workspaceID, "imagestore.release_expired", func(tx *dbconnect.Tx) error {
			if err := lockContent(ctx, tx, c.workspaceID, c.hash); err != nil {
				return err
			}
			tag, err := tx.Exec(ctx, `DELETE FROM session_attachments WHERE workspace_id=$1 AND attachment_id=ANY($2)`, c.workspaceID, expired[c])
			if err != nil {
				return err
			}
			if deleted, err = tag.RowsAffected(); err != nil {
				return err
			}
			return releaseBlob(ctx, tx, blobs, c.workspaceID, c.hash)
		})
		if ctx.Err() != nil {
			return result, ctx.Err()
		}
		if err != nil {
			result.Failed++
			continue
		}
		result.Deleted += int(deleted)
	}
	failed, err := releaseDueUploads(ctx, client, blobs, limit)
	result.Failed += failed
	return result, err
}

// releaseDueUploads releases at most limit upload entries past check_after,
// across Workspaces, oldest first, each in a transaction of its Workspace
// under its content's lock that deletes the entry and the blob no row or
// other entry names. It returns how many it could not release.
func releaseDueUploads(ctx context.Context, client *dbconnect.Client, blobs blob.BlobStore, limit int) (int, error) {
	type due struct{ workspaceID, uploadID, hash string }
	var dues []due
	err := client.WithTx(ctx, "imagestore.due_uploads", nil, func(tx *dbconnect.Tx) error {
		if _, err := tx.Exec(ctx, "SELECT set_config('tetral.attachment_gc', 'true', true)"); err != nil {
			return err
		}
		rows, err := tx.Query(ctx, `SELECT workspace_id, upload_id, content_sha256 FROM session_attachment_uploads
			WHERE check_after <= clock_timestamp() ORDER BY check_after LIMIT $1`, limit)
		if err != nil {
			return err
		}
		defer func() { _ = rows.Close() }()
		for rows.Next() {
			var d due
			if err := rows.Scan(&d.workspaceID, &d.uploadID, &d.hash); err != nil {
				return err
			}
			dues = append(dues, d)
		}
		return rows.Err()
	})
	if err != nil {
		return 0, err
	}
	failed := 0
	for _, d := range dues {
		err := client.WithWorkspaceTx(ctx, d.workspaceID, "imagestore.release_upload", func(tx *dbconnect.Tx) error {
			if err := lockContent(ctx, tx, d.workspaceID, d.hash); err != nil {
				return err
			}
			if _, err := tx.Exec(ctx, `DELETE FROM session_attachment_uploads WHERE workspace_id=$1 AND upload_id=$2`, d.workspaceID, d.uploadID); err != nil {
				return err
			}
			return releaseBlob(ctx, tx, blobs, d.workspaceID, d.hash)
		})
		if ctx.Err() != nil {
			return failed, ctx.Err()
		}
		if err != nil {
			failed++
		}
	}
	return failed, nil
}

// DeleteSession deletes every row of sessionID and the blob of each of its
// contents that no other row or upload entry of ws names, one content per
// transaction under
// its lock, so a deletion that stops midway leaves the rest for its retry.
func DeleteSession(ctx context.Context, client *dbconnect.Client, blobs blob.BlobStore, ws workspace.ID, sessionID string) error {
	var hashes []string
	err := client.WithWorkspaceReadOnlyTx(ctx, string(ws), "imagestore.session_contents", func(tx *dbconnect.Tx) error {
		rows, err := tx.Query(ctx, `SELECT DISTINCT content_sha256 FROM session_attachments WHERE workspace_id=$1 AND session_id=$2`, string(ws), sessionID)
		if err != nil {
			return err
		}
		defer func() { _ = rows.Close() }()
		for rows.Next() {
			var hash string
			if err := rows.Scan(&hash); err != nil {
				return err
			}
			hashes = append(hashes, hash)
		}
		return rows.Err()
	})
	if err != nil {
		return err
	}
	for _, hash := range hashes {
		if err := client.WithWorkspaceTx(ctx, string(ws), "imagestore.delete_session_content", func(tx *dbconnect.Tx) error {
			if err := lockContent(ctx, tx, string(ws), hash); err != nil {
				return err
			}
			if _, err := tx.Exec(ctx, `DELETE FROM session_attachments WHERE workspace_id=$1 AND session_id=$2 AND content_sha256=$3`, string(ws), sessionID, hash); err != nil {
				return err
			}
			return releaseBlob(ctx, tx, blobs, string(ws), hash)
		}); err != nil {
			return err
		}
	}
	return nil
}

// lockContent takes the advisory lock of contentSHA256 in workspaceID for the
// rest of tx.
func lockContent(ctx context.Context, tx *dbconnect.Tx, workspaceID, contentSHA256 string) error {
	_, err := tx.Exec(ctx, "SELECT pg_advisory_xact_lock($1, $2)", contentLockCategory, contentLockResource(workspaceID, contentSHA256))
	return err
}

// releaseBlob deletes the blob of contentSHA256 when no row and no upload
// entry of workspaceID names it; the caller holds the content's lock. One
// statement reads both, so a Store committing meanwhile is seen either
// before, through its entry, or after, through its row.
func releaseBlob(ctx context.Context, tx *dbconnect.Tx, blobs blob.BlobStore, workspaceID, contentSHA256 string) error {
	var named bool
	if err := tx.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM session_attachments WHERE workspace_id=$1 AND content_sha256=$2)
		OR EXISTS (SELECT 1 FROM session_attachment_uploads WHERE workspace_id=$1 AND content_sha256=$2)`, workspaceID, contentSHA256).Scan(&named); err != nil {
		return err
	}
	if named {
		return nil
	}
	if blobs == nil {
		return errors.New("imagestore: blob store is unavailable")
	}
	err := blobs.Delete(ctx, blobKey(workspaceID, contentSHA256))
	var notFound *blob.NotFoundError
	if errors.As(err, &notFound) {
		return nil
	}
	return err
}
