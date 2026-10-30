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

// Store stores data, of mediaType, as an attachment of thread threadID of
// sessionID in tx and returns its reference. Under the advisory lock of its
// content it puts the bytes at their blob key, an existing object counting
// as stored, and inserts the row.
func Store(ctx context.Context, tx *dbconnect.Tx, blobs blob.BlobStore, ws workspace.ID, sessionID, threadID, mediaType string, data []byte) (Reference, error) {
	if !mediaTypes[mediaType] {
		return Reference{}, fmt.Errorf("imagestore: unsupported media type %q", mediaType)
	}
	hash := contentHash(data)
	if err := lockContent(ctx, tx, string(ws), hash); err != nil {
		return Reference{}, err
	}
	if err := blobs.Put(ctx, blobKey(string(ws), hash), bytes.NewReader(data), int64(len(data))); err != nil {
		var duplicate *blob.DuplicateKeyError
		if !errors.As(err, &duplicate) {
			return Reference{}, err
		}
	}
	reference := Reference{AttachmentID: id.New("att_"), MediaType: mediaType, SizeBytes: int64(len(data))}
	_, err := tx.Exec(ctx, `INSERT INTO session_attachments (workspace_id,attachment_id,session_id,session_thread_id,media_type,size_bytes,content_sha256)
		VALUES ($1,$2,$3,$4,$5,$6,$7)`, string(ws), reference.AttachmentID, sessionID, threadID, mediaType, reference.SizeBytes, hash)
	if err != nil {
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
// whose rows or blob it could not delete.
type CollectResult struct {
	Deleted, Failed int
}

// CollectExpired deletes at most limit rows past lifetime, across
// Workspaces, and the blob of each of their contents that no row of its
// Workspace names any more. Each content's expired rows and its blob go in
// one transaction of its Workspace under the content's lock, so a pass that
// stops midway leaves every unreleased blob still named by a row.
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
	return result, nil
}

// DeleteSession deletes every row of sessionID and the blob of each of its
// contents that no other row of ws names, one content per transaction under
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

// releaseBlob deletes the blob of contentSHA256 when no row of workspaceID
// names it; the caller holds the content's lock.
func releaseBlob(ctx context.Context, tx *dbconnect.Tx, blobs blob.BlobStore, workspaceID, contentSHA256 string) error {
	var named bool
	if err := tx.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM session_attachments WHERE workspace_id=$1 AND content_sha256=$2)`, workspaceID, contentSHA256).Scan(&named); err != nil {
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
