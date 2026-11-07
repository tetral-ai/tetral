package drive

import (
	"context"
	"database/sql"
	"errors"
	"io"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/protocol"
	"github.com/tetral-ai/tetral/internal/workspace"
)

// The drive transfer reads of computer-hub. Each list page holds at most
// protocol.ListPageMaxItems entries, and fewer when the next entry would take
// its compact JSON body past protocol.ListPageMaxJSONBytes.

// Notice is the payload of NoticeChannel.
type Notice struct {
	WorkspaceID string `json:"workspace_id"`
	DriveID     string `json:"drive_id"`
	Seq         int64  `json:"seq"`
}

// LastSeqs is, for each live drive of driveIDs that has a commit, a
// drive.changed with its last_seq, in UTF-8 byte order of drive_id.
func LastSeqs(ctx context.Context, tx *dbconnect.Tx, ws workspace.ID, driveIDs []string) ([]protocol.DriveChangedPayload, error) {
	if len(driveIDs) == 0 {
		return nil, nil
	}
	rows, err := tx.Query(ctx, `SELECT drive_id,last_seq FROM drives WHERE workspace_id=$1 AND drive_id=ANY($2) AND last_seq>0 AND deleted_at IS NULL ORDER BY drive_id`,
		string(ws), driveIDs)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var notices []protocol.DriveChangedPayload
	for rows.Next() {
		var notice protocol.DriveChangedPayload
		if err := rows.Scan(&notice.DriveID, &notice.Seq); err != nil {
			return nil, err
		}
		notices = append(notices, notice)
	}
	return notices, rows.Err()
}

// TransferFiles is one page of the drive's current files after after, in
// UTF-8 byte order of path. The page asked for without after carries the
// drive's last_seq, read before the page, as start_seq.
func (s *Service) TransferFiles(ctx context.Context, ws workspace.ID, driveID string, after protocol.Optional[string]) (protocol.ListFilesResponse, error) {
	page := protocol.ListFilesResponse{Files: []protocol.FileEntry{}}
	if !after.Set {
		seq, err := s.LastSeq(ctx, ws, driveID)
		if err != nil {
			return protocol.ListFilesResponse{}, err
		}
		page.StartSeq = protocol.Some(seq)
	}
	files, more, err := s.ListFiles(ctx, ws, driveID, FileQuery{After: after.Value, Limit: protocol.ListPageMaxItems})
	if err != nil {
		return protocol.ListFilesResponse{}, err
	}
	entries := make([]protocol.FileEntry, 0, len(files))
	for _, file := range files {
		entries = append(entries, protocol.FileEntry{Path: file.Path, VersionID: file.VersionID, ContentSHA256: file.ContentSHA256, SizeBytes: file.SizeBytes, Executable: file.Executable})
	}
	var cut bool
	page.Files, cut = fitPage(entries, len(protocol.Encode(page)), func(entry protocol.FileEntry) int { return len(protocol.Encode(entry)) })
	page.HasMore = more || cut
	return page, nil
}

// TransferChanges is one page of the drive's listed change records after the
// cursor (afterSeq, afterIndex), or after the whole commit afterSeq when
// afterIndex is unset, in (seq, index) order. A cursor below the newest
// removed record returns ErrCursorExpired.
func (s *Service) TransferChanges(ctx context.Context, ws workspace.ID, driveID string, afterSeq int64, afterIndex protocol.Optional[int64]) (protocol.ListChangesResponse, error) {
	query := ChangeQuery{AfterSeq: &afterSeq, Limit: protocol.ListPageMaxItems}
	if afterIndex.Set {
		index := int(afterIndex.Value)
		query.AfterIndex = &index
	}
	listed, more, err := s.ListChanges(ctx, ws, driveID, query)
	if err != nil {
		return protocol.ListChangesResponse{}, err
	}
	records := make([]protocol.ChangeRecord, 0, len(listed))
	for _, record := range listed {
		content := protocol.Null[string]()
		if record.ContentSHA256 != nil {
			content = protocol.Value(*record.ContentSHA256)
		}
		records = append(records, protocol.ChangeRecord{Seq: record.Seq, Index: int64(record.Index), Path: record.Path, Operation: string(record.Operation),
			VersionID: record.VersionID, ContentSHA256: content, SizeBytes: record.SizeBytes, Executable: record.Executable})
	}
	page := protocol.ListChangesResponse{Records: []protocol.ChangeRecord{}}
	var cut bool
	page.Records, cut = fitPage(records, len(protocol.Encode(page)), func(record protocol.ChangeRecord) int { return len(protocol.Encode(record)) })
	page.HasMore = more || cut
	return page, nil
}

// fitPage keeps the longest prefix of entries, at most
// protocol.ListPageMaxItems, whose page stays within
// protocol.ListPageMaxJSONBytes, empty being the size of the page without
// entries and with has_more false, which is longer than with true; it
// reports whether it left an entry out.
func fitPage[T any](entries []T, empty int, size func(T) int) ([]T, bool) {
	total := empty
	for i, entry := range entries {
		add := size(entry)
		if i > 0 {
			add++
		}
		if i == protocol.ListPageMaxItems || total+add > protocol.ListPageMaxJSONBytes {
			return entries[:i], true
		}
		total += add
	}
	return entries, false
}

// HeldContent is content that a kept, unredacted version of a drive names,
// with the blob that holds it.
type HeldContent struct {
	Content
	blobKey string
}

// FindContent reads the content sha256 while a kept, unredacted version of
// driveID names it, and returns ErrNotFound otherwise, also when only another
// drive holds it.
func (s *Service) FindContent(ctx context.Context, ws workspace.ID, driveID, sha256 string) (HeldContent, error) {
	held := HeldContent{Content: Content{SHA256: sha256}}
	err := s.read(ctx, ws, driveID, "drive.content_read", func(tx *dbconnect.Tx) error {
		err := tx.QueryRow(ctx, `SELECT c.blob_key,c.size_bytes FROM drive_contents c WHERE c.workspace_id=$1 AND c.drive_id=$2 AND c.content_sha256=$3
			AND EXISTS (SELECT 1 FROM drive_versions v WHERE v.workspace_id=c.workspace_id AND v.drive_id=c.drive_id AND v.content_sha256=c.content_sha256 AND `+namesContent("v")+`)`,
			string(ws), driveID, sha256).Scan(&held.blobKey, &held.SizeBytes)
		if errors.Is(err, sql.ErrNoRows) {
			return ErrNotFound
		}
		return err
	})
	return held, err
}

// rangeReader is a blob store that reads part of a blob.
type rangeReader interface {
	GetRange(ctx context.Context, key string, offset, length int64) (io.ReadCloser, error)
}

// OpenRange opens length bytes of held from byte first; a store without
// range reads skips the bytes before first.
func (s *Service) OpenRange(ctx context.Context, held HeldContent, first, length int64) (io.ReadCloser, error) {
	if ranged, ok := s.blobs.(rangeReader); ok && length > 0 {
		return ranged.GetRange(ctx, held.blobKey, first, length)
	}
	body, err := s.blobs.Get(ctx, held.blobKey)
	if err != nil {
		return nil, err
	}
	if _, err := io.CopyN(io.Discard, body, first); err != nil {
		_ = body.Close()
		return nil, err
	}
	return struct {
		io.Reader
		io.Closer
	}{io.LimitReader(body, length), body}, nil
}
