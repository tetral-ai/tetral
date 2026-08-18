package files

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/tetral-ai/tetral/internal/blob"
	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/files/pdfcount"
	"github.com/tetral-ai/tetral/internal/id"
	"github.com/tetral-ai/tetral/internal/storage"
	"github.com/tetral-ai/tetral/internal/workspace"
)

// MaxRetainedBytesPerWorkspace caps unique retained file_objects bytes.
const MaxRetainedBytesPerWorkspace int64 = 500000000000

// MaxFileIdentitiesPerWorkspace caps active plus tombstoned files rows.
const MaxFileIdentitiesPerWorkspace = 100000

const (
	blobKeyPrefix = "files"

	// MaxEventImageBytes, MaxEventPDFBytesPerRequest, and
	// MaxEventPDFPagesPerRequest bound the user.message media one events
	// request may carry. The three values match the upstream provider's
	// use-time media limits: 10 MB per image, 32 MB and 600 pages of PDF.
	//
	// Enforcement shape (ValidateEventAttachments):
	//   - MaxEventImageBytes is a PER-FILE cap. Each image reference's stored
	//     size is checked on its own; image sizes are never summed.
	//   - MaxEventPDFBytesPerRequest and MaxEventPDFPagesPerRequest are
	//     PER-REQUEST AGGREGATES summed over document references as written.
	//     A file referenced twice contributes its bytes and pages twice, so a
	//     duplicated reference can only push a request over a cap, never under
	//     it (fails safe).
	//
	// Admissible media types are fixed in validateEventAttachmentMIME:
	// image = {image/jpeg, image/png, image/gif, image/webp};
	// document = {application/pdf, text/plain}. Only application/pdf is
	// byte- and page-bounded here; text/plain document references carry no
	// admission byte cap, so oversize text is not rejected at admission.
	//
	// UPDATE-WITH: ValidateEventAttachments, validateEventAttachmentMIME,
	// uploadedPDFPageCount, countStoredPDF, internal/files/pdfcount.
	MaxEventImageBytes         int64 = 10000000
	MaxEventPDFBytesPerRequest int64 = 32000000
	MaxEventPDFPagesPerRequest int64 = 600
)

// StoreLimits carries store quota limits. Zero values select the
// production constants.
type StoreLimits struct {
	MaxRetainedBytesPerWorkspace  int64
	MaxFileIdentitiesPerWorkspace int
}

// PostgreSQLFileStore implements the Files metadata store over PostgreSQL
// plus BlobStore bytes.
type PostgreSQLFileStore struct {
	client *dbconnect.Client
	blob   blob.BlobStore

	limits StoreLimits
	clock  func() time.Time

	fileIDStrategy   func() string
	objectIDStrategy func() string

	txAndCleanup func(ctx context.Context, workspaceID string, fn func(Transaction) error, onCommitFailure func()) error
}

// EventAttachmentPageCountRequiredError asks the admission owner to let the
// Files domain durably initialize legacy PDF page-count cache rows, then retry
// the otherwise read-only validation transaction.
type EventAttachmentPageCountRequiredError struct{}

func (e *EventAttachmentPageCountRequiredError) Error() string {
	return "files: legacy PDF page count initialization is required"
}

// Transaction is the Files-owned transaction capability used by test
// seams. Production code adapts dbconnect.Tx into this interface so
// provider-owned transaction types do not leak through Files boundaries.
type Transaction interface {
	Exec(ctx context.Context, query string, args ...any) (sql.Result, error)
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	Query(ctx context.Context, query string, args ...any) (Rows, error)
	QueryRow(ctx context.Context, query string, args ...any) Row
}

type SessionTransaction interface {
	ExecContext(ctx context.Context, query string, args ...any) (interface {
		RowsAffected() (int64, error)
	}, error)
	Query(ctx context.Context, query string, args ...any) (Rows, error)
	QueryRow(ctx context.Context, query string, args ...any) Row
}

// Row is the Files-owned row scanner capability.
type Row interface {
	Scan(dest ...any) error
}

// Rows is the Files-owned row iteration capability.
type Rows interface {
	Next() bool
	Scan(dest ...any) error
	Err() error
	Close() error
}

type runtimeTransaction struct {
	tx *dbconnect.Tx
}

func (t runtimeTransaction) Exec(ctx context.Context, query string, args ...any) (sql.Result, error) {
	return t.tx.Exec(ctx, query, args...)
}

func (t runtimeTransaction) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	return t.tx.ExecContext(ctx, query, args...)
}

func (t runtimeTransaction) Query(ctx context.Context, query string, args ...any) (Rows, error) {
	return t.tx.Query(ctx, query, args...)
}

func (t runtimeTransaction) QueryRow(ctx context.Context, query string, args ...any) Row {
	return t.tx.QueryRow(ctx, query, args...)
}

// NewPostgreSQLStore constructs the production-limits Files store.
func NewPostgreSQLStore(runtimeClient *dbconnect.Client, blobStore blob.BlobStore) *PostgreSQLFileStore {
	return NewPostgreSQLStoreWithLimits(runtimeClient, blobStore, StoreLimits{})
}

// NewPostgreSQLStoreWithLimits constructs a Files store with optional
// test limits. Production callers should use NewPostgreSQLStore.
func NewPostgreSQLStoreWithLimits(runtimeClient *dbconnect.Client, blobStore blob.BlobStore, limits StoreLimits) *PostgreSQLFileStore {
	return &PostgreSQLFileStore{
		client:           runtimeClient,
		blob:             blobStore,
		limits:           normalizeStoreLimits(limits),
		clock:            func() time.Time { return storage.Now() },
		fileIDStrategy:   func() string { return id.New(IDPrefix) },
		objectIDStrategy: func() string { return id.New(ObjectIDPrefix) },
		txAndCleanup: func(ctx context.Context, workspaceID string, fn func(Transaction) error, onCommitFailure func()) error {
			return runtimeClient.WithWorkspaceTxAndCleanup(ctx, workspaceID, "files.transaction", func(tx *dbconnect.Tx) error {
				return fn(runtimeTransaction{tx: tx})
			}, onCommitFailure)
		},
	}
}

// SetClock overrides the time source. Test-only seam.
func (s *PostgreSQLFileStore) SetClock(clock func() time.Time) {
	s.clock = clock
}

// SetTxRunner overrides the workspace-scoped tx + cleanup runner.
// Test-only seam used to synthesize commit failure.
func (s *PostgreSQLFileStore) SetTxRunner(runner func(ctx context.Context, workspaceID string, fn func(Transaction) error, onCommitFailure func()) error) {
	s.txAndCleanup = runner
}

func normalizeStoreLimits(limits StoreLimits) StoreLimits {
	if limits.MaxRetainedBytesPerWorkspace == 0 {
		limits.MaxRetainedBytesPerWorkspace = MaxRetainedBytesPerWorkspace
	}
	if limits.MaxFileIdentitiesPerWorkspace == 0 {
		limits.MaxFileIdentitiesPerWorkspace = MaxFileIdentitiesPerWorkspace
	}
	return limits
}

// CreateUploadedFile persists one uploaded original file identity and
// its stored bytes. The staged temp file is removed before return on
// success and failure.
func (s *PostgreSQLFileStore) CreateUploadedFile(ctx context.Context, ws workspace.ID, staged *StagedUpload) (*FileMetadata, error) {
	if err := requireWorkspace(ws); err != nil {
		return nil, err
	}
	if staged == nil {
		return nil, errors.New("files: staged upload is required")
	}
	defer func() { _ = staged.Cleanup() }()
	pdfPageCount := uploadedPDFPageCount(staged)

	now := s.clock()
	createdAt := now.Format(time.RFC3339)
	fileID := s.fileIDStrategy()
	objectID := s.objectIDStrategy()
	blobKey := blobKeyFor(ws, objectID)

	var (
		result    *FileMetadata
		blobPutOK string
	)
	err := s.txAndCleanup(ctx, string(ws),
		func(tx Transaction) error {
			if err := storage.AcquireWorkspaceFilesLock(ctx, tx, string(ws)); err != nil {
				return err
			}
			if err := s.assertFileIdentityQuota(ctx, tx, ws); err != nil {
				return err
			}
			if err := s.assertRetainedBytesQuota(ctx, tx, ws, staged.SizeBytes); err != nil {
				return err
			}
			if _, err := tx.Exec(ctx,
				`INSERT INTO file_objects (object_id, workspace_id, blob_key, size_bytes, sha256, pdf_page_count, created_at)
				 VALUES ($1, $2, $3, $4, $5, $6, $7)`,
				objectID, string(ws), blobKey, staged.SizeBytes, staged.SHA256, pdfPageCount, createdAt,
			); err != nil {
				return err
			}
			if _, err := tx.Exec(ctx,
				`INSERT INTO files (file_id, workspace_id, object_id, filename, mime_type, downloadable, created_at)
				 VALUES ($1, $2, $3, $4, $5, false, $6)`,
				fileID, string(ws), objectID, staged.Filename, staged.MIMEType, createdAt,
			); err != nil {
				return err
			}
			reader, err := staged.Open()
			if err != nil {
				return err
			}
			defer func() { _ = reader.Close() }()
			if err := s.blob.Put(ctx, blobKey, reader, staged.SizeBytes); err != nil {
				return s.blobPutFailure(ctx, blobKey, err)
			}
			blobPutOK = blobKey
			result = &FileMetadata{
				ID:           fileID,
				Type:         "file",
				Filename:     staged.Filename,
				MIMEType:     staged.MIMEType,
				SizeBytes:    staged.SizeBytes,
				CreatedAt:    now,
				Downloadable: false,
			}
			return nil
		},
		func() { s.bestEffortDelete(blobPutOK) },
	)
	if err != nil {
		return nil, err
	}
	return result, nil
}

// uploadedPDFPageCount computes the value written to file_objects.pdf_page_count
// at upload time. That column is a tri-state cache of a PDF's page count:
//
//	value  meaning                                    writers
//	NULL   no count stored: a non-PDF upload, or a     uploadedPDFPageCount (non-PDF);
//	       PDF row created before the column existed    legacy rows predating the column
//	-1     uncountable PDF (corrupt, encrypted, or     uploadedPDFPageCount, countStoredPDF
//	       mislabeled as application/pdf)
//	>=0    counted page count                          uploadedPDFPageCount, countStoredPDF
//
// Every application/pdf upload is counted once here through the Files-owned
// bounded counter (internal/files/pdfcount); a counting failure yields -1 and
// never fails the upload, which accepts the bytes regardless. Legacy NULL PDF
// rows are counted lazily at their first document-block reference:
// ValidateEventAttachments raises EventAttachmentPageCountRequiredError and
// PrimeEventAttachmentPDFCounts fills the value via countStoredPDF. The fill is
// once-only (the priming UPDATE matches pdf_page_count IS NULL), so a non-NULL
// value is never rewritten; legal transitions are only NULL -> -1 and
// NULL -> >=0. A document-block reference to a -1 row is rejected as not a
// readable PDF.
//
// UPDATE-WITH: ValidateEventAttachments, PrimeEventAttachmentPDFCounts,
// countStoredPDF, internal/files/pdfcount.
func uploadedPDFPageCount(staged *StagedUpload) sql.NullInt64 {
	if staged.MIMEType != "application/pdf" {
		return sql.NullInt64{}
	}
	source, err := os.Open(staged.tempPath) //nolint:gosec // server-generated staging path.
	if err != nil {
		return sql.NullInt64{Int64: -1, Valid: true}
	}
	defer func() { _ = source.Close() }()
	pages, err := pdfcount.Count(source)
	if err != nil {
		return sql.NullInt64{Int64: -1, Valid: true}
	}
	return sql.NullInt64{Int64: pages, Valid: true}
}

// ValidateEventAttachments validates every file-backed user.message media
// reference inside the caller's admission transaction. Legacy PDFs are
// counted once while their file-object row is locked.
func (s *PostgreSQLFileStore) ValidateEventAttachments(
	ctx context.Context,
	tx SessionTransaction,
	ws workspace.ID,
	sessionID string,
	references []EventAttachmentReference,
) error {
	if err := requireWorkspace(ws); err != nil {
		return err
	}
	if tx == nil {
		return errors.New("files: transaction is required")
	}
	var pdfBytes int64
	var pdfPages int64
	records, err := s.loadEventAttachmentRecords(ctx, tx, ws, sessionID, references)
	if err != nil {
		return err
	}
	pageCountRequired := false
	for _, reference := range references {
		record := records[reference.FileID]
		if err := validateEventAttachmentMIME(reference.BlockType, record.mimeType); err != nil {
			return err
		}
		if reference.BlockType == "image" {
			if record.sizeBytes > MaxEventImageBytes {
				return &ValidationError{Message: "image file exceeds the 10 MB limit"}
			}
			continue
		}
		if record.mimeType != "application/pdf" {
			continue
		}
		if !record.pdfPageCount.Valid {
			pageCountRequired = true
		}
		pdfBytes += record.sizeBytes
		if pdfBytes > MaxEventPDFBytesPerRequest {
			return &ValidationError{Message: "PDF files exceed the 32 MB per-request limit"}
		}
	}
	if pageCountRequired {
		return &EventAttachmentPageCountRequiredError{}
	}
	for _, reference := range references {
		record := records[reference.FileID]
		if reference.BlockType != "document" || record.mimeType != "application/pdf" {
			continue
		}
		if record.pdfPageCount.Int64 < 0 {
			return &ValidationError{Message: "referenced file is not a readable PDF"}
		}
		pdfPages += record.pdfPageCount.Int64
		if pdfPages > MaxEventPDFPagesPerRequest {
			return &ValidationError{Message: "PDF files exceed the 600-page per-request limit"}
		}
	}
	return nil
}

// PrimeEventAttachmentPDFCounts initializes every legacy PDF object referenced
// by one admission attempt. It commits independently from event admission so
// the immutable cache survives a later public validation rejection.
func (s *PostgreSQLFileStore) PrimeEventAttachmentPDFCounts(
	ctx context.Context,
	ws workspace.ID,
	sessionID string,
	references []EventAttachmentReference,
) error {
	if err := requireWorkspace(ws); err != nil {
		return err
	}
	if s == nil || s.client == nil {
		return errors.New("files: store is required")
	}
	return s.client.WithWorkspaceTx(ctx, string(ws), "files.prime_event_pdf_counts", func(tx *dbconnect.Tx) error {
		records, err := s.loadEventAttachmentRecords(ctx, filesSessionTransaction{tx: tx}, ws, sessionID, references)
		if err != nil {
			return err
		}
		resolvedObjects := make(map[string]struct{}, len(records))
		for _, reference := range references {
			record := records[reference.FileID]
			if reference.BlockType != "document" || record.mimeType != "application/pdf" || record.pdfPageCount.Valid {
				continue
			}
			if _, ok := resolvedObjects[record.objectID]; ok {
				continue
			}
			pageCount, err := s.countStoredPDF(ctx, record.blobKey, record.sizeBytes)
			if err != nil {
				return err
			}
			if _, err := tx.ExecContext(ctx,
				`UPDATE file_objects
				    SET pdf_page_count = $1
				  WHERE workspace_id = $2
				    AND object_id = $3
				    AND pdf_page_count IS NULL`,
				pageCount, string(ws), record.objectID,
			); err != nil {
				return err
			}
			resolvedObjects[record.objectID] = struct{}{}
		}
		return nil
	})
}

type filesSessionTransaction struct {
	tx *dbconnect.Tx
}

func (t filesSessionTransaction) ExecContext(ctx context.Context, query string, args ...any) (interface {
	RowsAffected() (int64, error)
}, error) {
	return t.tx.ExecContext(ctx, query, args...)
}

func (t filesSessionTransaction) Query(ctx context.Context, query string, args ...any) (Rows, error) {
	return t.tx.Query(ctx, query, args...)
}

func (t filesSessionTransaction) QueryRow(ctx context.Context, query string, args ...any) Row {
	return t.tx.QueryRow(ctx, query, args...)
}

type eventAttachmentRecord struct {
	objectID     string
	mimeType     string
	sizeBytes    int64
	pdfPageCount sql.NullInt64
	blobKey      string
}

type eventAttachmentFileRecord struct {
	objectID string
	mimeType string
}

type eventAttachmentObjectRecord struct {
	sizeBytes    int64
	pdfPageCount sql.NullInt64
	blobKey      string
}

func orderedUniqueEventAttachmentIDs(references []EventAttachmentReference) []string {
	seen := make(map[string]struct{}, len(references))
	ids := make([]string, 0, len(references))
	for _, reference := range references {
		if _, ok := seen[reference.FileID]; ok {
			continue
		}
		seen[reference.FileID] = struct{}{}
		ids = append(ids, reference.FileID)
	}
	sort.Strings(ids)
	return ids
}

func (s *PostgreSQLFileStore) loadEventAttachmentRecords(
	ctx context.Context,
	tx SessionTransaction,
	ws workspace.ID,
	sessionID string,
	references []EventAttachmentReference,
) (map[string]eventAttachmentRecord, error) {
	fileRecords := make(map[string]eventAttachmentFileRecord, len(references))
	objectIDs := make(map[string]struct{}, len(references))
	for _, fileID := range orderedUniqueEventAttachmentIDs(references) {
		var record eventAttachmentFileRecord
		err := tx.QueryRow(ctx,
			`SELECT object_id, mime_type
			   FROM files
			  WHERE workspace_id = $1
			    AND file_id = $2
			    AND deleted_at IS NULL
			    AND (
			      (scope_type IS NULL AND scope_id IS NULL)
			      OR (scope_type = 'session' AND scope_id = $3)
			    )
			  FOR UPDATE`,
			string(ws), fileID, sessionID,
		).Scan(&record.objectID, &record.mimeType)
		if errors.Is(err, sql.ErrNoRows) {
			return nil, &ValidationError{Message: "file_id is invalid"}
		}
		if err != nil {
			return nil, err
		}
		fileRecords[fileID] = record
		objectIDs[record.objectID] = struct{}{}
	}

	orderedObjectIDs := make([]string, 0, len(objectIDs))
	for objectID := range objectIDs {
		orderedObjectIDs = append(orderedObjectIDs, objectID)
	}
	sort.Strings(orderedObjectIDs)
	objectRecords := make(map[string]eventAttachmentObjectRecord, len(orderedObjectIDs))
	for _, objectID := range orderedObjectIDs {
		var record eventAttachmentObjectRecord
		err := tx.QueryRow(ctx,
			`SELECT size_bytes, pdf_page_count, blob_key
			   FROM file_objects
			  WHERE workspace_id = $1 AND object_id = $2
			  FOR UPDATE`,
			string(ws), objectID,
		).Scan(&record.sizeBytes, &record.pdfPageCount, &record.blobKey)
		if errors.Is(err, sql.ErrNoRows) {
			return nil, &ValidationError{Message: "file_id is invalid"}
		}
		if err != nil {
			return nil, err
		}
		objectRecords[objectID] = record
	}

	records := make(map[string]eventAttachmentRecord, len(fileRecords))
	for fileID, fileRecord := range fileRecords {
		objectRecord := objectRecords[fileRecord.objectID]
		records[fileID] = eventAttachmentRecord{
			objectID:     fileRecord.objectID,
			mimeType:     fileRecord.mimeType,
			sizeBytes:    objectRecord.sizeBytes,
			pdfPageCount: objectRecord.pdfPageCount,
			blobKey:      objectRecord.blobKey,
		}
	}
	return records, nil
}

func validateEventAttachmentMIME(blockType string, mimeType string) error {
	valid := false
	switch blockType {
	case "image":
		valid = mimeType == "image/jpeg" ||
			mimeType == "image/png" ||
			mimeType == "image/gif" ||
			mimeType == "image/webp"
	case "document":
		valid = mimeType == "application/pdf" || mimeType == "text/plain"
	}
	if !valid {
		return &ValidationError{Message: "file type does not match the content block type"}
	}
	return nil
}

func (s *PostgreSQLFileStore) countStoredPDF(ctx context.Context, blobKey string, sizeBytes int64) (int64, error) {
	if s.blob == nil {
		return 0, errors.New("files: blob store is required")
	}
	source, err := s.blob.Get(ctx, blobKey)
	if err != nil {
		return 0, err
	}
	defer func() { _ = source.Close() }()

	staged, err := os.CreateTemp("", "tetral-pdf-count-*")
	if err != nil {
		return 0, errors.New("files: PDF count staging failed")
	}
	stagedPath := staged.Name()
	defer func() {
		_ = staged.Close()
		_ = os.Remove(stagedPath)
	}()
	copied, err := io.Copy(staged, io.LimitReader(source, sizeBytes+1))
	if err != nil {
		return 0, errors.New("files: PDF count source read failed")
	}
	if copied != sizeBytes {
		return 0, errors.New("files: stored file size does not match metadata")
	}
	if _, err := staged.Seek(0, io.SeekStart); err != nil {
		return 0, errors.New("files: PDF count staging seek failed")
	}
	pages, err := pdfcount.Count(staged)
	if err != nil {
		return -1, nil
	}
	return pages, nil
}

// ListFiles returns visible, non-deleted file identities for one
// workspace, ordered by storage_sequence ASC.
func (s *PostgreSQLFileStore) ListFiles(ctx context.Context, ws workspace.ID, options ListOptions) (ListResult, error) {
	if err := requireWorkspace(ws); err != nil {
		return ListResult{}, err
	}
	limit := options.Limit
	if limit == 0 {
		limit = 20
	}
	if limit < 0 {
		return ListResult{}, &ValidationError{Message: "file list limit is invalid"}
	}
	var result ListResult
	err := s.client.WithWorkspaceTx(ctx, string(ws), "files.transaction", func(tx *dbconnect.Tx) error {
		filesTx := runtimeTransaction{tx: tx}
		afterSeq, hasAfter, err := s.resolveListCursor(ctx, filesTx, ws, options.AfterID, options.ScopeID)
		if err != nil {
			return err
		}
		beforeSeq, hasBefore, err := s.resolveListCursor(ctx, filesTx, ws, options.BeforeID, options.ScopeID)
		if err != nil {
			return err
		}
		if hasAfter && hasBefore && afterSeq >= beforeSeq {
			result = emptyListResult()
			return nil
		}
		items, hasMore, err := s.queryList(ctx, filesTx, ws, options.ScopeID, afterSeq, hasAfter, beforeSeq, hasBefore, limit)
		if err != nil {
			return err
		}
		result = listResultFromItems(items, hasMore)
		return nil
	})
	if err != nil {
		return ListResult{}, err
	}
	return result, nil
}

// GetFile returns public metadata for a visible file identity.
func (s *PostgreSQLFileStore) GetFile(ctx context.Context, ws workspace.ID, fileID string) (*FileMetadata, error) {
	if err := requireWorkspace(ws); err != nil {
		return nil, err
	}
	var result *FileMetadata
	err := s.client.WithWorkspaceTx(ctx, string(ws), "files.transaction", func(tx *dbconnect.Tx) error {
		metadata, _, err := s.lookupVisibleFile(ctx, runtimeTransaction{tx: tx}, ws, fileID)
		if err != nil {
			return err
		}
		result = metadata
		return nil
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

// DeleteFile tombstones one visible uploaded file identity. It
// intentionally leaves BlobStore bytes and file_objects untouched.
func (s *PostgreSQLFileStore) DeleteFile(ctx context.Context, ws workspace.ID, fileID string) (*DeleteResponse, error) {
	if err := requireWorkspace(ws); err != nil {
		return nil, err
	}
	err := s.client.WithWorkspaceTx(ctx, string(ws), "files.transaction", func(tx *dbconnect.Tx) error {
		if err := storage.AcquireWorkspaceFilesLock(ctx, tx, string(ws)); err != nil {
			return err
		}
		var scopeType sql.NullString
		err := tx.QueryRow(ctx,
			`SELECT scope_type
			   FROM files
			  WHERE workspace_id = $1 AND file_id = $2 AND deleted_at IS NULL
			  FOR UPDATE`,
			string(ws), fileID,
		).Scan(&scopeType)
		if errors.Is(err, sql.ErrNoRows) {
			return &NotFoundError{Message: "file not found"}
		}
		if err != nil {
			return err
		}
		if scopeType.Valid && scopeType.String == "session" {
			return &ValidationError{Message: "session-scoped files must be deleted through session resources"}
		}
		var returnedID string
		err = tx.QueryRow(ctx,
			`UPDATE files
			    SET deleted_at = $1
			  WHERE workspace_id = $2 AND file_id = $3 AND deleted_at IS NULL
			  RETURNING file_id`,
			s.clock().Format(time.RFC3339), string(ws), fileID,
		).Scan(&returnedID)
		if errors.Is(err, sql.ErrNoRows) {
			return &NotFoundError{Message: "file not found"}
		}
		return err
	})
	if err != nil {
		return nil, err
	}
	return &DeleteResponse{ID: fileID, Type: "file_deleted"}, nil
}

// OpenContent opens public downloadable bytes. Uploaded originals are
// intentionally non-downloadable through this public boundary.
func (s *PostgreSQLFileStore) OpenContent(ctx context.Context, ws workspace.ID, fileID string) (*ContentStream, error) {
	if err := requireWorkspace(ws); err != nil {
		return nil, err
	}
	var (
		metadata *FileMetadata
		blobKey  string
	)
	err := s.client.WithWorkspaceTx(ctx, string(ws), "files.transaction", func(tx *dbconnect.Tx) error {
		var err error
		metadata, blobKey, err = s.lookupVisibleFile(ctx, runtimeTransaction{tx: tx}, ws, fileID)
		return err
	})
	if err != nil {
		return nil, err
	}
	if !metadata.Downloadable {
		return nil, &PermissionError{Message: "file content is not downloadable"}
	}
	reader, err := s.blob.Get(ctx, blobKey)
	if err != nil {
		return nil, err
	}
	return &ContentStream{Metadata: metadata, Reader: reader}, nil
}

// ResolveSource returns a non-HTTP opener for internal session-copy
// code. It ignores public downloadable=false but still enforces
// workspace ownership and tombstones.
func (s *PostgreSQLFileStore) ResolveSource(ctx context.Context, ws workspace.ID, fileID string) (*Source, error) {
	if err := requireWorkspace(ws); err != nil {
		return nil, err
	}
	var (
		metadata *FileMetadata
		blobKey  string
	)
	err := s.client.WithWorkspaceTx(ctx, string(ws), "files.transaction", func(tx *dbconnect.Tx) error {
		var err error
		metadata, blobKey, err = s.lookupVisibleFile(ctx, runtimeTransaction{tx: tx}, ws, fileID)
		return err
	})
	if err != nil {
		return nil, err
	}
	return &Source{
		Metadata: metadata,
		Open: func(openCtx context.Context) (io.ReadCloser, error) {
			return s.blob.Get(openCtx, blobKey)
		},
	}, nil
}

// ResolveMountSource returns storage-internal object data for a
// committed session-scoped file identity owned by the supplied session.
func (s *PostgreSQLFileStore) ResolveMountSource(ctx context.Context, ws workspace.ID, sessionID string, fileID string) (*MountSource, error) {
	if err := requireWorkspace(ws); err != nil {
		return nil, err
	}
	if sessionID == "" || fileID == "" {
		return nil, &ValidationError{Message: "mount source request is invalid"}
	}
	var source *MountSource
	err := s.client.WithWorkspaceTx(ctx, string(ws), "files.transaction", func(tx *dbconnect.Tx) error {
		row := tx.QueryRow(ctx,
			`SELECT f.workspace_id, f.file_id, o.blob_key, o.size_bytes, o.sha256
			   FROM files f
			   JOIN file_objects o ON o.workspace_id = f.workspace_id AND o.object_id = f.object_id
			  WHERE f.workspace_id = $1
			    AND f.file_id = $2
			    AND f.scope_type = 'session'
			    AND f.scope_id = $3
			    AND f.deleted_at IS NULL`,
			string(ws), fileID, sessionID,
		)
		out := &MountSource{}
		var workspaceID string
		if err := row.Scan(&workspaceID, &out.FileID, &out.ObjectKey, &out.SizeBytes, &out.SHA256); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return &NotFoundError{Message: "file not found"}
			}
			return err
		}
		out.WorkspaceID = workspace.ID(workspaceID)
		source = out
		return nil
	})
	if err != nil {
		return nil, err
	}
	return source, nil
}

// ValidateSessionFileSource verifies a visible workspace-owned original
// file identity can be used to create a session-scoped file identity.
func (s *PostgreSQLFileStore) ValidateSessionFileSource(ctx context.Context, ws workspace.ID, fileID string) error {
	if err := requireWorkspace(ws); err != nil {
		return err
	}
	return s.client.WithWorkspaceTx(ctx, string(ws), "files.transaction", func(tx *dbconnect.Tx) error {
		_, _, err := s.lookupVisibleFileObject(ctx, runtimeTransaction{tx: tx}, ws, fileID)
		return err
	})
}

func (s *PostgreSQLFileStore) CreateSessionFileIdentity(ctx context.Context, tx SessionTransaction, ws workspace.ID, request SessionFileIdentityRequest) (*FileMetadata, error) {
	if err := requireWorkspace(ws); err != nil {
		return nil, err
	}
	if tx == nil {
		return nil, errors.New("files: transaction is required")
	}
	if request.SourceFileID == "" || request.SessionID == "" || request.SessionFileID == "" {
		return nil, &ValidationError{Message: "session file identity request is invalid"}
	}
	if err := storage.AcquireWorkspaceFilesLockForSession(ctx, tx, string(ws)); err != nil {
		return nil, err
	}
	if err := s.assertSessionFileIdentityQuota(ctx, tx, ws); err != nil {
		return nil, err
	}
	source, objectID, err := s.lookupVisibleFileObject(ctx, tx, ws, request.SourceFileID)
	if err != nil {
		return nil, err
	}
	createdAt := request.CreatedAt
	if createdAt.IsZero() {
		createdAt = s.clock()
	}
	if _, err := tx.ExecContext(ctx,
		`INSERT INTO files (file_id, workspace_id, object_id, filename, mime_type, downloadable, scope_type, scope_id, created_at)
		 VALUES ($1, $2, $3, $4, $5, $6, 'session', $7, $8)`,
		request.SessionFileID,
		string(ws),
		objectID,
		source.Filename,
		source.MIMEType,
		source.Downloadable,
		request.SessionID,
		createdAt.Format(time.RFC3339),
	); err != nil {
		return nil, err
	}
	return &FileMetadata{
		ID:           request.SessionFileID,
		Type:         "file",
		Filename:     source.Filename,
		MIMEType:     source.MIMEType,
		SizeBytes:    source.SizeBytes,
		CreatedAt:    createdAt,
		Downloadable: source.Downloadable,
		Scope:        &FileScope{Type: "session", ID: request.SessionID},
	}, nil
}

func (s *PostgreSQLFileStore) TombstoneSessionFileIdentity(ctx context.Context, tx SessionTransaction, ws workspace.ID, sessionID string, fileID string) error {
	if err := requireWorkspace(ws); err != nil {
		return err
	}
	if tx == nil {
		return errors.New("files: transaction is required")
	}
	if err := storage.AcquireWorkspaceFilesLockForSession(ctx, tx, string(ws)); err != nil {
		return err
	}
	var returnedID string
	err := tx.QueryRow(ctx,
		`UPDATE files
		    SET deleted_at = COALESCE(deleted_at, $1)
		  WHERE workspace_id = $2
		    AND file_id = $3
		    AND scope_type = 'session'
		    AND scope_id = $4
		  RETURNING file_id`,
		s.clock().Format(time.RFC3339), string(ws), fileID, sessionID,
	).Scan(&returnedID)
	if errors.Is(err, sql.ErrNoRows) {
		return &NotFoundError{Message: "file not found"}
	}
	return err
}

func emptyListResult() ListResult {
	return ListResult{Data: []*FileMetadata{}, HasMore: false}
}

func listResultFromItems(items []*FileMetadata, hasMore bool) ListResult {
	result := ListResult{Data: items, HasMore: hasMore}
	if len(items) > 0 {
		firstID := items[0].ID
		lastID := items[len(items)-1].ID
		result.FirstID = &firstID
		result.LastID = &lastID
	}
	return result
}

func (s *PostgreSQLFileStore) resolveListCursor(ctx context.Context, tx Transaction, ws workspace.ID, cursor string, scopeID string) (int64, bool, error) {
	if cursor == "" {
		return 0, false, nil
	}
	if !validFileID(cursor) {
		return 0, false, &ValidationError{Message: "invalid file cursor"}
	}
	query := `SELECT f.storage_sequence FROM files f WHERE f.workspace_id = $1 AND f.file_id = $2 AND f.deleted_at IS NULL AND ` + visibleFileScopePredicate("f")
	args := []any{string(ws), cursor}
	if scopeID != "" {
		query += ` AND scope_type = 'session' AND scope_id = $3`
		args = append(args, scopeID)
	}
	var seq int64
	if err := tx.QueryRow(ctx, query, args...).Scan(&seq); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return 0, false, &ValidationError{Message: "invalid file cursor"}
		}
		return 0, false, err
	}
	return seq, true, nil
}

//nolint:gosec // SQL fragments are fixed by option presence; every value remains a bind parameter.
func (s *PostgreSQLFileStore) queryList(ctx context.Context, tx Transaction, ws workspace.ID, scopeID string, afterSeq int64, hasAfter bool, beforeSeq int64, hasBefore bool, limit int) ([]*FileMetadata, bool, error) {
	query := `SELECT f.file_id, f.filename, f.mime_type, o.size_bytes, f.created_at, f.downloadable, f.scope_type, f.scope_id
	    FROM files f
	    JOIN file_objects o ON o.workspace_id = f.workspace_id AND o.object_id = f.object_id
	   WHERE f.workspace_id = $1 AND f.deleted_at IS NULL AND ` + visibleFileScopePredicate("f")
	args := []any{string(ws)}
	if scopeID != "" {
		args = append(args, scopeID)
		query += fmt.Sprintf(" AND f.scope_type = 'session' AND f.scope_id = $%d", len(args))
	}
	if hasAfter {
		args = append(args, afterSeq)
		query += fmt.Sprintf(" AND f.storage_sequence > $%d", len(args))
	}
	if hasBefore {
		args = append(args, beforeSeq)
		query += fmt.Sprintf(" AND f.storage_sequence < $%d", len(args))
	}
	args = append(args, limit+1)
	query += fmt.Sprintf(" ORDER BY f.storage_sequence ASC LIMIT $%d", len(args))

	rows, err := tx.Query(ctx, query, args...)
	if err != nil {
		return nil, false, err
	}
	defer func() { _ = rows.Close() }()

	items := make([]*FileMetadata, 0, limit)
	for rows.Next() {
		metadata, err := scanFileMetadata(rows)
		if err != nil {
			return nil, false, err
		}
		items = append(items, metadata)
	}
	if err := rows.Err(); err != nil {
		return nil, false, err
	}
	hasMore := len(items) > limit
	if hasMore {
		items = items[:limit]
	}
	return items, hasMore, nil
}

func (s *PostgreSQLFileStore) lookupVisibleFile(ctx context.Context, tx Transaction, ws workspace.ID, fileID string) (*FileMetadata, string, error) {
	row := tx.QueryRow(ctx,
		`SELECT f.file_id, f.filename, f.mime_type, o.size_bytes, f.created_at, f.downloadable, f.scope_type, f.scope_id, o.blob_key
		   FROM files f
		   JOIN file_objects o ON o.workspace_id = f.workspace_id AND o.object_id = f.object_id
		  WHERE f.workspace_id = $1 AND f.file_id = $2 AND f.deleted_at IS NULL AND `+visibleFileScopePredicate("f"),
		string(ws), fileID,
	)
	metadata, blobKey, err := scanFileMetadataWithBlobKey(row)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, "", &NotFoundError{Message: "file not found"}
		}
		return nil, "", err
	}
	return metadata, blobKey, nil
}

func (s *PostgreSQLFileStore) lookupVisibleFileObject(ctx context.Context, tx fileObjectLookupTransaction, ws workspace.ID, fileID string) (*FileMetadata, string, error) {
	row := tx.QueryRow(ctx,
		`SELECT f.file_id, f.filename, f.mime_type, o.size_bytes, f.created_at, f.downloadable, f.scope_type, f.scope_id, f.object_id
		   FROM files f
		   JOIN file_objects o ON o.workspace_id = f.workspace_id AND o.object_id = f.object_id
		  WHERE f.workspace_id = $1
		    AND f.file_id = $2
		    AND f.scope_type IS NULL
		    AND f.scope_id IS NULL
		    AND f.deleted_at IS NULL`,
		string(ws), fileID,
	)
	metadata, objectID, err := scanFileMetadataWithObjectID(row)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, "", &NotFoundError{Message: "file not found"}
		}
		return nil, "", err
	}
	return metadata, objectID, nil
}

type fileObjectLookupTransaction interface {
	QueryRow(ctx context.Context, query string, args ...any) Row
}

func visibleFileScopePredicate(alias string) string {
	return fmt.Sprintf(`(
		%s.scope_type IS NULL
		OR %s.scope_type <> 'session'
		OR EXISTS (
			SELECT 1
			  FROM sessions s
			 WHERE s.workspace_id = %s.workspace_id
			   AND s.id = %s.scope_id
			   AND s.lifecycle_state <> 'deleted'
		)
	)`, alias, alias, alias, alias)
}

type fileMetadataScanner interface {
	Scan(dest ...any) error
}

func scanFileMetadata(scanner fileMetadataScanner) (*FileMetadata, error) {
	var (
		metadata  FileMetadata
		createdAt time.Time
		scopeType sql.NullString
		scopeID   sql.NullString
	)
	if err := scanner.Scan(
		&metadata.ID,
		&metadata.Filename,
		&metadata.MIMEType,
		&metadata.SizeBytes,
		&createdAt,
		&metadata.Downloadable,
		&scopeType,
		&scopeID,
	); err != nil {
		return nil, err
	}
	if err := finishFileMetadata(&metadata, createdAt, scopeType, scopeID); err != nil {
		return nil, err
	}
	return &metadata, nil
}

func scanFileMetadataWithBlobKey(scanner fileMetadataScanner) (*FileMetadata, string, error) {
	var (
		metadata  FileMetadata
		createdAt time.Time
		scopeType sql.NullString
		scopeID   sql.NullString
		blobKey   string
	)
	if err := scanner.Scan(
		&metadata.ID,
		&metadata.Filename,
		&metadata.MIMEType,
		&metadata.SizeBytes,
		&createdAt,
		&metadata.Downloadable,
		&scopeType,
		&scopeID,
		&blobKey,
	); err != nil {
		return nil, "", err
	}
	if err := finishFileMetadata(&metadata, createdAt, scopeType, scopeID); err != nil {
		return nil, "", err
	}
	return &metadata, blobKey, nil
}

func scanFileMetadataWithObjectID(scanner fileMetadataScanner) (*FileMetadata, string, error) {
	var (
		metadata  FileMetadata
		createdAt time.Time
		scopeType sql.NullString
		scopeID   sql.NullString
		objectID  string
	)
	if err := scanner.Scan(
		&metadata.ID,
		&metadata.Filename,
		&metadata.MIMEType,
		&metadata.SizeBytes,
		&createdAt,
		&metadata.Downloadable,
		&scopeType,
		&scopeID,
		&objectID,
	); err != nil {
		return nil, "", err
	}
	if err := finishFileMetadata(&metadata, createdAt, scopeType, scopeID); err != nil {
		return nil, "", err
	}
	return &metadata, objectID, nil
}

func finishFileMetadata(metadata *FileMetadata, createdAt time.Time, scopeType sql.NullString, scopeID sql.NullString) error {
	parsed := createdAt.UTC()
	metadata.Type = "file"
	metadata.CreatedAt = parsed
	if scopeType.Valid || scopeID.Valid {
		metadata.Scope = &FileScope{Type: scopeType.String, ID: scopeID.String}
	}
	return nil
}

func validFileID(fileID string) bool {
	if fileID == "" || len(fileID) > 256 || !strings.HasPrefix(fileID, IDPrefix) || !utf8.ValidString(fileID) {
		return false
	}
	for _, r := range fileID {
		if unicode.IsControl(r) {
			return false
		}
	}
	return true
}

func (s *PostgreSQLFileStore) assertFileIdentityQuota(ctx context.Context, tx Transaction, ws workspace.ID) error {
	var count int
	if err := tx.QueryRow(ctx,
		`SELECT count(*) FROM files WHERE workspace_id = $1`,
		string(ws),
	).Scan(&count); err != nil {
		return err
	}
	if count >= s.limits.MaxFileIdentitiesPerWorkspace {
		return &QuotaError{Kind: QuotaKindCount, Message: "workspace file identity quota exceeded"}
	}
	return nil
}

func (s *PostgreSQLFileStore) assertSessionFileIdentityQuota(ctx context.Context, tx SessionTransaction, ws workspace.ID) error {
	var count int
	if err := tx.QueryRow(ctx,
		`SELECT count(*) FROM files WHERE workspace_id = $1`,
		string(ws),
	).Scan(&count); err != nil {
		return err
	}
	if count >= s.limits.MaxFileIdentitiesPerWorkspace {
		return &QuotaError{Kind: QuotaKindCount, Message: "workspace file identity quota exceeded"}
	}
	return nil
}

func (s *PostgreSQLFileStore) assertRetainedBytesQuota(ctx context.Context, tx Transaction, ws workspace.ID, addBytes int64) error {
	var sum int64
	if err := tx.QueryRow(ctx,
		`SELECT COALESCE(SUM(size_bytes), 0) FROM file_objects WHERE workspace_id = $1`,
		string(ws),
	).Scan(&sum); err != nil {
		return err
	}
	if sum+addBytes > s.limits.MaxRetainedBytesPerWorkspace {
		return &QuotaError{Kind: QuotaKindRetainedBytes, Message: "workspace retained file bytes quota exceeded"}
	}
	return nil
}

func (s *PostgreSQLFileStore) blobPutFailure(ctx context.Context, key string, err error) error {
	var duplicate *blob.DuplicateKeyError
	if errors.As(err, &duplicate) {
		return &ConflictError{Message: "file object already exists"}
	}
	s.bestEffortDelete(key)
	return errors.New("files: blob storage write failed")
}

func (s *PostgreSQLFileStore) bestEffortDelete(key string) {
	if key == "" {
		return
	}
	cleanupCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	_ = s.blob.Delete(cleanupCtx, key)
}

func blobKeyFor(ws workspace.ID, objectID string) string {
	return blobKeyPrefix + "/" + string(ws) + "/" + objectID
}

func requireWorkspace(ws workspace.ID) error {
	if ws == "" {
		return &ValidationError{Message: "workspace id is required"}
	}
	return nil
}
