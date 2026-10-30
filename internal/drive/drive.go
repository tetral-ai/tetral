package drive

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/tetral-ai/tetral/internal/blob"
	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/id"
	"github.com/tetral-ai/tetral/internal/workspace"
)

// Service is the one reader and writer of drive state. Each method runs its own
// Workspace-scoped transaction.
type Service struct {
	db       *dbconnect.Client
	blobs    blob.BlobStore
	stageDir string
}

// NewService stages uploads of unknown size under dataDir.
func NewService(db *dbconnect.Client, blobs blob.BlobStore, dataDir string) (*Service, error) {
	if db == nil || blobs == nil {
		return nil, errors.New("drive: database and blob store are required")
	}
	stageDir, err := newStageDir(dataDir)
	if err != nil {
		return nil, err
	}
	return &Service{db: db, blobs: blobs, stageDir: stageDir}, nil
}

// Drive is a drive row with its usage.
type Drive struct {
	ID                 string
	SkillsPath         string
	MemoryPath         string
	MemoryInstructions *string
	MaxBytes           *int64
	MaxFiles           *int64
	Metadata           map[string]string
	UsageBytes         int64
	UsageFiles         int64
	StoredBytes        *int64
	LastSeq            int64
	CreatedAt          time.Time
}

// Settings are a new drive's settings; empty folder paths take the defaults.
type Settings struct {
	SkillsPath         string
	MemoryPath         string
	MemoryInstructions *string
	MaxBytes           *int64
	MaxFiles           *int64
	Metadata           map[string]string
}

// SettingsUpdate replaces each setting it sets. Memory replaces the path and
// the instructions together, and Limits both caps together; a Metadata key
// mapped to nil is deleted and every other key is set.
type SettingsUpdate struct {
	SkillsPath *string
	Memory     *MemorySettings
	Limits     *LimitSettings
	Metadata   map[string]*string
}

type MemorySettings struct {
	Path         string
	Instructions *string
}

type LimitSettings struct {
	MaxBytes *int64
	MaxFiles *int64
}

// WriterKind names who wrote a version, change or redaction.
type WriterKind string

const (
	WriterAPIKey         WriterKind = "api_actor"
	WriterUser           WriterKind = "user_actor"
	WriterServiceAccount WriterKind = "service_account_actor"
	WriterSession        WriterKind = "session_actor"
)

// Writer is an API key, a direct human or service token, or one of a Session's
// computers. ID is the key, user, service account or Session ID.
type Writer struct {
	Kind       WriterKind
	ID         string
	ComputerID string
}

func (w Writer) validate() error {
	switch w.Kind {
	case WriterAPIKey, WriterUser, WriterServiceAccount:
		if w.ID == "" || w.ComputerID != "" {
			return fmt.Errorf("drive: invalid %s writer", w.Kind)
		}
	case WriterSession:
		if w.ID == "" || w.ComputerID == "" {
			return errors.New("drive: a session writer names its Session and computer")
		}
	default:
		return errors.New("drive: unknown writer kind")
	}
	return nil
}

// MarshalJSON writes the writer object of the API: its type and the ID
// fields that type names.
func (w Writer) MarshalJSON() ([]byte, error) {
	switch w.Kind {
	case WriterAPIKey:
		return json.Marshal(struct {
			Type     WriterKind `json:"type"`
			APIKeyID string     `json:"api_key_id"`
		}{w.Kind, w.ID})
	case WriterUser:
		return json.Marshal(struct {
			Type   WriterKind `json:"type"`
			UserID string     `json:"user_id"`
		}{w.Kind, w.ID})
	case WriterServiceAccount:
		return json.Marshal(struct {
			Type             WriterKind `json:"type"`
			ServiceAccountID string     `json:"service_account_id"`
		}{w.Kind, w.ID})
	case WriterSession:
		return json.Marshal(struct {
			Type       WriterKind `json:"type"`
			SessionID  string     `json:"session_id"`
			ComputerID string     `json:"computer_id"`
		}{w.Kind, w.ID, w.ComputerID})
	default:
		return nil, errors.New("drive: unknown writer kind")
	}
}

// Create inserts a drive with zero usage.
func (s *Service) Create(ctx context.Context, ws workspace.ID, settings Settings) (Drive, error) {
	if settings.SkillsPath == "" {
		settings.SkillsPath = "skills/"
	}
	if settings.MemoryPath == "" {
		settings.MemoryPath = "memory/"
	}
	metadata, err := json.Marshal(nonNilMetadata(settings.Metadata))
	if err != nil {
		return Drive{}, err
	}
	var created Drive
	err = s.db.WithWorkspaceTx(ctx, string(ws), "drive.create", func(tx *dbconnect.Tx) error {
		row := tx.QueryRow(ctx, `INSERT INTO drives(workspace_id,drive_id,skills_path,memory_path,memory_instructions,max_bytes,max_files,metadata)
			VALUES ($1,$2,$3,$4,$5,$6,$7,$8) RETURNING `+driveColumns,
			string(ws), id.New("drive_"), settings.SkillsPath, settings.MemoryPath, settings.MemoryInstructions, settings.MaxBytes, settings.MaxFiles, metadata)
		var scanErr error
		created, scanErr = scanDrive(row)
		return scanErr
	})
	return created, err
}

// Get reads an active drive.
func (s *Service) Get(ctx context.Context, ws workspace.ID, driveID string) (Drive, error) {
	var found Drive
	err := s.db.WithWorkspaceReadOnlyTx(ctx, string(ws), "drive.read", func(tx *dbconnect.Tx) error {
		var readErr error
		found, readErr = scanDrive(tx.QueryRow(ctx, `SELECT `+driveColumns+` FROM drives WHERE workspace_id=$1 AND drive_id=$2 AND deleted_at IS NULL`, string(ws), driveID))
		return readErr
	})
	return found, err
}

// DriveCursor is the place of the last drive of a page in creation order.
type DriveCursor struct {
	CreatedAt time.Time
	ID        string
}

// List returns at most limit active drives after the cursor, oldest first, and
// whether more exist.
func (s *Service) List(ctx context.Context, ws workspace.ID, after *DriveCursor, limit int) ([]Drive, bool, error) {
	var drives []Drive
	err := s.db.WithWorkspaceReadOnlyTx(ctx, string(ws), "drive.list", func(tx *dbconnect.Tx) error {
		var afterAt, afterID any
		if after != nil {
			afterAt, afterID = after.CreatedAt, after.ID
		}
		rows, err := tx.Query(ctx, `SELECT `+driveColumns+` FROM drives WHERE workspace_id=$1 AND deleted_at IS NULL
			AND ($2::timestamptz IS NULL OR (created_at,drive_id)>($2,$3::text)) ORDER BY created_at,drive_id LIMIT $4`, string(ws), afterAt, afterID, limit+1)
		if err != nil {
			return err
		}
		defer func() { _ = rows.Close() }()
		for rows.Next() {
			found, err := scanDrive(rows)
			if err != nil {
				return err
			}
			drives = append(drives, found)
		}
		return rows.Err()
	})
	more := len(drives) > limit
	if more {
		drives = drives[:limit]
	}
	return drives, more, err
}

// WorkspaceUsage is the bytes and files of the current files over the
// Workspace's drives; zero before its first file.
func (s *Service) WorkspaceUsage(ctx context.Context, ws workspace.ID) (bytes, files int64, err error) {
	err = s.db.WithWorkspaceReadOnlyTx(ctx, string(ws), "drive.usage_totals_read", func(tx *dbconnect.Tx) error {
		err := tx.QueryRow(ctx, `SELECT bytes,files FROM drive_workspace_usage WHERE workspace_id=$1`, string(ws)).Scan(&bytes, &files)
		if errors.Is(err, sql.ErrNoRows) {
			return nil
		}
		return err
	})
	return bytes, files, err
}

// Update replaces the settings the update names, under the drive lock.
func (s *Service) Update(ctx context.Context, ws workspace.ID, driveID string, update SettingsUpdate) (Drive, error) {
	set, deleted := map[string]string{}, []string{}
	for key, value := range update.Metadata {
		if value == nil {
			deleted = append(deleted, key)
		} else {
			set[key] = *value
		}
	}
	setJSON, err := json.Marshal(set)
	if err != nil {
		return Drive{}, err
	}
	var updated Drive
	err = s.db.WithWorkspaceTx(ctx, string(ws), "drive.update", func(tx *dbconnect.Tx) error {
		if _, err := lockDrive(ctx, tx, ws, driveID); err != nil {
			return err
		}
		var memoryPath, instructions any
		var limits bool
		var maxBytes, maxFiles any
		if update.Memory != nil {
			memoryPath, instructions = update.Memory.Path, update.Memory.Instructions
		}
		if update.Limits != nil {
			limits, maxBytes, maxFiles = true, update.Limits.MaxBytes, update.Limits.MaxFiles
		}
		row := tx.QueryRow(ctx, `UPDATE drives SET
			skills_path=COALESCE($3,skills_path),
			memory_path=COALESCE($4,memory_path),
			memory_instructions=CASE WHEN $4::text IS NULL THEN memory_instructions ELSE $5 END,
			max_bytes=CASE WHEN $6 THEN $7 ELSE max_bytes END,
			max_files=CASE WHEN $6 THEN $8 ELSE max_files END,
			metadata=(metadata || $9::jsonb) - $10::text[]
			WHERE workspace_id=$1 AND drive_id=$2 RETURNING `+driveColumns,
			string(ws), driveID, update.SkillsPath, memoryPath, instructions, limits, maxBytes, maxFiles, setJSON, deleted)
		var scanErr error
		updated, scanErr = scanDrive(row)
		return scanErr
	})
	return updated, err
}

// Delete marks the drive deleted, drops its files, versions and change records
// and takes its usage off the Workspace total. The daily cleanup removes its
// content and row.
func (s *Service) Delete(ctx context.Context, ws workspace.ID, driveID string) error {
	return s.db.WithWorkspaceTx(ctx, string(ws), "drive.delete", func(tx *dbconnect.Tx) error {
		locked, err := lockDrive(ctx, tx, ws, driveID)
		if err != nil {
			return err
		}
		for _, table := range []string{"drive_files", "drive_versions", "drive_changes"} {
			//nolint:gosec // G202: table comes from the fixed list above.
			if _, err := tx.ExecContext(ctx, `DELETE FROM `+table+` WHERE workspace_id=$1 AND drive_id=$2`, string(ws), driveID); err != nil {
				return err
			}
		}
		if _, err := tx.ExecContext(ctx, `UPDATE drive_workspace_usage SET bytes=bytes-$2, files=files-$3 WHERE workspace_id=$1`,
			string(ws), locked.usageBytes, locked.usageFiles); err != nil {
			return err
		}
		_, err = tx.ExecContext(ctx, `UPDATE drives SET deleted_at=clock_timestamp(), usage_bytes=0, usage_files=0 WHERE workspace_id=$1 AND drive_id=$2`, string(ws), driveID)
		return err
	})
}

// lockedDrive is the part of the drive row a commit reads under its lock.
type lockedDrive struct {
	maxBytes, maxFiles     sql.NullInt64
	usageBytes, usageFiles int64
	lastSeq                int64
}

// lockDrive takes the drive row lock that serializes every commit, redaction,
// restore and cleanup run of the drive, whichever service calls.
func lockDrive(ctx context.Context, tx *dbconnect.Tx, ws workspace.ID, driveID string) (lockedDrive, error) {
	var locked lockedDrive
	err := tx.QueryRow(ctx, `SELECT max_bytes,max_files,usage_bytes,usage_files,last_seq FROM drives
		WHERE workspace_id=$1 AND drive_id=$2 AND deleted_at IS NULL FOR UPDATE`, string(ws), driveID).
		Scan(&locked.maxBytes, &locked.maxFiles, &locked.usageBytes, &locked.usageFiles, &locked.lastSeq)
	if errors.Is(err, sql.ErrNoRows) {
		return lockedDrive{}, ErrDriveNotFound
	}
	return locked, err
}

func nonNilMetadata(metadata map[string]string) map[string]string {
	if metadata == nil {
		return map[string]string{}
	}
	return metadata
}
