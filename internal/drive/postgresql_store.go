package drive

import (
	"database/sql"
	"encoding/json"
	"errors"
	"time"
)

type scanner interface{ Scan(dest ...any) error }

const driveColumns = `drive_id,skills_path,memory_path,memory_instructions,max_bytes,max_files,metadata,usage_bytes,usage_files,stored_bytes,last_seq,created_at`

func scanDrive(row scanner) (Drive, error) {
	var d Drive
	var instructions sql.NullString
	var maxBytes, maxFiles, stored sql.NullInt64
	var metadata []byte
	err := row.Scan(&d.ID, &d.SkillsPath, &d.MemoryPath, &instructions, &maxBytes, &maxFiles, &metadata, &d.UsageBytes, &d.UsageFiles, &stored, &d.LastSeq, &d.CreatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return Drive{}, ErrDriveNotFound
	}
	if err != nil {
		return Drive{}, err
	}
	if instructions.Valid {
		d.MemoryInstructions = &instructions.String
	}
	d.MaxBytes, d.MaxFiles, d.StoredBytes = nullableInt(maxBytes), nullableInt(maxFiles), nullableInt(stored)
	if err := json.Unmarshal(metadata, &d.Metadata); err != nil {
		return Drive{}, err
	}
	return d, nil
}

func nullableInt(value sql.NullInt64) *int64 {
	if !value.Valid {
		return nil
	}
	return &value.Int64
}

// File is a current file.
type File struct {
	Path          string
	ContentSHA256 string
	SizeBytes     int64
	VersionID     string
	Executable    bool
}

const fileColumns = `path,content_sha256,size_bytes,version_id,executable`

func scanFile(row scanner) (File, error) {
	var f File
	err := row.Scan(&f.Path, &f.ContentSHA256, &f.SizeBytes, &f.VersionID, &f.Executable)
	return f, err
}

// Operation is a version's or change record's kind of change.
type Operation string

const (
	OperationCreated  Operation = "created"
	OperationModified Operation = "modified"
	OperationDeleted  Operation = "deleted"
)

// Version is a kept version. A deletion has no ContentSHA256, size 0 and is
// not executable.
type Version struct {
	VersionID     string
	Path          string
	Operation     Operation
	ContentSHA256 *string
	SizeBytes     int64
	Executable    bool
	Writer        Writer
	CreatedAt     time.Time
	Seq           int64
	RedactedAt    *time.Time
	RedactedBy    *Writer
}

const versionColumns = `version_id,path,operation,content_sha256,size_bytes,executable,writer_kind,writer_id,writer_computer_id,created_at,seq,redacted_at,redacted_by_kind,redacted_by_id,redacted_by_computer_id`

func scanVersion(row scanner) (Version, error) {
	var v Version
	var sha, computer, redactedKind, redactedID, redactedComputer sql.NullString
	var redactedAt sql.NullTime
	err := row.Scan(&v.VersionID, &v.Path, &v.Operation, &sha, &v.SizeBytes, &v.Executable, &v.Writer.Kind, &v.Writer.ID, &computer, &v.CreatedAt, &v.Seq,
		&redactedAt, &redactedKind, &redactedID, &redactedComputer)
	if err != nil {
		return Version{}, err
	}
	if sha.Valid {
		v.ContentSHA256 = &sha.String
	}
	v.Writer.ComputerID = computer.String
	if redactedAt.Valid {
		v.RedactedAt = &redactedAt.Time
		v.RedactedBy = &Writer{Kind: WriterKind(redactedKind.String), ID: redactedID.String, ComputerID: redactedComputer.String}
	}
	return v, nil
}

// ChangeRecord is one listed change record.
type ChangeRecord struct {
	Seq           int64
	Index         int
	Path          string
	Operation     Operation
	VersionID     string
	ContentSHA256 *string
	SizeBytes     int64
	Executable    bool
	Writer        Writer
	CreatedAt     time.Time
}

const changeColumns = `seq,change_index,path,operation,version_id,content_sha256,size_bytes,executable,writer_kind,writer_id,writer_computer_id,created_at`

func scanChange(row scanner) (ChangeRecord, error) {
	var c ChangeRecord
	var sha, computer sql.NullString
	err := row.Scan(&c.Seq, &c.Index, &c.Path, &c.Operation, &c.VersionID, &sha, &c.SizeBytes, &c.Executable, &c.Writer.Kind, &c.Writer.ID, &computer, &c.CreatedAt)
	if err != nil {
		return ChangeRecord{}, err
	}
	if sha.Valid {
		c.ContentSHA256 = &sha.String
	}
	c.Writer.ComputerID = computer.String
	return c, nil
}

// writerArgs are the three writer columns in table order.
func writerArgs(w Writer) []any {
	var computer any
	if w.ComputerID != "" {
		computer = w.ComputerID
	}
	return []any{string(w.Kind), w.ID, computer}
}
