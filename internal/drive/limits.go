package drive

import "errors"

// The contract fixes these; none is a setting.
const (
	MaxFileBytes      int64 = 500_000_000
	WorkspaceMaxBytes int64 = 500_000_000_000
	WorkspaceMaxFiles int64 = 100_000

	// keptOldVersions is how many old versions of a path a commit keeps.
	keptOldVersions = 10
)

// retentionInterval is how long an old version, a current deletion and a change
// record are kept; uploadGraceInterval how long an upload's blob outlives no
// version naming it. Both are PostgreSQL interval literals, because every
// read compares them with the database clock.
const (
	retentionInterval   = `interval '30 days'`
	uploadGraceInterval = `interval '24 hours'`
)

var (
	ErrDriveNotFound = errors.New("drive not found")
	// ErrNotFound is a version or content that is not kept, not of this drive,
	// a deletion or redacted.
	ErrNotFound            = errors.New("drive version not found")
	ErrRedactCurrent       = errors.New("the current version cannot be redacted; overwrite or delete the file first")
	ErrCursorExpired       = errors.New("drive change cursor expired")
	ErrContentHashMismatch = errors.New("drive content does not match its expected hash")
	ErrContentTooLarge     = errors.New("drive content exceeds the per-file limit")
)

// ContentMissingError is a commit naming content the drive does not hold;
// nothing was applied.
type ContentMissingError struct {
	MissingSHA256 []string
}

func (e *ContentMissingError) Error() string { return "drive content missing" }

// CommitRejectedError is an all_or_nothing commit rejected by the change at
// Index, in request order; nothing was applied.
type CommitRejectedError struct {
	Index  int
	Result ChangeResult
}

func (e *CommitRejectedError) Error() string {
	return "drive commit rejected: " + string(e.Result.Reason)
}
