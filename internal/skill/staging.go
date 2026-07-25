package skill

import (
	"errors"
	"io"
	"os"
	"path/filepath"

	"github.com/tetral-ai/tetral/internal/id"
)

// MaxFileCount caps accepted package entries to keep upload parsing bounded.
const MaxFileCount = 1000

// SkillMDFilename is the required metadata file basename directly
// inside the uploaded package root directory.
const SkillMDFilename = "SKILL.md"

// errCompressedCapExceeded is the sentinel returned by
// countingLimitedReader when reads exceed the caller-supplied cap.
var errCompressedCapExceeded = errors.New("skill: read cap exceeded")

// countingLimitedReader wraps an io.Reader and stops reads once N
// bytes have been consumed. Callers map errCompressedCapExceeded to
// the typed package-size error for their boundary.
type countingLimitedReader struct {
	R       io.Reader
	N       int64
	read    int64
	stopped bool
}

func (l *countingLimitedReader) Read(p []byte) (int, error) {
	if l.stopped {
		return 0, errCompressedCapExceeded
	}
	if l.N <= 0 {
		l.stopped = true
		return 0, errCompressedCapExceeded
	}
	if int64(len(p)) > l.N {
		p = p[:l.N]
	}
	n, err := l.R.Read(p)
	l.N -= int64(n)
	l.read += int64(n)
	return n, err
}

// NewStageDir creates a new server-owned temp directory for files[]
// part staging and normalized package staging.
//
// The directory is created with mode 0700 so only the Engine process
// can read or list staged files. The id-prefixed name keeps multiple
// Engine instances on the same host from colliding. Caller is
// responsible for removing it on shutdown.
func NewStageDir(parent string) (string, error) {
	dir := id.New("skill-stage-")
	full := filepath.Join(parent, dir)
	if err := os.MkdirAll(full, 0o700); err != nil {
		return "", err
	}
	return full, nil
}
